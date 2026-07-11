package service

import (
	"context"
	"database/sql"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

const (
	burnPromoteCooldownPriority = 10000
	burnPromoteDefaultBatchSize = 20
	burnPromoteLeaderLockKey    = "burn-promote:leader"
	burnPromoteLeaderLockTTL    = 3 * time.Minute
)

// burnPromoteRow holds only the fields we need from the accounts table.
type burnPromoteRow struct {
	id              int64
	currentPriority int
	totalCost       float64 // 30-day rolling actual_cost sum
	tempBlocked     bool    // temp_unschedulable_until > now
}

// BurnPromoteService adjusts account priorities based on 30-day total cost,
// routing more traffic to accounts with higher cumulative consumption.
// Settings (enabled/interval/batchSize) are read from settingService on
// each poll so changes take effect without a restart.
//
// Algorithm:
//  1. Accounts with temp_unschedulable_until > now are skipped (priority unchanged).
//  2. Active accounts sorted by 30-day total_cost descending, batched by BATCH_SIZE.
//  3. Highest-cost batch → priority 1 (= highest scheduling priority).
//     Subsequent batches get priority 2, 3, … etc.
type BurnPromoteService struct {
	db             *sql.DB
	lockCache      LeaderLockCache
	settingService *SettingService
	instanceID     string
	lastRunAt      time.Time

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func NewBurnPromoteService(db *sql.DB, lockCache LeaderLockCache, settingService *SettingService) *BurnPromoteService {
	return &BurnPromoteService{
		db:             db,
		lockCache:      lockCache,
		settingService: settingService,
		instanceID:     uuid.NewString(),
		stopCh:         make(chan struct{}),
	}
}

func (s *BurnPromoteService) Start() {
	if s == nil || s.db == nil {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// Poll every 5s so interval changes in settings take effect quickly.
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				s.runCycle()
			}
		}
	}()
}

func (s *BurnPromoteService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
}

func (s *BurnPromoteService) runCycle() {
	// Read settings on every poll; changes take effect without restart.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	settings, err := s.settingService.GetBurnPromoteSettings(ctx)
	if err != nil || !settings.Enabled {
		return
	}

	interval := time.Duration(settings.IntervalSeconds) * time.Second
	if time.Since(s.lastRunAt) < interval {
		return
	}
	s.lastRunAt = time.Now()

	// Acquire leader lock so only one instance runs per cycle.
	if s.lockCache != nil {
		acquired, err := s.lockCache.TryAcquireLeaderLock(ctx, burnPromoteLeaderLockKey, s.instanceID, burnPromoteLeaderLockTTL)
		if err != nil {
			slog.Warn("burn_promote_leader_lock_error", "error", err)
			return
		}
		if !acquired {
			return // another instance holds the lock
		}
		defer func() {
			bgCtx, bgCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer bgCancel()
			_ = s.lockCache.ReleaseLeaderLock(bgCtx, burnPromoteLeaderLockKey, s.instanceID)
		}()
	}

	rows, err := s.fetchRows(ctx)
	if err != nil {
		slog.Warn("burn_promote_fetch_failed", "error", err)
		return
	}
	if len(rows) == 0 {
		return
	}

	updates := s.computePriorityUpdates(rows, settings.BatchSize)
	if len(updates) == 0 {
		return
	}

	changed, err := s.applyUpdates(ctx, updates)
	if err != nil {
		slog.Warn("burn_promote_apply_failed", "error", err)
		return
	}
	if changed > 0 {
		slog.Info("burn_promote_applied", "accounts_updated", changed, "priority_tiers", len(updates))
	}
}

// fetchRows reads account data needed for the algorithm using raw SQL.
func (s *BurnPromoteService) fetchRows(ctx context.Context) ([]burnPromoteRow, error) {
	const query = `
		SELECT
			a.id,
			a.priority,
			COALESCE((
				SELECT SUM(ul.actual_cost)
				FROM usage_logs ul
				WHERE ul.account_id = a.id
				  AND ul.created_at > NOW() - INTERVAL '30 days'
			), 0) AS total_cost_30d,
			(a.temp_unschedulable_until IS NOT NULL AND a.temp_unschedulable_until > NOW()) AS temp_blocked
		FROM accounts a
		WHERE
			a.platform    = 'openai'
			AND a.type    = 'oauth'
			AND a.status  = 'active'
			AND a.deleted_at IS NULL
	`
	sqlRows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	var out []burnPromoteRow
	for sqlRows.Next() {
		var r burnPromoteRow
		if err := sqlRows.Scan(&r.id, &r.currentPriority, &r.totalCost, &r.tempBlocked); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, sqlRows.Err()
}

// computePriorityUpdates returns a map of priority → []accountID for accounts
// whose priority needs to change.
func (s *BurnPromoteService) computePriorityUpdates(rows []burnPromoteRow, batchSize int) map[int][]int64 {
	type active struct {
		id   int64
		cost float64
	}
	var eligible []active

	for _, r := range rows {
		if r.tempBlocked {
			continue // skip temporarily unschedulable accounts
		}
		eligible = append(eligible, active{r.id, r.totalCost})
	}

	// Sort descending by 30-day cost: highest cost → highest priority (priority=1)
	sort.Slice(eligible, func(i, j int) bool { return eligible[i].cost > eligible[j].cost })

	type batch struct {
		ids      []int64
		priority int
	}
	var batches []batch
	for i := 0; i < len(eligible); i += batchSize {
		end := i + batchSize
		if end > len(eligible) {
			end = len(eligible)
		}
		var ids []int64
		for _, a := range eligible[i:end] {
			ids = append(ids, a.id)
		}
		// batch[0] → priority 1, batch[1] → priority 2, …
		batches = append(batches, batch{ids: ids, priority: len(batches) + 1})
	}

	currentPriority := make(map[int64]int, len(rows))
	for _, r := range rows {
		currentPriority[r.id] = r.currentPriority
	}

	updates := make(map[int][]int64)
	for _, b := range batches {
		for _, id := range b.ids {
			if currentPriority[id] != b.priority {
				updates[b.priority] = append(updates[b.priority], id)
			}
		}
	}

	return updates
}

// applyUpdates executes one UPDATE per priority tier using ANY($ids).
func (s *BurnPromoteService) applyUpdates(ctx context.Context, updates map[int][]int64) (int, error) {
	changed := 0
	for priority, ids := range updates {
		res, err := s.db.ExecContext(ctx,
			`UPDATE accounts SET priority=$1, updated_at=NOW() WHERE id=ANY($2) AND deleted_at IS NULL`,
			priority, pq.Array(ids),
		)
		if err != nil {
			return changed, err
		}
		n, _ := res.RowsAffected()
		changed += int(n)
	}
	return changed, nil
}
