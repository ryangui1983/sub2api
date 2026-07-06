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
// Reading just these 5 columns avoids loading credentials/extra in full.
type burnPromoteRow struct {
	id               int64
	currentPriority  int
	fiveHourPct      *float64 // extra->>'codex_5h_used_percent'
	fiveHourResetAt  *time.Time
	rateLimitResetAt *time.Time
}

// BurnPromoteService adjusts account priorities based on 5h quota usage,
// routing more traffic to accounts with higher remaining quota headroom.
// Settings (enabled/interval/batchSize) are read from settingService on
// each poll so changes take effect without a restart.
//
// Algorithm (mirrors ops-assistant burn-promote.ts):
//  1. Cooldown accounts (rate_limit_reset_at > now) → priority COOLDOWN_PRIORITY
//  2. Active accounts sorted by 5h usage % descending, batched by BATCH_SIZE
//  3. Highest-usage batch → lowest priority number (= highest scheduling priority)
//     Subsequent batches +1; stale/zero usage → last batch
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

// fetchRows reads only the 5 fields needed for the algorithm using raw SQL.
// Selecting a narrow projection avoids loading large credentials/extra JSONB columns.
func (s *BurnPromoteService) fetchRows(ctx context.Context) ([]burnPromoteRow, error) {
	const query = `
		SELECT
			id,
			priority,
			(extra->>'codex_5h_used_percent')::float,
			CASE WHEN extra->>'codex_5h_reset_at' <> '' THEN (extra->>'codex_5h_reset_at')::timestamptz ELSE NULL END,
			rate_limit_reset_at
		FROM accounts
		WHERE
			platform = 'openai'
			AND type   = 'oauth'
			AND status = 'active'
			AND schedulable = TRUE
			AND deleted_at IS NULL
	`
	sqlRows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	var out []burnPromoteRow
	for sqlRows.Next() {
		var r burnPromoteRow
		if err := sqlRows.Scan(&r.id, &r.currentPriority, &r.fiveHourPct, &r.fiveHourResetAt, &r.rateLimitResetAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, sqlRows.Err()
}

// computePriorityUpdates returns a map of priority → []accountID for accounts
// whose priority needs to change.  Only changed accounts are included.
func (s *BurnPromoteService) computePriorityUpdates(rows []burnPromoteRow, batchSize int) map[int][]int64 {
	now := time.Now()
	var cooldown []burnPromoteRow
	var active []struct {
		id  int64
		pct float64
	}

	for _, r := range rows {
		if r.rateLimitResetAt != nil && r.rateLimitResetAt.After(now) {
			cooldown = append(cooldown, r)
			continue
		}
		// Stale usage window → treat as 0 % (sort to last batch)
		stale := r.fiveHourResetAt != nil && !r.fiveHourResetAt.After(now)
		pct := 0.0
		if !stale && r.fiveHourPct != nil {
			pct = *r.fiveHourPct
		}
		active = append(active, struct {
			id  int64
			pct float64
		}{r.id, pct})
	}

	// Sort active descending by usage % so highest-usage accounts get highest priority.
	sort.Slice(active, func(i, j int) bool { return active[i].pct > active[j].pct })

	// Build batches.
	type batch struct {
		ids      []int64
		priority int
	}
	var batches []batch
	for i := 0; i < len(active); i += batchSize {
		end := i + batchSize
		if end > len(active) {
			end = len(active)
		}
		var ids []int64
		for _, a := range active[i:end] {
			ids = append(ids, a.id)
		}
		batches = append(batches, batch{ids: ids})
	}

	totalBatches := len(batches)
	for i := range batches {
		// batch[0] (highest usage) gets lowest number = highest scheduling priority
		batches[i].priority = burnPromoteCooldownPriority - totalBatches + i
	}

	// Build a lookup of current priorities for change detection.
	currentPriority := make(map[int64]int, len(rows))
	for _, r := range rows {
		currentPriority[r.id] = r.currentPriority
	}

	updates := make(map[int][]int64)

	// Cooldown accounts.
	for _, r := range cooldown {
		if r.currentPriority != burnPromoteCooldownPriority {
			updates[burnPromoteCooldownPriority] = append(updates[burnPromoteCooldownPriority], r.id)
		}
	}

	// Active accounts.
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
