package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// SinkRequestEvent is a single request event (success or failure) pushed to ops-assistant.
type SinkRequestEvent struct {
	InstanceID  string  `json:"instance_id"`
	AccountID   int64   `json:"account_id"`
	WorkspaceID string  `json:"workspace_id,omitempty"`
	Email       string  `json:"email,omitempty"`
	Success     bool    `json:"success"`
	StatusCode  int     `json:"status_code,omitempty"`
	ActualCost  float64 `json:"actual_cost"`
	ErrorCode   string  `json:"error_code,omitempty"`
	ErrorDetail string  `json:"error_detail,omitempty"`
	CreatedAt   int64   `json:"created_at"` // unix ms
}

// SinkAccountSnapshot is the current state of one account, pushed periodically.
type SinkAccountSnapshot struct {
	InstanceID              string  `json:"instance_id"`
	AccountID               int64   `json:"account_id"`
	WorkspaceID             string  `json:"workspace_id,omitempty"`
	Email                   string  `json:"email,omitempty"`
	Status                  string  `json:"status"`
	ErrorMessage            string  `json:"error_message,omitempty"`
	TempUnschedulableUntil  *int64  `json:"temp_unschedulable_until,omitempty"` // unix ms
	TempUnschedulableReason string  `json:"temp_unschedulable_reason,omitempty"`
	Schedulable             bool    `json:"schedulable"`
	TotalCost               float64 `json:"total_cost"` // 30-day rolling
	LastUsedAt              *int64  `json:"last_used_at,omitempty"`
	AccountCreatedAt        int64   `json:"account_created_at"`
	SnapshottedAt           int64   `json:"snapshotted_at"`
}

type sinkPayload struct {
	Events    []SinkRequestEvent   `json:"events,omitempty"`
	Snapshots []SinkAccountSnapshot `json:"snapshots,omitempty"`
}

// UsageSinkService polls usage_logs and ops_error_logs and pushes events to
// ops-assistant for cross-instance aggregation. No-op when UsageSinkURL is empty.
type UsageSinkService struct {
	db         *sql.DB
	cfg        *config.Config
	stopCh     chan struct{}
	wg         sync.WaitGroup
	httpClient *http.Client
}

func NewUsageSinkService(db *sql.DB, cfg *config.Config) *UsageSinkService {
	return &UsageSinkService{
		db:         db,
		cfg:        cfg,
		stopCh:     make(chan struct{}),
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

func (s *UsageSinkService) Start() {
	if s.cfg.Gateway.UsageSink.URL == "" {
		return
	}
	s.wg.Add(1)
	go s.run()
}

func (s *UsageSinkService) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *UsageSinkService) run() {
	defer s.wg.Done()

	interval := time.Duration(s.cfg.Gateway.UsageSink.IntervalSeconds) * time.Second
	if interval < 10*time.Second {
		interval = 30 * time.Second
	}
	const snapshotInterval = 5 * time.Minute

	var lastEventAt time.Time
	var lastErrorAt time.Time
	var lastSnapshotAt time.Time

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.syncEvents(&lastEventAt, &lastErrorAt)
			if time.Since(lastSnapshotAt) >= snapshotInterval {
				s.syncSnapshots()
				lastSnapshotAt = time.Now()
			}
		}
	}
}

func (s *UsageSinkService) syncEvents(lastEventAt, lastErrorAt *time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var events []SinkRequestEvent

	success := s.pollUsageLogs(ctx, *lastEventAt)
	events = append(events, success...)
	if len(success) > 0 {
		*lastEventAt = time.UnixMilli(success[len(success)-1].CreatedAt)
	}

	errors := s.pollErrorLogs(ctx, *lastErrorAt)
	events = append(events, errors...)
	if len(errors) > 0 {
		*lastErrorAt = time.UnixMilli(errors[len(errors)-1].CreatedAt)
	}

	if len(events) > 0 {
		s.push(sinkPayload{Events: events})
	}
}

func (s *UsageSinkService) pollUsageLogs(ctx context.Context, since time.Time) []SinkRequestEvent {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ul.account_id,
		       COALESCE(a.credentials->>'workspace_id', a.credentials->>'chatgpt_account_id', '') AS workspace_id,
		       COALESCE(a.credentials->>'email', '') AS email,
		       ul.actual_cost,
		       EXTRACT(EPOCH FROM ul.created_at) * 1000 AS created_ms
		FROM usage_logs ul
		LEFT JOIN accounts a ON a.id = ul.account_id
		WHERE ul.created_at > $1
		ORDER BY ul.created_at ASC
		LIMIT 200`, since)
	if err != nil {
		log.Printf("[UsageSink] poll usage_logs: %v", err)
		return nil
	}
	defer rows.Close()

	var out []SinkRequestEvent
	for rows.Next() {
		var e SinkRequestEvent
		var ms float64
		if err := rows.Scan(&e.AccountID, &e.WorkspaceID, &e.Email, &e.ActualCost, &ms); err != nil {
			continue
		}
		e.InstanceID = s.cfg.Gateway.UsageSink.InstanceID
		e.Success = true
		e.StatusCode = 200
		e.CreatedAt = int64(ms)
		out = append(out, e)
	}
	return out
}

func (s *UsageSinkService) pollErrorLogs(ctx context.Context, since time.Time) []SinkRequestEvent {
	rows, err := s.db.QueryContext(ctx, `
		SELECT oel.account_id,
		       COALESCE(a.credentials->>'workspace_id', a.credentials->>'chatgpt_account_id', '') AS workspace_id,
		       COALESCE(a.credentials->>'email', '') AS email,
		       COALESCE(oel.upstream_status_code, oel.status_code, 0) AS status_code,
		       COALESCE(oel.provider_error_code, '') AS error_code,
		       COALESCE(LEFT(oel.upstream_error_detail, 512), '') AS error_detail,
		       EXTRACT(EPOCH FROM oel.created_at) * 1000 AS created_ms
		FROM ops_error_logs oel
		LEFT JOIN accounts a ON a.id = oel.account_id
		WHERE oel.created_at > $1 AND oel.account_id IS NOT NULL
		ORDER BY oel.created_at ASC
		LIMIT 200`, since)
	if err != nil {
		log.Printf("[UsageSink] poll ops_error_logs: %v", err)
		return nil
	}
	defer rows.Close()

	var out []SinkRequestEvent
	for rows.Next() {
		var e SinkRequestEvent
		var ms float64
		if err := rows.Scan(&e.AccountID, &e.WorkspaceID, &e.Email, &e.StatusCode, &e.ErrorCode, &e.ErrorDetail, &ms); err != nil {
			continue
		}
		e.InstanceID = s.cfg.Gateway.UsageSink.InstanceID
		e.Success = false
		e.CreatedAt = int64(ms)
		out = append(out, e)
	}
	return out
}

func (s *UsageSinkService) syncSnapshots() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, `
		SELECT a.id,
		       COALESCE(a.credentials->>'workspace_id', a.credentials->>'chatgpt_account_id', '') AS workspace_id,
		       COALESCE(a.credentials->>'email', '') AS email,
		       a.status,
		       COALESCE(a.error_message, '') AS error_message,
		       a.temp_unschedulable_until,
		       COALESCE(a.temp_unschedulable_reason, '') AS temp_reason,
		       a.schedulable,
		       a.last_used_at,
		       a.created_at,
		       COALESCE((
		           SELECT SUM(ul.actual_cost) FROM usage_logs ul
		           WHERE ul.account_id = a.id
		           AND ul.created_at > NOW() - INTERVAL '30 days'
		       ), 0) AS total_cost_30d
		FROM accounts a
		WHERE a.deleted_at IS NULL AND a.platform = 'openai'
		ORDER BY a.id`)
	if err != nil {
		log.Printf("[UsageSink] sync snapshots: %v", err)
		return
	}
	defer rows.Close()

	now := time.Now().UnixMilli()
	var snapshots []SinkAccountSnapshot
	for rows.Next() {
		var snap SinkAccountSnapshot
		var tempUntil sql.NullTime
		var lastUsed sql.NullTime
		var createdAt time.Time

		if err := rows.Scan(
			&snap.AccountID, &snap.WorkspaceID, &snap.Email,
			&snap.Status, &snap.ErrorMessage,
			&tempUntil, &snap.TempUnschedulableReason,
			&snap.Schedulable,
			&lastUsed, &createdAt,
			&snap.TotalCost,
		); err != nil {
			continue
		}
		snap.InstanceID = s.cfg.Gateway.UsageSink.InstanceID
		snap.AccountCreatedAt = createdAt.UnixMilli()
		snap.SnapshottedAt = now
		if tempUntil.Valid {
			ms := tempUntil.Time.UnixMilli()
			snap.TempUnschedulableUntil = &ms
		}
		if lastUsed.Valid {
			ms := lastUsed.Time.UnixMilli()
			snap.LastUsedAt = &ms
		}
		snapshots = append(snapshots, snap)
	}

	if len(snapshots) > 0 {
		s.push(sinkPayload{Snapshots: snapshots})
	}
}

func (s *UsageSinkService) push(payload sinkPayload) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, s.cfg.Gateway.UsageSink.URL+"/internal/sub2/events", bytes.NewReader(data))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.Gateway.UsageSink.Token != "" {
		req.Header.Set("X-Sink-Token", s.cfg.Gateway.UsageSink.Token)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Printf("[UsageSink] push error: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		log.Printf("[UsageSink] push returned %d", resp.StatusCode)
	}
}
