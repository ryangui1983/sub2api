package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

const (
	sinkBatchSize              = 1000 // records per DB query batch
	sinkMaxBatchesPerTick      = 4    // usage/account catch-up cap per tick
	sinkMaxErrorBatchesPerTick = 32   // 429s drain before usage; 0 would be unbounded
	sinkLastUsedGrace          = 2 * time.Second
)

const (
	sinkUsageLimitReachedMessage = "The usage limit has been reached"
	sinkUsageLimitReachedType    = "usage_limit_reached"
)

// sinkCursor is a keyset watermark: (timestamp, row id).
// Using unix-ms alone skips remaining rows that share the same millisecond.
type sinkCursor struct {
	at time.Time
	id int64
}

// SinkRequestEvent is a single request event (success or failure) pushed to ops-assistant.
type SinkRequestEvent struct {
	InstanceID       string  `json:"instance_id"`
	AccountID        int64   `json:"account_id"`
	RequestID        string  `json:"request_id,omitempty"`
	ChatgptUserID    string  `json:"chatgpt_user_id,omitempty"`
	ChatgptAccountID string  `json:"chatgpt_account_id,omitempty"`
	Email            string  `json:"email,omitempty"`
	Success          bool    `json:"success"`
	StatusCode       int     `json:"status_code,omitempty"`
	ActualCost       float64 `json:"actual_cost"`
	ErrorCode        string  `json:"error_code,omitempty"`
	ErrorDetail      string  `json:"error_detail,omitempty"`
	CreatedAt        int64   `json:"created_at"` // unix ms
}

// SinkAccountEvent is the current state of one account.
// Used for incremental updated_at polling — TotalCost is not included;
// ops-assistant computes total_cost by summing pushed request events.
type SinkAccountEvent struct {
	InstanceID              string `json:"instance_id"`
	AccountID               int64  `json:"account_id"`
	ChatgptUserID           string `json:"chatgpt_user_id,omitempty"`
	ChatgptAccountID        string `json:"chatgpt_account_id,omitempty"`
	Email                   string `json:"email,omitempty"`
	Status                  string `json:"status"`
	ErrorMessage            string `json:"error_message,omitempty"`
	TempUnschedulableUntil  *int64 `json:"temp_unschedulable_until,omitempty"` // unix ms
	TempUnschedulableReason string `json:"temp_unschedulable_reason,omitempty"`
	Schedulable             bool   `json:"schedulable"`
	LastUsedAt              *int64 `json:"last_used_at,omitempty"`
	AccountCreatedAt        int64  `json:"account_created_at"`
	UpdatedAt               int64  `json:"updated_at"`
	// Internal only — used to synthesize a 429 event; never serialized.
	RateLimitResetAt *int64 `json:"-"`
	PlanType         string `json:"-"`
}

type sinkPayload struct {
	Events   []SinkRequestEvent `json:"events,omitempty"`
	Accounts []SinkAccountEvent `json:"accounts,omitempty"`
}

// UsageSinkService polls usage_logs, ops_error_logs and accounts for changes,
// pushing them to ops-assistant for cross-instance aggregation.
// No-op when UsageSinkURL is empty.
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
	if interval < 5*time.Second {
		interval = 10 * time.Second
	}

	// Start from now — history is synced separately via migration script.
	now := time.Now()
	lastEvent := sinkCursor{at: now}
	lastError := sinkCursor{at: now}
	lastAccount := sinkCursor{at: now}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			// Error events first (high batch cap). Account snapshots still run so
			// already-429 accounts without a new ops_error_log emit a synthesized
			// event. usage_logs wait until errors catch up.
			caughtUp := s.drainErrorLogs(&lastError)
			s.drainAccountChanges(&lastAccount)
			if caughtUp {
				s.drainUsageLogs(&lastEvent)
			}
		}
	}
}

func (s *UsageSinkService) drainUsageLogs(cur *sinkCursor) bool {
	return s.drainRequestEvents(cur, "usage_logs", s.pollUsageLogs, sinkMaxBatchesPerTick)
}

func (s *UsageSinkService) drainErrorLogs(cur *sinkCursor) bool {
	return s.drainRequestEvents(cur, "ops_error_logs", s.pollErrorLogs, sinkMaxErrorBatchesPerTick)
}

func (s *UsageSinkService) drainRequestEvents(cur *sinkCursor, source string, poll func(context.Context, sinkCursor) ([]SinkRequestEvent, sinkCursor, bool), maxBatches int) bool {
	for n := 0; n < maxBatches; n++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		batch, next, ok := poll(ctx, *cur)
		cancel()
		if !ok {
			return false
		}
		if len(batch) == 0 {
			return true
		}
		if !s.push(sinkPayload{Events: batch}) {
			return false // push failed — keep watermark, retry next tick
		}
		*cur = next
		if len(batch) < sinkBatchSize {
			return true // caught up
		}
	}
	log.Printf("[UsageSink] %s catching up", source)
	return false
}

func (s *UsageSinkService) drainAccountChanges(cur *sinkCursor) bool {
	for n := 0; n < sinkMaxBatchesPerTick; n++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		batch, next, ok := s.pollAccountChanges(ctx, *cur)
		cancel()
		if !ok {
			return false
		}
		if len(batch) == 0 {
			return true
		}
		if evs := sinkAccount429Events(batch, time.Now()); len(evs) > 0 {
			if !s.push(sinkPayload{Events: evs}) {
				return false
			}
		}
		if !s.push(sinkPayload{Accounts: batch}) {
			return false
		}
		*cur = next
		if len(batch) < sinkBatchSize {
			return true
		}
	}
	log.Printf("[UsageSink] accounts catching up")
	return false
}

func (s *UsageSinkService) pollUsageLogs(ctx context.Context, cur sinkCursor) ([]SinkRequestEvent, sinkCursor, bool) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT ul.id,
		       ul.account_id,
		       COALESCE(ul.request_id, '') AS request_id,
		       COALESCE(a.credentials->>'chatgpt_user_id', '') AS chatgpt_user_id,
		       COALESCE(a.credentials->>'chatgpt_account_id', '') AS chatgpt_account_id,
		       COALESCE(a.credentials->>'email', '') AS email,
		       ul.total_cost,
		       ul.created_at
		FROM usage_logs ul
		LEFT JOIN accounts a ON a.id = ul.account_id
		WHERE (ul.created_at, ul.id) > ($1, $2)
		ORDER BY ul.created_at ASC, ul.id ASC
		LIMIT $3`, cur.at, cur.id, sinkBatchSize)
	if err != nil {
		log.Printf("[UsageSink] poll usage_logs: %v", err)
		return nil, cur, false
	}
	defer rows.Close()

	var out []SinkRequestEvent
	next := cur
	var scanErrs int
	for rows.Next() {
		var e SinkRequestEvent
		var id int64
		var createdAt time.Time
		if err := rows.Scan(&id, &e.AccountID, &e.RequestID, &e.ChatgptUserID, &e.ChatgptAccountID, &e.Email, &e.ActualCost, &createdAt); err != nil {
			log.Printf("[UsageSink] scan usage_logs: %v", err)
			scanErrs++
			continue
		}
		e.InstanceID = s.cfg.Gateway.UsageSink.InstanceID
		e.Success = true
		e.StatusCode = 200
		e.CreatedAt = createdAt.UnixMilli()
		out = append(out, e)
		next = sinkCursor{at: createdAt, id: id}
	}
	if err := rows.Err(); err != nil {
		log.Printf("[UsageSink] poll usage_logs iterate: %v", err)
		return nil, cur, false
	}
	if len(out) == 0 && scanErrs > 0 {
		return nil, cur, false
	}
	return out, next, true
}

func (s *UsageSinkService) pollErrorLogs(ctx context.Context, cur sinkCursor) ([]SinkRequestEvent, sinkCursor, bool) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT oel.id,
		       oel.account_id,
		       COALESCE(oel.request_id, '') AS request_id,
		       COALESCE(a.credentials->>'chatgpt_user_id', '') AS chatgpt_user_id,
		       COALESCE(a.credentials->>'chatgpt_account_id', '') AS chatgpt_account_id,
		       COALESCE(a.credentials->>'email', '') AS email,
		       COALESCE(oel.upstream_status_code, oel.status_code, 0) AS status_code,
		       COALESCE(oel.provider_error_code, '') AS error_code,
		       COALESCE(oel.upstream_error_detail, '') AS error_detail,
		       LEFT(COALESCE(oel.error_body, ''), 4096) AS error_body,
		       COALESCE(oel.upstream_error_message, oel.error_message, '') AS error_message,
		       COALESCE(a.credentials->>'plan_type', '') AS plan_type,
		       EXTRACT(EPOCH FROM a.rate_limit_reset_at) AS reset_unix,
		       oel.created_at
		FROM ops_error_logs oel
		LEFT JOIN accounts a ON a.id = oel.account_id
		WHERE (oel.created_at, oel.id) > ($1, $2) AND oel.account_id IS NOT NULL
		ORDER BY oel.created_at ASC, oel.id ASC
		LIMIT $3`, cur.at, cur.id, sinkBatchSize)
	if err != nil {
		log.Printf("[UsageSink] poll ops_error_logs: %v", err)
		return nil, cur, false
	}
	defer rows.Close()

	var out []SinkRequestEvent
	next := cur
	var scanErrs int
	for rows.Next() {
		var e SinkRequestEvent
		var id int64
		var detail, body, message, planType string
		var resetUnix sql.NullFloat64
		var createdAt time.Time
		if err := rows.Scan(
			&id, &e.AccountID, &e.RequestID, &e.ChatgptUserID, &e.ChatgptAccountID, &e.Email,
			&e.StatusCode, &e.ErrorCode,
			&detail, &body, &message, &planType, &resetUnix, &createdAt,
		); err != nil {
			log.Printf("[UsageSink] scan ops_error_logs: %v", err)
			scanErrs++
			continue
		}
		e.InstanceID = s.cfg.Gateway.UsageSink.InstanceID
		e.Success = false
		e.CreatedAt = createdAt.UnixMilli()
		e.ErrorDetail = buildSinkErrorDetail(e.StatusCode, detail, body, message, "", planType, resetUnix)
		out = append(out, e)
		next = sinkCursor{at: createdAt, id: id}
	}
	if err := rows.Err(); err != nil {
		log.Printf("[UsageSink] poll ops_error_logs iterate: %v", err)
		return nil, cur, false
	}
	if len(out) == 0 && scanErrs > 0 {
		return nil, cur, false
	}
	return out, next, true
}

// pollAccountChanges returns accounts with a real state change since cur.
// last_used-only bumps (Ent also rewrites updated_at) are skipped so 2000+
// account instances do not flood the sink with snapshots that starve 429s.
func (s *UsageSinkService) pollAccountChanges(ctx context.Context, cur sinkCursor) ([]SinkAccountEvent, sinkCursor, bool) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT a.id,
		       COALESCE(a.credentials->>'chatgpt_user_id', '') AS chatgpt_user_id,
		       COALESCE(a.credentials->>'chatgpt_account_id', '') AS chatgpt_account_id,
		       COALESCE(a.credentials->>'email', '') AS email,
		       COALESCE(a.credentials->>'plan_type', '') AS plan_type,
		       a.status,
		       COALESCE(a.error_message, '') AS error_message,
		       a.temp_unschedulable_until,
		       COALESCE(a.temp_unschedulable_reason, '') AS temp_reason,
		       a.schedulable,
		       a.last_used_at,
		       a.rate_limited_at,
		       a.rate_limit_reset_at,
		       a.created_at,
		       a.updated_at
		FROM accounts a
		WHERE (a.updated_at, a.id) > ($1, $2)
		  AND a.deleted_at IS NULL
		  AND a.platform = 'openai'
		  AND (
		        a.last_used_at IS NULL
		        OR a.updated_at > a.last_used_at + interval '2 seconds'
		        OR a.temp_unschedulable_until > NOW()
		        OR NOT a.schedulable
		        OR a.status <> 'active'
		        OR COALESCE(a.error_message, '') <> ''
		        OR (a.rate_limit_reset_at IS NOT NULL AND a.rate_limit_reset_at > NOW())
		        OR (a.rate_limited_at IS NOT NULL AND a.updated_at <= a.rate_limited_at + interval '2 seconds')
		      )
		ORDER BY a.updated_at ASC, a.id ASC
		LIMIT $3`, cur.at, cur.id, sinkBatchSize)
	if err != nil {
		log.Printf("[UsageSink] poll account changes: %v", err)
		return nil, cur, false
	}
	defer rows.Close()

	var out []SinkAccountEvent
	next := cur
	var scanErrs int
	for rows.Next() {
		var snap SinkAccountEvent
		var tempUntil sql.NullTime
		var lastUsed sql.NullTime
		var rateLimitedAt sql.NullTime
		var rateLimitResetAt sql.NullTime
		var createdAt time.Time
		var updatedAt time.Time

		if err := rows.Scan(
			&snap.AccountID, &snap.ChatgptUserID, &snap.ChatgptAccountID, &snap.Email, &snap.PlanType,
			&snap.Status, &snap.ErrorMessage,
			&tempUntil, &snap.TempUnschedulableReason,
			&snap.Schedulable,
			&lastUsed, &rateLimitedAt, &rateLimitResetAt, &createdAt,
			&updatedAt,
		); err != nil {
			log.Printf("[UsageSink] scan account changes: %v", err)
			scanErrs++
			continue
		}
		snap.InstanceID = s.cfg.Gateway.UsageSink.InstanceID
		snap.AccountCreatedAt = createdAt.UnixMilli()
		snap.UpdatedAt = updatedAt.UnixMilli()
		if tempUntil.Valid {
			ms := tempUntil.Time.UnixMilli()
			snap.TempUnschedulableUntil = &ms
		}
		if lastUsed.Valid {
			ms := lastUsed.Time.UnixMilli()
			snap.LastUsedAt = &ms
		}
		if rateLimitResetAt.Valid {
			ms := rateLimitResetAt.Time.UnixMilli()
			snap.RateLimitResetAt = &ms
		}
		out = append(out, snap)
		next = sinkCursor{at: updatedAt, id: snap.AccountID}
	}
	if err := rows.Err(); err != nil {
		log.Printf("[UsageSink] poll account changes iterate: %v", err)
		return nil, cur, false
	}
	if len(out) == 0 && scanErrs > 0 {
		return nil, cur, false
	}
	return out, next, true
}

func (s *UsageSinkService) push(payload sinkPayload) bool {
	data, err := json.Marshal(payload)
	if err != nil {
		return false
	}
	req, err := http.NewRequest(http.MethodPost, s.cfg.Gateway.UsageSink.URL+"/internal/sub2/events", bytes.NewReader(data))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.Gateway.UsageSink.Token != "" {
		req.Header.Set("X-Sink-Token", s.cfg.Gateway.UsageSink.Token)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Printf("[UsageSink] push error: %v", err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		log.Printf("[UsageSink] push returned %d", resp.StatusCode)
		return false
	}
	return true
}

// sinkAccountHasNonLastUsedChange matches the SQL filter in pollAccountChanges.
// last_used bumps rewrite updated_at via TimeMixin; keep 429 / disable / error.
// OpenAI quota 429 is SetRateLimited (rate_limit_reset_at), not temp_unschedulable.
func sinkAccountHasNonLastUsedChange(status string, schedulable bool, errorMessage string, tempUntil sql.NullTime, updatedAt time.Time, lastUsed, rateLimitedAt, rateLimitResetAt sql.NullTime, now time.Time) bool {
	if !lastUsed.Valid {
		return true
	}
	if updatedAt.After(lastUsed.Time.Add(sinkLastUsedGrace)) {
		return true
	}
	if tempUntil.Valid && tempUntil.Time.After(now) {
		return true
	}
	if !schedulable {
		return true
	}
	if status != "active" {
		return true
	}
	if strings.TrimSpace(errorMessage) != "" {
		return true
	}
	if rateLimitResetAt.Valid && rateLimitResetAt.Time.After(now) {
		return true
	}
	return rateLimitedAt.Valid && !updatedAt.After(rateLimitedAt.Time.Add(sinkLastUsedGrace))
}

func sinkAccountIsActive429(snap SinkAccountEvent, now time.Time) bool {
	nowMs := now.UnixMilli()
	if snap.RateLimitResetAt != nil && *snap.RateLimitResetAt > nowMs {
		return true
	}
	reason := strings.ToLower(snap.TempUnschedulableReason)
	if snap.TempUnschedulableUntil != nil && *snap.TempUnschedulableUntil > nowMs &&
		(strings.Contains(reason, "429") || strings.Contains(reason, "rate_limit") || strings.Contains(reason, "usage_limit")) {
		return true
	}
	return false
}

func sinkAccount429Event(snap SinkAccountEvent, now time.Time) (SinkRequestEvent, bool) {
	if !sinkAccountIsActive429(snap, now) {
		return SinkRequestEvent{}, false
	}
	var resetUnix sql.NullFloat64
	if snap.RateLimitResetAt != nil && *snap.RateLimitResetAt > 0 {
		resetUnix = sql.NullFloat64{Float64: float64(*snap.RateLimitResetAt / 1000), Valid: true}
	}
	return SinkRequestEvent{
		InstanceID:       snap.InstanceID,
		AccountID:        snap.AccountID,
		ChatgptUserID:    snap.ChatgptUserID,
		ChatgptAccountID: snap.ChatgptAccountID,
		Email:            snap.Email,
		Success:          false,
		StatusCode:       http.StatusTooManyRequests,
		ErrorDetail:      buildSinkErrorDetail(http.StatusTooManyRequests, "", "", sinkUsageLimitReachedMessage, "", snap.PlanType, resetUnix),
		CreatedAt:        snap.UpdatedAt,
	}, true
}

func sinkAccount429Events(batch []SinkAccountEvent, now time.Time) []SinkRequestEvent {
	var out []SinkRequestEvent
	for _, snap := range batch {
		if ev, ok := sinkAccount429Event(snap, now); ok {
			out = append(out, ev)
		}
	}
	return out
}

// buildSinkErrorDetail returns a JSON object string for error_detail.
// ops-assistant JSON-parses this and matches error.message / plan_type / resets_at
// to delete free-plan usage-limit accounts. Empty means omitempty → null on the wire.
func buildSinkErrorDetail(statusCode int, detail, body, message, upstreamErrors, planType string, resetUnix sql.NullFloat64) string {
	var resetAt int64
	if resetUnix.Valid && resetUnix.Float64 > 0 {
		resetAt = int64(resetUnix.Float64)
	}
	planType = strings.ToLower(strings.TrimSpace(planType))
	message = strings.TrimSpace(message)

	candidates := []string{detail, body}
	candidates = append(candidates, extractSinkErrorJSONCandidates(upstreamErrors)...)
	candidates = append(candidates, message)

	for _, raw := range candidates {
		if filled := normalizeSinkErrorJSON(raw, message, planType, resetAt); filled != "" {
			return filled
		}
	}

	if statusCode != http.StatusTooManyRequests {
		return strings.TrimSpace(detail)
	}
	if !shouldSynthesizeSinkUsageLimit(message, planType, resetAt) {
		return strings.TrimSpace(detail)
	}
	return marshalSinkUsageLimitError(planType, resetAt)
}

func extractSinkErrorJSONCandidates(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" || raw == "[]" {
		return nil
	}
	var events []struct {
		Detail               string `json:"detail"`
		UpstreamResponseBody string `json:"upstream_response_body"`
		Message              string `json:"message"`
	}
	if err := json.Unmarshal([]byte(raw), &events); err != nil {
		return nil
	}
	var out []string
	for i := len(events) - 1; i >= 0; i-- {
		for _, s := range []string{events[i].Detail, events[i].UpstreamResponseBody, events[i].Message} {
			if strings.TrimSpace(s) != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

func normalizeSinkErrorJSON(raw, message, planType string, resetAt int64) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || !json.Valid([]byte(raw)) || raw[0] != '{' {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return ""
	}

	errObj, ok := payload["error"].(map[string]any)
	if !ok {
		if isSinkUsageLimitErrObj(payload) {
			errObj = payload
			payload = map[string]any{"error": errObj}
		} else {
			return raw
		}
	}

	fillSinkUsageLimitErrObj(errObj, message, planType, resetAt)
	b, err := json.Marshal(payload)
	if err != nil {
		return raw
	}
	return string(b)
}

func isSinkUsageLimitErrObj(obj map[string]any) bool {
	t, _ := obj["type"].(string)
	t = strings.ToLower(strings.TrimSpace(t))
	if t == sinkUsageLimitReachedType || t == "rate_limit_exceeded" {
		return true
	}
	msg, _ := obj["message"].(string)
	return strings.Contains(strings.ToLower(msg), "usage limit has been reached")
}

func fillSinkUsageLimitErrObj(errObj map[string]any, message, planType string, resetAt int64) {
	if planType != "" {
		if cur, _ := errObj["plan_type"].(string); strings.TrimSpace(cur) == "" {
			errObj["plan_type"] = planType
		}
	}
	if resetAt > 0 {
		if _, ok := sinkJSONInt64(errObj["resets_at"]); !ok {
			errObj["resets_at"] = resetAt
		}
	}
	errType, _ := errObj["type"].(string)
	errType = strings.ToLower(strings.TrimSpace(errType))
	if errType == sinkUsageLimitReachedType || errType == "rate_limit_exceeded" || isSinkUsageLimitErrObj(errObj) {
		errObj["message"] = sinkUsageLimitReachedMessage
		if errType == "" {
			errObj["type"] = sinkUsageLimitReachedType
		}
	} else if cur, _ := errObj["message"].(string); strings.TrimSpace(cur) == "" && strings.TrimSpace(message) != "" {
		errObj["message"] = strings.TrimSpace(message)
	}
}

func shouldSynthesizeSinkUsageLimit(message, planType string, resetAt int64) bool {
	if strings.Contains(strings.ToLower(message), "usage limit has been reached") ||
		strings.Contains(strings.ToLower(message), "usage_limit_reached") {
		return true
	}
	return planType != "" && resetAt > time.Now().Unix()
}

func marshalSinkUsageLimitError(planType string, resetAt int64) string {
	errObj := map[string]any{
		"message": sinkUsageLimitReachedMessage,
		"type":    sinkUsageLimitReachedType,
	}
	if planType != "" {
		errObj["plan_type"] = planType
	}
	if resetAt > 0 {
		errObj["resets_at"] = resetAt
		if in := resetAt - time.Now().Unix(); in > 0 {
			errObj["resets_in_seconds"] = in
		}
	}
	b, err := json.Marshal(map[string]any{"error": errObj})
	if err != nil {
		return ""
	}
	return string(b)
}

func sinkJSONInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case float64:
		if t > 0 {
			return int64(t), true
		}
	case json.Number:
		n, err := t.Int64()
		return n, err == nil && n > 0
	case int64:
		return t, t > 0
	case int:
		return int64(t), t > 0
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return n, err == nil && n > 0
	}
	return 0, false
}
