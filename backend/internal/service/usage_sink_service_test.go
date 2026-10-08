package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
)

func TestBuildSinkErrorDetail_PassthroughUsageLimitJSON(t *testing.T) {
	raw := `{"error":{"eligible_promo":null,"message":"The usage limit has been reached","plan_type":"free","resets_at":1792499641,"resets_in_seconds":2586640,"type":"usage_limit_reached"}}`
	got := buildSinkErrorDetail(http.StatusTooManyRequests, raw, "", "", "", "", sql.NullFloat64{})
	assertSinkUsageLimit(t, got, "free", 1792499641)
}

func TestBuildSinkErrorDetail_EmptyDetailReconstructedFromAccount(t *testing.T) {
	resetAt := time.Now().Add(30 * 24 * time.Hour).Unix()
	got := buildSinkErrorDetail(
		http.StatusTooManyRequests,
		"",
		"",
		"",
		"",
		"free",
		sql.NullFloat64{Float64: float64(resetAt), Valid: true},
	)
	assertSinkUsageLimit(t, got, "free", resetAt)
}

func TestBuildSinkErrorDetail_TruncatedJSONFallsBackToBody(t *testing.T) {
	truncated := `{"error":{"message":"The usage limit has been rea`
	body := `{"error":{"message":"The usage limit has been reached","plan_type":"free","resets_at":1792499641,"type":"usage_limit_reached"}}`
	got := buildSinkErrorDetail(http.StatusTooManyRequests, truncated, body, "", "", "", sql.NullFloat64{})
	assertSinkUsageLimit(t, got, "free", 1792499641)
}

func TestBuildSinkErrorDetail_FillsMissingPlanAndReset(t *testing.T) {
	raw := `{"error":{"message":"The usage limit has been reached","type":"usage_limit_reached"}}`
	got := buildSinkErrorDetail(
		http.StatusTooManyRequests,
		raw,
		"",
		"",
		"",
		"free",
		sql.NullFloat64{Float64: 1792499641, Valid: true},
	)
	assertSinkUsageLimit(t, got, "free", 1792499641)
}

func TestBuildSinkErrorDetail_UpstreamErrorsLastEvent(t *testing.T) {
	events := `[{"detail":""},{"detail":"{\"error\":{\"message\":\"The usage limit has been reached\",\"plan_type\":\"free\",\"resets_at\":1792499641,\"type\":\"usage_limit_reached\"}}"}]`
	got := buildSinkErrorDetail(http.StatusTooManyRequests, "", "", "", events, "", sql.NullFloat64{})
	assertSinkUsageLimit(t, got, "free", 1792499641)
}

func TestBuildSinkErrorDetail_EmptyNon429StaysEmpty(t *testing.T) {
	got := buildSinkErrorDetail(http.StatusBadGateway, "", "", "Upstream request failed", "", "", sql.NullFloat64{})
	if got != "" {
		t.Fatalf("expected empty error_detail, got %q", got)
	}
}

func TestBuildSinkErrorDetail_429WithoutEvidenceStaysEmpty(t *testing.T) {
	got := buildSinkErrorDetail(http.StatusTooManyRequests, "", "", "Too Many Requests", "", "", sql.NullFloat64{})
	if got != "" {
		t.Fatalf("expected empty error_detail without plan/reset, got %q", got)
	}
}

func TestBuildSinkErrorDetail_MessageOnlyUsageLimit(t *testing.T) {
	resetAt := time.Now().Add(time.Hour).Unix()
	got := buildSinkErrorDetail(
		http.StatusTooManyRequests,
		"",
		"",
		"The usage limit has been reached",
		"",
		"",
		sql.NullFloat64{Float64: float64(resetAt), Valid: true},
	)
	assertSinkUsageLimit(t, got, "", resetAt)
}

func TestBuildSinkErrorDetail_StaleAccountResetDoesNotFabricateUsageLimit(t *testing.T) {
	got := buildSinkErrorDetail(
		http.StatusTooManyRequests,
		"",
		"",
		"Too Many Requests",
		"",
		"free",
		sql.NullFloat64{Float64: float64(time.Now().Add(-time.Hour).Unix()), Valid: true},
	)
	if got != "" {
		t.Fatalf("stale rate_limit_reset_at should not synthesize usage_limit JSON, got %q", got)
	}
}

func TestSinkRequestEventOmitsEmptyErrorDetail(t *testing.T) {
	data, err := json.Marshal(SinkRequestEvent{
		InstanceID: "api6",
		AccountID:  75572,
		Success:    false,
		StatusCode: 429,
		CreatedAt:  1789913026979,
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "error_detail") {
		t.Fatalf("empty error_detail should be omitted, got %s", data)
	}
}

func TestSinkAccountEventJSONKeepsOriginalKeys(t *testing.T) {
	resetMs := time.Now().Add(time.Hour).UnixMilli()
	data, err := json.Marshal(SinkAccountEvent{
		InstanceID:       "api6",
		AccountID:        1,
		Status:           "active",
		Schedulable:      true,
		RateLimitResetAt: &resetMs,
		PlanType:         "free",
		AccountCreatedAt: 1,
		UpdatedAt:        2,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := string(data)
	for _, leaked := range []string{"rate_limit_reset_at", "plan_type"} {
		if strings.Contains(raw, leaked) {
			t.Fatalf("account JSON must not grow new keys, leaked %s: %s", leaked, raw)
		}
	}
	for _, keep := range []string{"instance_id", "account_id", "status", "schedulable", "account_created_at", "updated_at"} {
		if !strings.Contains(raw, keep) {
			t.Fatalf("missing original key %s: %s", keep, raw)
		}
	}
}

func TestSinkAccountHasNonLastUsedChange(t *testing.T) {
	now := time.Now()
	lastUsed := sql.NullTime{Time: now, Valid: true}
	none := sql.NullTime{}

	if sinkAccountHasNonLastUsedChange("active", true, "", none, now, lastUsed, none, none, now) {
		t.Fatal("last_used-only bump should be skipped")
	}
	if sinkAccountHasNonLastUsedChange("active", true, "", none, now.Add(time.Second), lastUsed, none, none, now) {
		t.Fatal("updated_at within last_used grace should be skipped")
	}
	if !sinkAccountHasNonLastUsedChange("active", true, "", none, now.Add(3*time.Second), lastUsed, none, none, now) {
		t.Fatal("updated well after last_used should push")
	}
	if !sinkAccountHasNonLastUsedChange("active", true, "", sql.NullTime{Time: now.Add(time.Hour), Valid: true}, now, lastUsed, none, none, now) {
		t.Fatal("temp unschedulable should push even when coinciding with last_used")
	}
	if sinkAccountHasNonLastUsedChange("active", true, "", sql.NullTime{Time: now.Add(-time.Hour), Valid: true}, now.Add(time.Second), lastUsed, none, none, now) {
		t.Fatal("expired temp unschedulable plus last_used bump should be skipped")
	}
	if !sinkAccountHasNonLastUsedChange("active", false, "", none, now, lastUsed, none, none, now) {
		t.Fatal("unschedulable should push")
	}
	if !sinkAccountHasNonLastUsedChange("error", true, "", none, now, lastUsed, none, none, now) {
		t.Fatal("non-active status should push")
	}
	if !sinkAccountHasNonLastUsedChange("active", true, "rate limited", none, now, lastUsed, none, none, now) {
		t.Fatal("error_message should push")
	}
	if !sinkAccountHasNonLastUsedChange("active", true, "", none, now, none, none, none, now) {
		t.Fatal("nil last_used should push")
	}

	rateLimited := sql.NullTime{Time: now.Add(50 * time.Millisecond), Valid: true}
	resetAt := sql.NullTime{Time: now.Add(24 * time.Hour), Valid: true}
	if !sinkAccountHasNonLastUsedChange("active", true, "", none, rateLimited.Time, lastUsed, rateLimited, resetAt, now) {
		t.Fatal("SetRateLimited coinciding with last_used must still push")
	}
	expiredReset := sql.NullTime{Time: now.Add(-time.Hour), Valid: true}
	if sinkAccountHasNonLastUsedChange("active", true, "", none, now.Add(time.Second), lastUsed, none, expiredReset, now) {
		t.Fatal("expired rate_limit_reset_at plus last_used bump should be skipped")
	}
}

func TestDrainRequestEventsYieldsAfterMaxBatches(t *testing.T) {
	var received int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p sinkPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Errorf("decode: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received += len(p.Events)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestUsageSink(srv)
	var calls int
	poll := func(_ context.Context, cur sinkCursor) ([]SinkRequestEvent, sinkCursor, bool) {
		calls++
		at := cur.at.Add(time.Millisecond)
		batch := make([]SinkRequestEvent, sinkBatchSize)
		for i := range batch {
			id := cur.id + int64(i) + 1
			batch[i] = SinkRequestEvent{AccountID: id, CreatedAt: at.UnixMilli(), Success: false, StatusCode: 429}
		}
		return batch, sinkCursor{at: at, id: cur.id + int64(sinkBatchSize)}, true
	}

	cur := sinkCursor{at: time.Unix(1, 0).UTC(), id: 0}
	s.drainRequestEvents(&cur, "ops_error_logs", poll, sinkMaxBatchesPerTick)

	if calls != sinkMaxBatchesPerTick {
		t.Fatalf("poll calls=%d want %d (must yield while catching up)", calls, sinkMaxBatchesPerTick)
	}
	want := sinkBatchSize * sinkMaxBatchesPerTick
	if received != want {
		t.Fatalf("pushed events=%d want %d", received, want)
	}
	if cur.id != int64(want) {
		t.Fatalf("cursor id=%d want %d", cur.id, want)
	}
}

func TestDrainRequestEventsStopsOnShortBatch(t *testing.T) {
	var received int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p sinkPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Errorf("decode: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received += len(p.Events)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestUsageSink(srv)
	var calls int
	poll := func(_ context.Context, cur sinkCursor) ([]SinkRequestEvent, sinkCursor, bool) {
		calls++
		at := cur.at.Add(time.Millisecond)
		return []SinkRequestEvent{{
			AccountID:  42,
			CreatedAt:  at.UnixMilli(),
			Success:    false,
			StatusCode: 429,
		}}, sinkCursor{at: at, id: 7}, true
	}

	cur := sinkCursor{at: time.Unix(1, 0).UTC()}
	s.drainRequestEvents(&cur, "ops_error_logs", poll, sinkMaxBatchesPerTick)
	if calls != 1 {
		t.Fatalf("poll calls=%d want 1", calls)
	}
	if received != 1 {
		t.Fatalf("pushed events=%d want 1", received)
	}
	if cur.id != 7 {
		t.Fatalf("cursor id=%d want 7", cur.id)
	}
}

func TestDrainRequestEventsKeepsCursorOnPushFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	s := newTestUsageSink(srv)
	start := sinkCursor{at: time.Unix(1, 0).UTC(), id: 3}
	cur := start
	s.drainRequestEvents(&cur, "ops_error_logs", func(_ context.Context, _ sinkCursor) ([]SinkRequestEvent, sinkCursor, bool) {
		return []SinkRequestEvent{{AccountID: 1, CreatedAt: time.Unix(2, 0).UnixMilli()}}, sinkCursor{at: time.Unix(2, 0).UTC(), id: 99}, true
	}, sinkMaxBatchesPerTick)
	if cur != start {
		t.Fatalf("cursor moved on failed push: %+v", cur)
	}
}

func TestDrainRequestEventsKeepsCursorOnPollFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	s := newTestUsageSink(srv)

	start := sinkCursor{at: time.Unix(1, 0).UTC(), id: 3}
	cur := start
	s.drainRequestEvents(&cur, "ops_error_logs", func(_ context.Context, _ sinkCursor) ([]SinkRequestEvent, sinkCursor, bool) {
		return nil, sinkCursor{at: time.Unix(9, 0).UTC(), id: 99}, false
	}, sinkMaxBatchesPerTick)
	if cur != start {
		t.Fatalf("cursor moved on poll failure: %+v", cur)
	}
}

func TestDrainErrorLogsUsesHigherCapThanUsage(t *testing.T) {
	if sinkMaxErrorBatchesPerTick <= sinkMaxBatchesPerTick {
		t.Fatalf("error drain cap %d must exceed usage/account cap %d", sinkMaxErrorBatchesPerTick, sinkMaxBatchesPerTick)
	}
	if sinkBatchSize != 1000 {
		t.Fatalf("batch size=%d want 1000", sinkBatchSize)
	}
	var received int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p sinkPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Errorf("decode: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received += len(p.Events)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestUsageSink(srv)
	var calls int
	poll := func(_ context.Context, cur sinkCursor) ([]SinkRequestEvent, sinkCursor, bool) {
		calls++
		at := cur.at.Add(time.Millisecond)
		batch := make([]SinkRequestEvent, sinkBatchSize)
		for i := range batch {
			batch[i] = SinkRequestEvent{AccountID: cur.id + int64(i) + 1, CreatedAt: at.UnixMilli(), Success: false, StatusCode: 429}
		}
		return batch, sinkCursor{at: at, id: cur.id + int64(sinkBatchSize)}, true
	}
	cur := sinkCursor{at: time.Unix(1, 0).UTC()}
	caughtUp := s.drainRequestEvents(&cur, "ops_error_logs", poll, sinkMaxErrorBatchesPerTick)
	if caughtUp {
		t.Fatal("full error backlog should not report caught up")
	}
	if calls != sinkMaxErrorBatchesPerTick {
		t.Fatalf("error poll calls=%d want %d", calls, sinkMaxErrorBatchesPerTick)
	}
	if received != sinkBatchSize*sinkMaxErrorBatchesPerTick {
		t.Fatalf("pushed=%d", received)
	}
}

func TestSinkAccount429EventFromRateLimitReset(t *testing.T) {
	now := time.Now()
	resetMs := now.Add(24 * time.Hour).UnixMilli()
	snap := SinkAccountEvent{
		InstanceID:       "api6",
		AccountID:        23931,
		ChatgptUserID:    "user-1",
		Email:            "a@b.c",
		PlanType:         "free",
		RateLimitResetAt: &resetMs,
		UpdatedAt:        now.UnixMilli(),
		Schedulable:      true,
		Status:           "active",
	}
	ev, ok := sinkAccount429Event(snap, now)
	if !ok {
		t.Fatal("active rate_limit_reset_at must emit a 429 event")
	}
	if ev.AccountID != 23931 || ev.StatusCode != http.StatusTooManyRequests || ev.Success {
		t.Fatalf("event=%+v", ev)
	}
	assertSinkUsageLimit(t, ev.ErrorDetail, "free", resetMs/1000)

	events := sinkAccount429Events([]SinkAccountEvent{snap, {AccountID: 1, Status: "active", Schedulable: true}}, now)
	if len(events) != 1 {
		t.Fatalf("events=%d want 1", len(events))
	}

	expired := resetMs - 48*time.Hour.Milliseconds()
	snap.RateLimitResetAt = &expired
	if _, ok := sinkAccount429Event(snap, now); ok {
		t.Fatal("expired reset must not emit 429 event")
	}
}

func TestSinkAccount429EventFromTempUnschedulableReason(t *testing.T) {
	now := time.Now()
	until := now.Add(time.Hour).UnixMilli()
	snap := SinkAccountEvent{
		AccountID:               7,
		TempUnschedulableUntil:  &until,
		TempUnschedulableReason: "429",
		UpdatedAt:               now.UnixMilli(),
	}
	if _, ok := sinkAccount429Event(snap, now); !ok {
		t.Fatal("temp unschedulable 429 must emit event")
	}
}

func TestSinkAccount429EventJSONUsesOriginalEventKeys(t *testing.T) {
	now := time.Now()
	resetMs := now.Add(24 * time.Hour).UnixMilli()
	ev, ok := sinkAccount429Event(SinkAccountEvent{
		InstanceID:       "api6",
		AccountID:        23931,
		PlanType:         "free",
		RateLimitResetAt: &resetMs,
		UpdatedAt:        now.UnixMilli(),
	}, now)
	if !ok {
		t.Fatal("expected 429 event")
	}
	data, err := json.Marshal(sinkPayload{Events: []SinkRequestEvent{ev}})
	if err != nil {
		t.Fatal(err)
	}
	raw := string(data)
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	if _, ok := envelope["accounts"]; ok {
		t.Fatalf("429 must go out as events-only payload, got %s", raw)
	}
	var events []map[string]any
	if err := json.Unmarshal(envelope["events"], &events); err != nil || len(events) != 1 {
		t.Fatalf("events: %v %s", err, raw)
	}
	allowed := map[string]bool{
		"instance_id": true, "account_id": true, "request_id": true,
		"chatgpt_user_id": true, "chatgpt_account_id": true, "email": true,
		"success": true, "status_code": true, "actual_cost": true,
		"error_code": true, "error_detail": true, "created_at": true,
	}
	for k := range events[0] {
		if !allowed[k] {
			t.Fatalf("new event key %s: %s", k, raw)
		}
	}
}

func TestPollErrorLogsSQLAvoidsHugeUpstreamErrorsAndReconstructs429(t *testing.T) {
	db, mock, lastSQL := newSinkSQLMock(t)
	s := &UsageSinkService{cfg: &config.Config{}, db: db}
	s.cfg.Gateway.UsageSink.InstanceID = "api-test"

	created := time.Now().UTC()
	resetUnix := float64(time.Now().Add(24 * time.Hour).Unix())
	rows := sqlmock.NewRows([]string{
		"id", "account_id", "request_id", "chatgpt_user_id", "chatgpt_account_id", "email",
		"status_code", "error_code", "error_detail", "error_body", "error_message",
		"plan_type", "reset_unix", "created_at",
	}).AddRow(11, int64(23931), "req-1", "user-1", "acct-1", "a@b.c",
		429, "", "", "", "Too Many Requests",
		"free", resetUnix, created)
	mock.ExpectQuery("").WillReturnRows(rows)

	out, next, ok := s.pollErrorLogs(context.Background(), sinkCursor{at: time.Unix(1, 0).UTC()})
	if !ok {
		t.Fatal("poll failed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(*lastSQL, "upstream_errors::text") {
		t.Fatalf("must not load full upstream_errors JSONB: %s", *lastSQL)
	}
	if !strings.Contains(*lastSQL, "LEFT(COALESCE(oel.error_body, ''), 4096)") {
		t.Fatalf("error_body must be bounded: %s", *lastSQL)
	}
	if len(out) != 1 || next.id != 11 {
		t.Fatalf("out=%d next=%+v", len(out), next)
	}
	assertSinkUsageLimit(t, out[0].ErrorDetail, "free", int64(resetUnix))
}

func TestPollErrorLogsScanMismatchKeepsCursor(t *testing.T) {
	db, mock, _ := newSinkSQLMock(t)
	s := &UsageSinkService{cfg: &config.Config{}, db: db}
	// 13 columns vs 14 expected — Scan must fail rather than skip-and-catch-up.
	rows := sqlmock.NewRows([]string{
		"id", "account_id", "request_id", "chatgpt_user_id", "chatgpt_account_id", "email",
		"status_code", "error_code", "error_detail", "error_body", "error_message",
		"plan_type", "created_at",
	}).AddRow(11, int64(1), "req", "", "", "", 429, "", "", "", "", "", time.Now())
	mock.ExpectQuery("").WillReturnRows(rows)

	start := sinkCursor{at: time.Unix(1, 0).UTC(), id: 3}
	out, next, ok := s.pollErrorLogs(context.Background(), start)
	if ok {
		t.Fatalf("scan mismatch must not report success, out=%d next=%+v", len(out), next)
	}
	if next != start {
		t.Fatalf("cursor moved: %+v", next)
	}
}

func TestDrainAccountChangesPushes429EventsThenAccountsSeparately(t *testing.T) {
	db, mock, lastSQL := newSinkSQLMock(t)
	var posts []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		posts = append(posts, string(b))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestUsageSink(srv)
	s.db = db

	now := time.Now().UTC()
	resetAt := now.Add(24 * time.Hour)
	rows := sqlmock.NewRows([]string{
		"id", "chatgpt_user_id", "chatgpt_account_id", "email", "plan_type",
		"status", "error_message", "temp_unschedulable_until", "temp_reason",
		"schedulable", "last_used_at", "rate_limited_at", "rate_limit_reset_at",
		"created_at", "updated_at",
	}).AddRow(int64(23931), "user-1", "acct-1", "a@b.c", "free",
		"active", "", nil, "",
		true, now, now, resetAt,
		now.Add(-time.Hour), now)
	mock.ExpectQuery("").WillReturnRows(rows)

	cur := sinkCursor{at: time.Unix(1, 0).UTC()}
	if !s.drainAccountChanges(&cur) {
		t.Fatal("drain failed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	sqlText := *lastSQL
	if !strings.Contains(sqlText, "rate_limit_reset_at > NOW()") {
		t.Fatalf("OpenAI SetRateLimited rows must stay in the filter: %s", sqlText)
	}
	if !strings.Contains(sqlText, "temp_unschedulable_until > NOW()") {
		t.Fatalf("expired temp unschedulable must not flood: %s", sqlText)
	}
	if len(posts) != 2 {
		t.Fatalf("want events POST then accounts POST, got %d %v", len(posts), posts)
	}
	var first, second map[string]json.RawMessage
	if err := json.Unmarshal([]byte(posts[0]), &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(posts[1]), &second); err != nil {
		t.Fatal(err)
	}
	if _, ok := first["accounts"]; ok {
		t.Fatalf("first POST must be events-only: %s", posts[0])
	}
	if _, ok := first["events"]; !ok {
		t.Fatalf("first POST missing events: %s", posts[0])
	}
	if _, ok := second["events"]; ok {
		t.Fatalf("second POST must be accounts-only: %s", posts[1])
	}
	if strings.Contains(posts[1], "rate_limit_reset_at") || strings.Contains(posts[1], "plan_type") {
		t.Fatalf("account JSON grew new keys: %s", posts[1])
	}
	var events []SinkRequestEvent
	if err := json.Unmarshal(first["events"], &events); err != nil || len(events) != 1 {
		t.Fatalf("events: %v %s", err, posts[0])
	}
	if events[0].StatusCode != http.StatusTooManyRequests || events[0].Success {
		t.Fatalf("event=%+v", events[0])
	}
	assertSinkUsageLimit(t, events[0].ErrorDetail, "free", resetAt.Unix())
	if cur.id != 23931 {
		t.Fatalf("cursor id=%d", cur.id)
	}
}

func newSinkSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *string) {
	t.Helper()
	var lastSQL string
	matcher := sqlmock.QueryMatcherFunc(func(_, actualSQL string) error {
		lastSQL = actualSQL
		return nil
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock, &lastSQL
}

func newTestUsageSink(srv *httptest.Server) *UsageSinkService {
	cfg := &config.Config{}
	cfg.Gateway.UsageSink.URL = srv.URL
	cfg.Gateway.UsageSink.InstanceID = "api-test"
	return &UsageSinkService{
		cfg:        cfg,
		httpClient: srv.Client(),
	}
}

func assertSinkUsageLimit(t *testing.T, raw, wantPlan string, wantReset int64) {
	t.Helper()
	if raw == "" {
		t.Fatal("expected reconstructed error_detail, got empty")
	}
	var payload struct {
		Error struct {
			Message  string `json:"message"`
			PlanType string `json:"plan_type"`
			ResetsAt int64  `json:"resets_at"`
			Type     string `json:"type"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("error_detail is not JSON: %v raw=%s", err, raw)
	}
	if payload.Error.Message != sinkUsageLimitReachedMessage {
		t.Fatalf("message=%q", payload.Error.Message)
	}
	if payload.Error.Type != sinkUsageLimitReachedType {
		t.Fatalf("type=%q", payload.Error.Type)
	}
	if wantPlan != "" && payload.Error.PlanType != wantPlan {
		t.Fatalf("plan_type=%q want %q", payload.Error.PlanType, wantPlan)
	}
	if wantReset > 0 && payload.Error.ResetsAt != wantReset {
		t.Fatalf("resets_at=%d want %d", payload.Error.ResetsAt, wantReset)
	}
}
