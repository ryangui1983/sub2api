package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/config"
)

func TestPollCreditsAccountsSQLFiltersParentsWithCredits(t *testing.T) {
	db, mock, lastSQL := newSinkSQLMock(t)
	s := &UsageSinkService{cfg: &config.Config{}, db: db}
	s.cfg.Gateway.UsageSink.InstanceID = "api-test"

	rows := sqlmock.NewRows([]string{
		"id", "chatgpt_user_id", "chatgpt_account_id", "email",
		"has_credits", "unlimited", "balance", "fetched_at",
	}).AddRow(int64(10), "user-1", "acct-1", "a@b.c", "true", "false", "12.34", int64(1_710_000_000))
	mock.ExpectQuery("").WillReturnRows(rows)

	out, ok := s.pollCreditsAccounts(context.Background(), 0)
	if !ok {
		t.Fatal("poll failed")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	sql := *lastSQL
	for _, want := range []string{
		"a.deleted_at IS NULL",
		"a.platform = 'openai'",
		"a.type = 'oauth'",
		"a.parent_account_id IS NULL",
		"a.id > $1",
		"{codex_credits_snapshot,credits,has_credits}",
		"{codex_credits_snapshot,credits,unlimited}",
		"IN ('true', 't', '1')",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("SQL missing %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "::boolean") {
		t.Fatalf("boolean cast can blow up on unexpected jsonb text:\n%s", sql)
	}
	if len(out) != 1 || out[0].AccountID != 10 || !out[0].HasCredits || out[0].Unlimited {
		t.Fatalf("out=%+v", out)
	}
	if out[0].Balance == nil || *out[0].Balance != "12.34" {
		t.Fatalf("balance=%v", out[0].Balance)
	}
	if out[0].FetchedAt != 1_710_000_000 {
		t.Fatalf("fetched_at=%d", out[0].FetchedAt)
	}
	if out[0].InstanceID != "api-test" {
		t.Fatalf("instance=%s", out[0].InstanceID)
	}
}

func TestDumpCreditsEmptyStillPostsFinishPage(t *testing.T) {
	db, mock, _ := newSinkSQLMock(t)
	var paths []string
	var bodies []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		paths = append(paths, r.URL.Path)
		bodies = append(bodies, string(b))
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	s := newTestUsageSink(srv)
	s.db = db
	mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{
		"id", "chatgpt_user_id", "chatgpt_account_id", "email",
		"has_credits", "unlimited", "balance", "fetched_at",
	}))

	s.dumpCredits()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != "/internal/sub2/credits" {
		t.Fatalf("paths=%v", paths)
	}
	var payload sinkCreditsPayload
	if err := json.Unmarshal([]byte(bodies[0]), &payload); err != nil {
		t.Fatalf("json: %v %s", err, bodies[0])
	}
	if payload.HasMore {
		t.Fatalf("empty dump must finish: %+v", payload)
	}
	if payload.InstanceID != "api-test" || payload.SyncedAt <= 0 {
		t.Fatalf("payload=%+v", payload)
	}
	if payload.Accounts == nil {
		t.Fatal("accounts must be [] not null so the receiver can clear")
	}
	if len(payload.Accounts) != 0 {
		t.Fatalf("accounts=%+v", payload.Accounts)
	}
}

func TestDumpCreditsPostsToCreditsPathNotEvents(t *testing.T) {
	db, mock, _ := newSinkSQLMock(t)
	var paths []string
	var gotToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-Sink-Token")
		if r.URL.Path == "/internal/sub2/events" {
			t.Error("credits dump must not hit /internal/sub2/events")
		}
		b, _ := io.ReadAll(r.Body)
		paths = append(paths, r.URL.Path)
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(b, &envelope); err != nil {
			t.Errorf("payload: %v", err)
		}
		if _, ok := envelope["events"]; ok {
			t.Error("credits payload must not contain events")
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	s := newTestUsageSink(srv)
	s.db = db
	s.cfg.Gateway.UsageSink.Token = "sink-token"
	balance := "9.5"
	rows := sqlmock.NewRows([]string{
		"id", "chatgpt_user_id", "chatgpt_account_id", "email",
		"has_credits", "unlimited", "balance", "fetched_at",
	}).AddRow(int64(42), "user-42", "ws-42", "c@d.e", "true", "false", balance, int64(99))
	mock.ExpectQuery("").WillReturnRows(rows)

	s.dumpCredits()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != "/internal/sub2/credits" {
		t.Fatalf("paths=%v", paths)
	}
	if gotToken != "sink-token" {
		t.Fatalf("token=%q", gotToken)
	}
}

func TestDumpCreditsUnlimitedParentIsPushed(t *testing.T) {
	db, mock, _ := newSinkSQLMock(t)
	var payload sinkCreditsPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &payload); err != nil {
			t.Errorf("json: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	s := newTestUsageSink(srv)
	s.db = db
	rows := sqlmock.NewRows([]string{
		"id", "chatgpt_user_id", "chatgpt_account_id", "email",
		"has_credits", "unlimited", "balance", "fetched_at",
	}).AddRow(int64(7), "user-7", "ws-7", "u@e.f", "false", "true", nil, int64(1))
	mock.ExpectQuery("").WillReturnRows(rows)

	s.dumpCredits()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if len(payload.Accounts) != 1 {
		t.Fatalf("accounts=%+v", payload.Accounts)
	}
	acc := payload.Accounts[0]
	if acc.AccountID != 7 || acc.HasCredits || !acc.Unlimited {
		t.Fatalf("acc=%+v", acc)
	}
	if acc.Balance != nil {
		t.Fatalf("unlimited balance=%v", acc.Balance)
	}
	if payload.HasMore {
		t.Fatal("single page must finish")
	}
}

func TestCreditsPayloadJSONKeepsDedicatedKeys(t *testing.T) {
	bal := "1.00"
	data, err := json.Marshal(sinkCreditsPayload{
		InstanceID: "api3",
		SyncedAt:   123,
		HasMore:    false,
		Accounts: []SinkCreditsAccount{{
			InstanceID: "api3",
			AccountID:  1,
			HasCredits: true,
			Balance:    &bal,
			FetchedAt:  9,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := string(data)
	for _, leaked := range []string{`"events"`, `"status"`, `"schedulable"`, `"temp_unschedulable_until"`} {
		if strings.Contains(raw, leaked) {
			t.Fatalf("credits JSON mixed events/accounts keys %s: %s", leaked, raw)
		}
	}
	for _, keep := range []string{"instance_id", "synced_at", "has_more", "accounts", "has_credits", "unlimited", "balance", "fetched_at"} {
		if !strings.Contains(raw, keep) {
			t.Fatalf("missing key %s: %s", keep, raw)
		}
	}
}
