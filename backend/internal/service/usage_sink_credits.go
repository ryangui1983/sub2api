package service

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

const sinkCreditsDumpInterval = time.Minute

// SinkCreditsAccount is one OpenAI OAuth parent that currently has Codex points.
// Pushed on POST {UsageSinkURL}/internal/sub2/credits — never mixed into events/accounts.
type SinkCreditsAccount struct {
	InstanceID       string  `json:"instance_id"`
	AccountID        int64   `json:"account_id"`
	ChatgptUserID    string  `json:"chatgpt_user_id,omitempty"`
	ChatgptAccountID string  `json:"chatgpt_account_id,omitempty"`
	Email            string  `json:"email,omitempty"`
	HasCredits       bool    `json:"has_credits"`
	Unlimited        bool    `json:"unlimited"`
	Balance          *string `json:"balance"`
	FetchedAt        int64   `json:"fetched_at,omitempty"`
}

type sinkCreditsPayload struct {
	InstanceID string               `json:"instance_id"`
	SyncedAt   int64                `json:"synced_at"`
	HasMore    bool                 `json:"has_more"`
	Accounts   []SinkCreditsAccount `json:"accounts"`
}

func (s *UsageSinkService) runCredits() {
	defer s.wg.Done()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-s.stopCh:
		return
	case <-timer.C:
		s.dumpCredits()
	}

	ticker := time.NewTicker(sinkCreditsDumpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.dumpCredits()
		}
	}
}

// dumpCredits full-dumps qualifying parents once a minute.
// Pages of sinkBatchSize by id cursor. The finishing page (has_more=false),
// including an empty set, is always POSTed so the receiver can drop stale rows.
func (s *UsageSinkService) dumpCredits() {
	syncedAt := time.Now().UnixMilli()
	afterID := int64(0)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		batch, ok := s.pollCreditsAccounts(ctx, afterID)
		cancel()
		if !ok {
			return
		}
		if batch == nil {
			batch = []SinkCreditsAccount{}
		}
		hasMore := len(batch) == sinkBatchSize
		if !s.pushCredits(sinkCreditsPayload{
			InstanceID: s.cfg.Gateway.UsageSink.InstanceID,
			SyncedAt:   syncedAt,
			HasMore:    hasMore,
			Accounts:   batch,
		}) {
			return
		}
		if !hasMore {
			return
		}
		afterID = batch[len(batch)-1].AccountID
	}
}

func (s *UsageSinkService) pollCreditsAccounts(ctx context.Context, afterID int64) ([]SinkCreditsAccount, bool) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT a.id,
		       COALESCE(a.credentials->>'chatgpt_user_id', '') AS chatgpt_user_id,
		       COALESCE(a.credentials->>'chatgpt_account_id', '') AS chatgpt_account_id,
		       COALESCE(a.credentials->>'email', '') AS email,
		       COALESCE(a.extra #>> '{codex_credits_snapshot,credits,has_credits}', '') AS has_credits,
		       COALESCE(a.extra #>> '{codex_credits_snapshot,credits,unlimited}', '') AS unlimited,
		       NULLIF(a.extra #>> '{codex_credits_snapshot,credits,balance}', '') AS balance,
		       CASE
		         WHEN (a.extra #>> '{codex_credits_snapshot,fetched_at}') ~ '^[0-9]+$'
		         THEN (a.extra #>> '{codex_credits_snapshot,fetched_at}')::bigint
		         ELSE NULL
		       END AS fetched_at
		FROM accounts a
		WHERE a.deleted_at IS NULL
		  AND a.platform = 'openai'
		  AND a.type = 'oauth'
		  AND a.parent_account_id IS NULL
		  AND a.id > $1
		  AND (
		        LOWER(a.extra #>> '{codex_credits_snapshot,credits,has_credits}') IN ('true', 't', '1')
		        OR LOWER(a.extra #>> '{codex_credits_snapshot,credits,unlimited}') IN ('true', 't', '1')
		      )
		ORDER BY a.id ASC
		LIMIT $2`, afterID, sinkBatchSize)
	if err != nil {
		log.Printf("[UsageSink] poll credits: %v", err)
		return nil, false
	}
	defer rows.Close()

	var out []SinkCreditsAccount
	var scanErrs int
	for rows.Next() {
		var acc SinkCreditsAccount
		var hasCredits, unlimited string
		var balance sql.NullString
		var fetchedAt sql.NullInt64
		if err := rows.Scan(
			&acc.AccountID, &acc.ChatgptUserID, &acc.ChatgptAccountID, &acc.Email,
			&hasCredits, &unlimited, &balance, &fetchedAt,
		); err != nil {
			log.Printf("[UsageSink] scan credits: %v", err)
			scanErrs++
			continue
		}
		acc.InstanceID = s.cfg.Gateway.UsageSink.InstanceID
		acc.HasCredits = sinkJSONTextBool(hasCredits)
		acc.Unlimited = sinkJSONTextBool(unlimited)
		if balance.Valid {
			b := balance.String
			acc.Balance = &b
		}
		if fetchedAt.Valid {
			acc.FetchedAt = fetchedAt.Int64
		}
		out = append(out, acc)
	}
	if err := rows.Err(); err != nil {
		log.Printf("[UsageSink] poll credits iterate: %v", err)
		return nil, false
	}
	if len(out) == 0 && scanErrs > 0 {
		return nil, false
	}
	return out, true
}

func (s *UsageSinkService) pushCredits(payload sinkCreditsPayload) bool {
	data, err := json.Marshal(payload)
	if err != nil {
		return false
	}
	req, err := http.NewRequest(http.MethodPost, s.cfg.Gateway.UsageSink.URL+"/internal/sub2/credits", bytes.NewReader(data))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.Gateway.UsageSink.Token != "" {
		req.Header.Set("X-Sink-Token", s.cfg.Gateway.UsageSink.Token)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Printf("[UsageSink] credits push error: %v", err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		log.Printf("[UsageSink] credits push returned %d", resp.StatusCode)
		return false
	}
	return true
}

func sinkJSONTextBool(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "true", "t", "1":
		return true
	default:
		return false
	}
}
