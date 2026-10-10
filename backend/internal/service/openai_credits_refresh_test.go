package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/stretchr/testify/require"
)

func (r *stubQuotaAccountRepo) ListWithFilters(_ context.Context, params pagination.PaginationParams, platform, accountType, _, _ string, _ int64, _ string) ([]Account, *pagination.PaginationResult, error) {
	var all []Account
	for _, acc := range r.accounts {
		if acc == nil {
			continue
		}
		if platform != "" && acc.Platform != platform {
			continue
		}
		if accountType != "" && acc.Type != accountType {
			continue
		}
		all = append(all, *acc)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pageSize := params.Limit()
	page := params.Page
	if page < 1 {
		page = 1
	}
	total := len(all)
	pages := (total + pageSize - 1) / pageSize
	if pages < 1 {
		pages = 1
	}
	start := (page - 1) * pageSize
	result := &pagination.PaginationResult{Total: int64(total), Page: page, PageSize: pageSize, Pages: pages}
	if start >= total {
		return nil, result, nil
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	return all[start:end], result, nil
}

func TestAccountEligibleForCreditsRefresh(t *testing.T) {
	now := time.Unix(1_710_000_000, 0)
	parentID := int64(1)
	stale := now.Add(-time.Minute).Unix()
	fresh := now.Add(-10 * time.Second).Unix()

	tests := []struct {
		name    string
		account *Account
		want    bool
	}{
		{name: "nil", want: false},
		{
			name: "has credits",
			account: &Account{ID: 10, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
				openaiQuotaCreditsKey: map[string]any{
					"credits":    map[string]any{"has_credits": true, "unlimited": false, "balance": "12.34"},
					"fetched_at": stale,
				},
			}},
			want: true,
		},
		{
			name: "unlimited without has_credits",
			account: &Account{ID: 11, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
				openaiQuotaCreditsKey: map[string]any{
					"credits":    map[string]any{"has_credits": false, "unlimited": true, "balance": nil},
					"fetched_at": stale,
				},
			}},
			want: true,
		},
		{
			name: "zero credits",
			account: &Account{ID: 12, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
				openaiQuotaCreditsKey: map[string]any{
					"credits":    map[string]any{"has_credits": false, "unlimited": false, "balance": "0"},
					"fetched_at": stale,
				},
			}},
		},
		{
			name:    "no snapshot",
			account: &Account{ID: 13, Platform: PlatformOpenAI, Type: AccountTypeOAuth},
		},
		{
			name: "shadow skipped",
			account: &Account{ID: 14, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: &parentID, Extra: map[string]any{
				openaiQuotaCreditsKey: map[string]any{
					"credits":    map[string]any{"has_credits": true, "unlimited": false, "balance": "1"},
					"fetched_at": stale,
				},
			}},
		},
		{
			name: "fresh snapshot skipped",
			account: &Account{ID: 15, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
				openaiQuotaCreditsKey: map[string]any{
					"credits":    map[string]any{"has_credits": true, "unlimited": false, "balance": "1"},
					"fetched_at": fresh,
				},
			}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, accountEligibleForCreditsRefresh(tc.account, now))
		})
	}
}

func TestCreditsRefreshListsOnlyEligibleParents(t *testing.T) {
	now := time.Now()
	stale := now.Add(-time.Minute).Unix()
	parentID := int64(1)
	withCredits := func(id int64, has, unlimited bool, parent *int64) *Account {
		return &Account{
			ID: id, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: parent,
			Extra: map[string]any{
				openaiQuotaCreditsKey: map[string]any{
					"credits":    map[string]any{"has_credits": has, "unlimited": unlimited, "balance": "3"},
					"fetched_at": stale,
				},
			},
		}
	}
	repo := &stubQuotaAccountRepo{accounts: map[int64]*Account{
		1: withCredits(1, true, false, nil),
		2: withCredits(2, false, true, nil),
		3: withCredits(3, false, false, nil),
		4: {ID: 4, Platform: PlatformOpenAI, Type: AccountTypeOAuth},
		5: withCredits(5, true, false, &parentID),
		6: {ID: 6, Platform: PlatformAnthropic, Type: AccountTypeOAuth, Extra: withCredits(6, true, false, nil).Extra},
	}}
	svc := NewOpenAICreditsRefreshService(repo, &stubCreditsRefreshQuota{}, nil)
	ids, err := svc.listEligibleAccountIDs(context.Background(), now)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, ids)
}

type stubCreditsRefreshQuota struct{}

func (stubCreditsRefreshQuota) RefreshCreditsSnapshot(context.Context, int64) error { return nil }

func TestRefreshCreditsSnapshotHitsUsageOnly(t *testing.T) {
	account := &Account{ID: 100, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
		Credentials: map[string]any{"chatgpt_account_id": "test-workspace"}}
	repo := &stubQuotaAccountRepo{accounts: map[int64]*Account{100: account}}
	tokens := &stubQuotaTokenCache{tokens: map[string]string{OpenAITokenCacheKey(account): "test-token"}}
	var usageHits, otherHits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		if r.URL.Path == "/backend-api/wham/usage" {
			usageHits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"credits":{"has_credits":true,"unlimited":false,"balance":"9.5"}}`))
			return
		}
		otherHits.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	defer srv.Close()
	svc := NewOpenAIQuotaService(repo, nil, NewOpenAITokenProvider(repo, tokens, nil), newQuotaRedirectingFactory(srv), nil)
	require.NoError(t, svc.RefreshCreditsSnapshot(context.Background(), 100))
	require.Equal(t, int32(1), usageHits.Load())
	require.Zero(t, otherHits.Load())
	encoded, err := json.Marshal(repo.extraUpdates[100][openaiQuotaCreditsKey])
	require.NoError(t, err)
	var snap openAICreditsSnapshot
	require.NoError(t, json.Unmarshal(encoded, &snap))
	require.NotNil(t, snap.Credits)
	require.True(t, snap.Credits.HasCredits)
	require.NotNil(t, snap.Credits.Balance)
	require.Equal(t, "9.5", *snap.Credits.Balance)
	require.Positive(t, snap.FetchedAt)
}

func TestCreditsRefreshIntervalIsOneMinute(t *testing.T) {
	require.Equal(t, time.Minute, openAICreditsRefreshInterval)
}

type recordingCreditsRefreshQuota struct {
	mu  sync.Mutex
	ids []int64
}

func (q *recordingCreditsRefreshQuota) RefreshCreditsSnapshot(_ context.Context, accountID int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ids = append(q.ids, accountID)
	return nil
}

func (q *recordingCreditsRefreshQuota) calls() []int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]int64(nil), q.ids...)
}

func TestAccountEligibleForCreditsDiscovery(t *testing.T) {
	parentID := int64(1)
	tests := []struct {
		name    string
		account *Account
		want    bool
	}{
		{name: "nil"},
		{
			name:    "new oauth parent",
			account: &Account{ID: 10, Platform: PlatformOpenAI, Type: AccountTypeOAuth},
			want:    true,
		},
		{
			name: "already has snapshot",
			account: &Account{ID: 11, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
				openaiQuotaCreditsKey: map[string]any{
					"credits":    map[string]any{"has_credits": true, "unlimited": false, "balance": "1"},
					"fetched_at": time.Now().Unix(),
				},
			}},
		},
		{
			name: "zero credits snapshot",
			account: &Account{ID: 12, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
				openaiQuotaCreditsKey: map[string]any{
					"credits":    map[string]any{"has_credits": false, "unlimited": false, "balance": "0"},
					"fetched_at": time.Now().Unix(),
				},
			}},
		},
		{
			name:    "shadow skipped",
			account: &Account{ID: 13, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: &parentID},
		},
		{
			name:    "other platform skipped",
			account: &Account{ID: 14, Platform: PlatformAnthropic, Type: AccountTypeOAuth},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, accountEligibleForCreditsDiscovery(tc.account))
		})
	}
}

func TestRefreshAccountDiscoversMissingSnapshot(t *testing.T) {
	account := &Account{ID: 42, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	repo := &stubQuotaAccountRepo{accounts: map[int64]*Account{42: account}}
	quota := &recordingCreditsRefreshQuota{}
	svc := NewOpenAICreditsRefreshService(repo, quota, nil)
	require.NoError(t, svc.refreshAccount(context.Background(), 42))
	require.Equal(t, []int64{42}, quota.calls())
}

func TestRefreshAccountSkipsKnownZeroCredits(t *testing.T) {
	now := time.Now()
	account := &Account{ID: 43, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Extra: map[string]any{
		openaiQuotaCreditsKey: map[string]any{
			"credits":    map[string]any{"has_credits": false, "unlimited": false, "balance": "0"},
			"fetched_at": now.Add(-time.Minute).Unix(),
		},
	}}
	repo := &stubQuotaAccountRepo{accounts: map[int64]*Account{43: account}}
	quota := &recordingCreditsRefreshQuota{}
	svc := NewOpenAICreditsRefreshService(repo, quota, nil)
	require.NoError(t, svc.refreshAccount(context.Background(), 43))
	require.Empty(t, quota.calls())
}

func TestRefreshAccountSkipsShadowWithoutSnapshot(t *testing.T) {
	parentID := int64(1)
	account := &Account{ID: 44, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: &parentID}
	repo := &stubQuotaAccountRepo{accounts: map[int64]*Account{44: account}}
	quota := &recordingCreditsRefreshQuota{}
	svc := NewOpenAICreditsRefreshService(repo, quota, nil)
	require.NoError(t, svc.refreshAccount(context.Background(), 44))
	require.Empty(t, quota.calls())
}

func TestNotifyOpenAICreditsDiscoveryEnqueuesParentsOnly(t *testing.T) {
	svc := NewOpenAICreditsRefreshService(&stubQuotaAccountRepo{accounts: map[int64]*Account{}}, &recordingCreditsRefreshQuota{}, nil)
	setOpenAICreditsRefreshNotifier(svc)
	t.Cleanup(func() { clearOpenAICreditsRefreshNotifier(svc) })

	parentID := int64(9)
	notifyOpenAICreditsDiscovery(nil)
	notifyOpenAICreditsDiscovery(&Account{ID: 1, Platform: PlatformAnthropic, Type: AccountTypeOAuth})
	notifyOpenAICreditsDiscovery(&Account{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: &parentID})
	notifyOpenAICreditsDiscovery(&Account{ID: 3, Platform: PlatformOpenAI, Type: AccountTypeAPIKey})
	notifyOpenAICreditsDiscovery(&Account{ID: 10, Platform: PlatformOpenAI, Type: AccountTypeOAuth})

	select {
	case id := <-svc.queue:
		require.Equal(t, int64(10), id)
	default:
		t.Fatal("expected new OpenAI OAuth parent to be enqueued")
	}
	select {
	case id := <-svc.queue:
		t.Fatalf("unexpected extra enqueue: %d", id)
	default:
	}
}
