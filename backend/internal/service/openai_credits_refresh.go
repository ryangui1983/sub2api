package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/google/uuid"
)

const (
	openAICreditsRefreshInterval    = time.Minute
	openAICreditsRefreshMinAge      = 50 * time.Second
	openAICreditsRefreshBatchSize   = 100
	openAICreditsRefreshWorkerCount = 4
	openAICreditsRefreshQueueSize   = 1024
	openAICreditsRefreshLeaderKey   = "jobs:openai-credits-refresh"
	openAICreditsRefreshLeaderTTL   = 55 * time.Second
	openAICreditsRefreshCallTimeout = openaiQuotaUpstreamTimeout + 5*time.Second
)

type openAICreditsRefreshQuota interface {
	RefreshCreditsSnapshot(ctx context.Context, accountID int64) error
}

var openAICreditsRefreshNotifierRegistry struct {
	sync.RWMutex
	service *OpenAICreditsRefreshService
}

func setOpenAICreditsRefreshNotifier(service *OpenAICreditsRefreshService) {
	openAICreditsRefreshNotifierRegistry.Lock()
	openAICreditsRefreshNotifierRegistry.service = service
	openAICreditsRefreshNotifierRegistry.Unlock()
}

func clearOpenAICreditsRefreshNotifier(service *OpenAICreditsRefreshService) {
	openAICreditsRefreshNotifierRegistry.Lock()
	if openAICreditsRefreshNotifierRegistry.service == service {
		openAICreditsRefreshNotifierRegistry.service = nil
	}
	openAICreditsRefreshNotifierRegistry.Unlock()
}

// NotifyOpenAICreditsRefresh 给新建的 OpenAI OAuth 母号发一次发现信号。
// 不走分钟扫描：没有快照的老账号不会被全量补查。
func NotifyOpenAICreditsRefresh(accountID int64) {
	openAICreditsRefreshNotifierRegistry.RLock()
	service := openAICreditsRefreshNotifierRegistry.service
	openAICreditsRefreshNotifierRegistry.RUnlock()
	if service != nil {
		service.enqueue(accountID)
	}
}

func notifyOpenAICreditsDiscovery(account *Account) {
	if account == nil || account.ID <= 0 || account.IsShadow() {
		return
	}
	if account.Platform != PlatformOpenAI || account.Type != AccountTypeOAuth {
		return
	}
	NotifyOpenAICreditsRefresh(account.ID)
}

// OpenAICreditsRefreshService re-queries ChatGPT /wham/usage once a minute for
// OpenAI OAuth parents that already have Codex points cached. It writes
// extra.codex_credits_snapshot so the admin card updates without a click.
// Accounts without points, shadows, and rows that never cached a snapshot are skipped
// by the scanner. New OpenAI OAuth parents are queried once via NotifyOpenAICreditsRefresh;
// a successful empty snapshot then keeps them out of the minute loop.
type OpenAICreditsRefreshService struct {
	accountRepo AccountRepository
	quota       openAICreditsRefreshQuota
	leaderLock  LeaderLockCache

	ctx     context.Context
	cancel  context.CancelFunc
	queue   chan int64
	pending sync.Map
	owner   string
	start   sync.Once
	stop    sync.Once
	wg      sync.WaitGroup
}

func NewOpenAICreditsRefreshService(
	accountRepo AccountRepository,
	quota openAICreditsRefreshQuota,
	leaderLock LeaderLockCache,
) *OpenAICreditsRefreshService {
	ctx, cancel := context.WithCancel(context.Background())
	return &OpenAICreditsRefreshService{
		accountRepo: accountRepo,
		quota:       quota,
		leaderLock:  leaderLock,
		ctx:         ctx,
		cancel:      cancel,
		queue:       make(chan int64, openAICreditsRefreshQueueSize),
		owner:       uuid.NewString(),
	}
}

func (s *OpenAICreditsRefreshService) Start() {
	if s == nil || s.accountRepo == nil || s.quota == nil {
		return
	}
	s.start.Do(func() {
		setOpenAICreditsRefreshNotifier(s)
		for range openAICreditsRefreshWorkerCount {
			s.wg.Add(1)
			go s.runWorker()
		}
		s.wg.Add(1)
		go s.runScanner()
	})
}

func (s *OpenAICreditsRefreshService) Stop() {
	if s == nil {
		return
	}
	s.stop.Do(func() {
		clearOpenAICreditsRefreshNotifier(s)
		s.cancel()
		s.wg.Wait()
	})
}

func (s *OpenAICreditsRefreshService) enqueue(accountID int64) {
	if s == nil || accountID <= 0 {
		return
	}
	if _, loaded := s.pending.LoadOrStore(accountID, struct{}{}); loaded {
		return
	}
	select {
	case <-s.ctx.Done():
		s.pending.Delete(accountID)
	case s.queue <- accountID:
	default:
		s.pending.Delete(accountID)
		slog.Warn("openai_credits_refresh_queue_full", "account_id", accountID)
	}
}

func (s *OpenAICreditsRefreshService) runWorker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case accountID := <-s.queue:
			ctx, cancel := context.WithTimeout(s.ctx, openAICreditsRefreshCallTimeout)
			if err := s.refreshAccount(ctx, accountID); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("openai_credits_refresh_failed", "account_id", accountID, "error", err)
			}
			cancel()
			s.pending.Delete(accountID)
		}
	}
}

func (s *OpenAICreditsRefreshService) runScanner() {
	defer s.wg.Done()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return
	case <-timer.C:
		s.scanEligibleAccounts(s.ctx)
	}
	ticker := time.NewTicker(openAICreditsRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.scanEligibleAccounts(s.ctx)
		}
	}
}

func (s *OpenAICreditsRefreshService) scanEligibleAccounts(ctx context.Context) {
	release, scan := tryAcquireSingletonLeaderLock(ctx, s.leaderLock, nil, openAICreditsRefreshLeaderKey, s.owner, openAICreditsRefreshLeaderTTL)
	if !scan {
		return
	}
	if release != nil {
		defer release()
	}
	ids, err := s.listEligibleAccountIDs(ctx, time.Now())
	if err != nil {
		slog.Warn("openai_credits_refresh_scan_failed", "error", err)
		return
	}
	for _, id := range ids {
		s.enqueue(id)
	}
}

func (s *OpenAICreditsRefreshService) listEligibleAccountIDs(ctx context.Context, now time.Time) ([]int64, error) {
	var ids []int64
	for page := 1; ; page++ {
		accounts, pageInfo, err := s.accountRepo.ListWithFilters(ctx, pagination.PaginationParams{
			Page: page, PageSize: openAICreditsRefreshBatchSize,
		}, PlatformOpenAI, AccountTypeOAuth, "", "", 0, "")
		if err != nil {
			return nil, err
		}
		for i := range accounts {
			if accountEligibleForCreditsRefresh(&accounts[i], now) {
				ids = append(ids, accounts[i].ID)
			}
		}
		if len(accounts) < openAICreditsRefreshBatchSize || pageInfo == nil || page >= pageInfo.Pages {
			return ids, nil
		}
	}
}

func (s *OpenAICreditsRefreshService) refreshAccount(ctx context.Context, accountID int64) error {
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil || account == nil {
		return err
	}
	if accountEligibleForCreditsRefresh(account, time.Now()) || accountEligibleForCreditsDiscovery(account) {
		return s.quota.RefreshCreditsSnapshot(ctx, accountID)
	}
	return nil
}

func accountEligibleForCreditsDiscovery(account *Account) bool {
	if account == nil || account.IsShadow() {
		return false
	}
	if account.Platform != PlatformOpenAI || account.Type != AccountTypeOAuth {
		return false
	}
	_, ok := cachedCodexCreditsSnapshot(account.Extra)
	return !ok
}

func accountEligibleForCreditsRefresh(account *Account, now time.Time) bool {
	if account == nil || account.IsShadow() {
		return false
	}
	if account.Platform != PlatformOpenAI || account.Type != AccountTypeOAuth {
		return false
	}
	snap, ok := cachedCodexCreditsSnapshot(account.Extra)
	if !ok || snap.Credits == nil {
		return false
	}
	if !snap.Credits.HasCredits && !snap.Credits.Unlimited {
		return false
	}
	if snap.FetchedAt > 0 {
		fetched := time.Unix(snap.FetchedAt, 0)
		if now.Sub(fetched) < openAICreditsRefreshMinAge {
			return false
		}
	}
	return true
}

func cachedCodexCreditsSnapshot(extra map[string]any) (openAICreditsSnapshot, bool) {
	if extra == nil {
		return openAICreditsSnapshot{}, false
	}
	raw, ok := extra[openaiQuotaCreditsKey]
	if !ok || raw == nil {
		return openAICreditsSnapshot{}, false
	}
	if snap, ok := raw.(openAICreditsSnapshot); ok {
		return snap, true
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return openAICreditsSnapshot{}, false
	}
	var snap openAICreditsSnapshot
	if err := json.Unmarshal(encoded, &snap); err != nil {
		return openAICreditsSnapshot{}, false
	}
	return snap, true
}
