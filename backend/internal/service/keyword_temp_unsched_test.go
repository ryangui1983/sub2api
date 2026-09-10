//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestDefaultKeywordTempUnschedSettings(t *testing.T) {
	d := DefaultKeywordTempUnschedSettings()
	require.False(t, d.Enabled)
	require.Equal(t, []string{"currently overloaded"}, d.Keywords)
	require.Equal(t, 5, d.DurationMinutes)
}

func TestGetKeywordTempUnschedSettings_DefaultsWhenNotSet(t *testing.T) {
	svc := NewSettingService(newMockSettingRepo(), &config.Config{})
	settings, err := svc.GetKeywordTempUnschedSettings(context.Background())
	require.NoError(t, err)
	require.False(t, settings.Enabled)
	require.Equal(t, []string{"currently overloaded"}, settings.Keywords)
	require.Equal(t, 5, settings.DurationMinutes)
}

func TestSetKeywordTempUnschedSettings_EnabledRequiresKeywords(t *testing.T) {
	svc := NewSettingService(newMockSettingRepo(), &config.Config{})
	err := svc.SetKeywordTempUnschedSettings(context.Background(), &KeywordTempUnschedSettings{
		Enabled:         true,
		Keywords:        nil,
		DurationMinutes: 5,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "keywords cannot be empty")
}

func TestHandleKeywordTempUnschedulable_PausesAccountOnOverloadMessage(t *testing.T) {
	settingRepo := newMockSettingRepo()
	data, err := json.Marshal(KeywordTempUnschedSettings{
		Enabled:         true,
		Keywords:        []string{"currently overloaded"},
		DurationMinutes: 3,
	})
	require.NoError(t, err)
	settingRepo.data[SettingKeyKeywordTempUnschedSettings] = string(data)

	accountRepo := &capacityShedAccountRepoStub{}
	rateLimit := NewRateLimitService(accountRepo, nil, &config.Config{}, nil, nil)
	rateLimit.SetSettingService(NewSettingService(settingRepo, &config.Config{}))

	account := &Account{ID: 11, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	payload := []byte(`{"error":{"message":"Our servers are currently overloaded. Please try again later."}}`)
	before := time.Now()
	matched := rateLimit.HandleKeywordTempUnschedulable(context.Background(), account, http.StatusBadRequest, payload)
	require.True(t, matched)
	require.Equal(t, 1, accountRepo.tempUnschedCalls)
	require.WithinDuration(t, before.Add(3*time.Minute), time.Now().Add(3*time.Minute), 2*time.Second)
}

func TestHandleOpenAIAccountUpstreamError_KeywordRuleBeatsCapacityShedSkip(t *testing.T) {
	settingRepo := newMockSettingRepo()
	data, err := json.Marshal(KeywordTempUnschedSettings{
		Enabled:         true,
		Keywords:        []string{"currently overloaded"},
		DurationMinutes: 5,
	})
	require.NoError(t, err)
	settingRepo.data[SettingKeyKeywordTempUnschedSettings] = string(data)

	accountRepo := &capacityShedAccountRepoStub{}
	rateLimit := NewRateLimitService(accountRepo, nil, &config.Config{}, nil, nil)
	rateLimit.SetSettingService(NewSettingService(settingRepo, &config.Config{}))
	gateway := &OpenAIGatewayService{rateLimitService: rateLimit}
	account := &Account{ID: 12, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	payload := []byte(`{"error":{"type":"server_error","message":"Our servers are currently overloaded. Please try again later."}}`)

	require.True(t, gateway.handleOpenAIAccountUpstreamError(
		context.Background(),
		account,
		http.StatusBadRequest,
		nil,
		payload,
		"gpt-5",
	))
	require.Equal(t, 1, accountRepo.tempUnschedCalls)
}

func TestHandleKeywordTempUnschedulable_DisabledDoesNotPause(t *testing.T) {
	accountRepo := &capacityShedAccountRepoStub{}
	rateLimit := NewRateLimitService(accountRepo, nil, &config.Config{}, nil, nil)
	rateLimit.SetSettingService(NewSettingService(newMockSettingRepo(), &config.Config{}))
	account := &Account{ID: 13, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	payload := []byte(`{"error":{"message":"Our servers are currently overloaded. Please try again later."}}`)
	require.False(t, rateLimit.HandleKeywordTempUnschedulable(context.Background(), account, http.StatusBadRequest, payload))
	require.Zero(t, accountRepo.tempUnschedCalls)
}

func TestHandleOpenAIStreamTerminalAccountSideEffects_KeywordPausesOverload(t *testing.T) {
	settingRepo := newMockSettingRepo()
	data, err := json.Marshal(KeywordTempUnschedSettings{
		Enabled:         true,
		Keywords:        []string{"currently overloaded"},
		DurationMinutes: 5,
	})
	require.NoError(t, err)
	settingRepo.data[SettingKeyKeywordTempUnschedSettings] = string(data)

	accountRepo := &capacityShedAccountRepoStub{}
	rateLimit := NewRateLimitService(accountRepo, nil, &config.Config{}, nil, nil)
	rateLimit.SetSettingService(NewSettingService(settingRepo, &config.Config{}))
	gateway := &OpenAIGatewayService{rateLimitService: rateLimit}
	account := &Account{ID: 14, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	payload := []byte(`{"type":"response.failed","response":{"error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`)

	status, _ := gateway.handleOpenAIStreamTerminalAccountSideEffects(nil, account, payload, "Our servers are currently overloaded. Please try again later.")
	require.Equal(t, http.StatusServiceUnavailable, status)
	require.Equal(t, 1, accountRepo.tempUnschedCalls)
}
