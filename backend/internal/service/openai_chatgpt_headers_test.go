package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubChatGPTHeadersRepo 是最小化 AccountRepository stub，仅实现 GetByID，
// 供 TestResolveAndSetOpenAIChatGPTAccountHeaders 使用。
type stubChatGPTHeadersRepo struct {
	AccountRepository
	byID map[int64]*Account
}

func (r *stubChatGPTHeadersRepo) GetByID(_ context.Context, id int64) (*Account, error) {
	return r.byID[id], nil
}

func (r *stubChatGPTHeadersRepo) UpdateCredentials(_ context.Context, id int64, credentials map[string]any) error {
	if acc := r.byID[id]; acc != nil {
		acc.Credentials = credentials
	}
	return nil
}

func TestResolveAndSetOpenAIChatGPTAccountHeaders(t *testing.T) {
	ctx := context.Background()
	pid := int64(100)

	parentCreds := map[string]any{"chatgpt_account_id": "org-parent"}
	parent := &Account{
		ID:          100,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Status:      StatusActive,
		Credentials: parentCreds,
	}
	repo := &stubChatGPTHeadersRepo{byID: map[int64]*Account{100: parent}}

	t.Run("shadow_resolves_to_parent_org", func(t *testing.T) {
		shadow := &Account{
			ID:              200,
			ParentAccountID: &pid,
			Platform:        PlatformOpenAI,
			Type:            AccountTypeOAuth,
		}
		headers := make(http.Header)
		err := resolveAndSetOpenAIChatGPTAccountHeaders(ctx, repo, headers, shadow)
		require.NoError(t, err)
		require.Equal(t, "org-parent", headers.Get("chatgpt-account-id"),
			"影子账号应透传母账号的 chatgpt-account-id")
	})

	t.Run("normal_account_passthrough", func(t *testing.T) {
		ownCreds := map[string]any{"chatgpt_account_id": "org-own"}
		normal := &Account{
			ID:          300,
			Platform:    PlatformOpenAI,
			Type:        AccountTypeOAuth,
			Credentials: ownCreds,
		}
		headers := make(http.Header)
		err := resolveAndSetOpenAIChatGPTAccountHeaders(ctx, repo, headers, normal)
		require.NoError(t, err)
		require.Equal(t, "org-own", headers.Get("chatgpt-account-id"),
			"普通账号应透传自身的 chatgpt-account-id")
	})

	t.Run("jwt_fallback_when_credential_missing", func(t *testing.T) {
		payload, err := json.Marshal(map[string]any{
			"https://api.openai.com/auth": map[string]any{
				"chatgpt_account_id": "acct-from-jwt",
				"chatgpt_user_id":    "user-jwt",
			},
		})
		require.NoError(t, err)
		token := "eyJhbGciOiJub25lIn0." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
		jwtAccount := &Account{
			ID:       400,
			Platform: PlatformOpenAI,
			Type:     AccountTypeOAuth,
			Credentials: map[string]any{
				"access_token": token,
				"id_token":     token,
			},
		}
		repo.byID[400] = jwtAccount
		headers := make(http.Header)
		err = resolveAndSetOpenAIChatGPTAccountHeaders(ctx, repo, headers, jwtAccount)
		require.NoError(t, err)
		require.Equal(t, "acct-from-jwt", headers.Get("chatgpt-account-id"))
		require.Equal(t, "acct-from-jwt", jwtAccount.GetCredential("chatgpt_account_id"))
	})
}
