package service

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
)

func setOpenAIChatGPTAccountHeaders(headers http.Header, account *Account) {
	if headers == nil || account == nil || !account.IsOpenAIOAuthLike() {
		return
	}
	if chatgptAccountID := account.GetChatGPTAccountID(); chatgptAccountID != "" {
		headers.Set("chatgpt-account-id", chatgptAccountID)
	}
	if account.IsChatGPTAccountFedRAMP() {
		headers.Set("x-openai-fedramp", "true")
	} else {
		headers.Del("x-openai-fedramp")
	}
}

// resolveAndSetOpenAIChatGPTAccountHeaders 解析 spark 影子账号至其母账号（凭据透传），
// 再调用 setOpenAIChatGPTAccountHeaders 写入 chatgpt-account-id / x-openai-fedramp 头。
// 普通账号（非影子）为直通，行为与直接调用 setOpenAIChatGPTAccountHeaders 一致。
func resolveAndSetOpenAIChatGPTAccountHeaders(ctx context.Context, repo AccountRepository, headers http.Header, account *Account) error {
	credAccount, err := resolveCredentialAccount(ctx, repo, account)
	if err != nil {
		return err
	}
	missingStored := credAccount != nil && strings.TrimSpace(credAccount.GetCredential("chatgpt_account_id")) == ""
	setOpenAIChatGPTAccountHeaders(headers, credAccount)
	if missingStored && credAccount != nil && strings.TrimSpace(credAccount.GetChatGPTAccountID()) != "" {
		persistChatGPTAccountIDFromMemory(ctx, repo, credAccount)
	}
	return nil
}

func persistChatGPTAccountIDFromMemory(ctx context.Context, repo AccountRepository, account *Account) {
	if repo == nil || account == nil {
		return
	}
	id := strings.TrimSpace(account.GetChatGPTAccountID())
	if id == "" {
		return
	}
	latest := account
	if loaded, err := repo.GetByID(ctx, account.ID); err == nil && loaded != nil {
		latest = loaded
	}
	creds := latest.Credentials
	if creds == nil {
		creds = map[string]any{"chatgpt_account_id": id}
	} else {
		creds = shallowCopyMap(creds)
		creds["chatgpt_account_id"] = id
	}
	if err := persistAccountCredentials(ctx, repo, latest, creds); err != nil {
		slog.Warn("persist chatgpt_account_id failed", "account_id", account.ID, "error", err)
	}
}
