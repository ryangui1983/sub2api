package service

import (
	"context"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

// GATEWAY_DUMP_OPENAI_HEADERS=1 时，把出站请求头打成脱敏摘要。
// 只记名字、是否存在、安全字段的字面值；token / account-id / session 真值不落日志。
func logRedactedOpenAIOutboundHeaders(ctx context.Context, source string, h http.Header) {
	if h == nil || strings.TrimSpace(os.Getenv("GATEWAY_DUMP_OPENAI_HEADERS")) != "1" {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	names := make([]string, 0, len(h))
	for name := range h {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return strings.ToLower(names[i]) < strings.ToLower(names[j])
	})

	fields := make([]string, 0, len(names))
	for _, name := range names {
		values := h.Values(name)
		fields = append(fields, redactOpenAIOutboundHeader(name, values))
	}

	logger.FromContext(ctx).Info("openai outbound headers dumped",
		zap.String("source", source),
		zap.Strings("headers", fields),
	)
}

func redactOpenAIOutboundHeader(name string, values []string) string {
	lower := strings.ToLower(strings.TrimSpace(name))
	joined := strings.Join(values, " | ")
	switch lower {
	case "authorization", "cookie", "x-api-key":
		return name + "=redacted(len=" + itoaLen(joined) + ")"
	case "chatgpt-account-id", "session-id", "session_id", "thread-id", "thread_id",
		"conversation_id", "x-codex-installation-id", "x-codex-window-id",
		"x-client-request-id", "x-codex-turn-state":
		if strings.TrimSpace(joined) == "" {
			return name + "=empty"
		}
		return name + "=set(len=" + itoaLen(joined) + ")"
	case "x-codex-turn-metadata":
		return name + "=set(len=" + itoaLen(joined) + ")"
	default:
		return name + "=" + joined
	}
}

func itoaLen(s string) string {
	n := len(s)
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
