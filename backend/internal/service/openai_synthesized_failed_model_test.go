package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const synthesizedBareErrorStream = "event: response.created\n" +
	`data: {"type":"response.created","response":{"id":"resp_hide","model":"gpt-6-luna"}}` + "\n\n" +
	"event: error\n" +
	`data: {"type":"error","error":{"code":"server_error","message":"internal model error"}}` + "\n\n"

func TestBuildOpenAIResponseFailedSSEUsesProvidedModel(t *testing.T) {
	got := buildOpenAIResponseFailedSSE("resp_hide", "gpt-6.1-sol", []byte(`{"error":{"code":"server_error","message":"internal model error"}}`), "")
	data, ok := extractOpenAISSEDataLine(strings.TrimSpace(strings.Split(got, "\n")[1]))
	require.True(t, ok)
	require.Equal(t, "gpt-6.1-sol", gjson.Get(data, "response.model").String())
	require.NotContains(t, got, "gpt-6-luna")
}

func TestBuildOpenAIWSHTTPBridgeFailedEventUsesProvidedModel(t *testing.T) {
	got := buildOpenAIWSHTTPBridgeFailedEvent("resp_hide", "gpt-6.1-sol", []byte(`{"error":{"code":"server_error","message":"internal model error"}}`), "")
	require.Equal(t, "gpt-6.1-sol", gjson.GetBytes(got, "response.model").String())
	require.NotContains(t, string(got), "gpt-6-luna")
}

func TestSynthesizedResponseFailedHidesMappedModel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name string
		hide bool
		want string
		leak string
		run  func(*OpenAIGatewayService, context.Context, *gin.Context, *http.Response, *Account) error
	}{
		{
			name: "native_hide",
			hide: true,
			want: "gpt-6.1-sol",
			leak: "gpt-6-luna",
			run: func(svc *OpenAIGatewayService, ctx context.Context, c *gin.Context, resp *http.Response, account *Account) error {
				_, err := svc.handleStreamingResponse(ctx, resp, c, account, time.Now(), "gpt-6-luna", "gpt-6-luna")
				return err
			},
		},
		{
			name: "native_hide_off",
			want: "gpt-6-luna",
			leak: "gpt-6.1-sol",
			run: func(svc *OpenAIGatewayService, ctx context.Context, c *gin.Context, resp *http.Response, account *Account) error {
				_, err := svc.handleStreamingResponse(ctx, resp, c, account, time.Now(), "gpt-6-luna", "gpt-6-luna")
				return err
			},
		},
		{
			name: "passthrough_hide",
			hide: true,
			want: "gpt-6.1-sol",
			leak: "gpt-6-luna",
			run: func(svc *OpenAIGatewayService, ctx context.Context, c *gin.Context, resp *http.Response, account *Account) error {
				_, err := svc.handleStreamingResponsePassthrough(ctx, resp, c, account, time.Now(), "gpt-6-luna", "gpt-6-luna")
				return err
			},
		},
		{
			name: "passthrough_hide_off",
			want: "gpt-6-luna",
			leak: "gpt-6.1-sol",
			run: func(svc *OpenAIGatewayService, ctx context.Context, c *gin.Context, resp *http.Response, account *Account) error {
				_, err := svc.handleStreamingResponsePassthrough(ctx, resp, c, account, time.Now(), "gpt-6-luna", "gpt-6-luna")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			ctx := bindSynthesizedFailedModelContext(c, tt.hide)

			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(synthesizedBareErrorStream)),
			}
			svc := &OpenAIGatewayService{
				cfg:           &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
				toolCorrector: NewCodexToolCorrector(),
			}
			account := &Account{ID: 131, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

			err := tt.run(svc, ctx, c, resp, account)
			require.Error(t, err)
			require.Contains(t, err.Error(), "internal model error")
			var failoverErr *UpstreamFailoverError
			require.False(t, errors.As(err, &failoverErr), "generic bare error must synthesize failed, not failover")

			body := rec.Body.String()
			require.Equal(t, tt.want, sseResponseFailedModel(t, body))
			require.NotContains(t, body, tt.leak)
			require.Contains(t, body, `"type":"response.failed"`)
		})
	}
}

func TestWSHTTPBridgeSynthesizedFailedHidesMappedModel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(synthesizedBareErrorStream)),
	}}
	svc := &OpenAIGatewayService{
		cfg:          &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream: upstream,
	}
	account := &Account{ID: 131, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1}
	payload := []byte(`{"type":"response.create","model":"gpt-6-luna","stream":true,"input":"hi"}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	ctx := bindSynthesizedFailedModelContext(c, true)

	var messages [][]byte
	_, err := svc.proxyOpenAIWSHTTPBridgeTurn(
		ctx, c, account, "test-token", payload, len(payload),
		"gpt-6-luna", "", "", "", "", 1,
		func(message []byte) error {
			messages = append(messages, append([]byte(nil), message...))
			return nil
		},
	)

	require.Error(t, err)
	require.Contains(t, err.Error(), "internal model error")
	require.Equal(t, "gpt-6.1-sol", wsResponseFailedModel(t, messages))
	for _, message := range messages {
		require.NotContains(t, string(message), "gpt-6-luna")
		if gjson.GetBytes(message, "type").String() == "response.created" {
			require.Equal(t, "gpt-6.1-sol", gjson.GetBytes(message, "response.model").String())
		}
	}
}

func bindSynthesizedFailedModelContext(c *gin.Context, hide bool) context.Context {
	ctx := EnsureRequestedPublicModel(c.Request.Context(), "gpt-6.1-sol")
	if hide {
		ctx = context.WithValue(ctx, ctxkey.ChannelMappingHideInResponse, true)
	}
	c.Request = c.Request.WithContext(ctx)
	return ctx
}

func sseResponseFailedModel(t *testing.T, body string) string {
	t.Helper()
	for _, block := range strings.Split(body, "\n\n") {
		for _, line := range strings.Split(block, "\n") {
			data, ok := extractOpenAISSEDataLine(line)
			if !ok {
				continue
			}
			if gjson.Get(data, "type").String() != "response.failed" {
				continue
			}
			return gjson.Get(data, "response.model").String()
		}
	}
	t.Fatalf("no response.failed event in body: %s", body)
	return ""
}

func wsResponseFailedModel(t *testing.T, messages [][]byte) string {
	t.Helper()
	for _, message := range messages {
		if gjson.GetBytes(message, "type").String() != "response.failed" {
			continue
		}
		return gjson.GetBytes(message, "response.model").String()
	}
	t.Fatalf("no response.failed websocket message in %d frames", len(messages))
	return ""
}
