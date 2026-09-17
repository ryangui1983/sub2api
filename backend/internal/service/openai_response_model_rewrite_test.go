package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestRewriteOpenAIResponseModelFieldsOverwritesInternalSlug(t *testing.T) {
	t.Parallel()

	body := []byte(`{"id":"resp_1","object":"response","model":"gpt-6-luna-exp-1p-arm2-codexswic-ev3"}`)
	got := rewriteOpenAIResponseModelFields(body, "gpt-5.6-terra")
	require.Equal(t, "gpt-5.6-terra", gjson.GetBytes(got, "model").String())
}

func TestRewriteOpenAIResponseModelFieldsOverwritesNestedResponseModel(t *testing.T) {
	t.Parallel()

	body := []byte(`{"type":"response.created","response":{"id":"resp_1","model":"gpt-6-luna-exp-1p-arm2-codexswic-ev3"}}`)
	got := rewriteOpenAIResponseModelFields(body, "gpt-5.6-terra")
	require.Equal(t, "gpt-5.6-terra", gjson.GetBytes(got, "response.model").String())
}

func TestReplaceModelInSSELineHidesInternalSlug(t *testing.T) {
	t.Parallel()

	svc := &OpenAIGatewayService{}
	line := `data: {"type":"response.created","response":{"model":"gpt-6-luna-exp-1p-arm2-codexswic-ev3"}}`
	got := svc.replaceModelInSSELine(line, "gpt-5.6-luna", "gpt-5.6-terra")
	require.Equal(t, "gpt-5.6-terra", gjson.Get(extractMustSSEData(t, got), "response.model").String())
}

func TestHideMappedResponseModelIfEnabledRewritesInternalSlug(t *testing.T) {
	t.Parallel()

	ctx := EnsureRequestedPublicModel(context.Background(), "gpt-5.6-terra")
	ctx = context.WithValue(ctx, ctxkey.ChannelMappingHideInResponse, true)
	clientModel, ok := hideMappedResponseModelIfEnabled(ctx, "gpt-5.6-luna")
	require.True(t, ok)
	require.Equal(t, "gpt-5.6-terra", clientModel)

	svc := &OpenAIGatewayService{}
	body := []byte(`{"model":"gpt-6-luna-exp-1p-arm2-codexswic-ev3","output":[]}`)
	got := svc.replaceModelInResponseBody(body, "gpt-5.6-luna", clientModel)
	require.Equal(t, "gpt-5.6-terra", gjson.GetBytes(got, "model").String())
}

func TestHideMappedModelInChatJSONOverwritesInternalSlug(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	ctx := EnsureRequestedPublicModel(c.Request.Context(), "gpt-5.6-terra")
	ctx = context.WithValue(ctx, ctxkey.ChannelMappingHideInResponse, true)
	c.Request = c.Request.WithContext(ctx)

	body := []byte(`{"id":"chatcmpl_1","object":"chat.completion","model":"gpt-6-luna-exp-1p-arm2-codexswic-ev3"}`)
	got := hideMappedModelInChatJSON(c, body, "gpt-5.6-luna", "gpt-5.6-luna", "gpt-5.6-luna")
	require.Equal(t, "gpt-5.6-terra", gjson.GetBytes(got, "model").String())
}

func extractMustSSEData(t *testing.T, line string) string {
	t.Helper()
	data, ok := extractOpenAISSEDataLine(line)
	require.True(t, ok)
	return data
}
