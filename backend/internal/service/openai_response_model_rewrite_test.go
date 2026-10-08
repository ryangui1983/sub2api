package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestReplaceOpenAIWSMessageModelHidesInternalSlug(t *testing.T) {
	t.Parallel()

	body := []byte(`{"type":"response.created","response":{"model":"gpt-6-luna-exp-1p-arm2-codexswic-ev3"}}`)
	got := replaceOpenAIWSMessageModel(body, "gpt-6-luna", "gpt-6.1-sol")
	require.Equal(t, "gpt-6.1-sol", gjson.GetBytes(got, "response.model").String())
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

func TestMappedResponseModelPreservesOtherData(t *testing.T) {
	svc := &OpenAIGatewayService{}
	body := `{"model":"alias","text":"mapped alias","tool":{"model":"mapped","arguments":"{\"model\":\"alias\"}"}}`
	want := `{"model":"public","text":"mapped alias","tool":{"model":"mapped","arguments":"{\"model\":\"alias\"}"}}`
	require.Equal(t, want, string(svc.replaceModelInResponseBody([]byte(body), "mapped", "public")))
	require.Equal(t, "data: "+want, svc.replaceModelInSSELine("data: "+body, "mapped", "public"))
	for _, body := range []string{
		`{"model":"alias",`, `{"model":"alias"} trailing`, `{"model":null}`, `{"model":42}`, `{"model":{}}`, `{"model":[]}`, `{"model":true}`,
		`{"text":"alias","tool":{"model":"alias"}}`,
	} {
		require.Equal(t, body, string(svc.replaceModelInResponseBody([]byte(body), "mapped", "public")))
		require.Equal(t, "data: "+body, svc.replaceModelInSSELine("data: "+body, "mapped", "public"))
	}
	for _, models := range [][2]string{{"same", "same"}, {"", "public"}, {"mapped", ""}} {
		body := `{"model":"alias","response":{"model":"alias"}}`
		require.Equal(t, body, string(svc.replaceModelInResponseBody([]byte(body), models[0], models[1])))
		require.Equal(t, "data: "+body, svc.replaceModelInSSELine("data: "+body, models[0], models[1]))
	}
	require.Equal(t, `data: {"response":{"model":42}}`, svc.replaceModelInSSELine(`data: {"response":{"model":42}}`, "mapped", "public"))
	require.Equal(t, `{"model":"public"}`, string(svc.replaceModelInResponseBody([]byte(`{"model":""}`), "mapped", "public")))
}

// Exercise both streaming processors so substring fast paths cannot bypass the rewrite.
func TestMappedResponseModelForwarding(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		for _, returned := range []string{"zhipu/glm-5.3", "glm-5.3-alias"} {
			for _, mapped := range []string{"ZHIPU/GLM-5.3", "public"} {
				for _, kind := range []string{"json", "chat", "responses"} {
					name := kind + "/" + returned + "/" + mapped
					if passthrough {
						name += "/passthrough"
					}
					t.Run(name, func(t *testing.T) {
						model := returned
						if mapped != "public" {
							model = "public"
						}
						payload := `{"model":"` + returned + `","choices":[{"delta":{"content":"keep alias","tool_calls":[{"function":{"arguments":"{\"model\":\"alias\"}"}}]}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`
						want := strings.Replace(payload, `"model":"`+returned+`"`, `"model":"`+model+`"`, 1)
						contentType := "application/json"
						body := payload
						if kind == "responses" {
							payload = `{"type":"response.completed","response":{"id":"resp_1","model":"` + returned + `","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`
							want = strings.Replace(payload, `"model":"`+returned+`"`, `"model":"`+model+`"`, 1)
						}
						if kind != "json" {
							contentType = "text/event-stream"
							body = "data: " + payload + "\n\ndata: [DONE]\n\n"
						}
						rec := httptest.NewRecorder()
						c, _ := gin.CreateTestContext(rec)
						c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
						resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(body))}
						svc := &OpenAIGatewayService{}
						account := &Account{ID: 1}
						var err error
						if kind == "json" {
							if passthrough {
								_, err = svc.handleNonStreamingResponsePassthrough(context.Background(), resp, c, account, "public", mapped)
							} else {
								_, err = svc.handleNonStreamingResponse(context.Background(), resp, c, account, "public", mapped)
							}
						} else {
							if passthrough {
								_, err = svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "public", mapped)
							} else {
								_, err = svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "public", mapped)
							}
						}
						require.NoError(t, err)
						require.Contains(t, rec.Body.String(), want)
						require.Equal(t, returned, observedUpstreamResponseModel(c))
					})
				}
			}
		}
	}
}
