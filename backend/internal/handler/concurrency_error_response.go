package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

const statusClientClosedRequest = 499

const (
	gatewayQueueFullCode        = "gateway_queue_full"
	gatewayConcurrencyLimitCode = "gateway_concurrency_limit"
)

// isAccountSlotWaitTimeout 判断错误是否为「等待账号槽位超时」——即账号本身健康、
// 只是槽位被占满导致本次等待超时。调用方据此排除该账号回全池重选，而不是把已经
// 等了一轮的请求直接 429 掉。客户端主动取消（context.Canceled）不算，它不该触发重选。
func isAccountSlotWaitTimeout(err error) bool {
	var concurrencyErr *ConcurrencyError
	if !errors.As(err, &concurrencyErr) {
		return false
	}
	return concurrencyErr.IsTimeout && !errors.Is(err, context.Canceled)
}

func concurrencyErrorResponse(err error, slotType string) (int, string, string, string) {
	var waitQueueFullErr *WaitQueueFullError
	if errors.As(err, &waitQueueFullErr) {
		return http.StatusTooManyRequests, "rate_limit_error", gatewayQueueFullCode,
			"Too many pending requests, please retry later"
	}

	var concurrencyErr *ConcurrencyError
	if errors.As(err, &concurrencyErr) {
		if concurrencyErr.SlotType != "" {
			slotType = concurrencyErr.SlotType
		}
		return http.StatusTooManyRequests, "rate_limit_error", gatewayConcurrencyLimitCode,
			fmt.Sprintf("Concurrency limit exceeded for %s, please retry later", slotType)
	}

	if errors.Is(err, context.Canceled) {
		return statusClientClosedRequest, "api_error", "", "context canceled"
	}

	return http.StatusServiceUnavailable, "api_error", "", "Service temporarily unavailable, please retry later"
}
