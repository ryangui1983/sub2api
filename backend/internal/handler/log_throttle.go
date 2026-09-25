package handler

import (
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// throttleWindow 是限流器的统计窗口。与 Zap 全局采样器不同：采样器按秒计数、
// 每个 message 独立，开启后仍会丢掉首条之外的大部分信号；这里按分钟聚合，
// 首 N 条逐条保留，其余压成一条汇总行，保证「信号不丢、噪声可控」。
const throttleWindow = time.Minute

// 每条 message 每分钟的逐条放行上限。实测负载下 failover_switching 一轮
// 868 条、forward_failed 191 条；压到每分钟 5 条量级后，一分钟的日志量
// 从数百行降到个位数，同时保留账号/状态码这些排查所需的字段。
const (
	failoverSwitchLogPerMinute = 5
	forwardFailedLogPerMinute  = 5
	waitQueueFullLogPerMinute  = 5
)

// logThrottleState 记录单个 message 在当前窗口内的放行数与被压条数。
type logThrottleState struct {
	windowStart time.Time
	emitted     int
	suppressed  int
}

var (
	logThrottleMu     sync.Mutex
	logThrottleStates = map[string]*logThrottleState{}
)

// logThrottled 对高频日志做「每分钟前 maxPerWindow 条逐条 + 一条汇总」的输出。
//
// 返回 true 表示本次调用已写出日志（逐条或汇总），调用方不应重复打印。
// 首个被压制的条目会立即产出汇总行，因此汇总里的 suppressed 是「至今被压条数」；
// 若该 message 在窗口内不再出现，最后几秒被压的条数不会单独补一行——
// 这是有意的取舍：日志静下来了，就没有再补一条的价值。
func logThrottled(reqLog *zap.Logger, level zapcore.Level, message string, maxPerWindow int, fields ...zap.Field) bool {
	if reqLog == nil {
		return false
	}
	if maxPerWindow <= 0 {
		maxPerWindow = 10
	}

	now := time.Now()
	key := level.String() + "|" + message
	fields = append(fields, zap.Int("throttle_max_per_window", maxPerWindow))

	logThrottleMu.Lock()
	state := logThrottleStates[key]
	if state == nil || now.Sub(state.windowStart) >= throttleWindow {
		state = &logThrottleState{windowStart: now}
		logThrottleStates[key] = state
	}

	var emit bool
	var summary bool
	if state.emitted < maxPerWindow {
		state.emitted++
		emit = true
	} else {
		state.suppressed++
		if state.suppressed == 1 {
			summary = true
			emit = true
		}
	}
	emitted := state.emitted
	suppressed := state.suppressed
	logThrottleMu.Unlock()

	if summary {
		reqLog.Warn(message+".throttled",
			append(fields,
				zap.Int("emitted_in_window", emitted),
				zap.Int("suppressed_in_window", suppressed),
			)...)
		return true
	}
	if emit {
		switch level {
		case zapcore.DebugLevel:
			reqLog.Debug(message, fields...)
		case zapcore.WarnLevel:
			reqLog.Warn(message, fields...)
		case zapcore.ErrorLevel:
			reqLog.Error(message, fields...)
		default:
			reqLog.Info(message, fields...)
		}
	}
	return emit
}
