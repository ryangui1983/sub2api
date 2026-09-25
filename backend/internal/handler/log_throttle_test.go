package handler

import (
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func resetLogThrottle(t *testing.T) {
	t.Helper()
	logThrottleMu.Lock()
	logThrottleStates = map[string]*logThrottleState{}
	logThrottleMu.Unlock()
}

// captureThrottleLogs 用 zap observer 捕获实际写出的日志条目。
func captureThrottleLogs(t *testing.T, fn func(log *zap.Logger)) []string {
	t.Helper()
	var mu sync.Mutex
	var lines []string
	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(writerFunc(func(p []byte) (int, error) {
			mu.Lock()
			lines = append(lines, strings.TrimSpace(string(p)))
			mu.Unlock()
			return len(p), nil
		})),
		zapcore.DebugLevel,
	)
	log := zap.New(core)
	defer func() { _ = log.Sync() }()
	fn(log)
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), lines...)
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

func TestLogThrottledEmitsFirstNThenSummary(t *testing.T) {
	resetLogThrottle(t)
	const msg = "test.throttle.first_n"

	lines := captureThrottleLogs(t, func(log *zap.Logger) {
		for i := 0; i < 12; i++ {
			logThrottled(log, zapcore.WarnLevel, msg, 5, zap.Int("i", i))
		}
	})

	if len(lines) != 6 {
		t.Fatalf("expected 5 individual + 1 summary = 6 lines, got %d: %v", len(lines), lines)
	}
	summary := lines[len(lines)-1]
	if !strings.Contains(summary, msg+".throttled") {
		t.Fatalf("last line should be the summary, got %q", summary)
	}
	for _, want := range []string{"suppressed_in_window", "emitted_in_window"} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary %q missing %q", summary, want)
		}
	}
	if !strings.Contains(summary, `"suppressed_in_window": 1`) {
		t.Fatalf("first summary should report suppressed=1, got %q", summary)
	}
}

func TestLogThrottledSummaryAbsorbsRemainder(t *testing.T) {
	resetLogThrottle(t)
	const msg = "test.throttle.absorb"

	lines := captureThrottleLogs(t, func(log *zap.Logger) {
		for i := 0; i < 9; i++ {
			logThrottled(log, zapcore.WarnLevel, msg, 3, zap.Int("i", i))
		}
	})

	// 3 条逐条 + 1 条汇总；第 5..9 条都被汇总吸收，不额外产生行。
	if len(lines) != 4 {
		t.Fatalf("expected 3 individual + 1 summary = 4 lines, got %d: %v", len(lines), lines)
	}
}

func TestLogThrottledWindowResets(t *testing.T) {
	resetLogThrottle(t)
	const msg = "test.throttle.window"

	// 先把窗口耗尽。
	captureThrottleLogs(t, func(log *zap.Logger) {
		for i := 0; i < 4; i++ {
			logThrottled(log, zapcore.WarnLevel, msg, 2, zap.Int("i", i))
		}
	})

	// 手动把窗口起点推到过去，模拟跨窗口。
	logThrottleMu.Lock()
	logThrottleStates[zapcore.WarnLevel.String()+"|"+msg].windowStart = time.Now().Add(-2 * throttleWindow)
	logThrottleMu.Unlock()

	lines := captureThrottleLogs(t, func(log *zap.Logger) {
		logThrottled(log, zapcore.WarnLevel, msg, 2, zap.Int("fresh", 1))
	})
	if len(lines) != 1 {
		t.Fatalf("expected the window reset to admit a fresh line, got %d: %v", len(lines), lines)
	}
	if strings.Contains(lines[0], ".throttled") {
		t.Fatalf("post-reset line should be a normal entry, got %q", lines[0])
	}
}

func TestLogThrottledDistinctMessagesIndependent(t *testing.T) {
	resetLogThrottle(t)

	lines := captureThrottleLogs(t, func(log *zap.Logger) {
		for i := 0; i < 4; i++ {
			logThrottled(log, zapcore.WarnLevel, "test.throttle.a", 2, zap.Int("i", i))
		}
		for i := 0; i < 4; i++ {
			logThrottled(log, zapcore.WarnLevel, "test.throttle.b", 2, zap.Int("i", i))
		}
	})

	// 每个 message 各自 2 条逐条 + 1 条汇总。
	if len(lines) != 6 {
		t.Fatalf("expected 2x(2 individual + 1 summary) = 6 lines, got %d: %v", len(lines), lines)
	}
}

func TestLogThrottledNilLoggerAndConcurrent(t *testing.T) {
	resetLogThrottle(t)
	if logThrottled(nil, zapcore.WarnLevel, "test.throttle.nil", 1) {
		t.Fatal("nil logger must not report emission")
	}

	const msg = "test.throttle.concurrent"
	lines := captureThrottleLogs(t, func(log *zap.Logger) {
		var wg sync.WaitGroup
		for g := 0; g < 20; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					logThrottled(log, zapcore.WarnLevel, msg, 5, zap.Int("i", i))
				}
			}()
		}
		wg.Wait()
	})

	// 并发下仍必须精确：5 条逐条 + 1 条汇总。
	if len(lines) != 6 {
		t.Fatalf("concurrent throttle must still emit exactly 6 lines, got %d", len(lines))
	}
}

// 确认限流器产出的行仍带着调用方给的诊断字段（账号、状态码等）。
func TestLogThrottledPreservesFields(t *testing.T) {
	resetLogThrottle(t)
	const msg = "test.throttle.fields"

	lines := captureThrottleLogs(t, func(log *zap.Logger) {
		logThrottled(log, zapcore.WarnLevel, msg, 1, zap.Int64("account_id", 94645), zap.Int("upstream_status", 429))
	})
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d", len(lines))
	}
	for _, want := range []string{"account_id", "94645", "upstream_status", "429", "throttle_max_per_window"} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("line %q missing %q", lines[0], want)
		}
	}
}
