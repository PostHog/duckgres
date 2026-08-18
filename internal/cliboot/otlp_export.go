package cliboot

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
)

// Process-lifetime OTLP log-export state. Workers report these on the
// health-check JSON (they do not serve Prometheus). The CP rolls worker
// counts up via last-seen deltas.
var (
	otlpExportEnabled  atomic.Bool
	otlpExportFailures atomic.Int64
	lastOTLPErrLog     atomic.Int64
	otlpErrorHook      atomic.Pointer[func(error)]
	otlpErrorHandler   sync.Once
)

// OTLPExportEnabled is true when this process has a live PostHog OTLP exporter.
func OTLPExportEnabled() bool { return otlpExportEnabled.Load() }

// OTLPExportFailures is the process-lifetime monotonic export-failure count.
func OTLPExportFailures() int64 { return otlpExportFailures.Load() }

// SetOTLPErrorHook registers an extra observer (the CP increments its
// Prometheus series). Workers leave this unset.
func SetOTLPErrorHook(fn func(error)) {
	if fn == nil {
		otlpErrorHook.Store(nil)
		return
	}
	otlpErrorHook.Store(&fn)
}

func markOTLPExportEnabled() {
	otlpExportEnabled.Store(true)
	installOTLPErrorHandler()
}

func installOTLPErrorHandler() {
	otlpErrorHandler.Do(func() {
		otel.SetErrorHandler(otel.ErrorHandlerFunc(handleOTLPError))
	})
}

func handleOTLPError(err error) {
	if err == nil {
		return
	}
	otlpExportFailures.Add(1)
	if hook := otlpErrorHook.Load(); hook != nil && *hook != nil {
		(*hook)(err)
	}
	// Never slog here: the exporter callback must not recurse into OTLP.
	now := time.Now().UnixNano()
	last := lastOTLPErrLog.Load()
	if last != 0 && now-last < int64(time.Minute) {
		return
	}
	if lastOTLPErrLog.CompareAndSwap(last, now) {
		fmt.Fprintf(os.Stderr, "PostHog OTLP log export failed: %v\n", err)
	}
}
