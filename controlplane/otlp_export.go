package controlplane

import (
	"strings"
	"sync"

	"github.com/posthog/duckgres/internal/cliboot"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// otlpLogExportFailuresTotal is scraped on the CP only. Labels are
// {source,reason} — never {org}. source=cp is this process's exporter
// (otel.SetErrorHandler). source=worker is the last-seen *delta* of
// worker health JSON otlp_export_failures.
var otlpLogExportFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_otlp_log_export_failures_total",
	Help: "OTLP log export failures. source=cp is this process; source=worker is rolled up from worker health checks. No org label.",
}, []string{"source", "reason"})

const (
	otlpExportSourceCP      = "cp"
	otlpExportSourceWorker  = "worker"
	otlpExportReasonWorker  = "worker"
	otlpExportReasonExport  = "export"
	otlpExportReasonTimeout = "timeout"
	otlpExportReasonAuth    = "auth"
)

func init() {
	cliboot.SetOTLPErrorHook(func(err error) {
		observeOTLPExportFailures(otlpExportSourceCP, classifyOTLPError(err), 1)
	})
}

func observeOTLPExportFailures(source, reason string, n int64) {
	if n <= 0 {
		return
	}
	if source == "" {
		source = otlpExportSourceCP
	}
	if reason == "" {
		reason = otlpExportReasonExport
	}
	otlpLogExportFailuresTotal.WithLabelValues(source, reason).Add(float64(n))
}

func classifyOTLPError(err error) string {
	if err == nil {
		return otlpExportReasonExport
	}
	s := strings.ToLower(err.Error())
	switch {
	case strings.Contains(s, "timeout") || strings.Contains(s, "deadline"):
		return otlpExportReasonTimeout
	case strings.Contains(s, "unauthorized") || strings.Contains(s, "forbidden") ||
		strings.Contains(s, "401") || strings.Contains(s, "403"):
		return otlpExportReasonAuth
	default:
		return otlpExportReasonExport
	}
}

// otlpExportFailureRollup converts a worker's process-lifetime monotonic
// failure count into CP-scrape deltas. Old workers omit the field (nil) and
// must not be treated as 0 (that would spike on the first modern worker).
type otlpExportFailureRollup struct {
	mu       sync.Mutex
	lastSeen map[int]int64
}

func newOTLPExportFailureRollup() *otlpExportFailureRollup {
	return &otlpExportFailureRollup{lastSeen: make(map[int]int64)}
}

var workerOTLPExportRollup = newOTLPExportFailureRollup()

func (r *otlpExportFailureRollup) observeFromHealth(workerID int, hc *healthCheckResult) {
	if r == nil || hc == nil {
		return
	}
	r.observe(workerID, hc.OTLPExportFailures)
}

func (r *otlpExportFailureRollup) observe(workerID int, n *int64) {
	if r == nil || n == nil {
		return
	}
	val := *n
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastSeen == nil {
		r.lastSeen = make(map[int]int64)
	}
	last, seen := r.lastSeen[workerID]
	if !seen {
		observeOTLPExportFailures(otlpExportSourceWorker, otlpExportReasonWorker, val)
		r.lastSeen[workerID] = val
		return
	}
	if val >= last {
		observeOTLPExportFailures(otlpExportSourceWorker, otlpExportReasonWorker, val-last)
		r.lastSeen[workerID] = val
		return
	}
	// Worker process restart (counter reset): treat last as 0, then Add(n).
	observeOTLPExportFailures(otlpExportSourceWorker, otlpExportReasonWorker, val)
	r.lastSeen[workerID] = val
}

func (r *otlpExportFailureRollup) forget(workerID int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	delete(r.lastSeen, workerID)
	r.mu.Unlock()
}

func observeWorkerOTLPExportFromHealth(workerID int, hc *healthCheckResult) {
	workerOTLPExportRollup.observeFromHealth(workerID, hc)
}

func forgetWorkerOTLPExport(workerID int) {
	workerOTLPExportRollup.forget(workerID)
}
