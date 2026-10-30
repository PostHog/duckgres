package cliboot

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/posthog/duckgres/internal/netlisten"
)

// InitMetrics starts the Prometheus metrics HTTP server on :9090/metrics
// and returns the *http.Server so callers can shut it down during graceful
// shutdown / handover. The listener loops on transient errors with a 1s
// backoff — production duckgres processes have run into ephemeral port
// rebinds during handover and we don't want metrics scraping to silently
// die. Workers in --mode duckdb-service intentionally do NOT call this:
// in K8s all worker pods would fight over :9090 since the control plane
// owns the metrics endpoint there.
func InitMetrics() *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	addr := ":9090"
	// Addr is informational only now that Serve takes a listener; callers read it
	// when they rebuild a metrics server after a failed handover.
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go func() {
		for {
			// Rebind each iteration: the listener is recreated after a failed
			// serve, and it carries the socket options (see internal/netlisten).
			ln, err := netlisten.Listen("tcp", addr)
			if err != nil {
				slog.Warn("Metrics listener error, retrying in 1s.", "addr", addr, "error", err)
				time.Sleep(1 * time.Second)
				continue
			}
			slog.Info("Starting metrics server", "addr", addr)
			if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
				slog.Warn("Metrics server error, retrying in 1s.", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}
			return
		}
	}()
	return srv
}
