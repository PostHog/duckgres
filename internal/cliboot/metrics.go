package cliboot

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	srv := &http.Server{
		Addr:    ":9090",
		Handler: mux,
	}
	go func() {
		for {
			slog.Info("Starting metrics server", "addr", srv.Addr)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Warn("Metrics server error, retrying in 1s.", "error", err)
				time.Sleep(1 * time.Second)
				continue
			}
			return
		}
	}()
	return srv
}
