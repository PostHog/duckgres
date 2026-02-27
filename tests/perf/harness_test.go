package perf

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/perf/core"
	flightdriver "github.com/posthog/duckgres/tests/perf/drivers/flight"
	pgdriver "github.com/posthog/duckgres/tests/perf/drivers/pgwire"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	perfRun         = flag.Bool("perf-run", false, "run the perf harness against the configured catalog")
	perfCatalog     = flag.String("perf-catalog", "queries/smoke.yaml", "path to perf catalog yaml")
	perfOutputBase  = flag.String("perf-output-base", filepath.Clean("../../artifacts/perf"), "base output directory for perf artifacts")
	perfRunID       = flag.String("perf-run-id", "", "override run id; default is timestamp")
	perfMetricsAddr = flag.String("perf-metrics-addr", ":9095", "runner metrics listen address")
)

type noOpExecutor struct{}

func (n *noOpExecutor) Execute(context.Context, string, []any) (int64, error) { return 1, nil }
func (n *noOpExecutor) Close() error                                          { return nil }

func TestGoldenQueryPerformanceHarness(t *testing.T) {
	if !*perfRun {
		t.Skip("set -perf-run to execute the perf harness")
	}

	catalog, err := core.LoadCatalog(*perfCatalog)
	if err != nil {
		t.Fatalf("LoadCatalog(%s): %v", *perfCatalog, err)
	}

	runID := *perfRunID
	if runID == "" {
		runID = time.Now().UTC().Format("20060102T150405Z")
	}
	outputDir := filepath.Join(*perfOutputBase, runID)
	sink, err := core.NewArtifactSink(outputDir)
	if err != nil {
		t.Fatalf("NewArtifactSink(%s): %v", outputDir, err)
	}

	runner := core.NewQueryRunner(core.RunnerConfig{
		Catalog: catalog,
		Drivers: map[core.Protocol]core.ProtocolDriver{
			core.ProtocolPGWire: pgdriver.NewWithExecutor(&noOpExecutor{}),
			core.ProtocolFlight: flightdriver.NewWithExecutor(&noOpExecutor{}),
		},
		Sink: sink,
	})

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(runner.MetricsGatherer(), promhttp.HandlerOpts{}))
	metricsServer := &http.Server{
		Handler: mux,
	}
	metricsListener, err := net.Listen("tcp", *perfMetricsAddr)
	if err != nil {
		t.Fatalf("listen on perf metrics addr %s: %v", *perfMetricsAddr, err)
	}
	go func() {
		_ = metricsServer.Serve(metricsListener)
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("runner.Run: %v", err)
	}

	logPath := filepath.Join(outputDir, "runner.log")
	logBody := fmt.Sprintf("run_id=%s total_queries=%d total_errors=%d\n", summary.RunID, summary.TotalQueries, summary.TotalErrors)
	if err := os.WriteFile(logPath, []byte(logBody), 0o644); err != nil {
		t.Fatalf("write runner.log: %v", err)
	}

	for _, artifact := range []string{"summary.json", "query_results.csv", "server_metrics.prom", "runner.log"} {
		if _, err := os.Stat(filepath.Join(outputDir, artifact)); err != nil {
			t.Fatalf("expected artifact %s: %v", artifact, err)
		}
	}
}
