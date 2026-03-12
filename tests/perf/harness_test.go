package perf

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/posthog/duckgres/server"
	"github.com/posthog/duckgres/tests/perf/core"
	"github.com/posthog/duckgres/tests/perf/datasets"
	flightdriver "github.com/posthog/duckgres/tests/perf/drivers/flight"
	pgdriver "github.com/posthog/duckgres/tests/perf/drivers/pgwire"
	"github.com/posthog/duckgres/tests/perf/publisher"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	perfRun                   = flag.Bool("perf-run", false, "run the perf harness against the configured catalog")
	perfCatalog               = flag.String("perf-catalog", "queries/smoke.yaml", "path to perf catalog yaml")
	perfOutputBase            = flag.String("perf-output-base", filepath.Clean("../../artifacts/perf"), "base output directory for perf artifacts")
	perfRunID                 = flag.String("perf-run-id", "", "override run id; default is timestamp")
	perfMetricsAddr           = flag.String("perf-metrics-addr", ":9095", "runner metrics listen address")
	perfPGWireDSN             = flag.String("perf-pgwire-dsn", "", "optional pre-existing pgwire DSN; if empty harness auto-starts duckgres")
	perfFlightAddr            = flag.String("perf-flight-addr", "", "optional pre-existing flight address host:port; if empty harness auto-starts duckgres")
	perfUsername              = flag.String("perf-username", "perfuser", "username for harness-created duckgres")
	perfPassword              = flag.String("perf-password", "perfpass", "password for harness-created duckgres")
	perfFlightInsecureSkipTLS = flag.Bool("perf-flight-insecure-skip-verify", true, "skip TLS cert verification for Flight client")
	perfDatasetManifestTable  = flag.String("perf-dataset-manifest-table", envOrDefault("DUCKGRES_PERF_DATASET_MANIFEST_TABLE", defaultDatasetManifestTable), "manifest table associated with frozen dataset runs")
	perfPublishDSN            = flag.String("perf-publish-dsn", os.Getenv("DUCKGRES_PERF_PUBLISH_DSN"), "optional Duckgres DSN for publishing perf artifacts")
	perfPublishPassword       = flag.String("perf-publish-password", os.Getenv("DUCKGRES_PERF_PUBLISH_PASSWORD"), "optional password override for the perf artifact publisher")
	perfPublishSchema         = flag.String("perf-publish-schema", envOrDefault("DUCKGRES_PERF_PUBLISH_SCHEMA", "duckgres_perf"), "target schema for published perf artifacts")
	perfPublishBootstrap      = flag.Bool("perf-publish-bootstrap-schema", envBoolOrDefault("DUCKGRES_PERF_PUBLISH_BOOTSTRAP_SCHEMA", true), "bootstrap publisher schema/table definitions before inserting rows")
)

var publishRunDir = publisher.PublishRunDir

func TestGoldenQueryPerformanceHarness(t *testing.T) {
	if !*perfRun {
		t.Skip("set -perf-run to execute the perf harness")
	}

	runID := *perfRunID
	if runID == "" {
		runID = time.Now().UTC().Format("20060102T150405Z")
	}
	nightlyMode := strings.HasPrefix(runID, "nightly-")

	catalog, err := core.LoadCatalog(*perfCatalog)
	if err != nil {
		t.Fatalf("LoadCatalog(%s): %v", *perfCatalog, err)
	}

	runtimeContract, err := datasets.ResolveRuntimeContract(defaultManifestPath(*perfCatalog))
	if err != nil {
		t.Fatalf("resolve dataset runtime contract: %v", err)
	}
	if nightlyMode && !runtimeContract.Frozen {
		t.Fatalf("nightly run requires %s", datasets.DatasetVersionEnv)
	}
	if nightlyMode || runtimeContract.Frozen {
		if err := core.ValidateReadOnlyCatalog(catalog); err != nil {
			t.Fatalf("read-only catalog validation failed: %v", err)
		}
	}

	outputDir := filepath.Join(*perfOutputBase, runID)
	sink, err := core.NewArtifactSink(outputDir)
	if err != nil {
		t.Fatalf("NewArtifactSink(%s): %v", outputDir, err)
	}

	pgwireDSN := *perfPGWireDSN
	flightAddr := *perfFlightAddr
	var cpLogs bytes.Buffer
	if pgwireDSN == "" || flightAddr == "" {
		h, err := startLocalDuckgres(t, *perfUsername, *perfPassword, &cpLogs)
		if err != nil {
			t.Fatalf("start local duckgres: %v", err)
		}
		t.Cleanup(func() { h.cleanup() })
		pgwireDSN = h.pgwireDSN
		flightAddr = h.flightAddr
	}
	pgwireDSN, err = pgwireDSNForHarness(pgwireDSN, *perfPassword)
	if err != nil {
		t.Fatalf("resolve pgwire dsn: %v", err)
	}

	pg, err := pgdriver.NewFromDSN(pgwireDSN)
	if err != nil {
		t.Fatalf("NewFromDSN(%s): %v", pgwireDSN, err)
	}
	t.Cleanup(func() { _ = pg.Close() })

	flight, err := flightdriver.NewFromAddress(flightAddr, *perfUsername, *perfPassword, *perfFlightInsecureSkipTLS)
	if err != nil {
		t.Fatalf("NewFromAddress(%s): %v", flightAddr, err)
	}
	t.Cleanup(func() { _ = flight.Close() })

	runner := core.NewQueryRunner(core.RunnerConfig{
		RunID:          runID,
		Catalog:        catalog,
		DatasetVersion: runtimeContract.DatasetVersion,
		Drivers: map[core.Protocol]core.ProtocolDriver{
			core.ProtocolPGWire: pg,
			core.ProtocolFlight: flight,
		},
		Sink: sink,
		OnSetup: func(ctx context.Context) error {
			if runtimeContract.Frozen {
				return nil
			}
			records, err := datasets.GenerateDeterministic(catalog.Seed, catalog.DatasetScale)
			if err != nil {
				return fmt.Errorf("generate dataset: %w", err)
			}
			if err := seedDatasetViaDriver(ctx, pg, records); err != nil {
				return fmt.Errorf("seed pgwire driver: %w", err)
			}
			if err := seedDatasetViaDriver(ctx, flight, records); err != nil {
				return fmt.Errorf("seed flight driver: %w", err)
			}
			return nil
		},
	})

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(runner.MetricsGatherer(), promhttp.HandlerOpts{}))
	metricsServer := &http.Server{Handler: mux}
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
	logBody := fmt.Sprintf(
		"run_id=%s total_queries=%d total_errors=%d\ncontrol_plane_logs:\n%s",
		summary.RunID,
		summary.TotalQueries,
		summary.TotalErrors,
		cpLogs.String(),
	)
	if err := os.WriteFile(logPath, []byte(logBody), 0o644); err != nil {
		t.Fatalf("write runner.log: %v", err)
	}

	for _, artifact := range []string{"summary.json", "query_results.csv", "server_metrics.prom", "runner.log"} {
		if _, err := os.Stat(filepath.Join(outputDir, artifact)); err != nil {
			t.Fatalf("expected artifact %s: %v", artifact, err)
		}
	}

	publishCfg := currentPublisherConfig()
	if err := finalizeRunArtifacts(
		context.Background(),
		publishCfg,
		runtimeContract,
		outputDir,
		*perfCatalog,
		*perfDatasetManifestTable,
		time.Now,
	); err != nil {
		t.Fatal(err)
	}
}

func defaultManifestPath(catalogPath string) string {
	catalogDir := filepath.Dir(filepath.Clean(catalogPath))
	return filepath.Join(filepath.Dir(catalogDir), "datasets", "manifest.yaml")
}

func currentPublisherConfig() publisher.Config {
	return publisher.Config{
		DSN:             *perfPublishDSN,
		Password:        *perfPublishPassword,
		Schema:          *perfPublishSchema,
		BootstrapSchema: *perfPublishBootstrap,
	}
}

func publishArtifactsIfConfigured(ctx context.Context, cfg publisher.Config, runDir string) error {
	if !cfg.Enabled() {
		return nil
	}
	if err := publishRunDir(ctx, cfg, runDir); err != nil {
		return fmt.Errorf("publish perf artifacts: %w", err)
	}
	return nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBoolOrDefault(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func seedDatasetViaDriver(ctx context.Context, driver core.ProtocolDriver, records []datasets.Record) error {
	createSQL := `
CREATE TABLE IF NOT EXISTS perf_orders(
  customer_id INTEGER,
  region TEXT,
  amount_cents INTEGER
)`
	if _, err := driver.Execute(
		ctx,
		core.Query{
			QueryID:    "setup_create_perf_orders",
			IntentID:   "setup_create_perf_orders",
			PGWireSQL:  createSQL,
			DuckhogSQL: createSQL,
		},
		nil,
	); err != nil {
		return fmt.Errorf("create perf_orders: %w", err)
	}

	if _, err := driver.Execute(
		ctx,
		core.Query{
			QueryID:    "setup_clear_perf_orders",
			IntentID:   "setup_clear_perf_orders",
			PGWireSQL:  "DELETE FROM perf_orders",
			DuckhogSQL: "DELETE FROM perf_orders",
		},
		nil,
	); err != nil {
		return fmt.Errorf("truncate perf_orders: %w", err)
	}

	for _, record := range records {
		insertSQL := fmt.Sprintf(
			"INSERT INTO perf_orders(customer_id, region, amount_cents) VALUES(%d, '%s', %d)",
			record.CustomerID,
			record.Region,
			record.AmountCents,
		)
		if _, err := driver.Execute(
			ctx,
			core.Query{
				QueryID:    "setup_insert_perf_orders",
				IntentID:   "setup_insert_perf_orders",
				PGWireSQL:  insertSQL,
				DuckhogSQL: insertSQL,
			},
			nil,
		); err != nil {
			return fmt.Errorf("insert seed row: %w", err)
		}
	}

	return nil
}

type localDuckgres struct {
	cmd        *exec.Cmd
	pgwireDSN  string
	flightAddr string
	socketDir  string
}

func startLocalDuckgres(t *testing.T, username, password string, logOutput *bytes.Buffer) (*localDuckgres, error) {
	t.Helper()
	root, err := findProjectRoot()
	if err != nil {
		return nil, err
	}
	tmpDir := t.TempDir()
	binaryPath := filepath.Join(tmpDir, "duckgres")

	build := exec.Command("go", "build", "-o", binaryPath, ".")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("build duckgres: %w\n%s", err, string(out))
	}

	certPath := filepath.Join(tmpDir, "server.crt")
	keyPath := filepath.Join(tmpDir, "server.key")
	if err := server.EnsureCertificates(certPath, keyPath); err != nil {
		return nil, fmt.Errorf("generate tls certs: %w", err)
	}

	port, err := freePort()
	if err != nil {
		return nil, fmt.Errorf("allocate pgwire port: %w", err)
	}
	flightPort, err := freePort()
	if err != nil {
		return nil, fmt.Errorf("allocate flight port: %w", err)
	}

	dataDir := filepath.Join(tmpDir, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	socketDir, err := os.MkdirTemp("/tmp", "perf-cp-")
	if err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}

	cfgPath := filepath.Join(tmpDir, "duckgres.yaml")
	cfg := fmt.Sprintf(`host: 127.0.0.1
port: %d
flight_port: %d
data_dir: %s
tls:
  cert: %s
  key: %s
users:
  %s: %s
`, port, flightPort, dataDir, certPath, keyPath, username, password)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		return nil, fmt.Errorf("write config: %w", err)
	}

	cmd := exec.Command(binaryPath, "--config", cfgPath, "--mode", "control-plane", "--socket-dir", socketDir)
	cmd.Stdout = logOutput
	cmd.Stderr = logOutput
	cmd.Env = []string{
		"HOME=" + os.Getenv("HOME"),
		"PATH=" + os.Getenv("PATH"),
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start duckgres: %w", err)
	}

	dsn := fmt.Sprintf(
		"host=127.0.0.1 port=%d user=%s password=%s sslmode=require connect_timeout=5",
		port,
		username,
		password,
	)
	if err := waitForPGWireReady(dsn, 30*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, fmt.Errorf("duckgres did not become ready: %w\nlogs:\n%s", err, logOutput.String())
	}

	return &localDuckgres{
		cmd:        cmd,
		pgwireDSN:  dsn,
		flightAddr: fmt.Sprintf("127.0.0.1:%d", flightPort),
		socketDir:  socketDir,
	}, nil
}

func (h *localDuckgres) cleanup() {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return
	}
	_ = h.cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() {
		_, _ = h.cmd.Process.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = h.cmd.Process.Kill()
		<-done
	}
	if h.socketDir != "" {
		_ = os.RemoveAll(h.socketDir)
	}
}

func waitForPGWireReady(dsn string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		db, err := sql.Open("postgres", dsn)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			pingErr := db.PingContext(ctx)
			cancel()
			_ = db.Close()
			if pingErr == nil {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout after %s", timeout)
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = ln.Close()
	}()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func findProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find project root from %s", strings.TrimSpace(dir))
		}
		dir = parent
	}
}
