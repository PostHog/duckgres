// duckgres-worker is the worker-only entry point that links the embedded
// DuckDB driver. It is the binary intended to ship in worker pods and the
// target of the per-DuckDB-version matrix build in CI.
//
// At runtime, this binary only supports `--mode duckdb-service` (the
// shape spawned by the control plane over Unix sockets / TCP). The
// standalone PG-wire path is not exposed here — for that, use the
// all-in-one duckgres binary in `--mode standalone`.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/posthog/duckgres/configloader"
	"github.com/posthog/duckgres/configresolve"
	"github.com/posthog/duckgres/duckdbservice"
	"github.com/posthog/duckgres/internal/cliboot"
	"github.com/posthog/duckgres/server"
)

// Each duckgres binary owns its own package-main version/commit/date because
// -ldflags -X main.* can only target package-main symbols. The actual
// build-info logic lives in internal/cliboot.
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func buildInfo() cliboot.BuildInfo {
	bi := cliboot.BuildInfo{Version: version, Commit: commit, Date: date}
	bi.Enrich()
	return bi
}

func main() {
	// Ignore SIGPIPE so DuckDB's C++ code (and libraries like libpq used by
	// DuckLake) don't crash the process when a network connection drops
	// mid-query. Same rationale as the all-in-one binary.
	signal.Ignore(syscall.SIGPIPE)

	// Worker-relevant flag subset. The all-in-one duckgres binary registers
	// ~60 flags covering standalone / control-plane / duckdb-service. The
	// worker only needs the bits that feed server.Config (DuckDB execution,
	// DuckLake, query log) plus the duckdb-service transport flags. The
	// rest (TLS/ACME, PG wire, control-plane pool, K8s, config-store,
	// process-isolation, REPL/psql) intentionally do not exist here.
	configFile := flag.String("config", configloader.Env("DUCKGRES_CONFIG", ""), "Path to YAML config file (env: DUCKGRES_CONFIG)")
	logLevel := flag.String("log-level", "", "Log level: debug, info, warn, error (env: DUCKGRES_LOG_LEVEL)")
	showVersion := flag.Bool("version", false, "Show version and exit")
	showHelp := flag.Bool("help", false, "Show help message")

	// DuckDB execution
	dataDir := flag.String("data-dir", "", "Directory for DuckDB files (env: DUCKGRES_DATA_DIR)")
	memoryLimit := flag.String("memory-limit", "", "DuckDB memory_limit per session (e.g., '4GB') (env: DUCKGRES_MEMORY_LIMIT)")
	threads := flag.Int("threads", 0, "DuckDB threads per session (env: DUCKGRES_THREADS)")
	memoryBudget := flag.String("memory-budget", "", "Total memory budget for all sessions on this worker (e.g., '24GB') (env: DUCKGRES_MEMORY_BUDGET)")
	filePersistence := flag.Bool("file-persistence", false, "Persist DuckDB to <data-dir>/<username>.duckdb instead of in-memory (env: DUCKGRES_FILE_PERSISTENCE)")
	idleTimeout := flag.String("idle-timeout", "", "Connection idle timeout (e.g., '30m', '1h', '-1' to disable) (env: DUCKGRES_IDLE_TIMEOUT)")
	sessionInitTimeout := flag.String("session-init-timeout", "", "Session startup metadata/probe timeout (e.g., '10s', '30s') (env: DUCKGRES_SESSION_INIT_TIMEOUT)")

	// DuckLake (workers attach DuckLake)
	duckLakeDeltaCatalogEnabled := flag.Bool("ducklake-delta-catalog-enabled", false, "Attach a Delta Lake catalog during DuckLake worker boot (env: DUCKGRES_DUCKLAKE_DELTA_CATALOG_ENABLED)")
	duckLakeDeltaCatalogPath := flag.String("ducklake-delta-catalog-path", "", "Delta Lake catalog/table path to attach (env: DUCKGRES_DUCKLAKE_DELTA_CATALOG_PATH)")
	duckLakeDefaultSpecVersion := flag.String("ducklake-default-spec-version", "", "Default DuckLake spec version for migration checks (env: DUCKGRES_DUCKLAKE_DEFAULT_SPEC_VERSION)")

	// Query log
	queryLog := flag.Bool("query-log", true, "Enable/disable DuckLake query log (use --query-log=false to disable; env: DUCKGRES_QUERY_LOG_ENABLED)")

	// DuckDB-service transport
	duckdbListen := flag.String("duckdb-listen", "", "Service listen address: 'unix:///path' or 'host:port' (env: DUCKGRES_DUCKDB_LISTEN)")
	duckdbListenFD := flag.Int("duckdb-listen-fd", 0, "Inherit a pre-bound listener FD instead of creating a new socket; set by control plane to avoid EROFS under ProtectSystem=strict")
	duckdbToken := flag.String("duckdb-token", "", "Bearer token for gRPC auth (env: DUCKGRES_DUCKDB_TOKEN)")
	duckdbMaxSessions := flag.Int("duckdb-max-sessions", 0, "Max concurrent sessions, 0=unlimited (env: DUCKGRES_DUCKDB_MAX_SESSIONS)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Duckgres worker %s — DuckDB Arrow Flight SQL service\n\n", version)
		fmt.Fprintln(os.Stderr, "DuckDB-service-only duckgres binary. Links libduckdb.")
		fmt.Fprintln(os.Stderr, "Serves Arrow Flight SQL on a local Unix socket or TCP port,")
		fmt.Fprintln(os.Stderr, "spawned by the control plane.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Each worker pod ships exactly one DuckDB version, pinned via")
		fmt.Fprintln(os.Stderr, "go.mod + the Dockerfile.worker DUCKDB_EXTENSION_VERSION build arg.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Options:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Precedence: CLI flags > environment variables > config file > defaults")
	}

	// -v shorthand before flag.Parse (Go's flag package has no short aliases).
	for _, arg := range os.Args[1:] {
		if arg == "-v" {
			fmt.Println(buildInfo().String())
			os.Exit(0)
		}
	}

	flag.Parse()

	if *showVersion {
		fmt.Println(buildInfo().String())
		os.Exit(0)
	}
	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	// Track explicitly-set flags so configresolve precedence (CLI > env > YAML > default) is consistent.
	cliSet := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		cliSet[f.Name] = true
	})

	// Auto-detect duckgres.yaml when no --config was given. Mirrors the
	// all-in-one binary so a worker pod can boot from a mounted ConfigMap
	// without needing the flag wired.
	if *configFile == "" {
		if _, err := os.Stat("duckgres.yaml"); err == nil {
			*configFile = "duckgres.yaml"
		}
	}

	var fileCfg *configloader.FileConfig
	if *configFile != "" {
		loaded, err := configloader.LoadFile(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load config file: %s\n", err)
			os.Exit(1)
		}
		fileCfg = loaded
	}

	// Log level: CLI flag > env > YAML > default. Set the env var so any
	// downstream goroutines that re-read it pick up the resolved level.
	if *logLevel != "" {
		_ = os.Setenv("DUCKGRES_LOG_LEVEL", *logLevel)
	} else if os.Getenv("DUCKGRES_LOG_LEVEL") == "" && fileCfg != nil && fileCfg.LogLevel != "" {
		_ = os.Setenv("DUCKGRES_LOG_LEVEL", fileCfg.LogLevel)
	}

	loggingShutdown := cliboot.InitLogging()
	defer loggingShutdown()
	tracingShutdown := cliboot.InitTracing()
	defer tracingShutdown()

	// Surface the running build identity for kubectl-logs triage. "worker"
	// here mirrors the mode label the control plane uses when spawning us.
	buildInfo().Log("worker")
	duckdbservice.LogCacheProxyStatus()

	server.SetProcessVersion(version)

	if fileCfg != nil {
		slog.Info("Loaded configuration", "path", *configFile)
	}

	// Sparse CLIInputs: only the worker-relevant fields are populated.
	// configresolve.ResolveEffective still reads the full env+YAML surface,
	// but CP-only fields land in Resolved slots the worker simply doesn't
	// read. Keeps a single source of truth for resolution rules.
	resolved := configresolve.ResolveEffective(fileCfg, configresolve.CLIInputs{
		Set:                         cliSet,
		DataDir:                     *dataDir,
		FilePersistence:             *filePersistence,
		IdleTimeout:                 *idleTimeout,
		SessionInitTimeout:          *sessionInitTimeout,
		MemoryLimit:                 *memoryLimit,
		Threads:                     *threads,
		MemoryBudget:                *memoryBudget,
		DuckLakeDeltaCatalogEnabled: *duckLakeDeltaCatalogEnabled,
		DuckLakeDeltaCatalogPath:    *duckLakeDeltaCatalogPath,
		DuckLakeDefaultSpecVersion:  *duckLakeDefaultSpecVersion,
		QueryLog:                    *queryLog,
	}, os.Getenv, func(msg string) {
		slog.Warn(msg)
	})
	cfg := resolved.Server

	// duckdb-service transport: CLI > env. ResolveEffective doesn't own
	// these because they're transport-shape, not server.Config; they live
	// in ServiceConfig directly.
	listenAddr := *duckdbListen
	if listenAddr == "" {
		listenAddr = configloader.Env("DUCKGRES_DUCKDB_LISTEN", "")
	}
	if *duckdbListenFD == 0 && listenAddr == "" {
		fmt.Fprintln(os.Stderr, "duckgres-worker requires --duckdb-listen, DUCKGRES_DUCKDB_LISTEN, or an inherited --duckdb-listen-fd from the control plane")
		os.Exit(2)
	}

	token := *duckdbToken
	if token == "" {
		token = configloader.Env("DUCKGRES_DUCKDB_TOKEN", "")
	}

	maxSessions := *duckdbMaxSessions
	if maxSessions == 0 {
		if v := configloader.Env("DUCKGRES_DUCKDB_MAX_SESSIONS", ""); v != "" {
			if n, err := strconv.Atoi(v); err != nil {
				slog.Warn("Invalid DUCKGRES_DUCKDB_MAX_SESSIONS", "value", v)
			} else {
				maxSessions = n
			}
		}
	}

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create data directory %q: %s\n", cfg.DataDir, err)
		os.Exit(1)
	}

	// No initMetrics() here. In control-plane mode all worker pods would
	// fight over :9090; the control plane process owns the metrics
	// endpoint. The duckdb-service exposes per-session metrics via its own
	// gRPC surface.
	duckdbservice.Run(duckdbservice.ServiceConfig{
		ListenAddr:   listenAddr,
		ListenFD:     *duckdbListenFD,
		ServerConfig: cfg,
		BearerToken:  token,
		MaxSessions:  maxSessions,
	})
}

