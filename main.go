package main // duckgres entry point

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/posthog/duckgres/configloader"
	"github.com/posthog/duckgres/configresolve"
	"github.com/posthog/duckgres/controlplane"
	"github.com/posthog/duckgres/duckdbservice"
	"github.com/posthog/duckgres/internal/cliboot"
	"github.com/posthog/duckgres/server"
)

// FileConfig is the YAML configuration schema, sourced from the
// configloader package so the new cmd/duckgres-controlplane and
// cmd/duckgres-worker binaries parse the same shape.
type FileConfig = configloader.FileConfig

// Type aliases for the nested configloader types so main.go's flag-binding
// code continues to refer to FileConfig / ProcessFileConfig / etc. without
// the configloader. prefix on every line. The actual resolver now lives in
// the configresolve package and takes *configloader.FileConfig directly.
type (
	ProcessFileConfig   = configloader.ProcessFileConfig
	K8sFileConfig       = configloader.K8sFileConfig
	QueryLogFileConfig  = configloader.QueryLogFileConfig
	TLSConfig           = configloader.TLSConfig
	ACMEConfig          = configloader.ACMEConfig
	RateLimitFileConfig = configloader.RateLimitFileConfig
	DuckLakeFileConfig  = configloader.DuckLakeFileConfig
)

// loadConfigFile + env are thin wrappers around configloader for back-compat
// with the rest of this file. New binaries should call configloader.LoadFile
// and configloader.Env directly.
var (
	loadConfigFile = configloader.LoadFile
	env            = configloader.Env
)

func main() {
	// Ignore SIGPIPE to prevent DuckDB's C++ code (and libraries like libpq
	// inside DuckLake) from crashing the process when a network connection
	// drops mid-query. Go already converts EPIPE to errors on Write; the
	// default SIGPIPE handler is a legacy Unix footgun that kills the process.
	signal.Ignore(syscall.SIGPIPE)

	// Set version on server package so catalog macros can expose it
	server.SetProcessVersion(version)

	// Check if we're running as a child worker process
	if os.Getenv("DUCKGRES_CHILD_MODE") == "1" {
		// Use the same logging/tracing setup as parent for consistent format
		loggingShutdown := cliboot.InitLogging()
		defer loggingShutdown()
		tracingShutdown := cliboot.InitTracing()
		defer tracingShutdown()
		duckdbservice.LogCacheProxyStatus()
		server.RunChildMode()
		return // RunChildMode calls os.Exit
	}

	// CLIInputs-backed flags (host/port/data-dir/k8s-*/etc) live in
	// configresolve so cmd/duckgres-controlplane can register the same set
	// without two copies of the table drifting. The harvest closure
	// produces a populated CLIInputs (with cli.Set) post-Parse.
	harvestCLIInputs := configresolve.RegisterCLIInputsFlags(flag.CommandLine)

	// Bespoke flags: not in CLIInputs because they don't flow through
	// ResolveEffective. --mode/--repl/--psql/--socket-dir/--duckdb-* are
	// consumed directly below; --config/--log-level/--version/--help are
	// pre-resolver runtime knobs.
	configFile := flag.String("config", env("DUCKGRES_CONFIG", ""), "Path to YAML config file (env: DUCKGRES_CONFIG)")
	logLevel := flag.String("log-level", "", "Log level: debug, info, warn, error (env: DUCKGRES_LOG_LEVEL)")
	repl := flag.Bool("repl", false, "Start an interactive SQL shell instead of the server")
	psql := flag.Bool("psql", false, "Launch psql connected to the local Duckgres server")
	showVersion := flag.Bool("version", false, "Show version and exit")
	showHelp := flag.Bool("help", false, "Show help message")
	mode := flag.String("mode", "standalone", "Run mode: standalone, control-plane, or duckdb-service")
	socketDir := flag.String("socket-dir", "/var/run/duckgres", "Unix socket directory (control-plane mode)")
	duckdbListen := flag.String("duckdb-listen", "", "DuckDB service listen address (duckdb-service mode, env: DUCKGRES_DUCKDB_LISTEN)")
	duckdbListenFD := flag.Int("duckdb-listen-fd", 0, "Inherit a pre-bound listener FD instead of creating a new socket (duckdb-service mode, set by control plane)")
	duckdbToken := flag.String("duckdb-token", "", "Bearer token for DuckDB service auth (duckdb-service mode, env: DUCKGRES_DUCKDB_TOKEN)")
	duckdbMaxSessions := flag.Int("duckdb-max-sessions", 0, "Max concurrent sessions, 0=unlimited (duckdb-service mode, env: DUCKGRES_DUCKDB_MAX_SESSIONS)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Duckgres %s - PostgreSQL wire protocol server for DuckDB\n\n", version)
		fmt.Fprintf(os.Stderr, "Usage: duckgres [options]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nEnvironment variables:\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_CONFIG             Path to YAML config file\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_HOST               Host to bind to (default: 0.0.0.0)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_PORT               Port to listen on (default: 5432)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_FLIGHT_PORT        Control-plane Arrow Flight SQL ingress port (default: disabled)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_FLIGHT_SESSION_IDLE_TTL      Flight auth session idle TTL (default: 10m)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_FLIGHT_SESSION_REAP_INTERVAL Flight auth session reap interval (default: 1m)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_FLIGHT_HANDLE_IDLE_TTL       Flight prepared/query handle idle TTL (default: 15m)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_FLIGHT_SESSION_TOKEN_TTL     Flight issued session token absolute TTL (default: 1h)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_DATA_DIR           Directory for DuckDB files (default: ./data)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_CERT               TLS certificate file (default: ./certs/server.crt)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_KEY                TLS private key file (default: ./certs/server.key)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_FILE_PERSISTENCE   Persist DuckDB to <data_dir>/<username>.duckdb (1 or true)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_PROCESS_ISOLATION  Enable process isolation (1 or true)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_IDLE_TIMEOUT       Connection idle timeout (e.g., 30m, 1h, -1 to disable)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_SESSION_INIT_TIMEOUT  Session startup metadata/probe timeout (default: 10s)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_MEMORY_LIMIT       DuckDB memory_limit per session (e.g., 4GB)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_THREADS            DuckDB threads per session\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_MEMORY_BUDGET      Total memory for all DuckDB sessions (e.g., 24GB)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_MEMORY_REBALANCE   Enable dynamic per-connection memory reallocation (1 or true)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_PROCESS_MIN_WORKERS  Pre-warm worker count for process workers (control-plane mode)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_PROCESS_MAX_WORKERS  Max process workers (control-plane mode)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_PROCESS_RETIRE_ON_SESSION_END  Retire process workers immediately after their last session ends (control-plane mode)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_WORKER_QUEUE_TIMEOUT  Worker queue timeout (default: 5m)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_HANDOVER_DRAIN_TIMEOUT  Planned shutdown/upgrade drain timeout (default: 24h in process mode, 15m in remote mode)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_ACME_DOMAIN        Domain for ACME/Let's Encrypt certificate\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_ACME_EMAIL         Contact email for Let's Encrypt notifications\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_ACME_CACHE_DIR     Directory for ACME certificate cache\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_ACME_DNS_PROVIDER  DNS provider for DNS-01 challenges (e.g. 'route53')\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_ACME_DNS_ZONE_ID   Route53 hosted zone ID for DNS-01 challenges\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_MAX_CONNECTIONS    Max concurrent connections (default: CPUs * 2)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_DUCKDB_LISTEN      DuckDB service listen address (duckdb-service mode)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_DUCKDB_TOKEN       DuckDB service bearer token (duckdb-service mode)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_DUCKDB_MAX_SESSIONS  DuckDB service max sessions (duckdb-service mode)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_CONFIG_STORE       PostgreSQL connection string for config store (multi-tenant)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_CONFIG_POLL_INTERVAL  Config store poll interval (default: 30s)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_INTERNAL_SECRET    Shared secret for API authentication\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_K8S_MAX_WORKERS    Max K8s workers in the shared pool\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_K8S_SHARED_WARM_TARGET  Neutral shared warm-worker target for K8s multi-tenant mode\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_AWS_REGION         AWS region for STS client\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_LOG_LEVEL          Log level: debug, info, warn, error (default: info)\n")
		fmt.Fprintf(os.Stderr, "\nPrecedence: CLI flags > environment variables > config file > defaults\n")
	}

	// Handle -v shorthand before flag.Parse (Go's flag package doesn't support short aliases)
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

	// Auto-detect duckgres.yaml if no config file was explicitly specified
	if *configFile == "" {
		if _, err := os.Stat("duckgres.yaml"); err == nil {
			*configFile = "duckgres.yaml"
		}
	}

	// Load config file early so log_level from YAML can feed into cliboot.InitLogging().
	var fileCfg *FileConfig
	if *configFile != "" {
		loadedCfg, err := loadConfigFile(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load config file: %s\n", err)
			os.Exit(1)
		}
		fileCfg = loadedCfg
	}

	// Resolve log level: CLI flag > env var > YAML config > default (info).
	// Set the env var so workers inherit it and cliboot's parseLogLevel picks it up.
	if *logLevel != "" {
		_ = os.Setenv("DUCKGRES_LOG_LEVEL", *logLevel)
	} else if os.Getenv("DUCKGRES_LOG_LEVEL") == "" && fileCfg != nil && fileCfg.LogLevel != "" {
		_ = os.Setenv("DUCKGRES_LOG_LEVEL", fileCfg.LogLevel)
	}

	loggingShutdown := cliboot.InitLogging()
	defer loggingShutdown()

	tracingShutdown := cliboot.InitTracing()
	defer tracingShutdown()

	buildInfo().Log(*mode)
	duckdbservice.LogCacheProxyStatus()

	if fileCfg != nil {
		slog.Info("Loaded configuration from " + *configFile)
	}

	fatal := func(msg string) {
		slog.Error(msg)
		loggingShutdown()
		os.Exit(1)
	}

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	resolved := configresolve.ResolveEffective(fileCfg, harvestCLIInputs(), os.Getenv, func(msg string) {
		slog.Warn(msg)
	})
	cfg := resolved.Server

	// Handle --psql: launch psql connected to the local Duckgres server
	if *psql {
		// Pick a non-passthrough user so psql gets full pg_catalog compatibility.
		// Falls back to any user if all are passthrough.
		var user, password string
		for u, p := range cfg.Users {
			user, password = u, p
			if !cfg.PassthroughUsers[u] {
				break
			}
		}

		connectHost := "127.0.0.1"
		psqlArgs := []string{
			"psql",
			fmt.Sprintf("host=%s port=%d user=%s sslmode=require", connectHost, cfg.Port, user),
		}

		psqlPath, err := exec.LookPath("psql")
		if err != nil {
			fatal("psql not found in PATH")
		}

		env := append(os.Environ(), "PGPASSWORD="+password)
		if err := syscall.Exec(psqlPath, psqlArgs, env); err != nil {
			fatal("Failed to exec psql: " + err.Error())
		}
		return
	}

	// Process isolation is incompatible with control-plane/worker mode — those modes
	// already provide process-level isolation via the worker pool. Disable it and warn.
	if *mode != "standalone" && cfg.ProcessIsolation {
		cfg.ProcessIsolation = false
		slog.Info("Process isolation disabled (not applicable in " + *mode + " mode)")
	}

	// Handle duckdb-service mode
	if *mode == "duckdb-service" {
		listenAddr := *duckdbListen
		if listenAddr == "" {
			listenAddr = env("DUCKGRES_DUCKDB_LISTEN", "")
		}
		if *duckdbListenFD == 0 && listenAddr == "" {
			fatal("duckdb-service mode requires --duckdb-listen flag or DUCKGRES_DUCKDB_LISTEN env var")
		}

		token := *duckdbToken
		if token == "" {
			token = env("DUCKGRES_DUCKDB_TOKEN", "")
		}

		maxSessions := *duckdbMaxSessions
		if maxSessions == 0 {
			if v := env("DUCKGRES_DUCKDB_MAX_SESSIONS", ""); v != "" {
				if n, err := strconv.Atoi(v); err != nil {
					slog.Warn("Invalid DUCKGRES_DUCKDB_MAX_SESSIONS", "value", v)
				} else {
					maxSessions = n
				}
			}
		}

		if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
			fatal("Failed to create data directory: " + err.Error())
		}

		// No initMetrics() here — in control-plane mode, workers are spawned
		// with --mode duckdb-service and would all fight over :9090. The
		// control plane process owns the metrics endpoint.

		duckdbservice.Run(duckdbservice.ServiceConfig{
			ListenAddr:   listenAddr,
			ListenFD:     *duckdbListenFD,
			ServerConfig: cfg,
			BearerToken:  token,
			MaxSessions:  maxSessions,
		})
		return
	}

	// Handle REPL mode (interactive SQL shell, no TLS/metrics/server needed)
	if *repl {
		if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
			fatal("Failed to create data directory: " + err.Error())
		}
		server.RunShell(cfg)
		return
	}

	metricsSrv := cliboot.InitMetrics()

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		fatal("Failed to create data directory: " + err.Error())
	}

	// Auto-generate self-signed certificates if they don't exist (skip when ACME is configured)
	if cfg.ACMEDomain == "" {
		if err := server.EnsureCertificates(cfg.TLSCertFile, cfg.TLSKeyFile); err != nil {
			fatal("Failed to ensure TLS certificates: " + err.Error())
		}
		slog.Info("Using TLS certificates", "cert_file", cfg.TLSCertFile, "key_file", cfg.TLSKeyFile)
	} else if cfg.ACMEDNSProvider != "" {
		slog.Info("ACME DNS-01 mode enabled", "domain", cfg.ACMEDomain, "provider", cfg.ACMEDNSProvider)
	} else {
		slog.Info("ACME/Let's Encrypt mode enabled", "domain", cfg.ACMEDomain)
	}

	// Handle control-plane mode
	if *mode == "control-plane" {
		cpCfg := controlplane.ControlPlaneConfig{
			Config: cfg,
			Process: controlplane.ProcessConfig{
				MinWorkers: resolved.ProcessMinWorkers,
				MaxWorkers: resolved.ProcessMaxWorkers,
			},
			SocketDir:                  *socketDir,
			ConfigPath:                 *configFile,
			WorkerQueueTimeout:         resolved.WorkerQueueTimeout,
			WorkerIdleTimeout:          resolved.WorkerIdleTimeout,
			RetireOnSessionEnd:         resolved.ProcessRetireOnSessionEnd,
			HandoverDrainTimeout:       resolved.HandoverDrainTimeout,
			MetricsServer:              metricsSrv,
			WorkerBackend:              resolved.WorkerBackend,
			ConfigStoreConn:            resolved.ConfigStoreConn,
			ConfigPollInterval:         resolved.ConfigPollInterval,
			InternalSecret:             resolved.InternalSecret,
			SNIRoutingMode:             resolved.SNIRoutingMode,
			ManagedHostnameSuffixes:    resolved.ManagedHostnameSuffixes,
			DuckLakeDefaultSpecVersion: resolved.DuckLakeDefaultSpecVersion,
			K8s: controlplane.K8sConfig{
				WorkerImage:           resolved.K8sWorkerImage,
				WorkerNamespace:       resolved.K8sWorkerNamespace,
				ControlPlaneID:        resolved.K8sControlPlaneID,
				WorkerPort:            resolved.K8sWorkerPort,
				WorkerSecret:          resolved.K8sWorkerSecret,
				WorkerConfigMap:       resolved.K8sWorkerConfigMap,
				ImagePullPolicy:       resolved.K8sWorkerImagePullPolicy,
				ServiceAccount:        resolved.K8sWorkerServiceAccount,
				MaxWorkers:            resolved.K8sMaxWorkers,
				SharedWarmTarget:      resolved.K8sSharedWarmTarget,
				WorkerCPURequest:      resolved.K8sWorkerCPURequest,
				WorkerMemoryRequest:   resolved.K8sWorkerMemoryRequest,
				WorkerNodeSelector:    resolved.K8sWorkerNodeSelector,
				WorkerTolerationKey:   resolved.K8sWorkerTolerationKey,
				WorkerTolerationValue: resolved.K8sWorkerTolerationValue,
				WorkerExclusiveNode:   resolved.K8sWorkerExclusiveNode,
				AWSRegion:             resolved.AWSRegion,
			},
		}
		controlplane.RunControlPlane(cpCfg)
		return
	}

	// Standalone mode (default)
	srv, err := server.New(cfg)
	if err != nil {
		fatal("Failed to create server: " + err.Error())
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		slog.Info("Shutting down...")
		if metricsSrv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = metricsSrv.Shutdown(ctx)
			cancel()
		}
		_ = srv.Close()
		loggingShutdown()
		os.Exit(0)
	}()

	slog.Info("Starting Duckgres server (TLS required)", "version", version, "host", cfg.Host, "port", cfg.Port)
	if err := srv.ListenAndServe(); err != nil {
		fatal("Server error: " + err.Error())
	}
}
