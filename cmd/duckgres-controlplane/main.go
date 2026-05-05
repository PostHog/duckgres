// duckgres-controlplane is a control-plane-only entry point that does NOT
// link the embedded DuckDB driver. It accepts the same configuration as the
// all-in-one duckgres binary running in `--mode control-plane`, but routes
// queries exclusively to remote duckdb-service workers over Arrow Flight SQL.
//
// The build is verified to be duckdb-go-free via:
//
//	go list -deps ./cmd/duckgres-controlplane | grep duckdb-go   # empty
//
// At runtime, this binary only supports control-plane mode; standalone /
// duckdb-service modes belong in the all-in-one duckgres binary or
// cmd/duckgres-worker respectively.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/posthog/duckgres/configloader"
	"github.com/posthog/duckgres/configresolve"
	"github.com/posthog/duckgres/controlplane"
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
	// Ignore SIGPIPE so libpq inside DuckLake metadata access (and any
	// other cgo libraries the CP transitively reaches) don't crash the
	// process on dropped network connections. Same rationale as the
	// all-in-one binary.
	signal.Ignore(syscall.SIGPIPE)

	// CLIInputs-backed flags are registered via the shared helper so this
	// binary and the all-in-one duckgres binary cannot drift on the
	// resolver's CLI surface. The bespoke flags below (--config,
	// --log-level, --mode, --socket-dir, --version, --help) don't flow
	// through ResolveEffective.
	harvestCLIInputs := configresolve.RegisterCLIInputsFlags(flag.CommandLine)

	configFile := flag.String("config", configloader.Env("DUCKGRES_CONFIG", ""), "Path to YAML config file (env: DUCKGRES_CONFIG)")
	logLevel := flag.String("log-level", "", "Log level: debug, info, warn, error (env: DUCKGRES_LOG_LEVEL)")
	showVersion := flag.Bool("version", false, "Show version and exit")
	showHelp := flag.Bool("help", false, "Show help message")
	socketDir := flag.String("socket-dir", "/var/run/duckgres", "Unix socket directory for process worker backend")
	// --mode is accepted but must be "control-plane". Symmetry with
	// cmd/duckgres-worker, which accepts --mode duckdb-service for
	// compatibility with hardcoded pod-spec args. This binary is
	// control-plane by definition; any other value is loud misuse.
	mode := flag.String("mode", "control-plane", "Run mode (must be 'control-plane'; accepted for symmetry with the all-in-one binary's CLI shape)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Duckgres control plane %s — PostgreSQL wire protocol + Flight ingress\n\n", version)
		fmt.Fprintln(os.Stderr, "Control-plane-only duckgres binary. Does NOT link libduckdb.")
		fmt.Fprintln(os.Stderr, "Routes all SQL execution to remote duckdb-service worker pods")
		fmt.Fprintln(os.Stderr, "via Arrow Flight SQL.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Configuration mirrors the all-in-one duckgres binary running in")
		fmt.Fprintln(os.Stderr, "`--mode control-plane`. The CLI flags backed by CLIInputs share")
		fmt.Fprintln(os.Stderr, "their definitions with the all-in-one binary via")
		fmt.Fprintln(os.Stderr, "configresolve.RegisterCLIInputsFlags so the two cannot drift.")
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
	if *mode != "control-plane" {
		fmt.Fprintf(os.Stderr, "duckgres-controlplane only supports --mode control-plane (got %q). Use the all-in-one duckgres binary for standalone or duckdb-service modes, or cmd/duckgres-worker for worker pods.\n", *mode)
		os.Exit(2)
	}

	// Auto-detect duckgres.yaml when no --config was given.
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

	// Log level: CLI flag > env > YAML > default.
	if *logLevel != "" {
		_ = os.Setenv("DUCKGRES_LOG_LEVEL", *logLevel)
	} else if os.Getenv("DUCKGRES_LOG_LEVEL") == "" && fileCfg != nil && fileCfg.LogLevel != "" {
		_ = os.Setenv("DUCKGRES_LOG_LEVEL", fileCfg.LogLevel)
	}

	loggingShutdown := cliboot.InitLogging()
	defer loggingShutdown()
	tracingShutdown := cliboot.InitTracing()
	defer tracingShutdown()

	buildInfo().Log("control-plane")
	server.SetProcessVersion(version)

	if fileCfg != nil {
		slog.Info("Loaded configuration", "path", *configFile)
	}

	fatal := func(msg string) {
		slog.Error(msg)
		loggingShutdown()
		os.Exit(1)
	}

	resolved := configresolve.ResolveEffective(fileCfg, harvestCLIInputs(), os.Getenv, func(msg string) {
		slog.Warn(msg)
	})
	cfg := resolved.Server

	// Process isolation is incompatible with control-plane mode — that mode
	// already provides process-level isolation via the worker pool.
	if cfg.ProcessIsolation {
		cfg.ProcessIsolation = false
		slog.Info("Process isolation disabled (not applicable in control-plane mode)")
	}

	metricsSrv := cliboot.InitMetrics()

	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		fatal("Failed to create data directory: " + err.Error())
	}

	// Auto-generate self-signed certificates when ACME is not configured.
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

	// Wire up SIGINT/SIGTERM so the metrics server gets a clean shutdown
	// during pod termination. RunControlPlane handles its own graceful
	// shutdown for the wire-protocol listeners + worker pool, but the
	// metrics server is owned here.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("Shutting down metrics server...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(ctx)
	}()

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
}
