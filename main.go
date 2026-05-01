package main // duckgres entry point

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
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
	"github.com/posthog/duckgres/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// FileConfig is the YAML configuration schema, sourced from the
// configloader package so the new cmd/duckgres-controlplane and
// cmd/duckgres-worker binaries parse the same shape.
type FileConfig = configloader.FileConfig

// Type aliases for the nested configloader types so the rest of main.go's
// resolveEffectiveConfig logic continues to compile unchanged.
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

// initMetrics starts the Prometheus metrics HTTP server on :9090/metrics.
// Returns the http.Server instance so it can be shut down during handover.
func initMetrics() *http.Server {
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
		loggingShutdown := initLogging()
		defer loggingShutdown()
		tracingShutdown := initTracing()
		defer tracingShutdown()
		duckdbservice.LogCacheProxyStatus()
		server.RunChildMode()
		return // RunChildMode calls os.Exit
	}

	// Define CLI flags with environment variable fallbacks
	configFile := flag.String("config", env("DUCKGRES_CONFIG", ""), "Path to YAML config file (env: DUCKGRES_CONFIG)")
	host := flag.String("host", "", "Host to bind to (env: DUCKGRES_HOST)")
	port := flag.Int("port", 0, "Port to listen on (env: DUCKGRES_PORT)")
	flightPort := flag.Int("flight-port", 0, "Control-plane Arrow Flight SQL ingress port, 0=disabled (env: DUCKGRES_FLIGHT_PORT)")
	flightSessionIdleTTL := flag.String("flight-session-idle-ttl", "", "Flight auth session idle TTL (e.g., '10m') (env: DUCKGRES_FLIGHT_SESSION_IDLE_TTL)")
	flightSessionReapInterval := flag.String("flight-session-reap-interval", "", "Flight auth session reap interval (e.g., '1m') (env: DUCKGRES_FLIGHT_SESSION_REAP_INTERVAL)")
	flightHandleIdleTTL := flag.String("flight-handle-idle-ttl", "", "Flight prepared/query handle idle TTL (e.g., '15m') (env: DUCKGRES_FLIGHT_HANDLE_IDLE_TTL)")
	flightSessionTokenTTL := flag.String("flight-session-token-ttl", "", "Flight issued session token absolute TTL (e.g., '1h') (env: DUCKGRES_FLIGHT_SESSION_TOKEN_TTL)")
	dataDir := flag.String("data-dir", "", "Directory for DuckDB files (env: DUCKGRES_DATA_DIR)")
	certFile := flag.String("cert", "", "TLS certificate file (env: DUCKGRES_CERT)")
	keyFile := flag.String("key", "", "TLS private key file (env: DUCKGRES_KEY)")
	filePersistence := flag.Bool("file-persistence", false, "Persist DuckDB to <data-dir>/<username>.duckdb instead of in-memory (env: DUCKGRES_FILE_PERSISTENCE)")
	processIsolation := flag.Bool("process-isolation", false, "Enable process isolation (spawn child process per connection)")
	idleTimeout := flag.String("idle-timeout", "", "Connection idle timeout (e.g., '30m', '1h', '-1' to disable) (env: DUCKGRES_IDLE_TIMEOUT)")
	sessionInitTimeout := flag.String("session-init-timeout", "", "Session startup metadata/probe timeout (e.g., '10s', '30s') (env: DUCKGRES_SESSION_INIT_TIMEOUT)")
	memoryLimit := flag.String("memory-limit", "", "DuckDB memory_limit per session (e.g., '4GB') (env: DUCKGRES_MEMORY_LIMIT)")
	threads := flag.Int("threads", 0, "DuckDB threads per session (env: DUCKGRES_THREADS)")
	memoryBudget := flag.String("memory-budget", "", "Total memory for all DuckDB sessions (e.g., '24GB') (env: DUCKGRES_MEMORY_BUDGET)")
	memoryRebalance := flag.Bool("memory-rebalance", false, "Enable dynamic per-connection memory reallocation (control-plane mode) (env: DUCKGRES_MEMORY_REBALANCE)")
	duckLakeDeltaCatalogEnabled := flag.Bool("ducklake-delta-catalog-enabled", false, "Attach a Delta Lake catalog during DuckLake worker boot (env: DUCKGRES_DUCKLAKE_DELTA_CATALOG_ENABLED)")
	duckLakeDeltaCatalogPath := flag.String("ducklake-delta-catalog-path", "", "Delta Lake catalog/table path to attach, defaults to sibling delta/ prefix at DuckLake object-store root (env: DUCKGRES_DUCKLAKE_DELTA_CATALOG_PATH)")
	duckLakeDefaultSpecVersion := flag.String("ducklake-default-spec-version", "", "Default DuckLake spec version for migration checks (env: DUCKGRES_DUCKLAKE_DEFAULT_SPEC_VERSION)")
	logLevel := flag.String("log-level", "", "Log level: debug, info, warn, error (env: DUCKGRES_LOG_LEVEL)")
	repl := flag.Bool("repl", false, "Start an interactive SQL shell instead of the server")
	psql := flag.Bool("psql", false, "Launch psql connected to the local Duckgres server")
	showVersion := flag.Bool("version", false, "Show version and exit")
	showHelp := flag.Bool("help", false, "Show help message")

	// Rate limiting flags
	maxConnections := flag.Int("max-connections", 0, "Max concurrent connections, 0=unlimited (env: DUCKGRES_MAX_CONNECTIONS)")

	// Control plane flags
	mode := flag.String("mode", "standalone", "Run mode: standalone, control-plane, or duckdb-service")
	processMinWorkers := flag.Int("process-min-workers", 0, "Pre-warm worker count at startup for process workers (control-plane mode) (env: DUCKGRES_PROCESS_MIN_WORKERS)")
	processMaxWorkers := flag.Int("process-max-workers", 0, "Max process workers, 0=auto-derived (control-plane mode) (env: DUCKGRES_PROCESS_MAX_WORKERS)")
	processRetireOnSessionEnd := flag.Bool("process-retire-on-session-end", false, "Retire a process worker immediately after its last session ends instead of keeping it warm for reuse (control-plane mode) (env: DUCKGRES_PROCESS_RETIRE_ON_SESSION_END)")
	workerQueueTimeout := flag.String("worker-queue-timeout", "", "How long to wait for an available worker slot (e.g., '5m') (env: DUCKGRES_WORKER_QUEUE_TIMEOUT)")
	workerIdleTimeout := flag.String("worker-idle-timeout", "", "How long to keep an idle worker alive (e.g., '5m') (env: DUCKGRES_WORKER_IDLE_TIMEOUT)")
	handoverDrainTimeout := flag.String("handover-drain-timeout", "", "How long to wait for planned shutdowns/upgrades to drain before forcing exit (default: '24h' in process mode, '15m' in remote mode) (env: DUCKGRES_HANDOVER_DRAIN_TIMEOUT)")
	socketDir := flag.String("socket-dir", "/var/run/duckgres", "Unix socket directory (control-plane mode)")
	workerBackend := flag.String("worker-backend", "", "Worker backend: process (default) or remote for config-store-backed K8s multitenant mode (env: DUCKGRES_WORKER_BACKEND)")
	k8sWorkerImage := flag.String("k8s-worker-image", "", "Container image for K8s worker pods (env: DUCKGRES_K8S_WORKER_IMAGE)")
	k8sWorkerNamespace := flag.String("k8s-worker-namespace", "", "K8s namespace for worker pods (env: DUCKGRES_K8S_WORKER_NAMESPACE)")
	k8sControlPlaneID := flag.String("k8s-control-plane-id", "", "Unique CP identifier for labeling worker pods (env: DUCKGRES_K8S_CONTROL_PLANE_ID)")
	k8sWorkerPort := flag.Int("k8s-worker-port", 0, "gRPC port on K8s worker pods (default: 8816) (env: DUCKGRES_K8S_WORKER_PORT)")
	k8sWorkerSecret := flag.String("k8s-worker-secret", "", "K8s Secret name for worker bearer token (env: DUCKGRES_K8S_WORKER_SECRET)")
	k8sWorkerConfigMap := flag.String("k8s-worker-configmap", "", "ConfigMap name for worker duckgres.yaml (env: DUCKGRES_K8S_WORKER_CONFIGMAP)")
	k8sWorkerImagePullPolicy := flag.String("k8s-worker-image-pull-policy", "", "Image pull policy for K8s worker pods: Always, IfNotPresent, Never (env: DUCKGRES_K8S_WORKER_IMAGE_PULL_POLICY)")
	k8sWorkerServiceAccount := flag.String("k8s-worker-service-account", "", "Neutral ServiceAccount name for K8s worker pods (default: duckgres-worker) (env: DUCKGRES_K8S_WORKER_SERVICE_ACCOUNT)")
	k8sMaxWorkers := flag.Int("k8s-max-workers", 0, "Max K8s workers in the shared pool, 0=auto-derived (env: DUCKGRES_K8S_MAX_WORKERS)")
	k8sSharedWarmTarget := flag.Int("k8s-shared-warm-target", 0, "Neutral shared warm-worker target for K8s multi-tenant mode, 0=disabled (env: DUCKGRES_K8S_SHARED_WARM_TARGET)")
	awsRegion := flag.String("aws-region", "", "AWS region for STS client (env: DUCKGRES_AWS_REGION)")

	// Config store flags (multi-tenant mode)
	configStore := flag.String("config-store", "", "PostgreSQL connection string for config store (env: DUCKGRES_CONFIG_STORE)")
	configPollInterval := flag.String("config-poll-interval", "", "How often to poll config store for changes (default: 30s) (env: DUCKGRES_CONFIG_POLL_INTERVAL)")
	internalSecret := flag.String("internal-secret", "", "Shared secret for API authentication (env: DUCKGRES_INTERNAL_SECRET)")
	sniRoutingMode := flag.String("sni-routing-mode", "", "Hostname-based org routing: 'off' (default), 'passthrough' (prefer SNI, log legacy), 'enforce' (reject without managed hostname). Multi-tenant only. (env: DUCKGRES_SNI_ROUTING_MODE)")
	managedHostnameSuffixes := flag.String("managed-hostname-suffixes", "", "Comma-separated DNS suffixes (each starting with '.') treated as authoritative for org routing, e.g. '.dw.us.postwh.com'. (env: DUCKGRES_MANAGED_HOSTNAME_SUFFIXES)")

	// ACME/Let's Encrypt flags
	acmeDomain := flag.String("acme-domain", "", "Domain for ACME/Let's Encrypt certificate (env: DUCKGRES_ACME_DOMAIN)")
	acmeEmail := flag.String("acme-email", "", "Contact email for Let's Encrypt notifications (env: DUCKGRES_ACME_EMAIL)")
	acmeCacheDir := flag.String("acme-cache-dir", "", "Directory for ACME certificate cache (env: DUCKGRES_ACME_CACHE_DIR)")
	acmeDNSProvider := flag.String("acme-dns-provider", "", "DNS provider for ACME DNS-01 challenges, e.g. 'route53' (env: DUCKGRES_ACME_DNS_PROVIDER)")
	acmeDNSZoneID := flag.String("acme-dns-zone-id", "", "Route53 hosted zone ID for ACME DNS-01 challenges (env: DUCKGRES_ACME_DNS_ZONE_ID)")

	// Query log flags
	queryLog := flag.Bool("query-log", true, "Enable/disable DuckLake query log (use --query-log=false to disable; env: DUCKGRES_QUERY_LOG_ENABLED)")

	// DuckDB service flags
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
			fmt.Println(versionString())
			os.Exit(0)
		}
	}

	flag.Parse()

	if *showVersion {
		fmt.Println(versionString())
		os.Exit(0)
	}

	// Track explicitly-set CLI flags so precedence is consistent.
	cliSet := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		cliSet[f.Name] = true
	})

	// Auto-detect duckgres.yaml if no config file was explicitly specified
	if *configFile == "" {
		if _, err := os.Stat("duckgres.yaml"); err == nil {
			*configFile = "duckgres.yaml"
		}
	}

	// Load config file early so log_level from YAML can feed into initLogging().
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
	// Set the env var so workers inherit it and parseLogLevel() picks it up.
	if *logLevel != "" {
		_ = os.Setenv("DUCKGRES_LOG_LEVEL", *logLevel)
	} else if os.Getenv("DUCKGRES_LOG_LEVEL") == "" && fileCfg != nil && fileCfg.LogLevel != "" {
		_ = os.Setenv("DUCKGRES_LOG_LEVEL", fileCfg.LogLevel)
	}

	loggingShutdown := initLogging()
	defer loggingShutdown()

	tracingShutdown := initTracing()
	defer tracingShutdown()

	logBuildInfo(*mode)
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

	resolved := configresolve.ResolveEffective(fileCfg, configresolve.CLIInputs{
		Set:                         cliSet,
		Host:                        *host,
		Port:                        *port,
		FlightPort:                  *flightPort,
		FlightSessionIdleTTL:        *flightSessionIdleTTL,
		FlightSessionReapInterval:   *flightSessionReapInterval,
		FlightHandleIdleTTL:         *flightHandleIdleTTL,
		FlightSessionTokenTTL:       *flightSessionTokenTTL,
		DataDir:                     *dataDir,
		CertFile:                    *certFile,
		KeyFile:                     *keyFile,
		FilePersistence:             *filePersistence,
		ProcessIsolation:            *processIsolation,
		IdleTimeout:                 *idleTimeout,
		SessionInitTimeout:          *sessionInitTimeout,
		MemoryLimit:                 *memoryLimit,
		Threads:                     *threads,
		MemoryBudget:                *memoryBudget,
		MemoryRebalance:             *memoryRebalance,
		DuckLakeDeltaCatalogEnabled: *duckLakeDeltaCatalogEnabled,
		DuckLakeDeltaCatalogPath:    *duckLakeDeltaCatalogPath,
		DuckLakeDefaultSpecVersion:  *duckLakeDefaultSpecVersion,
		ProcessMinWorkers:           *processMinWorkers,
		ProcessMaxWorkers:           *processMaxWorkers,
		ProcessRetireOnSessionEnd:   *processRetireOnSessionEnd,
		WorkerQueueTimeout:          *workerQueueTimeout,
		WorkerIdleTimeout:           *workerIdleTimeout,
		HandoverDrainTimeout:        *handoverDrainTimeout,
		ACMEDomain:                  *acmeDomain,
		ACMEEmail:                   *acmeEmail,
		ACMECacheDir:                *acmeCacheDir,
		ACMEDNSProvider:             *acmeDNSProvider,
		ACMEDNSZoneID:               *acmeDNSZoneID,
		MaxConnections:              *maxConnections,
		ConfigStoreConn:             *configStore,
		ConfigPollInterval:          *configPollInterval,
		InternalSecret:              *internalSecret,
		SNIRoutingMode:              *sniRoutingMode,
		ManagedHostnameSuffixes:     *managedHostnameSuffixes,
		WorkerBackend:               *workerBackend,
		K8sWorkerImage:              *k8sWorkerImage,
		K8sWorkerNamespace:          *k8sWorkerNamespace,
		K8sControlPlaneID:           *k8sControlPlaneID,
		K8sWorkerPort:               *k8sWorkerPort,
		K8sWorkerSecret:             *k8sWorkerSecret,
		K8sWorkerConfigMap:          *k8sWorkerConfigMap,
		K8sWorkerImagePullPolicy:    *k8sWorkerImagePullPolicy,
		K8sWorkerServiceAccount:     *k8sWorkerServiceAccount,
		K8sMaxWorkers:               *k8sMaxWorkers,
		K8sSharedWarmTarget:         *k8sSharedWarmTarget,
		AWSRegion:                   *awsRegion,
		QueryLog:                    *queryLog,
	}, os.Getenv, func(msg string) {
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

	metricsSrv := initMetrics()

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
