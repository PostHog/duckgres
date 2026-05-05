package configresolve

import "flag"

// RegisterCLIInputsFlags registers every CLIInputs-backed CLI flag onto fs
// and returns a harvest closure that — once fs.Parse has run — produces a
// fully populated CLIInputs (including the Set map indicating which flags
// were explicitly provided on the command line). Each duckgres binary calls
// this to share one source of truth for the resolver's CLI surface; flags
// that don't flow through ResolveEffective (--mode, --config, --log-level,
// --repl, --psql, --socket-dir, --duckdb-*) remain bespoke per binary.
//
// A handful of CLIInputs fields (the K8s pod-scheduling knobs:
// K8sWorkerCPURequest, K8sWorkerMemoryRequest, K8sWorkerNodeSelector,
// K8sWorkerTolerationKey, K8sWorkerTolerationValue, K8sWorkerExclusiveNode)
// are env-only by design — see CLAUDE.md "K8s pod scheduling knobs are
// env-only." They stay zero in the harvested CLIInputs and ResolveEffective
// reads them directly from os.Getenv. Adding a flag for any of them is a
// fine future change; just add it here and the harvest closure below.
//
// The flag-coverage test in cliflags_test.go asserts every CLI-backed
// CLIInputs field is registered, so adding a new field without a flag
// fails CI loudly instead of silently doing nothing.
func RegisterCLIInputsFlags(fs *flag.FlagSet) func() CLIInputs {
	host := fs.String("host", "", "Host to bind to (env: DUCKGRES_HOST)")
	port := fs.Int("port", 0, "Port to listen on (env: DUCKGRES_PORT)")
	flightPort := fs.Int("flight-port", 0, "Control-plane Arrow Flight SQL ingress port, 0=disabled (env: DUCKGRES_FLIGHT_PORT)")
	flightSessionIdleTTL := fs.String("flight-session-idle-ttl", "", "Flight auth session idle TTL (e.g., '10m') (env: DUCKGRES_FLIGHT_SESSION_IDLE_TTL)")
	flightSessionReapInterval := fs.String("flight-session-reap-interval", "", "Flight auth session reap interval (e.g., '1m') (env: DUCKGRES_FLIGHT_SESSION_REAP_INTERVAL)")
	flightHandleIdleTTL := fs.String("flight-handle-idle-ttl", "", "Flight prepared/query handle idle TTL (e.g., '15m') (env: DUCKGRES_FLIGHT_HANDLE_IDLE_TTL)")
	flightSessionTokenTTL := fs.String("flight-session-token-ttl", "", "Flight issued session token absolute TTL (e.g., '1h') (env: DUCKGRES_FLIGHT_SESSION_TOKEN_TTL)")
	dataDir := fs.String("data-dir", "", "Directory for DuckDB files (env: DUCKGRES_DATA_DIR)")
	certFile := fs.String("cert", "", "TLS certificate file (env: DUCKGRES_CERT)")
	keyFile := fs.String("key", "", "TLS private key file (env: DUCKGRES_KEY)")
	filePersistence := fs.Bool("file-persistence", false, "Persist DuckDB to <data-dir>/<username>.duckdb instead of in-memory (env: DUCKGRES_FILE_PERSISTENCE)")
	processIsolation := fs.Bool("process-isolation", false, "Enable process isolation (spawn child process per connection)")
	idleTimeout := fs.String("idle-timeout", "", "Connection idle timeout (e.g., '30m', '1h', '-1' to disable) (env: DUCKGRES_IDLE_TIMEOUT)")
	sessionInitTimeout := fs.String("session-init-timeout", "", "Session startup metadata/probe timeout (e.g., '10s', '30s') (env: DUCKGRES_SESSION_INIT_TIMEOUT)")
	memoryLimit := fs.String("memory-limit", "", "DuckDB memory_limit per session (e.g., '4GB') (env: DUCKGRES_MEMORY_LIMIT)")
	threads := fs.Int("threads", 0, "DuckDB threads per session (env: DUCKGRES_THREADS)")
	memoryBudget := fs.String("memory-budget", "", "Total memory for all DuckDB sessions (e.g., '24GB') (env: DUCKGRES_MEMORY_BUDGET)")
	memoryRebalance := fs.Bool("memory-rebalance", false, "Enable dynamic per-connection memory reallocation (control-plane mode) (env: DUCKGRES_MEMORY_REBALANCE)")
	duckLakeDeltaCatalogEnabled := fs.Bool("ducklake-delta-catalog-enabled", false, "Attach a Delta Lake catalog during DuckLake worker boot (env: DUCKGRES_DUCKLAKE_DELTA_CATALOG_ENABLED)")
	duckLakeDeltaCatalogPath := fs.String("ducklake-delta-catalog-path", "", "Delta Lake catalog/table path to attach, defaults to sibling delta/ prefix at DuckLake object-store root (env: DUCKGRES_DUCKLAKE_DELTA_CATALOG_PATH)")
	duckLakeDefaultSpecVersion := fs.String("ducklake-default-spec-version", "", "Default DuckLake spec version for migration checks (env: DUCKGRES_DUCKLAKE_DEFAULT_SPEC_VERSION)")
	processMinWorkers := fs.Int("process-min-workers", 0, "Pre-warm worker count at startup for process workers (control-plane mode) (env: DUCKGRES_PROCESS_MIN_WORKERS)")
	processMaxWorkers := fs.Int("process-max-workers", 0, "Max process workers, 0=auto-derived (control-plane mode) (env: DUCKGRES_PROCESS_MAX_WORKERS)")
	processRetireOnSessionEnd := fs.Bool("process-retire-on-session-end", false, "Retire a process worker immediately after its last session ends instead of keeping it warm for reuse (control-plane mode) (env: DUCKGRES_PROCESS_RETIRE_ON_SESSION_END)")
	workerQueueTimeout := fs.String("worker-queue-timeout", "", "How long to wait for an available worker slot (e.g., '5m') (env: DUCKGRES_WORKER_QUEUE_TIMEOUT)")
	workerIdleTimeout := fs.String("worker-idle-timeout", "", "How long to keep an idle worker alive (e.g., '5m') (env: DUCKGRES_WORKER_IDLE_TIMEOUT)")
	handoverDrainTimeout := fs.String("handover-drain-timeout", "", "How long to wait for planned shutdowns/upgrades to drain before forcing exit (default: '24h' in process mode, '15m' in remote mode) (env: DUCKGRES_HANDOVER_DRAIN_TIMEOUT)")
	acmeDomain := fs.String("acme-domain", "", "Domain for ACME/Let's Encrypt certificate (env: DUCKGRES_ACME_DOMAIN)")
	acmeEmail := fs.String("acme-email", "", "Contact email for Let's Encrypt notifications (env: DUCKGRES_ACME_EMAIL)")
	acmeCacheDir := fs.String("acme-cache-dir", "", "Directory for ACME certificate cache (env: DUCKGRES_ACME_CACHE_DIR)")
	acmeDNSProvider := fs.String("acme-dns-provider", "", "DNS provider for ACME DNS-01 challenges, e.g. 'route53' (env: DUCKGRES_ACME_DNS_PROVIDER)")
	acmeDNSZoneID := fs.String("acme-dns-zone-id", "", "Route53 hosted zone ID for ACME DNS-01 challenges (env: DUCKGRES_ACME_DNS_ZONE_ID)")
	maxConnections := fs.Int("max-connections", 0, "Max concurrent connections, 0=unlimited (env: DUCKGRES_MAX_CONNECTIONS)")
	configStoreConn := fs.String("config-store", "", "PostgreSQL connection string for config store (env: DUCKGRES_CONFIG_STORE)")
	configPollInterval := fs.String("config-poll-interval", "", "How often to poll config store for changes (default: 30s) (env: DUCKGRES_CONFIG_POLL_INTERVAL)")
	internalSecret := fs.String("internal-secret", "", "Shared secret for API authentication (env: DUCKGRES_INTERNAL_SECRET)")
	sniRoutingMode := fs.String("sni-routing-mode", "", "Hostname-based org routing: 'off' (default), 'passthrough' (prefer SNI, log legacy), 'enforce' (reject without managed hostname). Multi-tenant only. (env: DUCKGRES_SNI_ROUTING_MODE)")
	managedHostnameSuffixes := fs.String("managed-hostname-suffixes", "", "Comma-separated DNS suffixes (each starting with '.') treated as authoritative for org routing, e.g. '.dw.us.postwh.com'. (env: DUCKGRES_MANAGED_HOSTNAME_SUFFIXES)")
	workerBackend := fs.String("worker-backend", "", "Worker backend: process (default) or remote for config-store-backed K8s multitenant mode (env: DUCKGRES_WORKER_BACKEND)")
	k8sWorkerImage := fs.String("k8s-worker-image", "", "Container image for K8s worker pods (env: DUCKGRES_K8S_WORKER_IMAGE)")
	k8sWorkerNamespace := fs.String("k8s-worker-namespace", "", "K8s namespace for worker pods (env: DUCKGRES_K8S_WORKER_NAMESPACE)")
	k8sControlPlaneID := fs.String("k8s-control-plane-id", "", "Unique CP identifier for labeling worker pods (env: DUCKGRES_K8S_CONTROL_PLANE_ID)")
	k8sWorkerPort := fs.Int("k8s-worker-port", 0, "gRPC port on K8s worker pods (default: 8816) (env: DUCKGRES_K8S_WORKER_PORT)")
	k8sWorkerSecret := fs.String("k8s-worker-secret", "", "K8s Secret name for worker bearer token (env: DUCKGRES_K8S_WORKER_SECRET)")
	k8sWorkerConfigMap := fs.String("k8s-worker-configmap", "", "ConfigMap name for worker duckgres.yaml (env: DUCKGRES_K8S_WORKER_CONFIGMAP)")
	k8sWorkerImagePullPolicy := fs.String("k8s-worker-image-pull-policy", "", "Image pull policy for K8s worker pods: Always, IfNotPresent, Never (env: DUCKGRES_K8S_WORKER_IMAGE_PULL_POLICY)")
	k8sWorkerServiceAccount := fs.String("k8s-worker-service-account", "", "Neutral ServiceAccount name for K8s worker pods (default: duckgres-worker) (env: DUCKGRES_K8S_WORKER_SERVICE_ACCOUNT)")
	k8sMaxWorkers := fs.Int("k8s-max-workers", 0, "Max K8s workers in the shared pool, 0=auto-derived (env: DUCKGRES_K8S_MAX_WORKERS)")
	k8sSharedWarmTarget := fs.Int("k8s-shared-warm-target", 0, "Neutral shared warm-worker target for K8s multi-tenant mode, 0=disabled (env: DUCKGRES_K8S_SHARED_WARM_TARGET)")
	awsRegion := fs.String("aws-region", "", "AWS region for STS client (env: DUCKGRES_AWS_REGION)")
	queryLog := fs.Bool("query-log", true, "Enable/disable DuckLake query log (use --query-log=false to disable; env: DUCKGRES_QUERY_LOG_ENABLED)")

	return func() CLIInputs {
		cli := CLIInputs{Set: map[string]bool{}}
		fs.Visit(func(f *flag.Flag) {
			cli.Set[f.Name] = true
		})
		cli.Host = *host
		cli.Port = *port
		cli.FlightPort = *flightPort
		cli.FlightSessionIdleTTL = *flightSessionIdleTTL
		cli.FlightSessionReapInterval = *flightSessionReapInterval
		cli.FlightHandleIdleTTL = *flightHandleIdleTTL
		cli.FlightSessionTokenTTL = *flightSessionTokenTTL
		cli.DataDir = *dataDir
		cli.CertFile = *certFile
		cli.KeyFile = *keyFile
		cli.FilePersistence = *filePersistence
		cli.ProcessIsolation = *processIsolation
		cli.IdleTimeout = *idleTimeout
		cli.SessionInitTimeout = *sessionInitTimeout
		cli.MemoryLimit = *memoryLimit
		cli.Threads = *threads
		cli.MemoryBudget = *memoryBudget
		cli.MemoryRebalance = *memoryRebalance
		cli.DuckLakeDeltaCatalogEnabled = *duckLakeDeltaCatalogEnabled
		cli.DuckLakeDeltaCatalogPath = *duckLakeDeltaCatalogPath
		cli.DuckLakeDefaultSpecVersion = *duckLakeDefaultSpecVersion
		cli.ProcessMinWorkers = *processMinWorkers
		cli.ProcessMaxWorkers = *processMaxWorkers
		cli.ProcessRetireOnSessionEnd = *processRetireOnSessionEnd
		cli.WorkerQueueTimeout = *workerQueueTimeout
		cli.WorkerIdleTimeout = *workerIdleTimeout
		cli.HandoverDrainTimeout = *handoverDrainTimeout
		cli.ACMEDomain = *acmeDomain
		cli.ACMEEmail = *acmeEmail
		cli.ACMECacheDir = *acmeCacheDir
		cli.ACMEDNSProvider = *acmeDNSProvider
		cli.ACMEDNSZoneID = *acmeDNSZoneID
		cli.MaxConnections = *maxConnections
		cli.ConfigStoreConn = *configStoreConn
		cli.ConfigPollInterval = *configPollInterval
		cli.InternalSecret = *internalSecret
		cli.SNIRoutingMode = *sniRoutingMode
		cli.ManagedHostnameSuffixes = *managedHostnameSuffixes
		cli.WorkerBackend = *workerBackend
		cli.K8sWorkerImage = *k8sWorkerImage
		cli.K8sWorkerNamespace = *k8sWorkerNamespace
		cli.K8sControlPlaneID = *k8sControlPlaneID
		cli.K8sWorkerPort = *k8sWorkerPort
		cli.K8sWorkerSecret = *k8sWorkerSecret
		cli.K8sWorkerConfigMap = *k8sWorkerConfigMap
		cli.K8sWorkerImagePullPolicy = *k8sWorkerImagePullPolicy
		cli.K8sWorkerServiceAccount = *k8sWorkerServiceAccount
		cli.K8sMaxWorkers = *k8sMaxWorkers
		cli.K8sSharedWarmTarget = *k8sSharedWarmTarget
		cli.AWSRegion = *awsRegion
		cli.QueryLog = *queryLog
		return cli
	}
}
