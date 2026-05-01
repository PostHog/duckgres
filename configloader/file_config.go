// Package configloader holds duckgres' YAML configuration schema and the
// helper that loads + parses a config file. It's the shared piece between
// the all-in-one duckgres binary, cmd/duckgres-controlplane, and
// cmd/duckgres-worker — each of which needs to read the same duckgres.yaml.
//
// Nothing here depends on github.com/duckdb/duckdb-go: a configloader
// import is safe in the duckdb-free CP binary.
package configloader

// FileConfig represents the YAML configuration file structure shared
// across all duckgres binaries. Mode-specific fields are present in
// every binary's view of the file even when they're irrelevant to that
// mode (e.g., the worker binary reads but ignores ControlPlane fields);
// the cost is one parsed-but-unused struct field per binary.
type FileConfig struct {
	Host                      string              `yaml:"host"`
	Port                      int                 `yaml:"port"`
	FlightPort                int                 `yaml:"flight_port"`                  // Control-plane Flight SQL ingress port (0 disables)
	FlightSessionIdleTTL      string              `yaml:"flight_session_idle_ttl"`      // e.g., "10m"
	FlightSessionReapInterval string              `yaml:"flight_session_reap_interval"` // e.g., "1m"
	FlightHandleIdleTTL       string              `yaml:"flight_handle_idle_ttl"`       // e.g., "15m"
	FlightSessionTokenTTL     string              `yaml:"flight_session_token_ttl"`     // e.g., "1h"
	DataDir                   string              `yaml:"data_dir"`
	TLS                       TLSConfig           `yaml:"tls"`
	Users                     map[string]string   `yaml:"users"`
	RateLimit                 RateLimitFileConfig `yaml:"rate_limit"`
	Extensions                []string            `yaml:"extensions"`
	DuckLake                  DuckLakeFileConfig  `yaml:"ducklake"`
	FilePersistence           bool                `yaml:"file_persistence"`
	ProcessIsolation          bool                `yaml:"process_isolation"`
	IdleTimeout               string              `yaml:"idle_timeout"`
	MemoryLimit               string              `yaml:"memory_limit"`
	Threads                   int                 `yaml:"threads"`
	MemoryBudget              string              `yaml:"memory_budget"`
	MemoryRebalance           *bool               `yaml:"memory_rebalance"`
	Process                   ProcessFileConfig   `yaml:"process"`
	WorkerQueueTimeout        string              `yaml:"worker_queue_timeout"`
	WorkerIdleTimeout         string              `yaml:"worker_idle_timeout"`
	HandoverDrainTimeout      string              `yaml:"handover_drain_timeout"`
	PassthroughUsers          []string            `yaml:"passthrough_users"`
	LogLevel                  string              `yaml:"log_level"`
	QueryLog                  QueryLogFileConfig  `yaml:"query_log"`

	// Worker backend configuration
	WorkerBackend string        `yaml:"worker_backend"` // "process" (default) or "remote"
	K8s           K8sFileConfig `yaml:"k8s"`
}

type ProcessFileConfig struct {
	MinWorkers         int   `yaml:"min_workers"`
	MaxWorkers         int   `yaml:"max_workers"`
	RetireOnSessionEnd *bool `yaml:"retire_on_session_end"`
}

// K8sFileConfig holds Kubernetes worker configuration from YAML.
type K8sFileConfig struct {
	WorkerImage           string `yaml:"worker_image"`
	WorkerNamespace       string `yaml:"worker_namespace"`
	ControlPlaneID        string `yaml:"control_plane_id"`
	WorkerPort            int    `yaml:"worker_port"`
	WorkerSecret          string `yaml:"worker_secret"`
	WorkerConfigMap       string `yaml:"worker_configmap"`
	WorkerImagePullPolicy string `yaml:"worker_image_pull_policy"`
	WorkerServiceAccount  string `yaml:"worker_service_account"`
	MaxWorkers            int    `yaml:"max_workers"`
	SharedWarmTarget      int    `yaml:"shared_warm_target"`
}

type QueryLogFileConfig struct {
	Enabled              *bool  `yaml:"enabled"`
	FlushInterval        string `yaml:"flush_interval"`
	BatchSize            int    `yaml:"batch_size"`
	CompactInterval      string `yaml:"compact_interval"`
	DataInliningRowLimit int    `yaml:"data_inlining_row_limit"`
}

type TLSConfig struct {
	Cert string     `yaml:"cert"`
	Key  string     `yaml:"key"`
	ACME ACMEConfig `yaml:"acme"`
}

type ACMEConfig struct {
	Domain      string `yaml:"domain"`
	Email       string `yaml:"email"`
	CacheDir    string `yaml:"cache_dir"`
	DNSProvider string `yaml:"dns_provider"`
	DNSZoneID   string `yaml:"dns_zone_id"`
}

type RateLimitFileConfig struct {
	MaxFailedAttempts   int    `yaml:"max_failed_attempts"`
	FailedAttemptWindow string `yaml:"failed_attempt_window"`
	BanDuration         string `yaml:"ban_duration"`
	MaxConnectionsPerIP int    `yaml:"max_connections_per_ip"`
	MaxConnections      int    `yaml:"max_connections"`
}

type DuckLakeFileConfig struct {
	MetadataStore string `yaml:"metadata_store"`
	ObjectStore   string `yaml:"object_store"`
	DataPath      string `yaml:"data_path"`

	// Delta catalog attachment. When enabled without an explicit path, the
	// catalog path is derived as a sibling delta/ prefix at the object store root.
	DeltaCatalogEnabled *bool  `yaml:"delta_catalog_enabled"`
	DeltaCatalogPath    string `yaml:"delta_catalog_path"`

	// Disable metadata postgres_scanner thread-local cache before ATTACH creates
	// the hidden metadata pool. Nil means use the server default.
	DisableMetadataThreadLocalCache *bool `yaml:"disable_metadata_thread_local_cache"`

	// S3 credential provider: "config" (explicit) or "credential_chain" (AWS SDK)
	S3Provider string `yaml:"s3_provider"`

	// Config provider settings (explicit credentials)
	S3Endpoint  string `yaml:"s3_endpoint"`
	S3AccessKey string `yaml:"s3_access_key"`
	S3SecretKey string `yaml:"s3_secret_key"`
	S3Region    string `yaml:"s3_region"`
	S3UseSSL    bool   `yaml:"s3_use_ssl"`
	S3URLStyle  string `yaml:"s3_url_style"`

	// Credential chain provider settings (AWS SDK credential chain)
	S3Chain   string `yaml:"s3_chain"`
	S3Profile string `yaml:"s3_profile"`

	// Checkpoint interval for DuckLake maintenance (e.g., "24h", "6h")
	CheckpointInterval string `yaml:"checkpoint_interval"`

	// DataInliningRowLimit controls max rows inlined in metadata per insert.
	// Default: 0 (disabled). Set to a positive value to enable inlining.
	DataInliningRowLimit *int `yaml:"data_inlining_row_limit"`

	// DefaultSpecVersion is the global default DuckLake spec version
	// used for migration checks when an org doesn't specify an override.
	DefaultSpecVersion string `yaml:"default_spec_version"`
}
