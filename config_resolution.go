package main

import (
	"strconv"
	"strings"
	"time"

	"github.com/posthog/duckgres/server"
)

type configCLIInputs struct {
	Set map[string]bool

	Host                      string
	Port                      int
	FlightPort                int
	FlightSessionIdleTTL      string
	FlightSessionReapInterval string
	FlightHandleIdleTTL       string
	FlightSessionTokenTTL     string
	DataDir                   string
	CertFile                  string
	KeyFile                   string
	ProcessIsolation          bool
	IdleTimeout               string
	MemoryLimit               string
	Threads                   int
	MemoryBudget              string
	MemoryRebalance           bool
	MaxWorkers                int
	MinWorkers                int
	WorkerQueueTimeout        string
	WorkerIdleTimeout         string
	HandoverDrainTimeout      string
	ACMEDomain                string
	ACMEEmail                 string
	ACMECacheDir              string
	ACMEDNSProvider           string
	ACMEDNSZoneID             string
	MaxConnections            int
	ConfigStoreConn           string
	ConfigPollInterval        string
	AdminToken                string
	WorkerBackend             string
	K8sWorkerImage            string
	K8sWorkerNamespace        string
	K8sControlPlaneID         string
	K8sWorkerPort             int
	K8sWorkerSecret           string
	K8sWorkerConfigMap        string
	K8sWorkerImagePullPolicy  string
	K8sWorkerServiceAccount   string
	QueryLog                  bool
}

type resolvedConfig struct {
	Server               server.Config
	WorkerQueueTimeout   time.Duration
	WorkerIdleTimeout    time.Duration
	HandoverDrainTimeout time.Duration
	WorkerBackend        string
	K8sWorkerImage       string
	K8sWorkerNamespace   string
	K8sControlPlaneID    string
	K8sWorkerPort        int
	K8sWorkerSecret      string
	K8sWorkerConfigMap   string
	K8sWorkerImagePullPolicy string
	K8sWorkerServiceAccount  string
	ConfigStoreConn      string
	ConfigPollInterval   time.Duration
	AdminToken           string
}

func defaultServerConfig() server.Config {
	return server.Config{
		Host:                      "0.0.0.0",
		Port:                      5432,
		FlightPort:                0,
		FlightSessionIdleTTL:      10 * time.Minute,
		FlightSessionReapInterval: 1 * time.Minute,
		FlightHandleIdleTTL:       15 * time.Minute,
		FlightSessionTokenTTL:     1 * time.Hour,
		DataDir:                   "./data",
		TLSCertFile:               "./certs/server.crt",
		TLSKeyFile:                "./certs/server.key",
		Users: map[string]string{
			"postgres": "postgres",
		},
		Extensions: []string{"ducklake"},
		QueryLog: server.QueryLogConfig{
			Enabled:              true,
			FlushInterval:        5 * time.Second,
			BatchSize:            1000,
			CompactInterval:      10 * time.Minute,
			DataInliningRowLimit: 1000,
		},
	}
}

func resolveEffectiveConfig(fileCfg *FileConfig, cli configCLIInputs, getenv func(string) string, warn func(string)) resolvedConfig {
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	if warn == nil {
		warn = func(string) {}
	}
	if cli.Set == nil {
		cli.Set = map[string]bool{}
	}

	cfg := defaultServerConfig()
	defaultQueryLog := cfg.QueryLog
	var workerQueueTimeout time.Duration
	var workerIdleTimeout time.Duration
	var handoverDrainTimeout time.Duration
	var workerBackend string
	var k8sWorkerImage, k8sWorkerNamespace, k8sControlPlaneID string
	var k8sWorkerPort int
	var k8sWorkerSecret, k8sWorkerConfigMap, k8sWorkerImagePullPolicy, k8sWorkerServiceAccount string
	var configStoreConn string
	var configPollInterval time.Duration
	var adminToken string

	if fileCfg != nil {
		if fileCfg.Host != "" {
			cfg.Host = fileCfg.Host
		}
		if fileCfg.Port != 0 {
			cfg.Port = fileCfg.Port
		}
		if fileCfg.FlightPort != 0 {
			cfg.FlightPort = fileCfg.FlightPort
		}
		if fileCfg.FlightSessionIdleTTL != "" {
			if d, err := time.ParseDuration(fileCfg.FlightSessionIdleTTL); err == nil {
				cfg.FlightSessionIdleTTL = d
			} else {
				warn("Invalid flight_session_idle_ttl duration: " + err.Error())
			}
		}
		if fileCfg.FlightSessionReapInterval != "" {
			if d, err := time.ParseDuration(fileCfg.FlightSessionReapInterval); err == nil {
				cfg.FlightSessionReapInterval = d
			} else {
				warn("Invalid flight_session_reap_interval duration: " + err.Error())
			}
		}
		if fileCfg.FlightHandleIdleTTL != "" {
			if d, err := time.ParseDuration(fileCfg.FlightHandleIdleTTL); err == nil {
				cfg.FlightHandleIdleTTL = d
			} else {
				warn("Invalid flight_handle_idle_ttl duration: " + err.Error())
			}
		}
		if fileCfg.FlightSessionTokenTTL != "" {
			if d, err := time.ParseDuration(fileCfg.FlightSessionTokenTTL); err == nil {
				cfg.FlightSessionTokenTTL = d
			} else {
				warn("Invalid flight_session_token_ttl duration: " + err.Error())
			}
		}
		if fileCfg.DataDir != "" {
			cfg.DataDir = fileCfg.DataDir
		}
		if fileCfg.TLS.Cert != "" {
			cfg.TLSCertFile = fileCfg.TLS.Cert
		}
		if fileCfg.TLS.Key != "" {
			cfg.TLSKeyFile = fileCfg.TLS.Key
		}
		if len(fileCfg.Users) > 0 {
			cfg.Users = fileCfg.Users
		}

		if fileCfg.RateLimit.MaxFailedAttempts > 0 {
			cfg.RateLimit.MaxFailedAttempts = fileCfg.RateLimit.MaxFailedAttempts
		}
		if fileCfg.RateLimit.MaxConnectionsPerIP > 0 {
			cfg.RateLimit.MaxConnectionsPerIP = fileCfg.RateLimit.MaxConnectionsPerIP
		}
		if fileCfg.RateLimit.MaxConnections > 0 {
			cfg.RateLimit.MaxConnections = fileCfg.RateLimit.MaxConnections
		}
		if fileCfg.RateLimit.FailedAttemptWindow != "" {
			if d, err := time.ParseDuration(fileCfg.RateLimit.FailedAttemptWindow); err == nil {
				cfg.RateLimit.FailedAttemptWindow = d
			} else {
				warn("Invalid failed_attempt_window duration: " + err.Error())
			}
		}
		if fileCfg.RateLimit.BanDuration != "" {
			if d, err := time.ParseDuration(fileCfg.RateLimit.BanDuration); err == nil {
				cfg.RateLimit.BanDuration = d
			} else {
				warn("Invalid ban_duration duration: " + err.Error())
			}
		}

		if len(fileCfg.Extensions) > 0 {
			cfg.Extensions = fileCfg.Extensions
		}

		if fileCfg.DuckLake.MetadataStore != "" {
			cfg.DuckLake.MetadataStore = fileCfg.DuckLake.MetadataStore
		}
		if fileCfg.DuckLake.ObjectStore != "" {
			cfg.DuckLake.ObjectStore = fileCfg.DuckLake.ObjectStore
		}
		if fileCfg.DuckLake.DataPath != "" {
			cfg.DuckLake.DataPath = fileCfg.DuckLake.DataPath
		}
		if fileCfg.DuckLake.S3Provider != "" {
			cfg.DuckLake.S3Provider = fileCfg.DuckLake.S3Provider
		}
		if fileCfg.DuckLake.S3Endpoint != "" {
			cfg.DuckLake.S3Endpoint = fileCfg.DuckLake.S3Endpoint
		}
		if fileCfg.DuckLake.S3AccessKey != "" {
			cfg.DuckLake.S3AccessKey = fileCfg.DuckLake.S3AccessKey
		}
		if fileCfg.DuckLake.S3SecretKey != "" {
			cfg.DuckLake.S3SecretKey = fileCfg.DuckLake.S3SecretKey
		}
		if fileCfg.DuckLake.S3Region != "" {
			cfg.DuckLake.S3Region = fileCfg.DuckLake.S3Region
		}
		cfg.DuckLake.S3UseSSL = fileCfg.DuckLake.S3UseSSL
		if fileCfg.DuckLake.S3URLStyle != "" {
			cfg.DuckLake.S3URLStyle = fileCfg.DuckLake.S3URLStyle
		}
		if fileCfg.DuckLake.S3Chain != "" {
			cfg.DuckLake.S3Chain = fileCfg.DuckLake.S3Chain
		}
		if fileCfg.DuckLake.S3Profile != "" {
			cfg.DuckLake.S3Profile = fileCfg.DuckLake.S3Profile
		}

		cfg.ProcessIsolation = fileCfg.ProcessIsolation
		if fileCfg.IdleTimeout != "" {
			if d, err := time.ParseDuration(fileCfg.IdleTimeout); err == nil {
				cfg.IdleTimeout = d
			} else {
				warn("Invalid idle_timeout duration: " + err.Error())
			}
		}
		if fileCfg.MemoryLimit != "" {
			cfg.MemoryLimit = fileCfg.MemoryLimit
		}
		if fileCfg.Threads != 0 {
			cfg.Threads = fileCfg.Threads
		}
		if fileCfg.MemoryBudget != "" {
			cfg.MemoryBudget = fileCfg.MemoryBudget
		}
		if fileCfg.MemoryRebalance != nil {
			cfg.MemoryRebalance = *fileCfg.MemoryRebalance
		}
		if fileCfg.MaxWorkers != 0 {
			cfg.MaxWorkers = fileCfg.MaxWorkers
		}
		if fileCfg.MinWorkers != 0 {
			cfg.MinWorkers = fileCfg.MinWorkers
		}
		if fileCfg.WorkerQueueTimeout != "" {
			if d, err := time.ParseDuration(fileCfg.WorkerQueueTimeout); err == nil {
				workerQueueTimeout = d
			} else {
				warn("Invalid worker_queue_timeout duration: " + err.Error())
			}
		}
		if fileCfg.WorkerIdleTimeout != "" {
			if d, err := time.ParseDuration(fileCfg.WorkerIdleTimeout); err == nil {
				workerIdleTimeout = d
			} else {
				warn("Invalid worker_idle_timeout duration: " + err.Error())
			}
		}
		if fileCfg.HandoverDrainTimeout != "" {
			if d, err := time.ParseDuration(fileCfg.HandoverDrainTimeout); err == nil {
				handoverDrainTimeout = d
			} else {
				warn("Invalid handover_drain_timeout duration: " + err.Error())
			}
		}
		if len(fileCfg.PassthroughUsers) > 0 {
			cfg.PassthroughUsers = make(map[string]bool, len(fileCfg.PassthroughUsers))
			for _, u := range fileCfg.PassthroughUsers {
				cfg.PassthroughUsers[u] = true
			}
		}

		// Query log configuration
		if fileCfg.QueryLog.Enabled != nil {
			cfg.QueryLog.Enabled = *fileCfg.QueryLog.Enabled
		}
		if fileCfg.QueryLog.FlushInterval != "" {
			if d, err := time.ParseDuration(fileCfg.QueryLog.FlushInterval); err == nil {
				cfg.QueryLog.FlushInterval = d
			} else {
				warn("Invalid query_log.flush_interval duration: " + err.Error())
			}
		}
		if fileCfg.QueryLog.BatchSize > 0 {
			cfg.QueryLog.BatchSize = fileCfg.QueryLog.BatchSize
		}
		if fileCfg.QueryLog.CompactInterval != "" {
			if d, err := time.ParseDuration(fileCfg.QueryLog.CompactInterval); err == nil {
				cfg.QueryLog.CompactInterval = d
			} else {
				warn("Invalid query_log.compact_interval duration: " + err.Error())
			}
		}
		if fileCfg.QueryLog.DataInliningRowLimit > 0 {
			cfg.QueryLog.DataInliningRowLimit = fileCfg.QueryLog.DataInliningRowLimit
		}

		if fileCfg.TLS.ACME.Domain != "" {
			cfg.ACMEDomain = fileCfg.TLS.ACME.Domain
		}
		if fileCfg.TLS.ACME.Email != "" {
			cfg.ACMEEmail = fileCfg.TLS.ACME.Email
		}
		if fileCfg.TLS.ACME.CacheDir != "" {
			cfg.ACMECacheDir = fileCfg.TLS.ACME.CacheDir
		}
		if fileCfg.TLS.ACME.DNSProvider != "" {
			cfg.ACMEDNSProvider = fileCfg.TLS.ACME.DNSProvider
		}
		if fileCfg.TLS.ACME.DNSZoneID != "" {
			cfg.ACMEDNSZoneID = fileCfg.TLS.ACME.DNSZoneID
		}

		if fileCfg.WorkerBackend != "" {
			workerBackend = fileCfg.WorkerBackend
		}
		if fileCfg.K8s.WorkerImage != "" {
			k8sWorkerImage = fileCfg.K8s.WorkerImage
		}
		if fileCfg.K8s.WorkerNamespace != "" {
			k8sWorkerNamespace = fileCfg.K8s.WorkerNamespace
		}
		if fileCfg.K8s.ControlPlaneID != "" {
			k8sControlPlaneID = fileCfg.K8s.ControlPlaneID
		}
		if fileCfg.K8s.WorkerPort != 0 {
			k8sWorkerPort = fileCfg.K8s.WorkerPort
		}
		if fileCfg.K8s.WorkerSecret != "" {
			k8sWorkerSecret = fileCfg.K8s.WorkerSecret
		}
		if fileCfg.K8s.WorkerConfigMap != "" {
			k8sWorkerConfigMap = fileCfg.K8s.WorkerConfigMap
		}
		if fileCfg.K8s.WorkerImagePullPolicy != "" {
			k8sWorkerImagePullPolicy = fileCfg.K8s.WorkerImagePullPolicy
		}
		if fileCfg.K8s.WorkerServiceAccount != "" {
			k8sWorkerServiceAccount = fileCfg.K8s.WorkerServiceAccount
		}
	}

	if v := getenv("DUCKGRES_HOST"); v != "" {
		cfg.Host = v
	}
	if v := getenv("DUCKGRES_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.Port = p
		}
	}
	if v := getenv("DUCKGRES_FLIGHT_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.FlightPort = p
		} else {
			warn("Invalid DUCKGRES_FLIGHT_PORT: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_FLIGHT_SESSION_IDLE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.FlightSessionIdleTTL = d
		} else {
			warn("Invalid DUCKGRES_FLIGHT_SESSION_IDLE_TTL duration: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_FLIGHT_SESSION_REAP_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.FlightSessionReapInterval = d
		} else {
			warn("Invalid DUCKGRES_FLIGHT_SESSION_REAP_INTERVAL duration: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_FLIGHT_HANDLE_IDLE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.FlightHandleIdleTTL = d
		} else {
			warn("Invalid DUCKGRES_FLIGHT_HANDLE_IDLE_TTL duration: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_FLIGHT_SESSION_TOKEN_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.FlightSessionTokenTTL = d
		} else {
			warn("Invalid DUCKGRES_FLIGHT_SESSION_TOKEN_TTL duration: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := getenv("DUCKGRES_CERT"); v != "" {
		cfg.TLSCertFile = v
	}
	if v := getenv("DUCKGRES_KEY"); v != "" {
		cfg.TLSKeyFile = v
	}
	if v := getenv("DUCKGRES_DUCKLAKE_METADATA_STORE"); v != "" {
		cfg.DuckLake.MetadataStore = v
	}
	if v := getenv("DUCKGRES_DUCKLAKE_OBJECT_STORE"); v != "" {
		cfg.DuckLake.ObjectStore = v
	}
	if v := getenv("DUCKGRES_DUCKLAKE_S3_PROVIDER"); v != "" {
		cfg.DuckLake.S3Provider = v
	}
	if v := getenv("DUCKGRES_DUCKLAKE_S3_ENDPOINT"); v != "" {
		cfg.DuckLake.S3Endpoint = v
	}
	if v := getenv("DUCKGRES_DUCKLAKE_S3_ACCESS_KEY"); v != "" {
		cfg.DuckLake.S3AccessKey = v
	}
	if v := getenv("DUCKGRES_DUCKLAKE_S3_SECRET_KEY"); v != "" {
		cfg.DuckLake.S3SecretKey = v
	}
	if v := getenv("DUCKGRES_DUCKLAKE_S3_REGION"); v != "" {
		cfg.DuckLake.S3Region = v
	}
	if v := getenv("DUCKGRES_DUCKLAKE_S3_USE_SSL"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.DuckLake.S3UseSSL = b
		} else {
			warn("Invalid DUCKGRES_DUCKLAKE_S3_USE_SSL: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_DUCKLAKE_S3_URL_STYLE"); v != "" {
		cfg.DuckLake.S3URLStyle = v
	}
	if v := getenv("DUCKGRES_DUCKLAKE_S3_CHAIN"); v != "" {
		cfg.DuckLake.S3Chain = v
	}
	if v := getenv("DUCKGRES_DUCKLAKE_S3_PROFILE"); v != "" {
		cfg.DuckLake.S3Profile = v
	}
	if v := getenv("DUCKGRES_PROCESS_ISOLATION"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.ProcessIsolation = b
		} else {
			warn("Invalid DUCKGRES_PROCESS_ISOLATION: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_IDLE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.IdleTimeout = d
		} else {
			warn("Invalid DUCKGRES_IDLE_TIMEOUT duration: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_MEMORY_LIMIT"); v != "" {
		cfg.MemoryLimit = v
	}
	if v := getenv("DUCKGRES_THREADS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Threads = n
		} else {
			warn("Invalid DUCKGRES_THREADS: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_MEMORY_BUDGET"); v != "" {
		cfg.MemoryBudget = v
	}
	if v := getenv("DUCKGRES_MEMORY_REBALANCE"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.MemoryRebalance = b
		} else {
			warn("Invalid DUCKGRES_MEMORY_REBALANCE: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_MIN_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MinWorkers = n
		} else {
			warn("Invalid DUCKGRES_MIN_WORKERS: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_MAX_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxWorkers = n
		} else {
			warn("Invalid DUCKGRES_MAX_WORKERS: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_WORKER_QUEUE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			workerQueueTimeout = d
		} else {
			warn("Invalid DUCKGRES_WORKER_QUEUE_TIMEOUT duration: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_WORKER_IDLE_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			workerIdleTimeout = d
		} else {
			warn("Invalid DUCKGRES_WORKER_IDLE_TIMEOUT duration: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_HANDOVER_DRAIN_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			handoverDrainTimeout = d
		} else {
			warn("Invalid DUCKGRES_HANDOVER_DRAIN_TIMEOUT duration: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_ACME_DOMAIN"); v != "" {
		cfg.ACMEDomain = v
	}
	if v := getenv("DUCKGRES_ACME_EMAIL"); v != "" {
		cfg.ACMEEmail = v
	}
	if v := getenv("DUCKGRES_ACME_CACHE_DIR"); v != "" {
		cfg.ACMECacheDir = v
	}
	if v := getenv("DUCKGRES_ACME_DNS_PROVIDER"); v != "" {
		cfg.ACMEDNSProvider = v
	}
	if v := getenv("DUCKGRES_ACME_DNS_ZONE_ID"); v != "" {
		cfg.ACMEDNSZoneID = v
	}
	if v := getenv("DUCKGRES_MAX_CONNECTIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.RateLimit.MaxConnections = n
		} else {
			warn("Invalid DUCKGRES_MAX_CONNECTIONS: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_CONFIG_STORE"); v != "" {
		configStoreConn = v
	}
	if v := getenv("DUCKGRES_CONFIG_POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			configPollInterval = d
		} else {
			warn("Invalid DUCKGRES_CONFIG_POLL_INTERVAL duration: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_ADMIN_TOKEN"); v != "" {
		adminToken = v
	}
	if v := getenv("DUCKGRES_WORKER_BACKEND"); v != "" {
		workerBackend = v
	}
	if v := getenv("DUCKGRES_K8S_WORKER_IMAGE"); v != "" {
		k8sWorkerImage = v
	}
	if v := getenv("DUCKGRES_K8S_WORKER_NAMESPACE"); v != "" {
		k8sWorkerNamespace = v
	}
	if v := getenv("DUCKGRES_K8S_CONTROL_PLANE_ID"); v != "" {
		k8sControlPlaneID = v
	}
	if v := getenv("DUCKGRES_K8S_WORKER_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			k8sWorkerPort = n
		} else {
			warn("Invalid DUCKGRES_K8S_WORKER_PORT: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_K8S_WORKER_SECRET"); v != "" {
		k8sWorkerSecret = v
	}
	if v := getenv("DUCKGRES_K8S_WORKER_CONFIGMAP"); v != "" {
		k8sWorkerConfigMap = v
	}
	if v := getenv("DUCKGRES_K8S_WORKER_IMAGE_PULL_POLICY"); v != "" {
		k8sWorkerImagePullPolicy = v
	}
	if v := getenv("DUCKGRES_K8S_WORKER_SERVICE_ACCOUNT"); v != "" {
		k8sWorkerServiceAccount = v
	}

	// Query log env vars
	if v := getenv("DUCKGRES_QUERY_LOG_ENABLED"); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			cfg.QueryLog.Enabled = b
		} else {
			warn("Invalid DUCKGRES_QUERY_LOG_ENABLED: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_QUERY_LOG_FLUSH_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.QueryLog.FlushInterval = d
		} else {
			warn("Invalid DUCKGRES_QUERY_LOG_FLUSH_INTERVAL duration: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_QUERY_LOG_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.QueryLog.BatchSize = n
		} else {
			warn("Invalid DUCKGRES_QUERY_LOG_BATCH_SIZE: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_QUERY_LOG_COMPACT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.QueryLog.CompactInterval = d
		} else {
			warn("Invalid DUCKGRES_QUERY_LOG_COMPACT_INTERVAL duration: " + err.Error())
		}
	}
	if v := getenv("DUCKGRES_QUERY_LOG_DATA_INLINING_ROW_LIMIT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.QueryLog.DataInliningRowLimit = n
		} else {
			warn("Invalid DUCKGRES_QUERY_LOG_DATA_INLINING_ROW_LIMIT: " + err.Error())
		}
	}

	if cli.Set["host"] {
		cfg.Host = cli.Host
	}
	if cli.Set["port"] {
		cfg.Port = cli.Port
	}
	if cli.Set["flight-port"] {
		cfg.FlightPort = cli.FlightPort
	}
	if cli.Set["flight-session-idle-ttl"] {
		if d, err := time.ParseDuration(cli.FlightSessionIdleTTL); err == nil {
			cfg.FlightSessionIdleTTL = d
		} else {
			warn("Invalid --flight-session-idle-ttl duration: " + err.Error())
		}
	}
	if cli.Set["flight-session-reap-interval"] {
		if d, err := time.ParseDuration(cli.FlightSessionReapInterval); err == nil {
			cfg.FlightSessionReapInterval = d
		} else {
			warn("Invalid --flight-session-reap-interval duration: " + err.Error())
		}
	}
	if cli.Set["flight-handle-idle-ttl"] {
		if d, err := time.ParseDuration(cli.FlightHandleIdleTTL); err == nil {
			cfg.FlightHandleIdleTTL = d
		} else {
			warn("Invalid --flight-handle-idle-ttl duration: " + err.Error())
		}
	}
	if cli.Set["flight-session-token-ttl"] {
		if d, err := time.ParseDuration(cli.FlightSessionTokenTTL); err == nil {
			cfg.FlightSessionTokenTTL = d
		} else {
			warn("Invalid --flight-session-token-ttl duration: " + err.Error())
		}
	}
	if cli.Set["data-dir"] {
		cfg.DataDir = cli.DataDir
	}
	if cli.Set["cert"] {
		cfg.TLSCertFile = cli.CertFile
	}
	if cli.Set["key"] {
		cfg.TLSKeyFile = cli.KeyFile
	}
	if cli.Set["process-isolation"] {
		cfg.ProcessIsolation = cli.ProcessIsolation
	}
	if cli.Set["idle-timeout"] {
		if d, err := time.ParseDuration(cli.IdleTimeout); err == nil {
			cfg.IdleTimeout = d
		} else {
			warn("Invalid --idle-timeout duration: " + err.Error())
		}
	}
	if cli.Set["memory-limit"] {
		cfg.MemoryLimit = cli.MemoryLimit
	}
	if cli.Set["threads"] {
		cfg.Threads = cli.Threads
	}
	if cli.Set["memory-budget"] {
		cfg.MemoryBudget = cli.MemoryBudget
	}
	if cli.Set["memory-rebalance"] {
		cfg.MemoryRebalance = cli.MemoryRebalance
	}
	if cli.Set["min-workers"] {
		cfg.MinWorkers = cli.MinWorkers
	}
	if cli.Set["max-workers"] {
		cfg.MaxWorkers = cli.MaxWorkers
	}
	if cli.Set["worker-queue-timeout"] {
		if d, err := time.ParseDuration(cli.WorkerQueueTimeout); err == nil {
			workerQueueTimeout = d
		} else {
			warn("Invalid --worker-queue-timeout duration: " + err.Error())
		}
	}
	if cli.Set["worker-idle-timeout"] {
		if d, err := time.ParseDuration(cli.WorkerIdleTimeout); err == nil {
			workerIdleTimeout = d
		} else {
			warn("Invalid --worker-idle-timeout duration: " + err.Error())
		}
	}
	if cli.Set["handover-drain-timeout"] {
		if d, err := time.ParseDuration(cli.HandoverDrainTimeout); err == nil {
			handoverDrainTimeout = d
		} else {
			warn("Invalid --handover-drain-timeout duration: " + err.Error())
		}
	}
	if cli.Set["acme-domain"] {
		cfg.ACMEDomain = cli.ACMEDomain
	}
	if cli.Set["acme-email"] {
		cfg.ACMEEmail = cli.ACMEEmail
	}
	if cli.Set["acme-cache-dir"] {
		cfg.ACMECacheDir = cli.ACMECacheDir
	}
	if cli.Set["acme-dns-provider"] {
		cfg.ACMEDNSProvider = cli.ACMEDNSProvider
	}
	if cli.Set["acme-dns-zone-id"] {
		cfg.ACMEDNSZoneID = cli.ACMEDNSZoneID
	}
	if cli.Set["max-connections"] {
		cfg.RateLimit.MaxConnections = cli.MaxConnections
	}
	if cli.Set["config-store"] {
		configStoreConn = cli.ConfigStoreConn
	}
	if cli.Set["config-poll-interval"] {
		if d, err := time.ParseDuration(cli.ConfigPollInterval); err == nil {
			configPollInterval = d
		} else {
			warn("Invalid --config-poll-interval duration: " + err.Error())
		}
	}
	if cli.Set["admin-token"] {
		adminToken = cli.AdminToken
	}
	if cli.Set["worker-backend"] {
		workerBackend = cli.WorkerBackend
	}
	if cli.Set["k8s-worker-image"] {
		k8sWorkerImage = cli.K8sWorkerImage
	}
	if cli.Set["k8s-worker-namespace"] {
		k8sWorkerNamespace = cli.K8sWorkerNamespace
	}
	if cli.Set["k8s-control-plane-id"] {
		k8sControlPlaneID = cli.K8sControlPlaneID
	}
	if cli.Set["k8s-worker-port"] {
		k8sWorkerPort = cli.K8sWorkerPort
	}
	if cli.Set["k8s-worker-secret"] {
		k8sWorkerSecret = cli.K8sWorkerSecret
	}
	if cli.Set["k8s-worker-configmap"] {
		k8sWorkerConfigMap = cli.K8sWorkerConfigMap
	}
	if cli.Set["k8s-worker-image-pull-policy"] {
		k8sWorkerImagePullPolicy = cli.K8sWorkerImagePullPolicy
	}
	if cli.Set["k8s-worker-service-account"] {
		k8sWorkerServiceAccount = cli.K8sWorkerServiceAccount
	}
	if cli.Set["query-log"] {
		cfg.QueryLog.Enabled = cli.QueryLog
	}

	if cfg.ACMEDNSProvider != "" {
		provider := strings.ToLower(cfg.ACMEDNSProvider)
		if provider != "route53" {
			warn("Unsupported ACME DNS provider: " + cfg.ACMEDNSProvider + " (only 'route53' is supported); disabling DNS-01")
			cfg.ACMEDNSProvider = ""
			cfg.ACMEDNSZoneID = ""
		} else {
			cfg.ACMEDNSProvider = provider
			if cfg.ACMEDomain == "" {
				warn("ACME DNS provider is set but ACME domain is empty; disabling DNS-01")
				cfg.ACMEDNSProvider = ""
				cfg.ACMEDNSZoneID = ""
			} else if cfg.ACMEDNSZoneID == "" {
				warn("ACME DNS provider 'route53' requires ACME DNS zone ID; disabling DNS-01")
				cfg.ACMEDNSProvider = ""
			}
		}
	} else if cfg.ACMEDNSZoneID != "" {
		warn("ACME DNS zone ID is set without ACME DNS provider; ignoring zone ID")
		cfg.ACMEDNSZoneID = ""
	}

	// Validate memory_limit format if explicitly set
	if cfg.MemoryLimit != "" && !server.ValidateMemoryLimit(cfg.MemoryLimit) {
		warn("Invalid memory_limit format: " + cfg.MemoryLimit + " (expected e.g. '4GB', '512MB')")
		cfg.MemoryLimit = "" // fall back to auto-detection
	}

	// Validate memory_budget format if explicitly set
	if cfg.MemoryBudget != "" && !server.ValidateMemoryLimit(cfg.MemoryBudget) {
		warn("Invalid memory_budget format: " + cfg.MemoryBudget + " (expected e.g. '24GB', '512MB')")
		cfg.MemoryBudget = "" // fall back to auto-detection
	}

	if cfg.QueryLog.FlushInterval <= 0 {
		warn("DUCKGRES_QUERY_LOG_FLUSH_INTERVAL must be > 0; using default")
		cfg.QueryLog.FlushInterval = defaultQueryLog.FlushInterval
	}
	if cfg.QueryLog.BatchSize <= 0 {
		warn("DUCKGRES_QUERY_LOG_BATCH_SIZE must be > 0; using default")
		cfg.QueryLog.BatchSize = defaultQueryLog.BatchSize
	}
	if cfg.QueryLog.CompactInterval <= 0 {
		warn("DUCKGRES_QUERY_LOG_COMPACT_INTERVAL must be > 0; using default")
		cfg.QueryLog.CompactInterval = defaultQueryLog.CompactInterval
	}

	return resolvedConfig{
		Server:                   cfg,
		WorkerQueueTimeout:       workerQueueTimeout,
		WorkerIdleTimeout:        workerIdleTimeout,
		HandoverDrainTimeout:     handoverDrainTimeout,
		WorkerBackend:            workerBackend,
		K8sWorkerImage:           k8sWorkerImage,
		K8sWorkerNamespace:       k8sWorkerNamespace,
		K8sControlPlaneID:        k8sControlPlaneID,
		K8sWorkerPort:            k8sWorkerPort,
		K8sWorkerSecret:          k8sWorkerSecret,
		K8sWorkerConfigMap:       k8sWorkerConfigMap,
		K8sWorkerImagePullPolicy: k8sWorkerImagePullPolicy,
		K8sWorkerServiceAccount:  k8sWorkerServiceAccount,
		ConfigStoreConn:          configStoreConn,
		ConfigPollInterval:       configPollInterval,
		AdminToken:               adminToken,
	}
}
