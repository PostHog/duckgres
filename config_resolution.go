package main

import (
	"strconv"
	"time"

	"github.com/posthog/duckgres/server"
)

type configCLIInputs struct {
	Set map[string]bool

	Host             string
	Port             int
	FlightPort       int
	DataDir          string
	CertFile         string
	KeyFile          string
	ProcessIsolation bool
	IdleTimeout      string
	MemoryLimit      string
	Threads          int
	MemoryBudget     string
	MemoryRebalance  bool
	MaxWorkers       int
	MinWorkers       int
	ACMEDomain       string
	ACMEEmail        string
	ACMECacheDir     string
}

type resolvedConfig struct {
	Server server.Config
}

func defaultServerConfig() server.Config {
	return server.Config{
		Host:        "0.0.0.0",
		Port:        5432,
		FlightPort:  0,
		DataDir:     "./data",
		TLSCertFile: "./certs/server.crt",
		TLSKeyFile:  "./certs/server.key",
		Users: map[string]string{
			"postgres": "postgres",
		},
		Extensions: []string{"ducklake"},
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
		if len(fileCfg.PassthroughUsers) > 0 {
			cfg.PassthroughUsers = make(map[string]bool, len(fileCfg.PassthroughUsers))
			for _, u := range fileCfg.PassthroughUsers {
				cfg.PassthroughUsers[u] = true
			}
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
	if v := getenv("DUCKGRES_ACME_DOMAIN"); v != "" {
		cfg.ACMEDomain = v
	}
	if v := getenv("DUCKGRES_ACME_EMAIL"); v != "" {
		cfg.ACMEEmail = v
	}
	if v := getenv("DUCKGRES_ACME_CACHE_DIR"); v != "" {
		cfg.ACMECacheDir = v
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
	if cli.Set["acme-domain"] {
		cfg.ACMEDomain = cli.ACMEDomain
	}
	if cli.Set["acme-email"] {
		cfg.ACMEEmail = cli.ACMEEmail
	}
	if cli.Set["acme-cache-dir"] {
		cfg.ACMECacheDir = cli.ACMECacheDir
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

	return resolvedConfig{
		Server: cfg,
	}
}
