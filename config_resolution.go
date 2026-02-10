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
	DataDir          string
	CertFile         string
	KeyFile          string
	ProcessIsolation bool
	IdleTimeout      string
	FlightHost       string
	FlightPort       int
}

type resolvedConfig struct {
	Server     server.Config
	FlightHost string
	FlightPort int
}

func defaultServerConfig() server.Config {
	return server.Config{
		Host:        "0.0.0.0",
		Port:        5432,
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
	flightHost := ""
	flightPort := 0

	if fileCfg != nil {
		if fileCfg.Host != "" {
			cfg.Host = fileCfg.Host
		}
		if fileCfg.Port != 0 {
			cfg.Port = fileCfg.Port
		}
		if fileCfg.Flight.Host != "" {
			flightHost = fileCfg.Flight.Host
		}
		if fileCfg.Flight.Port != 0 {
			flightPort = fileCfg.Flight.Port
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
	}

	if v := getenv("DUCKGRES_HOST"); v != "" {
		cfg.Host = v
	}
	if v := getenv("DUCKGRES_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.Port = p
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
	if v := getenv("DUCKGRES_FLIGHT_HOST"); v != "" {
		flightHost = v
	}
	if v := getenv("DUCKGRES_FLIGHT_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			flightPort = p
		} else {
			warn("Invalid DUCKGRES_FLIGHT_PORT: " + err.Error())
		}
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

	if cli.Set["host"] {
		cfg.Host = cli.Host
	}
	if cli.Set["port"] {
		cfg.Port = cli.Port
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
	if cli.Set["flight-host"] {
		flightHost = cli.FlightHost
	}
	if cli.Set["flight-port"] {
		flightPort = cli.FlightPort
	}

	return resolvedConfig{
		Server:     cfg,
		FlightHost: flightHost,
		FlightPort: flightPort,
	}
}

func effectiveFlightConfig(cfg server.Config, flightHost string, flightPort int) (string, int) {
	if flightHost == "" {
		flightHost = cfg.Host
	}
	if flightPort == 0 {
		flightPort = 8815
	}
	return flightHost, flightPort
}
