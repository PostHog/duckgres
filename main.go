package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/posthog/duckgres/controlplane"
	"github.com/posthog/duckgres/duckdbservice"
	"github.com/posthog/duckgres/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"
)

// FileConfig represents the YAML configuration file structure
type FileConfig struct {
	Host             string              `yaml:"host"`
	Port             int                 `yaml:"port"`
	Flight           FlightFileConfig    `yaml:"flight"`
	DataDir          string              `yaml:"data_dir"`
	TLS              TLSConfig           `yaml:"tls"`
	Users            map[string]string   `yaml:"users"`
	RateLimit        RateLimitFileConfig `yaml:"rate_limit"`
	Extensions       []string            `yaml:"extensions"`
	DuckLake         DuckLakeFileConfig  `yaml:"ducklake"`
	ProcessIsolation bool                `yaml:"process_isolation"` // Enable process isolation per connection
	IdleTimeout      string              `yaml:"idle_timeout"`      // e.g., "24h", "1h", "-1" to disable
}

type FlightFileConfig struct {
	Port int    `yaml:"port"`
}

type TLSConfig struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

type RateLimitFileConfig struct {
	MaxFailedAttempts   int    `yaml:"max_failed_attempts"`
	FailedAttemptWindow string `yaml:"failed_attempt_window"` // e.g., "5m"
	BanDuration         string `yaml:"ban_duration"`          // e.g., "15m"
	MaxConnectionsPerIP int    `yaml:"max_connections_per_ip"`
}

type DuckLakeFileConfig struct {
	MetadataStore string `yaml:"metadata_store"` // e.g., "postgres:host=localhost user=ducklake password=secret dbname=ducklake"
	ObjectStore   string `yaml:"object_store"`   // e.g., "s3://bucket/path/" for S3/MinIO storage
	DataPath      string `yaml:"data_path"`      // Local file path for data storage (alternative to object_store)

	// S3 credential provider: "config" (explicit) or "credential_chain" (AWS SDK)
	S3Provider string `yaml:"s3_provider"`

	// Config provider settings (explicit credentials)
	S3Endpoint  string `yaml:"s3_endpoint"`   // e.g., "localhost:9000" for MinIO
	S3AccessKey string `yaml:"s3_access_key"` // S3 access key ID
	S3SecretKey string `yaml:"s3_secret_key"` // S3 secret access key
	S3Region    string `yaml:"s3_region"`     // S3 region (default: us-east-1)
	S3UseSSL    bool   `yaml:"s3_use_ssl"`    // Use HTTPS for S3 connections
	S3URLStyle  string `yaml:"s3_url_style"`  // "path" or "vhost" (default: path)

	// Credential chain provider settings (AWS SDK credential chain)
	S3Chain   string `yaml:"s3_chain"`   // e.g., "env;config" - which credential sources to check
	S3Profile string `yaml:"s3_profile"` // AWS profile name for config chain
}

// loadConfigFile loads configuration from a YAML file
func loadConfigFile(path string) (*FileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg FileConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	return &cfg, nil
}

// env returns the environment variable value or a default
func env(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// initMetrics starts the Prometheus metrics HTTP server on :9090/metrics
func initMetrics() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		slog.Info("Starting metrics server", "addr", ":9090")
		if err := http.ListenAndServe(":9090", nil); err != nil {
			slog.Error("Metrics server error", "error", err)
		}
	}()
}

func main() {
	// Check if we're running as a child worker process
	if os.Getenv("DUCKGRES_CHILD_MODE") == "1" {
		// Use the same logging setup as parent for consistent log format
		loggingShutdown := initLogging()
		defer loggingShutdown()
		server.RunChildMode()
		return // RunChildMode calls os.Exit
	}

	// Define CLI flags with environment variable fallbacks
	configFile := flag.String("config", env("DUCKGRES_CONFIG", ""), "Path to YAML config file (env: DUCKGRES_CONFIG)")
	host := flag.String("host", "", "Host to bind to (env: DUCKGRES_HOST)")
	port := flag.Int("port", 0, "Port to listen on (env: DUCKGRES_PORT)")
	dataDir := flag.String("data-dir", "", "Directory for DuckDB files (env: DUCKGRES_DATA_DIR)")
	certFile := flag.String("cert", "", "TLS certificate file (env: DUCKGRES_CERT)")
	keyFile := flag.String("key", "", "TLS private key file (env: DUCKGRES_KEY)")
	processIsolation := flag.Bool("process-isolation", false, "Enable process isolation (spawn child process per connection)")
	idleTimeout := flag.String("idle-timeout", "", "Connection idle timeout (e.g., '30m', '1h', '-1' to disable) (env: DUCKGRES_IDLE_TIMEOUT)")
	repl := flag.Bool("repl", false, "Start an interactive SQL shell instead of the server")
	psql := flag.Bool("psql", false, "Launch psql connected to the local Duckgres server")
	showVersion := flag.Bool("version", false, "Show version and exit")
	showHelp := flag.Bool("help", false, "Show help message")

	// Control plane flags
	mode := flag.String("mode", "standalone", "Run mode: standalone, control-plane, or duckdb-service")
	workerCount := flag.Int("worker-count", 4, "Number of worker processes (control-plane mode)")
	socketDir := flag.String("socket-dir", "/var/run/duckgres", "Unix socket directory (control-plane mode)")
	handoverSocket := flag.String("handover-socket", "", "Handover socket for graceful deployment (control-plane mode)")

	// DuckDB service flags
	duckdbListen := flag.String("duckdb-listen", "", "DuckDB service listen address (duckdb-service mode, env: DUCKGRES_DUCKDB_LISTEN)")
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
		fmt.Fprintf(os.Stderr, "  DUCKGRES_DATA_DIR           Directory for DuckDB files (default: ./data)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_CERT               TLS certificate file (default: ./certs/server.crt)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_KEY                TLS private key file (default: ./certs/server.key)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_PROCESS_ISOLATION  Enable process isolation (1 or true)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_IDLE_TIMEOUT       Connection idle timeout (e.g., 30m, 1h, -1 to disable)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_DUCKDB_LISTEN      DuckDB service listen address (duckdb-service mode)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_DUCKDB_TOKEN       DuckDB service bearer token (duckdb-service mode)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_DUCKDB_MAX_SESSIONS  DuckDB service max sessions (duckdb-service mode)\n")
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

	loggingShutdown := initLogging()
	defer loggingShutdown()

	fatal := func(msg string) {
		slog.Error(msg)
		loggingShutdown()
		os.Exit(1)
	}

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	// Auto-detect duckgres.yaml if no config file was explicitly specified
	if *configFile == "" {
		if _, err := os.Stat("duckgres.yaml"); err == nil {
			*configFile = "duckgres.yaml"
		}
	}

	var fileCfg *FileConfig
	// Load config file if specified (or auto-detected)
	if *configFile != "" {
		loadedCfg, err := loadConfigFile(*configFile)
		if err != nil {
			slog.Error("Failed to load config file: " + err.Error())
			os.Exit(1)
		}
		slog.Info("Loaded configuration from " + *configFile)
		fileCfg = loadedCfg
	}

	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{
		Set:              cliSet,
		Host:             *host,
		Port:             *port,
		DataDir:          *dataDir,
		CertFile:         *certFile,
		KeyFile:          *keyFile,
		ProcessIsolation: *processIsolation,
		IdleTimeout:      *idleTimeout,
	}, os.Getenv, func(msg string) {
		slog.Warn(msg)
	})
	cfg := resolved.Server

	// Handle --psql: launch psql connected to the local Duckgres server
	if *psql {
		// Pick the first user from the config
		var user, password string
		for u, p := range cfg.Users {
			user, password = u, p
			break
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
		if listenAddr == "" {
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

		initMetrics()

		duckdbservice.Run(duckdbservice.ServiceConfig{
			ListenAddr:   listenAddr,
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

	initMetrics()

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		fatal("Failed to create data directory: " + err.Error())
	}

	// Auto-generate self-signed certificates if they don't exist
	if err := server.EnsureCertificates(cfg.TLSCertFile, cfg.TLSKeyFile); err != nil {
		fatal("Failed to ensure TLS certificates: " + err.Error())
	}
	slog.Info("Using TLS certificates", "cert_file", cfg.TLSCertFile, "key_file", cfg.TLSKeyFile)

	// Handle control-plane mode
	if *mode == "control-plane" {
		cpCfg := controlplane.ControlPlaneConfig{
			Config:         cfg,
			WorkerCount:    *workerCount,
			SocketDir:      *socketDir,
			ConfigPath:     *configFile,
			HandoverSocket: *handoverSocket,
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
		_ = srv.Close()
		loggingShutdown()
		os.Exit(0)
	}()

	slog.Info("Starting Duckgres server (TLS required)", "version", version, "host", cfg.Host, "port", cfg.Port)
	if err := srv.ListenAndServe(); err != nil {
		fatal("Server error: " + err.Error())
	}
}
