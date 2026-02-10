package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/posthog/duckgres/controlplane"
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
	showHelp := flag.Bool("help", false, "Show help message")

	// Control plane flags
	mode := flag.String("mode", "standalone", "Run mode: standalone (default), control-plane, or worker")
	workerCount := flag.Int("worker-count", 4, "Number of worker processes (control-plane mode)")
	socketDir := flag.String("socket-dir", "/var/run/duckgres", "Unix socket directory (control-plane mode)")
	handoverSocket := flag.String("handover-socket", "", "Handover socket for graceful deployment (control-plane mode)")
	flightPort := flag.Int("flight-port", 0, "Flight SQL port to listen on (control-plane mode, env: DUCKGRES_FLIGHT_PORT)")
	grpcSocket := flag.String("grpc-socket", "", "gRPC socket path (worker mode, set by control-plane)")
	fdSocket := flag.String("fd-socket", "", "FD passing socket path (worker mode, set by control-plane)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Duckgres - PostgreSQL wire protocol server for DuckDB\n\n")
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
		fmt.Fprintf(os.Stderr, "  DUCKGRES_FLIGHT_PORT        Flight SQL port (control-plane mode, default: 8815)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_PROCESS_ISOLATION  Enable process isolation (1 or true)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_IDLE_TIMEOUT       Connection idle timeout (e.g., 30m, 1h, -1 to disable)\n")
		fmt.Fprintf(os.Stderr, "\nPrecedence: CLI flags > environment variables > config file > defaults\n")
	}

	flag.Parse()

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
		FlightPort:       *flightPort,
	}, os.Getenv, func(msg string) {
		slog.Warn(msg)
	})
	cfg := resolved.Server
	flightCfgPort := resolved.FlightPort

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

	// Handle worker mode early (before metrics, certs, etc.)
	if *mode == "worker" {
		if *grpcSocket == "" || *fdSocket == "" {
			fatal("Worker mode requires --grpc-socket and --fd-socket flags")
		}
		controlplane.RunWorker(*grpcSocket, *fdSocket)
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
		effectiveFlightPort := effectiveFlightConfig(cfg, flightCfgPort)

		cpCfg := controlplane.ControlPlaneConfig{
			Config:         cfg,
			WorkerCount:    *workerCount,
			SocketDir:      *socketDir,
			HandoverSocket: *handoverSocket,
			FlightPort:     effectiveFlightPort,
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

	slog.Info("Starting Duckgres server (TLS required)", "host", cfg.Host, "port", cfg.Port)
	if err := srv.ListenAndServe(); err != nil {
		fatal("Server error: " + err.Error())
	}
}
