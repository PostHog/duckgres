package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/posthog/duckgres/server"
	"gopkg.in/yaml.v3"
)

// FileConfig represents the YAML configuration file structure
type FileConfig struct {
	Host      string              `yaml:"host"`
	Port      int                 `yaml:"port"`
	DataDir   string              `yaml:"data_dir"`
	TLS       TLSConfig           `yaml:"tls"`
	Users     map[string]string   `yaml:"users"`
	RateLimit RateLimitFileConfig `yaml:"rate_limit"`
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

// envInt returns the environment variable as int or a default
func envInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func main() {
	// Define CLI flags with environment variable fallbacks
	configFile := flag.String("config", env("DUCKGRES_CONFIG", ""), "Path to YAML config file (env: DUCKGRES_CONFIG)")
	host := flag.String("host", "", "Host to bind to (env: DUCKGRES_HOST)")
	port := flag.Int("port", 0, "Port to listen on (env: DUCKGRES_PORT)")
	dataDir := flag.String("data-dir", "", "Directory for DuckDB files (env: DUCKGRES_DATA_DIR)")
	certFile := flag.String("cert", "", "TLS certificate file (env: DUCKGRES_CERT)")
	keyFile := flag.String("key", "", "TLS private key file (env: DUCKGRES_KEY)")
	showHelp := flag.Bool("help", false, "Show help message")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Duckgres - PostgreSQL wire protocol server for DuckDB\n\n")
		fmt.Fprintf(os.Stderr, "Usage: duckgres [options]\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nEnvironment variables:\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_CONFIG    Path to YAML config file\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_HOST      Host to bind to (default: 0.0.0.0)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_PORT      Port to listen on (default: 5432)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_DATA_DIR  Directory for DuckDB files (default: ./data)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_CERT      TLS certificate file (default: ./certs/server.crt)\n")
		fmt.Fprintf(os.Stderr, "  DUCKGRES_KEY       TLS private key file (default: ./certs/server.key)\n")
		fmt.Fprintf(os.Stderr, "\nPrecedence: CLI flags > environment variables > config file > defaults\n")
	}

	flag.Parse()

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	// Start with defaults
	cfg := server.Config{
		Host:        "0.0.0.0",
		Port:        5432,
		DataDir:     "./data",
		TLSCertFile: "./certs/server.crt",
		TLSKeyFile:  "./certs/server.key",
		Users: map[string]string{
			"postgres": "postgres",
		},
	}

	// Load config file if specified
	if *configFile != "" {
		fileCfg, err := loadConfigFile(*configFile)
		if err != nil {
			log.Fatalf("Failed to load config file: %v", err)
		}
		log.Printf("Loaded configuration from %s", *configFile)

		// Apply config file values
		if fileCfg.Host != "" {
			cfg.Host = fileCfg.Host
		}
		if fileCfg.Port != 0 {
			cfg.Port = fileCfg.Port
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

		// Apply rate limit config
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
				log.Printf("Warning: invalid failed_attempt_window duration: %v", err)
			}
		}
		if fileCfg.RateLimit.BanDuration != "" {
			if d, err := time.ParseDuration(fileCfg.RateLimit.BanDuration); err == nil {
				cfg.RateLimit.BanDuration = d
			} else {
				log.Printf("Warning: invalid ban_duration duration: %v", err)
			}
		}
	}

	// Apply environment variables (override config file)
	if v := os.Getenv("DUCKGRES_HOST"); v != "" {
		cfg.Host = v
	}
	if v := os.Getenv("DUCKGRES_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			cfg.Port = p
		}
	}
	if v := os.Getenv("DUCKGRES_DATA_DIR"); v != "" {
		cfg.DataDir = v
	}
	if v := os.Getenv("DUCKGRES_CERT"); v != "" {
		cfg.TLSCertFile = v
	}
	if v := os.Getenv("DUCKGRES_KEY"); v != "" {
		cfg.TLSKeyFile = v
	}

	// Apply CLI flags (highest priority)
	if *host != "" {
		cfg.Host = *host
	}
	if *port != 0 {
		cfg.Port = *port
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if *certFile != "" {
		cfg.TLSCertFile = *certFile
	}
	if *keyFile != "" {
		cfg.TLSKeyFile = *keyFile
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Auto-generate self-signed certificates if they don't exist
	if err := server.EnsureCertificates(cfg.TLSCertFile, cfg.TLSKeyFile); err != nil {
		log.Fatalf("Failed to ensure TLS certificates: %v", err)
	}
	log.Printf("Using TLS certificates: %s, %s", cfg.TLSCertFile, cfg.TLSKeyFile)

	srv, err := server.New(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		srv.Close()
		os.Exit(0)
	}()

	log.Printf("Starting Duckgres server on %s:%d (TLS required)", cfg.Host, cfg.Port)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
