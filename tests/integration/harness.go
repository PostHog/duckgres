package integration

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/posthog/duckgres/server"
	_ "github.com/lib/pq"
)

// TestHarness manages PostgreSQL and Duckgres instances for side-by-side testing
type TestHarness struct {
	PostgresDB  *sql.DB
	DuckgresDB  *sql.DB
	duckgresSrv *server.Server
	tmpDir      string
	pgPort      int
	dgPort      int
	mu          sync.Mutex
}

// HarnessConfig configures the test harness
type HarnessConfig struct {
	// PostgresPort is the port for the PostgreSQL container (default: 35432)
	PostgresPort int
	// SkipPostgres skips PostgreSQL setup (for Duckgres-only tests)
	SkipPostgres bool
	// Verbose enables verbose logging
	Verbose bool
}

// DefaultConfig returns the default harness configuration
func DefaultConfig() HarnessConfig {
	return HarnessConfig{
		PostgresPort: 35432,
		SkipPostgres: false,
		Verbose:      os.Getenv("DUCKGRES_TEST_VERBOSE") != "",
	}
}

// NewTestHarness creates a new test harness
func NewTestHarness(cfg HarnessConfig) (*TestHarness, error) {
	h := &TestHarness{
		pgPort: cfg.PostgresPort,
	}

	// Create temp directory for Duckgres
	tmpDir, err := os.MkdirTemp("", "duckgres-integration-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	h.tmpDir = tmpDir

	// Start Duckgres server
	if err := h.startDuckgres(); err != nil {
		os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to start Duckgres: %w", err)
	}

	// Connect to PostgreSQL (if not skipped)
	if !cfg.SkipPostgres {
		if err := h.connectPostgres(); err != nil {
			h.Close()
			return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
		}
	}

	// Connect to Duckgres
	if err := h.connectDuckgres(); err != nil {
		h.Close()
		return nil, fmt.Errorf("failed to connect to Duckgres: %w", err)
	}

	// Load test fixtures into Duckgres
	if err := h.loadFixtures(); err != nil {
		h.Close()
		return nil, fmt.Errorf("failed to load fixtures: %w", err)
	}

	return h, nil
}

// startDuckgres starts the Duckgres server
func (h *TestHarness) startDuckgres() error {
	port := findAvailablePort()
	h.dgPort = port

	// Generate certs
	certFile := filepath.Join(h.tmpDir, "server.crt")
	keyFile := filepath.Join(h.tmpDir, "server.key")
	if err := server.EnsureCertificates(certFile, keyFile); err != nil {
		return fmt.Errorf("failed to generate certificates: %w", err)
	}

	// Create server config
	cfg := server.Config{
		Host:        "127.0.0.1",
		Port:        port,
		DataDir:     h.tmpDir,
		TLSCertFile: certFile,
		TLSKeyFile:  keyFile,
		Users: map[string]string{
			"testuser": "testpass",
		},
	}

	srv, err := server.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}
	h.duckgresSrv = srv

	// Start server in background
	go func() {
		srv.ListenAndServe()
	}()

	// Wait for server to be ready
	time.Sleep(100 * time.Millisecond)
	return nil
}

// connectPostgres connects to the PostgreSQL container
func (h *TestHarness) connectPostgres() error {
	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres password=postgres dbname=testdb sslmode=disable", h.pgPort)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection with retry
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for PostgreSQL: %w", ctx.Err())
		default:
			if err := db.PingContext(ctx); err == nil {
				h.PostgresDB = db
				return nil
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// connectDuckgres connects to the Duckgres server
func (h *TestHarness) connectDuckgres() error {
	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass dbname=test sslmode=require", h.dgPort)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Test connection with retry
	var pingErr error
	for i := 0; i < 20; i++ {
		if pingErr = db.Ping(); pingErr == nil {
			h.DuckgresDB = db
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("failed to connect to Duckgres: %w", pingErr)
}

// loadFixtures loads the test schema and data into Duckgres
func (h *TestHarness) loadFixtures() error {
	// Read and execute schema
	schemaPath := filepath.Join(getTestDir(), "fixtures", "schema.sql")
	schemaSQL, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to read schema: %w", err)
	}

	// Split and execute statements
	statements := splitSQLStatements(string(schemaSQL))
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := h.DuckgresDB.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute schema statement: %s: %w", truncate(stmt, 50), err)
		}
	}

	// Read and execute data
	dataPath := filepath.Join(getTestDir(), "fixtures", "data.sql")
	dataSQL, err := os.ReadFile(dataPath)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}

	statements = splitSQLStatements(string(dataSQL))
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := h.DuckgresDB.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute data statement: %s: %w", truncate(stmt, 50), err)
		}
	}

	return nil
}

// Close shuts down the test harness
func (h *TestHarness) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var errs []error

	if h.PostgresDB != nil {
		if err := h.PostgresDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close PostgreSQL: %w", err))
		}
	}

	if h.DuckgresDB != nil {
		if err := h.DuckgresDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close Duckgres: %w", err))
		}
	}

	if h.duckgresSrv != nil {
		if err := h.duckgresSrv.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close Duckgres server: %w", err))
		}
	}

	if h.tmpDir != "" {
		if err := os.RemoveAll(h.tmpDir); err != nil {
			errs = append(errs, fmt.Errorf("failed to remove temp dir: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}
	return nil
}

// StartPostgresContainer starts the PostgreSQL Docker container
func StartPostgresContainer() error {
	testDir := getTestDir()
	cmd := exec.Command("docker-compose", "-f", filepath.Join(testDir, "docker-compose.yml"), "up", "-d")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start PostgreSQL container: %w", err)
	}

	// Wait for PostgreSQL to be ready
	time.Sleep(2 * time.Second)
	return nil
}

// StopPostgresContainer stops the PostgreSQL Docker container
func StopPostgresContainer() error {
	testDir := getTestDir()
	cmd := exec.Command("docker-compose", "-f", filepath.Join(testDir, "docker-compose.yml"), "down", "-v")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

// IsPostgresRunning checks if the PostgreSQL container is running
func IsPostgresRunning(port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// Helper functions

func findAvailablePort() int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 35433 // fallback
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port
}

func getTestDir() string {
	// Get the directory of this source file
	_, filename, _, ok := getCallerInfo()
	if !ok {
		return "tests/integration"
	}
	return filepath.Dir(filename)
}

func getCallerInfo() (pc uintptr, file string, line int, ok bool) {
	// This is a placeholder - in real code we'd use runtime.Caller
	// For now, we'll use a relative path approach
	cwd, _ := os.Getwd()
	// Check if we're in the integration directory
	if _, err := os.Stat(filepath.Join(cwd, "docker-compose.yml")); err == nil {
		return 0, filepath.Join(cwd, "harness.go"), 0, true
	}
	// Check if we're in the project root
	if _, err := os.Stat(filepath.Join(cwd, "tests", "integration", "docker-compose.yml")); err == nil {
		return 0, filepath.Join(cwd, "tests", "integration", "harness.go"), 0, true
	}
	return 0, "", 0, false
}

func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		// Handle string literals
		if (c == '\'' || c == '"') && (i == 0 || sql[i-1] != '\\') {
			if !inString {
				inString = true
				stringChar = c
			} else if c == stringChar {
				inString = false
			}
		}

		// Handle statement terminator
		if c == ';' && !inString {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteByte(c)
	}

	// Add any remaining statement
	if stmt := strings.TrimSpace(current.String()); stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
