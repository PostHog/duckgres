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
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/posthog/duckgres/server"
	_ "github.com/lib/pq"
)

var duckLakeInfraServices = []string{"ducklake-metadata", "minio", "minio-init"}

// TestHarness manages PostgreSQL and Duckgres instances for side-by-side testing
type TestHarness struct {
	PostgresDB   *sql.DB
	DuckgresDB   *sql.DB
	duckgresSrv  *server.Server
	latencyProxy *LatencyProxy
	tmpDir       string
	pgPort       int
	dgPort       int
	useDuckLake  bool
	mu           sync.Mutex
}

// HarnessConfig configures the test harness
type HarnessConfig struct {
	// PostgresPort is the port for the PostgreSQL container (default: 35432)
	PostgresPort int
	// SkipPostgres skips PostgreSQL setup (for Duckgres-only tests)
	SkipPostgres bool
	// Verbose enables verbose logging
	Verbose bool
	// UseDuckLake enables DuckLake mode (requires ducklake-metadata and minio)
	UseDuckLake bool
	// DuckLakeMetadataPort is the port for the DuckLake metadata PostgreSQL (default: 35433)
	DuckLakeMetadataPort int
	// MinIOPort is the port for MinIO S3 API (default: 39000)
	MinIOPort int
	// MetadataLatency adds artificial one-way latency between DuckDB/DuckLake
	// and the metadata PostgreSQL via a TCP proxy. Total RTT overhead = 2x this value.
	// Zero means no proxy (direct connection).
	MetadataLatency time.Duration
}

// DefaultConfig returns the default harness configuration
func DefaultConfig() HarnessConfig {
	// Default to DuckLake mode unless DUCKGRES_TEST_NO_DUCKLAKE is set
	useDuckLake := os.Getenv("DUCKGRES_TEST_NO_DUCKLAKE") == ""
	return HarnessConfig{
		PostgresPort:         35432,
		SkipPostgres:         false,
		Verbose:              os.Getenv("DUCKGRES_TEST_VERBOSE") != "",
		UseDuckLake:          useDuckLake,
		DuckLakeMetadataPort: 35433,
		MinIOPort:            39000,
	}
}

// NewTestHarness creates a new test harness
func NewTestHarness(cfg HarnessConfig) (*TestHarness, error) {
	h := &TestHarness{
		pgPort:      cfg.PostgresPort,
		useDuckLake: cfg.UseDuckLake,
	}

	// Create temp directory for Duckgres
	tmpDir, err := os.MkdirTemp("", "duckgres-integration-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	h.tmpDir = tmpDir

	// Start Duckgres server
	if err := h.startDuckgres(cfg); err != nil {
		_ = os.RemoveAll(tmpDir)
		return nil, fmt.Errorf("failed to start Duckgres: %w", err)
	}

	// Connect to PostgreSQL (if not skipped)
	if !cfg.SkipPostgres {
		if err := h.connectPostgres(); err != nil {
			_ = h.Close()
			return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
		}
	}

	// Connect to Duckgres
	if err := h.connectDuckgres(); err != nil {
		_ = h.Close()
		return nil, fmt.Errorf("failed to connect to Duckgres: %w", err)
	}

	// Load test fixtures into Duckgres
	if err := h.loadFixtures(); err != nil {
		_ = h.Close()
		return nil, fmt.Errorf("failed to load fixtures: %w", err)
	}

	// Load fixtures into PostgreSQL for comparison tests
	if !cfg.SkipPostgres {
		if err := h.loadPostgresFixtures(); err != nil {
			_ = h.Close()
			return nil, fmt.Errorf("failed to load PostgreSQL fixtures: %w", err)
		}
	}

	return h, nil
}

// startDuckgres starts the Duckgres server
func (h *TestHarness) startDuckgres(harnessCfg HarnessConfig) error {
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
		Extensions: []string{"ducklake"},
		RateLimit: server.RateLimitConfig{
			MaxConnections: 100,
		},
	}

	// Configure DuckLake if enabled
	if harnessCfg.UseDuckLake {
		// Try the upstream postgres_scanner fix from core_nightly in the
		// DuckLake-backed integration harness without changing global defaults.
		cfg.Extensions = []string{server.PostgresCoreNightlyExtension, "ducklake"}

		metadataPort := harnessCfg.DuckLakeMetadataPort

		// If latency injection is requested, start a TCP proxy in front of the
		// metadata PostgreSQL and point DuckLake at the proxy port instead.
		if harnessCfg.MetadataLatency > 0 {
			target := fmt.Sprintf("127.0.0.1:%d", harnessCfg.DuckLakeMetadataPort)
			proxy, err := NewLatencyProxy(target, harnessCfg.MetadataLatency)
			if err != nil {
				return fmt.Errorf("failed to start latency proxy: %w", err)
			}
			h.latencyProxy = proxy
			metadataPort = proxy.Port()
		}

		cfg.DuckLake = server.DuckLakeConfig{
			MetadataStore: fmt.Sprintf("postgres:host=127.0.0.1 port=%d user=ducklake password=ducklake dbname=ducklake", metadataPort),
			ObjectStore:   "s3://ducklake/data/",
			S3Provider:    "config",
			S3Endpoint:    fmt.Sprintf("127.0.0.1:%d", harnessCfg.MinIOPort),
			S3AccessKey:   "minioadmin",
			S3SecretKey:   "minioadmin",
			S3Region:      "us-east-1",
			S3UseSSL:      false,
			S3URLStyle:    "path",
		}
	}

	srv, err := server.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}
	h.duckgresSrv = srv

	// Start server in background
	go func() {
		_ = srv.ListenAndServe()
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
// In DuckLake mode, tables are automatically created in ducklake.main
// because the server runs "USE ducklake" to set the default catalog
func (h *TestHarness) loadFixtures() error {
	// In DuckLake mode, drop existing tables first since metadata persists
	if h.useDuckLake {
		if err := h.cleanupDuckLakeTables(); err != nil {
			// Log but don't fail - tables might not exist
			fmt.Printf("Warning: cleanup failed (may be OK): %v\n", err)
		}
	}

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

// loadPostgresFixtures loads the test schema and data into PostgreSQL for comparison tests
func (h *TestHarness) loadPostgresFixtures() error {
	if h.PostgresDB == nil {
		return nil
	}

	if err := h.cleanupPostgresRuntimeSchemas(); err != nil {
		return fmt.Errorf("failed to cleanup PostgreSQL runtime schemas: %w", err)
	}

	// Drop existing objects first (in reverse dependency order)
	dropStatements := []string{
		"DROP VIEW IF EXISTS order_details",
		"DROP VIEW IF EXISTS user_stats",
		"DROP VIEW IF EXISTS active_users",
		"DROP TABLE IF EXISTS test_schema.schema_test",
		"DROP SCHEMA IF EXISTS test_schema CASCADE",
		"DROP TABLE IF EXISTS array_test",
		"DROP TABLE IF EXISTS documents",
		"DROP TABLE IF EXISTS metrics",
		"DROP TABLE IF EXISTS empty_table",
		"DROP TABLE IF EXISTS nullable_test",
		"DROP TABLE IF EXISTS json_data",
		"DROP TABLE IF EXISTS events",
		"DROP TABLE IF EXISTS sales",
		"DROP TABLE IF EXISTS categories",
		"DROP TABLE IF EXISTS order_items",
		"DROP TABLE IF EXISTS orders",
		"DROP TABLE IF EXISTS products",
		"DROP TABLE IF EXISTS users",
		"DROP TABLE IF EXISTS types_test",
	}
	for _, stmt := range dropStatements {
		_, _ = h.PostgresDB.Exec(stmt)
	}

	// Read and execute schema
	schemaPath := filepath.Join(getTestDir(), "fixtures", "schema.sql")
	schemaSQL, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to read schema: %w", err)
	}

	statements := splitSQLStatements(string(schemaSQL))
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := h.PostgresDB.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute PostgreSQL schema statement: %s: %w", truncate(stmt, 50), err)
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
		if _, err := h.PostgresDB.Exec(stmt); err != nil {
			return fmt.Errorf("failed to execute PostgreSQL data statement: %s: %w", truncate(stmt, 50), err)
		}
	}

	return nil
}

func (h *TestHarness) cleanupPostgresRuntimeSchemas() error {
	rows, err := h.PostgresDB.Query(`
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name = 'cp_runtime'
			OR schema_name LIKE 'managed_warehouse\_%\_runtime' ESCAPE '\'
	`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return err
		}
		if isEphemeralPostgresRuntimeSchema(schema) {
			schemas = append(schemas, schema)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, schema := range schemas {
		if _, err := h.PostgresDB.Exec(`DROP SCHEMA IF EXISTS ` + quotePostgresIdentifier(schema) + ` CASCADE`); err != nil {
			return err
		}
	}

	return nil
}

func isEphemeralPostgresRuntimeSchema(schema string) bool {
	return schema == "cp_runtime" || (strings.HasPrefix(schema, "managed_warehouse_") && strings.HasSuffix(schema, "_runtime"))
}

func quotePostgresIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// cleanupDuckLakeTables drops existing tables in DuckLake before loading fixtures
func (h *TestHarness) cleanupDuckLakeTables() error {
	// Tests may leave the session on a non-default catalog (e.g. USE memory).
	// Force cleanup to run against the DuckLake catalog that holds persisted fixtures.
	_, _ = h.DuckgresDB.Exec("USE ducklake")

	// Drop views first (they depend on tables)
	views := []string{"order_details", "user_stats", "active_users"}
	for _, v := range views {
		_, _ = h.DuckgresDB.Exec(fmt.Sprintf("DROP VIEW IF EXISTS ducklake.main.%s", v))
	}

	// Drop tables in reverse dependency order
	// Include both fixture tables and tables created by other tests (DDL tests, etc.)
	tables := []string{
		// Fixture tables
		"test_schema.schema_test",
		"array_test",
		"documents",
		"metrics",
		"empty_table",
		"nullable_test",
		"json_data",
		"events",
		"sales",
		"categories",
		"order_items",
		"orders",
		"products",
		"users",
		"types_test",
		// DDL test tables that may persist in DuckLake
		"ddl_test_basic",
		"ddl_test_types",
		"ddl_test_pk",
		"ddl_test_notnull",
		"ddl_test_default",
		"ddl_test_unique",
		"ddl_test_as",
		"ddl_alter_test",
		"ddl_drop_test1",
		"ddl_drop_test2",
		"ddl_drop_test3",
		"ddl_index_test",
		"ddl_truncate_test",
		"ddl_comment_test",
		"ddl_constraint_pk",
		"ddl_constraint_unique",
		"ddl_constraint_check",
		"ddl_constraint_fk",
		// DML test tables
		"dml_insert_test",
		"dml_insert_target",
		"dml_insert_default",
		"dml_returning_test",
		"dml_upsert_test",
		"dml_update_test",
		"dml_update_returning",
		"dml_update_target",
		"dml_update_source",
		"dml_delete_test",
		"dml_delete_returning",
		"dml_delete_main",
		"dml_delete_filter",
		"dml_cte_source",
		"dml_cte_target",
		// Protocol test tables
		"protocol_insert_test",
		"tx_test",
		"tx_rollback_test",
		"tx_isolation_test",
		"interleave_test",
		// COPY test tables
		"copy_test",
		"copy_special_test",
		"copy_json_test",
		"copy_out_test",
		// Edge case test tables
		"error_recovery_tx",
		"error_then_tx",
		"prepare_ddl_test",
		"stmt_across_tx",
		"savepoint_basic",
		"savepoint_nested",
		"savepoint_release",
		"savepoint_error",
		"unicode_test",
		"empty_vs_null",
		"long_text_test",
		"special_chars_test",
		"quoted_cols",
		"explain_insert_test",
		"explain_a",
		"explain_b",
		"empty_update",
		"empty_delete",
		"empty_agg",
		"join_populated",
		"join_empty",
		"multi_stmt_test",
		"concurrent_write_0",
		"concurrent_write_1",
		"concurrent_write_2",
		"concurrent_write_3",
		"concurrent_write_4",
		"concurrent_ddl_test",
		"concurrent_dml_test",
		// DuckLake concurrency test tables
		"dl_conc_insert",
		"dl_conc_tx_insert",
		"dl_conc_update",
		"dl_conc_del",
		"dl_conc_orders",
		"dl_conc_items",
		"dl_conc_rw",
		"dl_conc_upsert",
		"dl_conc_ddl_write",
		"dl_conc_batch",
		"dl_conc_rapid",
		"dl_conc_rollback",
		"dl_conc_sustained",
		// DuckLake CTAS concurrency test tables
		"dl_ctas_source",
		"dl_cortas_source",
		"dl_ctas_live_source",
		"dl_replace_source_a",
		"dl_replace_source_b",
		"dl_replace_target",
	}

	// Also clean up dynamically-named tables from DuckLake concurrency lifecycle test
	for w := range 4 {
		for c := range 8 {
			tables = append(tables, fmt.Sprintf("dl_conc_lifecycle_%d_%d", w, c))
		}
	}
	for i := range 6 {
		tables = append(tables, fmt.Sprintf("dl_conc_sep_%d", i))
	}
	// CTAS output tables
	for w := range 8 {
		for c := range 10 {
			tables = append(tables, fmt.Sprintf("dl_ctas_out_%d_%d", w, c))
		}
	}
	// CTAS snapshot tables
	for i := range 20 {
		tables = append(tables, fmt.Sprintf("dl_ctas_snapshot_%d", i))
	}
	// CREATE OR REPLACE target tables
	for i := range 4 {
		tables = append(tables, fmt.Sprintf("dl_cortas_target_%d", i))
	}
	// SQLMesh CTAS reproduction tables
	tables = append(tables, "dl_sqlmesh_source", "dl_sqlmesh_raw", "dl_sqlmesh_comment_src")
	for i := range 30 {
		tables = append(tables, fmt.Sprintf("dl_sqlmesh_cmt_%d", i))
	}
	for i := range 30 {
		tables = append(tables, fmt.Sprintf("dl_sqlmesh_model_%d", i))
	}
	for i := range 5 {
		tables = append(tables, fmt.Sprintf("dl_sqlmesh_t1_%d", i))
	}
	for i := range 6 {
		tables = append(tables, fmt.Sprintf("dl_sqlmesh_t2_%d", i))
	}

	for _, t := range tables {
		// Ignore errors - table might not exist or schema might not exist
		objectName := fmt.Sprintf("ducklake.main.%s", t)
		if strings.Contains(t, ".") {
			objectName = "ducklake." + t
		}
		_, _ = h.DuckgresDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", objectName))
	}

	// Drop test schemas after contained objects have been removed.
	_, _ = h.DuckgresDB.Exec("DROP SCHEMA IF EXISTS ducklake.test_schema CASCADE")
	_, _ = h.DuckgresDB.Exec("DROP SCHEMA IF EXISTS ducklake.bill CASCADE")
	_, _ = h.DuckgresDB.Exec("DROP SCHEMA IF EXISTS ducklake.ddl_schema_test CASCADE")
	_, _ = h.DuckgresDB.Exec("DROP SCHEMA IF EXISTS ducklake.dbt_test CASCADE")

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

	if h.latencyProxy != nil {
		if err := h.latencyProxy.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close latency proxy: %w", err))
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

func dockerComposeArgs(composeFile string, command string, args ...string) []string {
	composeArgs := []string{"-f", composeFile, command}
	return append(composeArgs, args...)
}

func dockerComposeCommand(composeFile string, command string, args ...string) (string, []string) {
	return "docker", append([]string{"compose"}, dockerComposeArgs(composeFile, command, args...)...)
}

func runDockerCompose(command string, args ...string) error {
	testDir := getTestDir()
	composeFile := filepath.Join(testDir, "docker-compose.yml")
	cmdName, cmdArgs := dockerComposeCommand(composeFile, command, args...)
	cmd := exec.Command(cmdName, cmdArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker compose %s %s: %w", command, strings.Join(args, " "), err)
	}
	return nil
}

// StartPostgresContainer starts only the PostgreSQL comparison container.
func StartPostgresContainer() error {
	if err := runDockerCompose("up", "-d", "postgres"); err != nil {
		return fmt.Errorf("failed to start PostgreSQL container: %w", err)
	}

	// Wait for PostgreSQL to be ready
	time.Sleep(2 * time.Second)
	return nil
}

// StartDuckLakeInfraContainers starts the local DuckLake metadata/object-store services.
func StartDuckLakeInfraContainers() error {
	if err := runDockerCompose("up", append([]string{"-d"}, duckLakeInfraServices...)...); err != nil {
		return fmt.Errorf("failed to start DuckLake infrastructure: %w", err)
	}
	return nil
}

// StopPostgresContainer stops the PostgreSQL Docker container
func StopPostgresContainer() error {
	testDir := getTestDir()
	cmdName, cmdArgs := dockerComposeCommand(filepath.Join(testDir, "docker-compose.yml"), "down", "-v")
	cmd := exec.Command(cmdName, cmdArgs...)
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
	_ = conn.Close()
	return true
}

// IsDuckLakeInfraRunning checks if the DuckLake infrastructure (metadata postgres + minio) is running
func IsDuckLakeInfraRunning(metadataPort, minioPort int) bool {
	// Check DuckLake metadata PostgreSQL
	metaConn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", metadataPort), time.Second)
	if err != nil {
		return false
	}
	_ = metaConn.Close()

	// Check MinIO
	minioConn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", minioPort), time.Second)
	if err != nil {
		return false
	}
	_ = minioConn.Close()

	return true
}

// WaitForDuckLakeInfra waits for DuckLake infrastructure to be ready
func WaitForDuckLakeInfra(metadataPort, minioPort int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if IsDuckLakeInfraRunning(metadataPort, minioPort) {
			// Give MinIO a bit more time to initialize the bucket
			time.Sleep(500 * time.Millisecond)
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for DuckLake infrastructure (metadata:%d, minio:%d)", metadataPort, minioPort)
}

// Helper functions

func findAvailablePort() int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 35433 // fallback
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}

func getTestDir() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "tests/integration"
	}
	return filepath.Dir(filename)
}

func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)
	inLineComment := false

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		// Handle line comments (--) when not in a string
		if !inString && !inLineComment && c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			inLineComment = true
			i++ // skip second dash
			continue
		}

		// End line comment on newline
		if inLineComment {
			if c == '\n' {
				inLineComment = false
				current.WriteByte(' ') // replace comment with space
			}
			continue
		}

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
