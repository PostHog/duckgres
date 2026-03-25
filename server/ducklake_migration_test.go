package server

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestBuildDuckLakeAttachStmt(t *testing.T) {
	tests := []struct {
		name    string
		dlCfg   DuckLakeConfig
		migrate bool
		want    string
	}{
		{
			name: "basic without data path",
			dlCfg: DuckLakeConfig{
				MetadataStore: "postgres:host=localhost dbname=dl",
			},
			migrate: false,
			want:    "ATTACH 'ducklake:postgres:host=localhost dbname=dl' AS ducklake",
		},
		{
			name: "with object store",
			dlCfg: DuckLakeConfig{
				MetadataStore: "postgres:host=localhost dbname=dl",
				ObjectStore:   "s3://bucket/path",
			},
			migrate: false,
			want:    "ATTACH 'ducklake:postgres:host=localhost dbname=dl' AS ducklake (DATA_PATH 's3://bucket/path')",
		},
		{
			name: "with data path fallback",
			dlCfg: DuckLakeConfig{
				MetadataStore: "postgres:host=localhost dbname=dl",
				DataPath:      "/local/data",
			},
			migrate: false,
			want:    "ATTACH 'ducklake:postgres:host=localhost dbname=dl' AS ducklake (DATA_PATH '/local/data')",
		},
		{
			name: "object store takes precedence over data path",
			dlCfg: DuckLakeConfig{
				MetadataStore: "postgres:host=localhost dbname=dl",
				ObjectStore:   "s3://bucket/path",
				DataPath:      "/local/data",
			},
			migrate: false,
			want:    "ATTACH 'ducklake:postgres:host=localhost dbname=dl' AS ducklake (DATA_PATH 's3://bucket/path')",
		},
		{
			name: "with migration flag",
			dlCfg: DuckLakeConfig{
				MetadataStore: "postgres:host=localhost dbname=dl",
				ObjectStore:   "s3://bucket/path",
			},
			migrate: true,
			want:    "ATTACH 'ducklake:postgres:host=localhost dbname=dl' AS ducklake (DATA_PATH 's3://bucket/path', AUTOMATIC_MIGRATION TRUE)",
		},
		{
			name: "migration without data path",
			dlCfg: DuckLakeConfig{
				MetadataStore: "postgres:host=localhost dbname=dl",
			},
			migrate: true,
			want:    "ATTACH 'ducklake:postgres:host=localhost dbname=dl' AS ducklake (AUTOMATIC_MIGRATION TRUE)",
		},
		{
			name: "escapes single quotes in connection string",
			dlCfg: DuckLakeConfig{
				MetadataStore: "postgres:host=localhost password=it's_secret",
			},
			migrate: false,
			want:    "ATTACH 'ducklake:postgres:host=localhost password=it''s_secret' AS ducklake",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildDuckLakeAttachStmt(tt.dlCfg, tt.migrate)
			if got != tt.want {
				t.Errorf("buildDuckLakeAttachStmt() =\n  %s\nwant:\n  %s", got, tt.want)
			}
		})
	}
}

func TestFormatSQLValue(t *testing.T) {
	tests := []struct {
		name string
		v    any
		want string
	}{
		{"nil", nil, "NULL"},
		{"true", true, "TRUE"},
		{"false", false, "FALSE"},
		{"int", int64(42), "42"},
		{"negative int", int64(-1), "-1"},
		{"float", float64(3.14), "3.14"},
		{"string", "hello", "'hello'"},
		{"string with quote", "it's", "'it''s'"},
		{"string with double quote", `say "hi"`, `'say "hi"'`},
		{"empty string", "", "''"},
		{"bytes", []byte("data"), "'data'"},
		{"bytes with quote", []byte("it's"), "'it''s'"},
		{"time", time.Date(2026, 3, 23, 12, 0, 0, 0, time.UTC), "'2026-03-23T12:00:00Z'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatSQLValue(tt.v)
			if got != tt.want {
				t.Errorf("formatSQLValue(%v) = %s, want %s", tt.v, got, tt.want)
			}
		})
	}
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"simple", "table_name", `"table_name"`},
		{"reserved word", "key", `"key"`},
		{"reserved word value", "value", `"value"`},
		{"with double quote", `say"hi`, `"say""hi"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := quoteIdent(tt.in)
			if got != tt.want {
				t.Errorf("quoteIdent(%q) = %s, want %s", tt.in, got, tt.want)
			}
		})
	}
}

func TestVersionLessThan(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"0.3", "0.4", true},
		{"0.4", "0.4", false},
		{"0.4", "0.3", false},
		{"0.4", "0.10", true},  // numeric, not lexicographic
		{"0.10", "0.4", false}, // numeric, not lexicographic
		{"0.9", "0.10", true},
		{"1.0", "0.10", false},
		{"0.3", "1.0", true},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			got, err := versionLessThan(tt.a, tt.b)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("versionLessThan(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestVersionLessThan_Invalid(t *testing.T) {
	tests := []struct {
		a, b string
	}{
		{"abc", "0.4"},
		{"0.4", "abc"},
		{"0.4.1", "0.4"},
		{"", "0.4"},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			_, err := versionLessThan(tt.a, tt.b)
			if err == nil {
				t.Errorf("versionLessThan(%q, %q) expected error, got nil", tt.a, tt.b)
			}
		})
	}
}

func TestDuckLakeMigrationNeeded_FalseWhenError(t *testing.T) {
	// Save and restore global state.
	dlMigration.mu.Lock()
	origDone := dlMigration.done
	origNeeded := dlMigration.needed
	origErr := dlMigration.err
	dlMigration.mu.Unlock()
	defer func() {
		dlMigration.mu.Lock()
		dlMigration.done = origDone
		dlMigration.needed = origNeeded
		dlMigration.err = origErr
		dlMigration.mu.Unlock()
	}()

	dlMigration.mu.Lock()
	dlMigration.needed = true
	dlMigration.err = fmt.Errorf("backup failed")
	dlMigration.mu.Unlock()

	if duckLakeMigrationNeeded() {
		t.Error("duckLakeMigrationNeeded() should return false when err is set")
	}
}

// TestBackupDuckLakeMetadata_Integration verifies the backup path end-to-end
// against a real PostgreSQL instance. Skipped when DUCKGRES_TEST_POSTGRES_DSN
// is not set (e.g., in CI without a local PostgreSQL).
//
// Run locally:
//
//	DUCKGRES_TEST_POSTGRES_DSN="host=localhost user=postgres dbname=postgres sslmode=disable" go test ./server/ -run TestBackupDuckLakeMetadata_Integration -v
func TestBackupDuckLakeMetadata_Integration(t *testing.T) {
	dsn := os.Getenv("DUCKGRES_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DUCKGRES_TEST_POSTGRES_DSN not set, skipping integration test")
	}

	pgDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer func() { _ = pgDB.Close() }()

	// Create ducklake-like tables in a unique schema to avoid collisions.
	suffix := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
	metaTable := "ducklake_metadata_test_" + suffix
	dataTable := "ducklake_data_test_" + suffix

	cleanup := func() {
		_, _ = pgDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(metaTable)))
		_, _ = pgDB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(dataTable)))
	}
	cleanup()
	t.Cleanup(cleanup)

	// Create tables.
	_, err = pgDB.Exec(fmt.Sprintf(`CREATE TABLE %s ("key" VARCHAR NOT NULL, "value" VARCHAR NOT NULL)`, quoteIdent(metaTable)))
	if err != nil {
		t.Fatalf("create metadata table: %v", err)
	}
	_, err = pgDB.Exec(fmt.Sprintf(`CREATE TABLE %s (id BIGINT NOT NULL, name VARCHAR, active BOOLEAN)`, quoteIdent(dataTable)))
	if err != nil {
		t.Fatalf("create data table: %v", err)
	}

	// Insert test data.
	_, err = pgDB.Exec(fmt.Sprintf(`INSERT INTO %s ("key", "value") VALUES ('version', '0.3'), ('name', 'test catalog')`, quoteIdent(metaTable)))
	if err != nil {
		t.Fatalf("insert metadata: %v", err)
	}
	_, err = pgDB.Exec(fmt.Sprintf(`INSERT INTO %s (id, name, active) VALUES (1, 'hello', TRUE), (2, 'it''s a test', FALSE), (3, NULL, NULL)`, quoteIdent(dataTable)))
	if err != nil {
		t.Fatalf("insert data: %v", err)
	}

	// Run backup to a temp directory.
	tmpDir := t.TempDir()

	// backupDuckLakeMetadata discovers tables matching "ducklake_%" — our test
	// tables match that pattern. We can call it directly.
	err = backupDuckLakeMetadata(pgDB, tmpDir, "0.3")
	if err != nil {
		t.Fatalf("backupDuckLakeMetadata: %v", err)
	}

	// Find the backup file.
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 backup file, got %d", len(entries))
	}

	content, err := os.ReadFile(tmpDir + "/" + entries[0].Name())
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}

	sql := string(content)

	// Verify structure.
	if !strings.Contains(sql, "BEGIN;") {
		t.Error("backup missing BEGIN")
	}
	if !strings.Contains(sql, "COMMIT;") {
		t.Error("backup missing COMMIT")
	}
	if !strings.Contains(sql, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s", quoteIdent(metaTable))) {
		t.Errorf("backup missing CREATE TABLE for %s", metaTable)
	}
	if !strings.Contains(sql, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s", quoteIdent(dataTable))) {
		t.Errorf("backup missing CREATE TABLE for %s", dataTable)
	}

	// Verify data rows.
	if !strings.Contains(sql, "'0.3'") {
		t.Error("backup missing version value '0.3'")
	}
	if !strings.Contains(sql, "'it''s a test'") {
		t.Error("backup missing escaped quote in 'it''s a test'")
	}
	if !strings.Contains(sql, "NULL") {
		t.Error("backup missing NULL values")
	}

	// Verify row counts — 2 metadata rows + 3 data rows = at least 5 INSERT statements.
	insertCount := strings.Count(sql, "INSERT INTO")
	if insertCount < 5 {
		t.Errorf("expected at least 5 INSERT statements, got %d", insertCount)
	}

	t.Logf("Backup file: %s (%d bytes, %d INSERTs)", entries[0].Name(), len(content), insertCount)
}
