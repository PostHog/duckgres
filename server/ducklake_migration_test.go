package server

import (
	"fmt"
	"testing"
	"time"
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
