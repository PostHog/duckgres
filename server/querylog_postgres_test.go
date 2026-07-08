package server

import (
	"strings"
	"testing"
	"time"
)

func TestPostgresQueryLogDSN(t *testing.T) {
	tests := []struct {
		name            string
		metadataStore   string
		applicationName string
		want            string
		wantErr         bool
	}{
		{
			name:          "ducklake postgres prefix",
			metadataStore: "postgres:host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake",
			want:          "host=metadata.internal port=5432 user=ducklake password=secret dbname=ducklake keepalives=1 keepalives_idle=60 keepalives_interval=10 keepalives_count=5 default_query_exec_mode=simple_protocol",
		},
		{
			name:            "application name",
			metadataStore:   "postgres:host=metadata.internal dbname=ducklake",
			applicationName: "duckgres/acme",
			want:            "host=metadata.internal dbname=ducklake keepalives=1 keepalives_idle=60 keepalives_interval=10 keepalives_count=5 application_name='duckgres/acme' default_query_exec_mode=simple_protocol",
		},
		{
			name:          "preserve caller options",
			metadataStore: "postgres:host=metadata.internal dbname=ducklake keepalives=1 application_name=custom default_query_exec_mode=exec",
			want:          "host=metadata.internal dbname=ducklake keepalives=1 application_name=custom default_query_exec_mode=exec",
		},
		{
			name:          "trim whitespace",
			metadataStore: "  postgres:host=metadata.internal dbname=ducklake  ",
			want:          "host=metadata.internal dbname=ducklake keepalives=1 keepalives_idle=60 keepalives_interval=10 keepalives_count=5 default_query_exec_mode=simple_protocol",
		},
		{
			name:          "empty",
			metadataStore: "",
			wantErr:       true,
		},
		{
			name:          "non postgres",
			metadataStore: "metadata.db",
			wantErr:       true,
		},
		{
			name:          "empty postgres dsn",
			metadataStore: "postgres:",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := postgresQueryLogDSN(DuckLakeConfig{
				MetadataStore:   tt.metadataStore,
				ApplicationName: tt.applicationName,
			})
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("postgresQueryLogDSN: %v", err)
			}
			if got != tt.want {
				t.Fatalf("DSN = %q, want %q", got, tt.want)
			}
			db, err := openPostgresQueryLogDB(got)
			if err != nil {
				t.Fatalf("openPostgresQueryLogDB with generated DSN: %v", err)
			}
			t.Cleanup(func() { _ = db.Close() })
		})
	}
}

func TestOpenPostgresQueryLogDBUsesSingleConnectionPool(t *testing.T) {
	db, err := openPostgresQueryLogDB("host=metadata.internal dbname=ducklake")
	if err != nil {
		t.Fatalf("openPostgresQueryLogDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	if got := db.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("MaxOpenConnections = %d, want 1", got)
	}
}

func TestPostgresQueryLogSchemaSQL(t *testing.T) {
	sql := strings.Join(postgresQueryLogSchemaSQLForTime(time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)), "\n")

	for _, want := range []string{
		"CREATE SCHEMA IF NOT EXISTS querylog",
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries",
		"PARTITION BY RANGE (event_time)",
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries_202606 PARTITION OF querylog.query_log_entries FOR VALUES FROM ('2026-06-01') TO ('2026-07-01')",
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries_202607 PARTITION OF querylog.query_log_entries FOR VALUES FROM ('2026-07-01') TO ('2026-08-01')",
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries_202608 PARTITION OF querylog.query_log_entries FOR VALUES FROM ('2026-08-01') TO ('2026-09-01')",
		"CREATE TABLE IF NOT EXISTS querylog.query_log_entries_default PARTITION OF querylog.query_log_entries DEFAULT",
		"idx_query_log_entries_event_time",
		"idx_query_log_entries_user_time",
		"idx_query_log_entries_hash_time",
		"cpu_time_s DOUBLE PRECISION DEFAULT 0",
		"peak_buffer_memory_bytes BIGINT DEFAULT 0",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("schema SQL missing %q:\n%s", want, sql)
		}
	}

	if strings.Contains(sql, "event_id") {
		t.Fatalf("Postgres query-log schema must not include event_id:\n%s", sql)
	}
	if strings.Contains(sql, "(org_id,") {
		t.Fatalf("Postgres query-log indexes should not lead with org_id:\n%s", sql)
	}
	currentPartition := strings.Index(sql, "querylog.query_log_entries_202607")
	defaultPartition := strings.Index(sql, "querylog.query_log_entries_default")
	if currentPartition < 0 || defaultPartition < 0 || currentPartition > defaultPartition {
		t.Fatalf("month partitions should be created before default partition:\n%s", sql)
	}
}
