package clusterqa

// Gated manual test: exercises the Iceberg `DROP SCHEMA ... CASCADE` fallback
// (server/iceberg_drop_schema.go) against the local multitenant cluster over
// Flight. This is the path that hit the Flight `?`-placeholder bug in prod
// (the internal list-tables query used a bound `?` that the Flight executor
// never interpolated, yielding "incorrect argument count ... have 0 want 1").
//
// Requires the Iceberg (Lakekeeper) local dev stack to be up and a pg
// port-forward to the control plane:
//
//   just run-multitenant-local            # brings up Lakekeeper + "both" tenant
//   just multitenant-port-forward-pg      # in another shell
//   just qa-iceberg-drop-schema           # runs this test
//
// Before the fix (main image) this FAILS at the DROP; after the fix it passes.
// pgx sets TLS ServerName independently of the dial address, so we dial
// 127.0.0.1 but present the managed SNI hostname duckgres.localhost. dbname=
// iceberg selects the Iceberg catalog for the session.

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestClusterIcebergDropSchema(t *testing.T) {
	if os.Getenv("DUCKGRES_QA_CLUSTER") != "1" {
		t.Skip("set DUCKGRES_QA_CLUSTER=1 to run against the local multitenant cluster")
	}

	cfg, err := pgx.ParseConfig("postgres://postgres:postgres@127.0.0.1:5432/iceberg")
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.TLSConfig = &tls.Config{ServerName: "duckgres.localhost", InsecureSkipVerify: true}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect (dbname=iceberg): %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// Sanity: confirm the session really landed on the Iceberg catalog.
	var db string
	if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&db); err != nil {
		t.Fatalf("current_database(): %v", err)
	}
	t.Logf("connected, current_database() = %q", db)

	// Unique schema name per run so CREATE always starts clean (a failed run
	// can't block the next one) and we never depend on a pre-cleanup DROP that
	// would itself exercise the buggy path.
	schema := fmt.Sprintf("qa_drop_%d", time.Now().UnixNano())

	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = conn.Exec(cctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	})

	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE SCHEMA %s", schema)); err != nil {
		t.Fatalf("CREATE SCHEMA %s: %v", schema, err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.t (id integer, name text)", schema)); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s.t VALUES (1, 'a'), (2, 'b')", schema)); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	t.Logf("seeded %s.t with 2 rows", schema)

	// The assertion: DROP SCHEMA ... CASCADE on a populated Iceberg schema.
	// This drives the iceberg_drop_schema.go fallback (list tables -> drop each
	// -> drop schema), whose list-tables query is where the `?` bug lived.
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)); err != nil {
		t.Fatalf("DROP SCHEMA %s CASCADE FAILED (the bug): %v", schema, err)
	}
	t.Logf("PASS: DROP SCHEMA %s CASCADE succeeded over Flight", schema)
}
