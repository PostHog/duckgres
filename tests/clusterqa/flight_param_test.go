package clusterqa

// Gated manual test: parameterized (extended-protocol) query against the local
// multitenant cluster (control plane -> worker over Flight), via the port-forward.
// pgx sets TLS ServerName independently of the dial address, so we dial
// 127.0.0.1 but present the managed SNI hostname duckgres.localhost.
//
//   just multitenant-port-forward-pg   # in another shell
//   DUCKGRES_QA_CLUSTER=1 go test ./tests/clusterqa/ -run TestClusterFlightParam -v

import (
	"context"
	"crypto/tls"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestClusterFlightParam(t *testing.T) {
	if os.Getenv("DUCKGRES_QA_CLUSTER") != "1" {
		t.Skip("set DUCKGRES_QA_CLUSTER=1 to run against the local multitenant cluster")
	}
	cfg, err := pgx.ParseConfig("postgres://postgres:postgres@127.0.0.1:5432/ducklake")
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.TLSConfig = &tls.Config{ServerName: "duckgres.localhost", InsecureSkipVerify: true}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec // unnamed prepared stmt: Parse/Bind/Execute with bound params

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	var control int
	if err := conn.QueryRow(ctx, "SELECT 40 + 2").Scan(&control); err != nil {
		t.Fatalf("control query: %v", err)
	}
	t.Logf("control (no params): 40+2 = %d", control)

	var sum int
	err = conn.QueryRow(ctx, "SELECT $1::int + $2::int", 2, 3).Scan(&sum)
	if err != nil {
		t.Fatalf("PARAM QUERY FAILED (the bug): %v", err)
	}
	if sum != 5 {
		t.Fatalf("sum = %d, want 5", sum)
	}
	t.Logf("PASS: parameterized $1+$2 (2,3) over Flight = %d", sum)
}
