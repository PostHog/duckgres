//go:build k8s_integration

package k8s_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SNI integration tests rely on the kind manifest at k8s/kind/control-plane.yaml
// running the control plane with --sni-routing-mode=enforce and
// --managed-hostname-suffixes=.dw.test.local. The seeded org is "local" with
// database name "duckgres" and user postgres/postgres
// (k8s/kind/config-store.seed.sql).
//
// Identity is established from the managed hostname (SNI) + username. The
// startup `database` param remains PostgreSQL-visible, while exact catalog names
// may select ducklake/iceberg.

const (
	sniManagedSuffix    = ".dw.test.local"
	sniSeedOrgName      = "local"
	sniSeedDatabaseName = "duckgres"
	sniSeedUser         = "postgres"
	sniSeedPassword     = "postgres"
	sniSeedCatalog      = "ducklake" // the default catalog for the "local" org
	sniBogusPrefix      = "ignored-by-test"
)

// connectWithSNI dials the control plane via port-forward, sets the TLS SNI
// to the provided ServerName, and attempts a Postgres handshake with the
// given database / user / password. Returns the error from the handshake or
// nil on success. Caller must close the conn if non-nil.
func connectWithSNI(ctx context.Context, sni, database, user, password string) (*pgx.Conn, error) {
	if portForward == nil {
		return nil, errors.New("port-forward not initialized")
	}
	pgPort := portForward.currentPort()
	if pgPort == 0 {
		return nil, errors.New("port-forward port not available")
	}

	cfg, err := pgx.ParseConfig(fmt.Sprintf(
		"postgres://%s:%s@127.0.0.1:%d/%s?sslmode=require&connect_timeout=15",
		user, password, pgPort, database,
	))
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.Database = database
	// Override SNI without changing the dial host. ServerName is the only
	// knob the control plane reads; InsecureSkipVerify is fine because the
	// kind cluster uses self-signed certs and port-forward already breaks
	// the canonical hostname.
	cfg.TLSConfig = &tls.Config{ServerName: sni, InsecureSkipVerify: true}

	return pgx.ConnectConfig(ctx, cfg)
}

// TestSNI_MatchedHostnameSelectsCatalog: a managed SNI resolves the org, and an
// explicit `database=ducklake` selects that catalog for the session.
func TestSNI_MatchedHostnameSelectsCatalog(t *testing.T) {
	if err := waitForDBReady(60 * time.Second); err != nil {
		t.Fatalf("waitForDBReady: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := connectWithSNI(ctx, sniSeedOrgName+sniManagedSuffix,
		sniSeedCatalog, sniSeedUser, sniSeedPassword)
	if err != nil {
		t.Fatalf("expected managed SNI=%q with catalog %q to authenticate; got: %v",
			sniSeedOrgName+sniManagedSuffix, sniSeedCatalog, err)
	}
	defer conn.Close(ctx)

	var current string
	if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&current); err != nil {
		t.Fatalf("SELECT current_database(): %v", err)
	}
	if current != sniSeedCatalog {
		t.Fatalf("current_database() should be the selected catalog; got %q, want %q",
			current, sniSeedCatalog)
	}
}

// TestSNI_MatchedHostnameDefaultsCatalogWhenDatabaseEmpty: an empty startup
// database lands the session in the org's default attached catalog.
func TestSNI_MatchedHostnameDefaultsCatalogWhenDatabaseEmpty(t *testing.T) {
	if err := waitForDBReady(60 * time.Second); err != nil {
		t.Fatalf("waitForDBReady: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := connectWithSNI(ctx, sniSeedOrgName+sniManagedSuffix,
		"", sniSeedUser, sniSeedPassword)
	if err != nil {
		t.Fatalf("expected managed SNI=%q without database param to authenticate; got: %v",
			sniSeedOrgName+sniManagedSuffix, err)
	}
	defer conn.Close(ctx)

	var current string
	if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&current); err != nil {
		t.Fatalf("SELECT current_database(): %v", err)
	}
	if current != sniSeedCatalog {
		t.Fatalf("empty database should default to the org catalog; got %q, want %q",
			current, sniSeedCatalog)
	}
}

// TestSNI_UnknownHostnameRejected: a managed-suffix hostname whose prefix
// resolves to no org is rejected — identity comes solely from the hostname.
func TestSNI_UnknownHostnameRejected(t *testing.T) {
	if err := waitForDBReady(60 * time.Second); err != nil {
		t.Fatalf("waitForDBReady: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := connectWithSNI(ctx, sniBogusPrefix+sniManagedSuffix,
		sniSeedCatalog, sniSeedUser, sniSeedPassword)
	if err == nil {
		t.Fatalf("expected unknown managed hostname to be rejected")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected pg error; got: %T %v", err, err)
	}
	if pgErr.Code != "08006" {
		t.Fatalf("SQLSTATE = %q, want 08006", pgErr.Code)
	}
}

// TestSNI_LegacyHostnameRejected: an unmanaged hostname (e.g. the raw
// port-forward host) has no org and is rejected under enforce — there is no
// database-param fallback.
func TestSNI_LegacyHostnameRejected(t *testing.T) {
	if err := waitForDBReady(60 * time.Second); err != nil {
		t.Fatalf("waitForDBReady: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := connectWithSNI(ctx, "127.0.0.1",
		sniSeedCatalog, sniSeedUser, sniSeedPassword)
	if err == nil {
		t.Fatalf("expected unmanaged hostname to be rejected under enforce")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected pg error; got: %T %v", err, err)
	}
	if pgErr.Code != "08006" {
		t.Fatalf("SQLSTATE = %q, want 08006", pgErr.Code)
	}
}

// TestSNI_InvalidCatalogRejected: a managed hostname authenticates, but an
// unknown database/catalog name fails with 3D000.
func TestSNI_InvalidCatalogRejected(t *testing.T) {
	if err := waitForDBReady(60 * time.Second); err != nil {
		t.Fatalf("waitForDBReady: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := connectWithSNI(ctx, sniSeedOrgName+sniManagedSuffix,
		"not_a_catalog", sniSeedUser, sniSeedPassword)
	if err == nil {
		t.Fatalf("expected an invalid catalog name to be rejected")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected pg error; got: %T %v", err, err)
	}
	if pgErr.Code != "3D000" {
		t.Fatalf("SQLSTATE = %q, want 3D000", pgErr.Code)
	}
}
