//go:build k8s_integration

package k8s_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// SNI integration tests rely on the kind manifest at k8s/kind/control-plane.yaml
// running the control plane with --sni-routing-mode=passthrough and
// --managed-hostname-suffixes=.dw.test.local. The seeded org is "local" with
// database name "duckgres" and user postgres/postgres
// (k8s/kind/config-store.seed.sql).

const (
	sniManagedSuffix    = ".dw.test.local"
	sniSeedOrgName      = "local"
	sniSeedDatabaseName = "duckgres"
	sniSeedUser         = "postgres"
	sniSeedPassword     = "postgres"
	sniBogusPrefix      = "ignored-by-test"
	// sniReportedCatalog is what current_database() returns for the
	// DuckLake-backed 'local' org: the stable physical catalog name, not the
	// per-connection routing dbname (see sessionmeta.ReportedDatabaseName).
	sniReportedCatalog = "ducklake"
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

// TestSNI_MatchedHostnameUsesDatabaseParam: a managed SNI resolves to the same
// org as the requested database, and the startup database remains the routing
// key when present.
func TestSNI_MatchedHostnameUsesDatabaseParam(t *testing.T) {
	if err := waitForDBReady(60 * time.Second); err != nil {
		t.Fatalf("waitForDBReady: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := connectWithSNI(ctx, sniSeedOrgName+sniManagedSuffix,
		sniSeedDatabaseName, sniSeedUser, sniSeedPassword)
	if err != nil {
		t.Fatalf("expected managed SNI=%q with database param %q to authenticate; got: %v",
			sniSeedOrgName+sniManagedSuffix, sniSeedDatabaseName, err)
	}
	defer conn.Close(ctx)

	var current string
	if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&current); err != nil {
		t.Fatalf("SELECT current_database(): %v", err)
	}
	// The 'local' org is DuckLake-backed, so current_database() reports the
	// stable physical catalog ("ducklake"), not the routing dbname. SNI
	// routing correctness is established by the successful connect above.
	if current != sniReportedCatalog {
		t.Fatalf("DuckLake-backed session should report the physical catalog; got %q, want %q",
			current, sniReportedCatalog)
	}
}

// TestSNI_MatchedHostnameUsesSNIWhenDatabaseParamEmpty: a client that omits
// the startup database can still route through a managed hostname that resolves
// to a configured org.
func TestSNI_MatchedHostnameUsesSNIWhenDatabaseParamEmpty(t *testing.T) {
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
	if current != sniReportedCatalog {
		t.Fatalf("DuckLake-backed session should report the physical catalog; got %q, want %q",
			current, sniReportedCatalog)
	}
}

// TestSNI_MatchedHostnameRejectsDifferentDatabaseOrg: a managed hostname
// cannot be used as a generic valid hostname for a different requested
// database. The URL and startup database must resolve to the same org.
func TestSNI_MatchedHostnameRejectsDifferentDatabaseOrg(t *testing.T) {
	if err := waitForDBReady(60 * time.Second); err != nil {
		t.Fatalf("waitForDBReady: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := connectWithSNI(ctx, sniBogusPrefix+sniManagedSuffix,
		sniSeedDatabaseName, sniSeedUser, sniSeedPassword)
	if err == nil {
		t.Fatalf("expected managed SNI for a different org to be rejected")
	}
	if !strings.Contains(err.Error(), "does not match managed hostname") {
		t.Fatalf("expected database/hostname mismatch error; got: %v", err)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected pg error; got: %T %v", err, err)
	}
	if pgErr.Code != "28000" {
		t.Fatalf("SQLSTATE = %q, want 28000", pgErr.Code)
	}
}

// TestSNI_LegacyHostnameFallsThroughInPassthrough: with the kind setup in
// passthrough mode, an SNI that doesn't match a managed suffix falls back
// to the database startup param. The connection should succeed AND the
// control plane should emit a warn log identifying the legacy caller.
// (We can't easily assert the log line from here without log scraping; the
// happy path itself proves the fallback works.)
func TestSNI_LegacyHostnameFallsThroughInPassthrough(t *testing.T) {
	if err := waitForDBReady(60 * time.Second); err != nil {
		t.Fatalf("waitForDBReady: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Use 127.0.0.1 as the SNI (matches what lib/pq would send when
	// connecting via port-forward without override). Definitely not on
	// .dw.test.local, so it falls through to the legacy database-param
	// path with a warn log.
	conn, err := connectWithSNI(ctx, "127.0.0.1",
		sniSeedDatabaseName, sniSeedUser, sniSeedPassword)
	if err != nil {
		t.Fatalf("legacy hostname should still authenticate via database-param fallback in passthrough mode; got: %v", err)
	}
	defer conn.Close(ctx)

	var current string
	if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&current); err != nil {
		t.Fatalf("SELECT current_database(): %v", err)
	}
	if current != sniReportedCatalog {
		t.Fatalf("DuckLake-backed session should report the physical catalog; got %q, want %q",
			current, sniReportedCatalog)
	}
}
