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
)

// SNI integration tests rely on the kind manifest at k8s/kind/control-plane.yaml
// running the control plane with --sni-routing-mode=passthrough and
// --managed-hostname-suffixes=.dw.test.local. The seeded org has database
// name "duckgres" with user postgres/postgres (k8s/kind/config-store.seed.sql).

const (
	sniManagedSuffix      = ".dw.test.local"
	sniSeedDatabaseName   = "duckgres"
	sniSeedUser           = "postgres"
	sniSeedPassword       = "postgres"
	sniBogusDatabaseParam = "ignored-by-test"
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
	// Override SNI without changing the dial host. ServerName is the only
	// knob the control plane reads; InsecureSkipVerify is fine because the
	// kind cluster uses self-signed certs and port-forward already breaks
	// the canonical hostname.
	cfg.TLSConfig = &tls.Config{ServerName: sni, InsecureSkipVerify: true}

	return pgx.ConnectConfig(ctx, cfg)
}

// TestSNI_MatchedHostnameOverridesDatabaseParam: the SNI resolves to the
// seeded org regardless of what database name the client passes in the
// startup packet. Proves SNI is authoritative when it matches a managed
// suffix (passthrough mode).
func TestSNI_MatchedHostnameOverridesDatabaseParam(t *testing.T) {
	if err := waitForDBReady(60 * time.Second); err != nil {
		t.Fatalf("waitForDBReady: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := connectWithSNI(ctx, sniSeedDatabaseName+sniManagedSuffix,
		sniBogusDatabaseParam, sniSeedUser, sniSeedPassword)
	if err != nil {
		t.Fatalf("expected SNI=%q to override database param %q and authenticate; got: %v",
			sniSeedDatabaseName+sniManagedSuffix, sniBogusDatabaseParam, err)
	}
	defer conn.Close(ctx)

	var current string
	if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&current); err != nil {
		t.Fatalf("SELECT current_database(): %v", err)
	}
	if current != sniSeedDatabaseName {
		t.Fatalf("SNI routing should land us in the SNI-named database; got %q, want %q",
			current, sniSeedDatabaseName)
	}
}

// TestSNI_MatchedHostnameWithUnknownOrg: SNI matches the managed suffix but
// the prefix doesn't resolve to a real org. The control plane must reject
// (no fallback to the database param) — silently routing to a different org
// would defeat the boundary. Error code is 3D000 (database does not exist).
func TestSNI_MatchedHostnameWithUnknownOrg(t *testing.T) {
	if err := waitForDBReady(60 * time.Second); err != nil {
		t.Fatalf("waitForDBReady: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err := connectWithSNI(ctx, "ghostorg"+sniManagedSuffix,
		sniSeedDatabaseName, sniSeedUser, sniSeedPassword)
	if err == nil {
		t.Fatalf("expected unknown SNI org to be rejected even with a valid database param")
	}
	if !strings.Contains(err.Error(), "ghostorg") || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("expected 'database \"ghostorg\" does not exist' error; got: %v", err)
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
	if current != sniSeedDatabaseName {
		t.Fatalf("legacy fallback should land us in the param-named database; got %q, want %q",
			current, sniSeedDatabaseName)
	}
}
