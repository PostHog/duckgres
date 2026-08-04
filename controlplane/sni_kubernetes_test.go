//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
)

func TestExtractOrgFromSNI(t *testing.T) {
	cp := &ControlPlane{
		cfg: ControlPlaneConfig{
			ManagedHostnameSuffixes: []string{".dw.us.postwh.com", ".dw.dev.postwh.com"},
		},
	}

	cases := []struct {
		name      string
		sni       string
		wantOrg   string
		wantMatch bool
	}{
		{"empty SNI", "", "", false},
		{"single-label match prod", "acme.dw.us.postwh.com", "acme", true},
		{"single-label match dev", "betalabs.dw.dev.postwh.com", "betalabs", true},
		{"unmanaged hostname", "duckgres-db.internal.ec2.us-east-1.dev.posthog.dev", "", false},
		{"bare suffix only (no prefix)", ".dw.us.postwh.com", "", false},
		{"multi-label prefix", "evil.acme.dw.us.postwh.com", "", false},
		{"different domain entirely", "example.com", "", false},
		{"prefix with hyphens is fine", "my-org.dw.us.postwh.com", "my-org", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotOrg, gotMatch := cp.extractOrgFromSNI(tc.sni)
			if gotOrg != tc.wantOrg || gotMatch != tc.wantMatch {
				t.Fatalf("extractOrgFromSNI(%q) = (%q, %v); want (%q, %v)",
					tc.sni, gotOrg, gotMatch, tc.wantOrg, tc.wantMatch)
			}
		})
	}
}

func TestExtractOrgFromSNIEmptySuffixes(t *testing.T) {
	cp := &ControlPlane{cfg: ControlPlaneConfig{}}
	if org, ok := cp.extractOrgFromSNI("acme.dw.us.postwh.com"); ok || org != "" {
		t.Fatalf("with no suffixes configured, want (\"\", false); got (%q, %v)", org, ok)
	}
}

func TestExtractMetadataOrgFromSNIAllManagedWarehouseEnvironments(t *testing.T) {
	cp := &ControlPlane{
		cfg: ControlPlaneConfig{
			MetadataHostnameSuffixes: []string{
				".md.dev.postwh.com",
				".md.us.postwh.com",
				".md.eu.postwh.com",
			},
		},
	}

	for _, hostname := range []string{
		"acme.md.dev.postwh.com",
		"acme.md.us.postwh.com",
		"acme.md.eu.postwh.com",
	} {
		t.Run(hostname, func(t *testing.T) {
			org, ok := cp.extractMetadataOrgFromSNI(hostname)
			if !ok || org != "acme" {
				t.Fatalf("extractMetadataOrgFromSNI(%q) = (%q, %v); want (\"acme\", true)", hostname, org, ok)
			}
		})
	}

	for _, hostname := range []string{
		"acme.md.postwh.com",
		"nested.acme.md.eu.postwh.com",
		".md.dev.postwh.com",
	} {
		t.Run(hostname, func(t *testing.T) {
			org, ok := cp.extractMetadataOrgFromSNI(hostname)
			if ok || org != "" {
				t.Fatalf("extractMetadataOrgFromSNI(%q) = (%q, %v); want (\"\", false)", hostname, org, ok)
			}
		})
	}
}

func TestManagedHostnameHint(t *testing.T) {
	cases := []struct {
		name     string
		suffixes []string
		want     string
	}{
		{
			name:     "single dev suffix",
			suffixes: []string{".dw.dev.postwh.com"},
			want:     "<org-id>.dw.dev.postwh.com",
		},
		{
			name:     "single prod-us suffix",
			suffixes: []string{".dw.us.postwh.com"},
			want:     "<org-id>.dw.us.postwh.com",
		},
		{
			name:     "multiple suffixes are listed with 'or'",
			suffixes: []string{".dw.us.postwh.com", ".dw.eu.postwh.com"},
			want:     "<org-id>.dw.us.postwh.com or <org-id>.dw.eu.postwh.com",
		},
		{
			name:     "empty suffixes fall back to a generic placeholder",
			suffixes: nil,
			want:     "<org-id>.<managed-suffix>",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cp := &ControlPlane{cfg: ControlPlaneConfig{ManagedHostnameSuffixes: tc.suffixes}}
			if got := cp.managedHostnameHint(); got != tc.want {
				t.Fatalf("managedHostnameHint() = %q; want %q", got, tc.want)
			}
		})
	}
}

// fakeConfigStore captures calls and lets each test choose what each method
// returns. Only methods used by SNI routing tests are implemented; the rest are
// stubbed to fail loudly if hit.
type fakeConfigStore struct {
	resolveDatabase           func(string) string
	databaseNameForSNIPrefix  func(string) string
	resolveSNIPrefix          func(string) (string, string)
	resolvePostgresConnection func(startupDatabase, sniPrefix string, useManagedSNI bool, username, password string) configstore.PostgresConnectionResolution

	resolveDatabaseCalls           int
	databaseNameForSNIPrefixCalls  int
	resolveSNIPrefixCalls          int
	resolvePostgresConnectionCalls int
}

func (f *fakeConfigStore) ResolveDatabase(database string) string {
	f.resolveDatabaseCalls++
	if f.resolveDatabase == nil {
		return ""
	}
	return f.resolveDatabase(database)
}
func (f *fakeConfigStore) DatabaseNameForSNIPrefix(prefix string) string {
	f.databaseNameForSNIPrefixCalls++
	if f.databaseNameForSNIPrefix == nil {
		return prefix // back-compat default: prefix is its own dbname
	}
	return f.databaseNameForSNIPrefix(prefix)
}
func (f *fakeConfigStore) ResolveSNIPrefix(prefix string) (string, string) {
	f.resolveSNIPrefixCalls++
	if f.resolveSNIPrefix == nil {
		return "", ""
	}
	return f.resolveSNIPrefix(prefix)
}
func (f *fakeConfigStore) ResolvePostgresConnection(startupDatabase, sniPrefix string, useManagedSNI bool, username, password string) configstore.PostgresConnectionResolution {
	f.resolvePostgresConnectionCalls++
	if f.resolvePostgresConnection == nil {
		return configstore.PostgresConnectionResolution{}
	}
	return f.resolvePostgresConnection(startupDatabase, sniPrefix, useManagedSNI, username, password)
}
func (f *fakeConfigStore) OrgWarehouseStatus(string) (string, bool) {
	// SNI tests don't exercise the warehouse-status connection-error path.
	return "", false
}
func (f *fakeConfigStore) OrgDefaultWorkerProfile(string) (string, string, string) {
	// SNI tests don't exercise worker-profile resolution; "not set" keeps the
	// default (nil) profile semantics.
	return "", "", ""
}
func (f *fakeConfigStore) OrgUsageTeamID(string, string) int64 {
	// SNI tests don't exercise analytics team attribution.
	return 0
}
func newSNIControlPlane(store *fakeConfigStore) *ControlPlane {
	return &ControlPlane{
		cfg: ControlPlaneConfig{
			ManagedHostnameSuffixes: []string{".dw.us.postwh.com"},
		},
		configStore: store,
	}
}

func TestPostgresSNIRequiresManagedOrgMatch(t *testing.T) {
	store := &fakeConfigStore{}
	cp := newSNIControlPlane(store)

	resolution := cp.resolvePostgresSNI(
		SNIRoutingEnforce,
		"test-org-smoke-1778167994.dw.us.postwh.com",
	)

	if !resolution.isManaged {
		t.Fatalf("expected SNI to match managed hostname")
	}
	if !resolution.useManagedSNI {
		t.Fatalf("managed SNI should require same-org validation")
	}
	if store.resolveSNIPrefixCalls != 0 {
		t.Fatalf("ResolveSNIPrefix calls = %d, want 0", store.resolveSNIPrefixCalls)
	}
}

func TestPostgresSNIOffIgnoresSNI(t *testing.T) {
	store := &fakeConfigStore{
		resolveSNIPrefix: func(prefix string) (string, string) {
			t.Fatalf("ResolveSNIPrefix should not be called in off mode; got %q", prefix)
			return "", ""
		},
	}
	cp := newSNIControlPlane(store)

	resolution := cp.resolvePostgresSNI(
		SNIRoutingOff,
		"test-org-smoke-1778167994.dw.us.postwh.com",
	)

	if resolution.useManagedSNI {
		t.Fatalf("off mode should not require SNI org validation")
	}
	if store.resolveSNIPrefixCalls != 0 {
		t.Fatalf("ResolveSNIPrefix calls = %d, want 0", store.resolveSNIPrefixCalls)
	}
}

func TestPostgresSNIUnknownModeIgnoresSNI(t *testing.T) {
	store := &fakeConfigStore{
		resolveSNIPrefix: func(prefix string) (string, string) {
			t.Fatalf("ResolveSNIPrefix should not be called for unknown mode; got %q", prefix)
			return "", ""
		},
	}
	cp := newSNIControlPlane(store)

	resolution := cp.resolvePostgresSNI(
		"passthru",
		"test-org-smoke-1778167994.dw.us.postwh.com",
	)

	if resolution.useManagedSNI {
		t.Fatalf("unknown mode should not require SNI org validation")
	}
	if store.resolveSNIPrefixCalls != 0 {
		t.Fatalf("ResolveSNIPrefix calls = %d, want 0", store.resolveSNIPrefixCalls)
	}
}

// A non-selectable database name (anything other than ducklake/empty)
// is rejected with 3D000 — the database param is now catalog selection, not an
// org/identity routing key.
func TestPostgresInvalidCatalogSQLSTATE(t *testing.T) {
	store := &fakeConfigStore{
		resolvePostgresConnection: func(startupDatabase, sniPrefix string, useManagedSNI bool, username, password string) configstore.PostgresConnectionResolution {
			if startupDatabase != "requested_db" || sniPrefix != "other-org" || !useManagedSNI || username != "root" || password != "secret" {
				t.Fatalf("unexpected ResolvePostgresConnection args: db=%q sni=%q use=%v user=%q pass=%q",
					startupDatabase, sniPrefix, useManagedSNI, username, password)
			}
			return configstore.PostgresConnectionResolution{
				OrgID:        "other-org",
				SNIOrgID:     "other-org",
				SNIResolved:  true,
				CatalogValid: false, // "requested_db" is not ducklake
				Valid:        true,
			}
		},
	}
	cp := newSNIControlPlane(store)
	cp.cfg.SNIRoutingMode = SNIRoutingEnforce
	cp.tlsConfig = testControlPlaneTLSConfig(t)

	cfg, err := pgconn.ParseConfig("postgres://root:secret@127.0.0.1/requested_db?sslmode=require")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	cfg.TLSConfig = &tls.Config{
		ServerName:         "other-org.dw.us.postwh.com",
		InsecureSkipVerify: true, // test self-signed cert
	}
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, serverConn := net.Pipe()
		go cp.handleConnection(serverConn)
		return client, nil
	}

	conn, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err == nil {
		_ = conn.Close(context.Background())
		t.Fatal("expected invalid catalog to reject connection")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected pg error; got: %T %v", err, err)
	}
	if pgErr.Code != "3D000" {
		t.Fatalf("SQLSTATE = %q, want 3D000", pgErr.Code)
	}
}

func testControlPlaneTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	if err := server.EnsureCertificates(certFile, keyFile); err != nil {
		t.Fatalf("EnsureCertificates: %v", err)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}
}
