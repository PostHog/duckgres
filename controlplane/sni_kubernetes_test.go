//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

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
	validateOrgUser           func(orgID, user, pass string) bool
	findAndValidateUser       func(user, pass string) (string, bool)

	resolveDatabaseCalls           int
	databaseNameForSNIPrefixCalls  int
	resolveSNIPrefixCalls          int
	resolvePostgresConnectionCalls int
	validateOrgUserCalls           int
	findAndValidateUserCalls       int
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
func (f *fakeConfigStore) ValidateOrgUser(orgID, user, pass string) bool {
	f.validateOrgUserCalls++
	if f.validateOrgUser == nil {
		return false
	}
	return f.validateOrgUser(orgID, user, pass)
}
func (f *fakeConfigStore) FindAndValidateUser(user, pass string) (string, bool) {
	f.findAndValidateUserCalls++
	if f.findAndValidateUser == nil {
		return "", false
	}
	return f.findAndValidateUser(user, pass)
}
func (f *fakeConfigStore) IsOrgUserPassthrough(string, string) bool {
	// SNI tests don't exercise passthrough; the real flag lookup is covered
	// elsewhere. Returning false keeps the existing assertions intact.
	return false
}
func (f *fakeConfigStore) ValidateOrgUserAndGetPassthrough(orgID, user, pass string) (bool, bool) {
	// SNI tests drive Flight SQL, not the PG auth path that uses the
	// combined call. Forward to ValidateOrgUser so the test fakes that set
	// validateOrgUser still work unchanged.
	return f.ValidateOrgUser(orgID, user, pass), false
}
func (f *fakeConfigStore) OrgWarehouseStatus(string) (string, bool) {
	// SNI tests don't exercise the warehouse-status connection-error path.
	return "", false
}
func (f *fakeConfigStore) UpsertFlightSessionRecord(*configstore.FlightSessionRecord) error {
	panic("UpsertFlightSessionRecord should not be called from SNI tests")
}
func (f *fakeConfigStore) GetFlightSessionRecord(string) (*configstore.FlightSessionRecord, error) {
	panic("GetFlightSessionRecord should not be called from SNI tests")
}
func (f *fakeConfigStore) TouchFlightSessionRecord(string, time.Time) error {
	panic("TouchFlightSessionRecord should not be called from SNI tests")
}
func (f *fakeConfigStore) CloseFlightSessionRecord(string, time.Time) error {
	panic("CloseFlightSessionRecord should not be called from SNI tests")
}

func newFlightValidator(t *testing.T, mode string, store *fakeConfigStore) *cpFlightCredentialValidator {
	t.Helper()
	cp := &ControlPlane{
		cfg: ControlPlaneConfig{
			SNIRoutingMode:          mode,
			ManagedHostnameSuffixes: []string{".dw.us.postwh.com"},
		},
		configStore: store,
	}
	provider := &orgRoutedSessionProvider{
		userOrg: make(map[string]string),
	}
	return &cpFlightCredentialValidator{cp: cp, orgProvider: provider}
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

func TestPostgresManagedHostnameMismatchSQLSTATE(t *testing.T) {
	store := &fakeConfigStore{
		resolvePostgresConnection: func(startupDatabase, sniPrefix string, useManagedSNI bool, username, password string) configstore.PostgresConnectionResolution {
			if startupDatabase != "requested_db" || sniPrefix != "other-org" || !useManagedSNI || username != "root" || password != "secret" {
				t.Fatalf("unexpected ResolvePostgresConnection args: db=%q sni=%q use=%v user=%q pass=%q",
					startupDatabase, sniPrefix, useManagedSNI, username, password)
			}
			return configstore.PostgresConnectionResolution{
				EffectiveDatabase: "requested_db",
				OrgID:             "requested-org",
				SNIOrgID:          "other-org",
				DatabaseExists:    true,
				HostnameMatches:   false,
			}
		},
	}
	cp := newSNIControlPlane(store)
	cp.cfg.SNIRoutingMode = SNIRoutingPassthrough
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
		t.Fatal("expected managed hostname mismatch to reject connection")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected pg error; got: %T %v", err, err)
	}
	if pgErr.Code != "28000" {
		t.Fatalf("SQLSTATE = %q, want 28000", pgErr.Code)
	}
	if pgErr.Message != "requested database does not match managed hostname" {
		t.Fatalf("message = %q", pgErr.Message)
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

// TestFlightValidatorOff: SNI ignored entirely. Both legacy and new
// hostnames go through FindAndValidateUser; ResolveDatabase / ValidateOrgUser
// are never called regardless of SNI.
func TestFlightValidatorOff(t *testing.T) {
	store := &fakeConfigStore{
		findAndValidateUser: func(user, pass string) (string, bool) {
			return "org-by-scan", user == "alice" && pass == "secret"
		},
	}
	v := newFlightValidator(t, SNIRoutingOff, store)

	cases := []struct {
		name string
		sni  string
	}{
		{"matching SNI", "acme.dw.us.postwh.com"},
		{"empty SNI", ""},
		{"unmanaged SNI", "duckgres-db.internal.ec2.us-east-1.dev.posthog.dev"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !v.ValidateCredentialsForSNI(tc.sni, "alice", "secret") {
				t.Fatalf("expected valid credentials to pass in off mode")
			}
		})
	}
	if store.resolveDatabaseCalls != 0 || store.validateOrgUserCalls != 0 {
		t.Fatalf("off mode must not consult ResolveDatabase / ValidateOrgUser; got %d / %d",
			store.resolveDatabaseCalls, store.validateOrgUserCalls)
	}
	if store.findAndValidateUserCalls != len(cases) {
		t.Fatalf("expected %d FindAndValidateUser calls, got %d",
			len(cases), store.findAndValidateUserCalls)
	}
}

// TestFlightValidatorPassthroughMatchedSNI: SNI matches, so we resolve and
// validate against a single org, never falling through to the scan.
func TestFlightValidatorPassthroughMatchedSNI(t *testing.T) {
	store := &fakeConfigStore{
		resolveDatabase: func(name string) string {
			if name == "acme" {
				return "org-acme"
			}
			return ""
		},
		validateOrgUser: func(orgID, user, pass string) bool {
			return orgID == "org-acme" && user == "alice" && pass == "secret"
		},
		findAndValidateUser: func(string, string) (string, bool) {
			t.Fatalf("FindAndValidateUser must not be called when SNI resolves an org")
			return "", false
		},
	}
	v := newFlightValidator(t, SNIRoutingPassthrough, store)

	if !v.ValidateCredentialsForSNI("acme.dw.us.postwh.com", "alice", "secret") {
		t.Fatalf("expected SNI-resolved org with valid creds to pass")
	}
	if store.resolveDatabaseCalls != 1 || store.validateOrgUserCalls != 1 {
		t.Fatalf("expected one ResolveDatabase + one ValidateOrgUser; got %d / %d",
			store.resolveDatabaseCalls, store.validateOrgUserCalls)
	}
	if got := v.orgProvider.userOrg["alice"]; got != "org-acme" {
		t.Fatalf("expected userOrg['alice'] = org-acme; got %q", got)
	}
}

// TestFlightValidatorPassthroughHostnameAliasResolves: SNI prefix is the
// hostname alias for an org whose dbname is something different. The
// validator must consult DatabaseNameForSNIPrefix to translate prefix →
// dbname before looking up the orgID.
func TestFlightValidatorPassthroughHostnameAliasResolves(t *testing.T) {
	store := &fakeConfigStore{
		databaseNameForSNIPrefix: func(prefix string) string {
			if prefix == "entirely-chief-wildcat" {
				return "portola" // alias-translated dbname
			}
			return prefix
		},
		resolveDatabase: func(name string) string {
			if name == "portola" {
				return "org-portola"
			}
			return ""
		},
		validateOrgUser: func(orgID, user, pass string) bool {
			return orgID == "org-portola" && user == "alice" && pass == "secret"
		},
		findAndValidateUser: func(string, string) (string, bool) {
			t.Fatalf("FindAndValidateUser must not be called when SNI alias resolves an org")
			return "", false
		},
	}
	v := newFlightValidator(t, SNIRoutingPassthrough, store)

	if !v.ValidateCredentialsForSNI("entirely-chief-wildcat.dw.us.postwh.com", "alice", "secret") {
		t.Fatalf("expected alias-resolved org with valid creds to pass")
	}
	if store.databaseNameForSNIPrefixCalls != 1 {
		t.Fatalf("expected DatabaseNameForSNIPrefix to be consulted exactly once; got %d", store.databaseNameForSNIPrefixCalls)
	}
	if store.resolveDatabaseCalls != 1 {
		t.Fatalf("expected one ResolveDatabase call (against translated dbname); got %d", store.resolveDatabaseCalls)
	}
	if got := v.orgProvider.userOrg["alice"]; got != "org-portola" {
		t.Fatalf("expected userOrg['alice'] = org-portola; got %q", got)
	}
}

// TestFlightValidatorPassthroughUnknownOrg: SNI matches the suffix, but the
// resolved org name doesn't exist in the config store. Must return false
// WITHOUT falling through to the scan (a managed hostname is authoritative —
// silently routing to a different org would defeat the boundary).
func TestFlightValidatorPassthroughUnknownOrg(t *testing.T) {
	store := &fakeConfigStore{
		resolveDatabase: func(string) string { return "" }, // unknown
		findAndValidateUser: func(string, string) (string, bool) {
			t.Fatalf("FindAndValidateUser must not be called for unknown SNI org")
			return "", false
		},
	}
	v := newFlightValidator(t, SNIRoutingPassthrough, store)

	if v.ValidateCredentialsForSNI("ghostorg.dw.us.postwh.com", "alice", "secret") {
		t.Fatalf("unknown SNI org must not authenticate")
	}
}

// TestFlightValidatorPassthroughLegacyHostname: SNI doesn't match a managed
// suffix → fall back to the scan path (with a warn log we don't assert here).
func TestFlightValidatorPassthroughLegacyHostname(t *testing.T) {
	store := &fakeConfigStore{
		findAndValidateUser: func(user, pass string) (string, bool) {
			return "org-from-scan", user == "alice" && pass == "secret"
		},
	}
	v := newFlightValidator(t, SNIRoutingPassthrough, store)

	if !v.ValidateCredentialsForSNI("duckgres-db.internal.ec2.us-east-1.dev.posthog.dev", "alice", "secret") {
		t.Fatalf("legacy hostname should pass via scan in passthrough mode")
	}
	if store.findAndValidateUserCalls != 1 {
		t.Fatalf("expected scan fallback; got %d FindAndValidateUser calls", store.findAndValidateUserCalls)
	}
	if got := v.orgProvider.userOrg["alice"]; got != "org-from-scan" {
		t.Fatalf("expected userOrg['alice'] = org-from-scan; got %q", got)
	}
}

// TestFlightValidatorEnforceMatchedSNI: same as passthrough+matched.
func TestFlightValidatorEnforceMatchedSNI(t *testing.T) {
	store := &fakeConfigStore{
		resolveDatabase: func(name string) string {
			if name == "acme" {
				return "org-acme"
			}
			return ""
		},
		validateOrgUser: func(orgID, user, pass string) bool {
			return orgID == "org-acme" && user == "alice" && pass == "secret"
		},
	}
	v := newFlightValidator(t, SNIRoutingEnforce, store)
	if !v.ValidateCredentialsForSNI("acme.dw.us.postwh.com", "alice", "secret") {
		t.Fatalf("expected enforce+matched to pass")
	}
}

// TestFlightValidatorEnforceLegacyHostnameRejected: the contract of enforce.
// Even with otherwise-valid credentials, a non-managed hostname must fail
// without hitting the scan.
func TestFlightValidatorEnforceLegacyHostnameRejected(t *testing.T) {
	store := &fakeConfigStore{
		findAndValidateUser: func(string, string) (string, bool) {
			t.Fatalf("FindAndValidateUser must not be called in enforce mode")
			return "", false
		},
	}
	v := newFlightValidator(t, SNIRoutingEnforce, store)

	if v.ValidateCredentialsForSNI("", "alice", "secret") {
		t.Fatalf("enforce must reject empty SNI")
	}
	if v.ValidateCredentialsForSNI("duckgres-db.internal.ec2.us-east-1.dev.posthog.dev", "alice", "secret") {
		t.Fatalf("enforce must reject legacy hostname")
	}
}
