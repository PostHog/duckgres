//go:build kubernetes

package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner/opa"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// --- fakes ---

type fakeTrinoStore struct {
	mu       sync.Mutex
	orgs     []configstore.TrinoEnabledOrg
	states   map[string]configstore.TrinoStateUpdate // captured per-org state writes
	cells    map[string]string                       // captured cell claims
	cellErr  error                                   // injectable: fail AssignTrinoCell
	claimLog []string
}

func (s *fakeTrinoStore) ListTrinoEnabledOrgs() ([]configstore.TrinoEnabledOrg, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]configstore.TrinoEnabledOrg, len(s.orgs))
	copy(out, s.orgs)
	return out, nil
}

func (s *fakeTrinoStore) UpdateTrinoState(orgID string, upd configstore.TrinoStateUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.states == nil {
		s.states = make(map[string]configstore.TrinoStateUpdate)
	}
	s.states[orgID] = upd
	return nil
}

func (s *fakeTrinoStore) AssignTrinoCell(orgID, cellID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimLog = append(s.claimLog, orgID+"->"+cellID)
	if s.cellErr != nil {
		return s.cellErr
	}
	if s.cells == nil {
		s.cells = make(map[string]string)
	}
	s.cells[orgID] = cellID
	// Mirror the real store: the claim is durable, so the next listing
	// returns the row already stamped.
	for i := range s.orgs {
		if s.orgs[i].OrgID == orgID && s.orgs[i].CellID == "" {
			s.orgs[i].CellID = cellID
		}
	}
	return nil
}

func (s *fakeTrinoStore) lastState(orgID string) (configstore.TrinoStateUpdate, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.states[orgID]
	return v, ok
}

type fakeWarehouseStore struct {
	rows map[string]*configstore.ManagedWarehouse
	err  error
}

func (s *fakeWarehouseStore) GetManagedWarehouseForTrino(orgID string) (*configstore.ManagedWarehouse, error) {
	if s.err != nil {
		return nil, s.err
	}
	row, ok := s.rows[orgID]
	if !ok {
		return nil, nil
	}
	cp := *row
	return &cp, nil
}

// readyWarehouse is a fully-provisioned cnpg-shard warehouse row — the
// shape every catalog input assertion starts from.
func readyWarehouse(orgID string) *configstore.ManagedWarehouse {
	return &configstore.ManagedWarehouse{
		OrgID: orgID,
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:         configstore.MetadataStoreKindCnpgShard,
			Endpoint:     "shard-001-pooler.cnpg-shards.svc.cluster.local",
			Port:         5432,
			DatabaseName: "mdstore_" + orgID,
			Username:     "mdstore_" + orgID,
		},
		S3: configstore.ManagedWarehouseS3{
			Bucket: "posthog-duckling-" + orgID + "-mw-dev",
			Region: "us-east-1",
		},
		WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
			IAMRoleARN: "arn:aws:iam::123456789012:role/duckling-" + orgID,
		},
	}
}

type fakeCatalogClient struct {
	mu        sync.Mutex
	existing  []string
	created   map[string]map[string]string
	dropped   []string
	listErr   error
	createErr error
}

func (c *fakeCatalogClient) ListCatalogs(ctx context.Context) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.listErr != nil {
		return nil, c.listErr
	}
	out := make([]string, len(c.existing))
	copy(out, c.existing)
	return out, nil
}

func (c *fakeCatalogClient) CreateCatalog(ctx context.Context, name string, props map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.createErr != nil {
		return c.createErr
	}
	if c.created == nil {
		c.created = make(map[string]map[string]string)
	}
	cp := make(map[string]string, len(props))
	for k, v := range props {
		cp[k] = v
	}
	c.created[name] = cp
	c.existing = append(c.existing, name)
	return nil
}

func (c *fakeCatalogClient) AlterCatalog(ctx context.Context, name string, props map[string]string) error {
	return c.CreateCatalog(ctx, name, props)
}

func (c *fakeCatalogClient) DropCatalog(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropped = append(c.dropped, name)
	filtered := c.existing[:0]
	for _, e := range c.existing {
		if e != name {
			filtered = append(filtered, e)
		}
	}
	c.existing = filtered
	return nil
}

// --- pure-function tests ---

func TestBuildTrinoAuthFiles_OneUserPerOrg(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", RootPasswordHash: "$2a$10$hash42"},
		{OrgID: "43", RootPasswordHash: "$2a$10$hash43"},
	}
	// Empty admin hash so the test focuses on the per-org projection;
	// admin-line projection is exercised in its own test below.
	pw, grp := BuildTrinoAuthFiles(orgs, "")

	wantPW := "42:$2a$10$hash42\n43:$2a$10$hash43\n"
	if pw != wantPW {
		t.Errorf("password.db =\n%q\nwant\n%q", pw, wantPW)
	}
	// group.db is group-first: <group>:<comma-separated users>. Easy to
	// get backwards, hence the explicit assertion.
	wantGrp := "org_42:42\norg_43:43\n"
	if grp != wantGrp {
		t.Errorf("group.db =\n%q\nwant\n%q", grp, wantGrp)
	}
}

func TestBuildTrinoAuthFiles_DeterministicOrder(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", RootPasswordHash: "$a"},
		{OrgID: "43", RootPasswordHash: "$b"},
	}
	pw1, grp1 := BuildTrinoAuthFiles(orgs, "")
	pw2, grp2 := BuildTrinoAuthFiles(orgs, "")
	if pw1 != pw2 || grp1 != grp2 {
		t.Errorf("projection not deterministic for identical input")
	}
}

func TestBuildTrinoAuthFiles_SkipsEmptyHashes(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", RootPasswordHash: ""},
		{OrgID: "43", RootPasswordHash: "$2a$10$hash43"},
		{OrgID: "", RootPasswordHash: "$2a$10$orphan"},
	}
	pw, grp := BuildTrinoAuthFiles(orgs, "")
	if strings.Contains(pw, "42:") {
		t.Errorf("expected org 42 (empty hash) to be skipped: %q", pw)
	}
	if strings.Contains(grp, "org_:") {
		t.Errorf("expected empty org id to be skipped: %q", grp)
	}
	if !strings.Contains(pw, "43:$2a$10$hash43") {
		t.Errorf("expected org 43 to be projected: %q", pw)
	}
}

func TestBuildTrinoAuthFiles_Empty(t *testing.T) {
	pw, grp := BuildTrinoAuthFiles(nil, "")
	if pw != "" || grp != "" {
		t.Errorf("expected empty files for empty input, got %q / %q", pw, grp)
	}
}

func TestBuildTrinoAuthFiles_IncludesAdminWhenHashProvided(t *testing.T) {
	// The OPA policy's is_admin is a CONJUNCTION of username and group
	// membership, so the admin has to appear in BOTH files or the
	// provisioner cannot manage catalogs at all.
	orgs := []configstore.TrinoEnabledOrg{{OrgID: "42", RootPasswordHash: "$2a$10$hash42"}}
	pw, grp := BuildTrinoAuthFiles(orgs, "$2a$10$adminhash")

	if !strings.HasPrefix(pw, opa.AdminPrincipal+":$2a$10$adminhash\n") {
		t.Errorf("password.db must start with the admin line, got %q", pw)
	}
	if !strings.HasPrefix(grp, opa.AdminGroup+":"+opa.AdminPrincipal+"\n") {
		t.Errorf("group.db must start with the admin group line, got %q", grp)
	}
	if !strings.Contains(pw, "42:$2a$10$hash42") {
		t.Errorf("admin line must not displace tenant lines, got %q", pw)
	}

	// Empty hash (unit-test wiring) omits the admin lines entirely
	// rather than projecting a hash-less, therefore un-authenticatable,
	// entry.
	pwNoAdmin, grpNoAdmin := BuildTrinoAuthFiles(orgs, "")
	if strings.Contains(pwNoAdmin, opa.AdminPrincipal) || strings.Contains(grpNoAdmin, opa.AdminPrincipal) {
		t.Errorf("empty admin hash must project no admin lines, got %q / %q", pwNoAdmin, grpNoAdmin)
	}
}

func TestTrinoCatalogName(t *testing.T) {
	// No `_iceberg` suffix: catalogs are DuckLake, and the OPA pattern +
	// policy.rego literal agree with this shape (see the three-way
	// contract test below).
	cases := map[string]string{
		"42":         "org_42",
		"acme":       "org_acme",
		"Acme-Corp":  "org_acme_corp",
		"42-numbers": "org_42_numbers",
		"with.dot":   "org_with_dot",
	}
	for in, want := range cases {
		if got := TrinoCatalogName(in); got != want {
			t.Errorf("TrinoCatalogName(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestTrinoCatalogNameMatchesManagedNamePattern closes the Go side of
// the Go ↔ Rego naming contract. The OPA policy authorizes admin
// catalog management on names matching opa.ManagedCatalogPattern, and
// the provisioner's own DROP filter compiles the same constant. Every
// name Go-side code produces must therefore match that pattern, or the
// admin loses the authority to manage catalogs Go just created
// (silently — the reconcile loop would log "created" while admin's
// next SHOW CATALOGS / DROP CATALOG hits permission-denied).
//
// Paired with opa/policy_test.go::TestPolicyRegoContainsManagedNamePattern,
// which closes the other side of the contract: the pattern in
// policy.rego must equal the constant. If both tests pass, the three
// surfaces — Go constant, Rego regex literal, TrinoCatalogName output —
// are in sync.
func TestTrinoCatalogNameMatchesManagedNamePattern(t *testing.T) {
	re, err := regexp.Compile(opa.ManagedCatalogPattern)
	if err != nil {
		t.Fatalf("opa.ManagedCatalogPattern is not a valid Go regex: %v", err)
	}

	// Positive cases: representative inputs the production path could
	// produce. Org names are DNS-1123 labels (validated by
	// validateDucklingOrgID in provisioning/api.go), e.g. "42", "acme",
	// "with-dash"; trinoSanitize maps non-[a-z0-9_] chars to '_'
	// (injective over DNS-1123). It also handles odder inputs (dots,
	// case, underscores) defensively — verify those too so any future
	// change that breaks the regex match is caught here.
	positive := []string{
		"42",
		"100",
		"999999",
		"acme",
		"acme_corp",
		"with-dash",                            // sanitize → with_dash
		"with.dot",                             // sanitize → with_dot
		"Mixed-Case",                           // sanitize → mixed_case
		"3fa85f64-5717-4562-b3fc-2c963f66afa6", // a canonical-UUID org id
	}
	for _, id := range positive {
		name := TrinoCatalogName(id)
		if !re.MatchString(name) {
			t.Errorf("TrinoCatalogName(%q) = %q does NOT match opa.ManagedCatalogPattern (%q).\n"+
				"Either trinoSanitize/TrinoCatalogName drifted, or the pattern needs widening — both sides must agree.",
				id, name, opa.ManagedCatalogPattern)
		}
	}

	// Negative cases: catalog names that must NOT match the pattern so
	// admin authority (and the provisioner's DROP filter) can't
	// accidentally cover them. These mirror the non-managed names
	// asserted in policy_test.go's adversarial suite.
	negative := []string{
		"system",
		"jmx",
		"ducklake_org_42",
		"ORG_42",   // uppercase prefix
		"org-42",   // hyphen is outside the sanitize grammar
		"org_42 ",  // trailing space
		"myorg_42", // prefix must be exactly "org_"
	}
	for _, name := range negative {
		if re.MatchString(name) {
			t.Errorf("opa.ManagedCatalogPattern incorrectly matches non-managed name %q.\n"+
				"The pattern is too permissive — admin authority would leak onto names "+
				"the provisioner never creates.", name)
		}
	}
}

func TestBuildTrinoResourceGroups_StructureAndTiers(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", Tier: "free"},
		{OrgID: "43", Tier: "growth"},
		{OrgID: "44", Tier: "scale"},
		{OrgID: "45", Tier: ""},
	}
	raw, err := BuildTrinoResourceGroups(orgs)
	if err != nil {
		t.Fatalf("BuildTrinoResourceGroups: %v", err)
	}
	var parsed resourceGroupsFile
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(parsed.RootGroups) != 1 || parsed.RootGroups[0].Name != "root" {
		t.Fatalf("expected single root group named 'root', got %+v", parsed.RootGroups)
	}
	tiers := parsed.RootGroups[0].SubGroups
	// Expect two sibling tiers: "admin" (for catalog DDL) and
	// "tenants" (for customer queries). Without the admin tier,
	// the provisioner's reconcile-path queries hit Trino's
	// "Query is not associated with any resource group" rejection.
	if len(tiers) != 2 {
		t.Fatalf("expected 2 tiers (admin + tenants), got %d: %+v", len(tiers), tiers)
	}
	tiersByName := map[string]resourceGroupTier{}
	for _, tier := range tiers {
		tiersByName[tier.Name] = tier
	}
	adminTier, hasAdmin := tiersByName["admin"]
	if !hasAdmin {
		t.Fatalf("expected admin tier under root, got tiers=%+v", tiers)
	}
	if len(adminTier.SubGroups) != 1 || adminTier.SubGroups[0].Name != opa.AdminPrincipal {
		t.Errorf("expected single admin subgroup named %q, got %+v", opa.AdminPrincipal, adminTier.SubGroups)
	}

	tenantsTier, hasTenants := tiersByName["tenants"]
	if !hasTenants {
		t.Fatalf("expected tenants tier under root, got tiers=%+v", tiers)
	}
	subs := tenantsTier.SubGroups
	if len(subs) != 4 {
		t.Fatalf("expected 4 org subgroups under tenants, got %d", len(subs))
	}
	// Selector + subgroup name == org name; verify the join.
	wantByName := map[string]int{ // hardConcurrencyLimit per tier
		"42": 3,  // free
		"43": 10, // growth
		"44": 25, // scale
		"45": 3,  // empty → default ("free")
	}
	for _, sg := range subs {
		want, ok := wantByName[sg.Name]
		if !ok {
			t.Errorf("unexpected subgroup name %q", sg.Name)
			continue
		}
		if sg.HardConcurrencyLimit != want {
			t.Errorf("subgroup %s: HardConcurrencyLimit = %d, want %d", sg.Name, sg.HardConcurrencyLimit, want)
		}
	}
	// Selectors: admin + one per org. Admin maps to root.admin.<admin>,
	// each tenant maps to root.tenants.<org_name>. Without the admin
	// selector the provisioner's own queries get rejected by Trino's
	// resource-group manager before reaching OPA — which would silently
	// break EVERY reconcile tick, not just one query.
	wantSel := map[string]string{
		opa.AdminPrincipal: "root.admin." + opa.AdminPrincipal,
		"42":               "root.tenants.42",
		"43":               "root.tenants.43",
		"44":               "root.tenants.44",
		"45":               "root.tenants.45",
	}
	if len(parsed.Selectors) != len(wantSel) {
		t.Fatalf("expected %d selectors (admin + %d orgs), got %d", len(wantSel), len(orgs), len(parsed.Selectors))
	}
	for _, sel := range parsed.Selectors {
		if want, ok := wantSel[sel.User]; !ok || sel.Group != want {
			t.Errorf("selector %+v unexpected (want user→%q)", sel, wantSel[sel.User])
		}
	}
}

func TestDuckLakeMetadataJDBCURL_SSLModeFollowsStoreKind(t *testing.T) {
	cases := []struct {
		name string
		kind string
		port int
		want string
	}{
		{
			name: "cnpg-shard is in-cluster plaintext",
			kind: configstore.MetadataStoreKindCnpgShard,
			port: 5432,
			want: "jdbc:postgresql://pg.example:5432/mdstore?sslmode=disable",
		},
		{
			name: "external requires TLS",
			kind: configstore.MetadataStoreKindExternal,
			port: 5432,
			want: "jdbc:postgresql://pg.example:5432/mdstore?sslmode=require",
		},
		{
			name: "unknown kind fails safe to TLS",
			kind: "some-future-backend",
			port: 5432,
			want: "jdbc:postgresql://pg.example:5432/mdstore?sslmode=require",
		},
		{
			name: "zero port defaults to 5432",
			kind: configstore.MetadataStoreKindCnpgShard,
			port: 0,
			want: "jdbc:postgresql://pg.example:5432/mdstore?sslmode=disable",
		},
		{
			name: "explicit non-default port is preserved",
			kind: configstore.MetadataStoreKindExternal,
			port: 6432,
			want: "jdbc:postgresql://pg.example:6432/mdstore?sslmode=require",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := &configstore.ManagedWarehouse{
				MetadataStore: configstore.ManagedWarehouseMetadataStore{
					Kind:         tc.kind,
					Endpoint:     "pg.example",
					Port:         tc.port,
					DatabaseName: "mdstore",
				},
			}
			if got := ducklakeMetadataJDBCURL(w); got != tc.want {
				t.Errorf("ducklakeMetadataJDBCURL = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDuckLakeDataPath(t *testing.T) {
	cases := []struct {
		name string
		s3   configstore.ManagedWarehouseS3
		want string
	}{
		{"no prefix", configstore.ManagedWarehouseS3{Bucket: "b"}, "s3://b/"},
		{"prefix", configstore.ManagedWarehouseS3{Bucket: "b", PathPrefix: "data"}, "s3://b/data/"},
		{"prefix with slashes", configstore.ManagedWarehouseS3{Bucket: "b", PathPrefix: "/data/"}, "s3://b/data/"},
		{"nested prefix", configstore.ManagedWarehouseS3{Bucket: "b", PathPrefix: "a/b"}, "s3://b/a/b/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ducklakeDataPath(tc.s3); got != tc.want {
				t.Errorf("ducklakeDataPath = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMissingCatalogInputs(t *testing.T) {
	full := readyWarehouse("42")
	if got := missingCatalogInputs(full, ""); len(got) != 0 {
		t.Fatalf("a ready warehouse must have no missing inputs, got %v", got)
	}

	cases := []struct {
		name     string
		mutate   func(*configstore.ManagedWarehouse)
		fallback string
		want     string
	}{
		{"endpoint", func(w *configstore.ManagedWarehouse) { w.MetadataStore.Endpoint = "" }, "", "metadata_store_endpoint"},
		{"database", func(w *configstore.ManagedWarehouse) { w.MetadataStore.DatabaseName = "" }, "", "metadata_store_database_name"},
		{"username", func(w *configstore.ManagedWarehouse) { w.MetadataStore.Username = "" }, "", "metadata_store_username"},
		{"bucket", func(w *configstore.ManagedWarehouse) { w.S3.Bucket = "" }, "", "s3_bucket"},
		{"region", func(w *configstore.ManagedWarehouse) { w.S3.Region = "" }, "", "s3_region"},
		{"iam role", func(w *configstore.ManagedWarehouse) { w.WorkerIdentity.IAMRoleARN = "" }, "", "worker_identity_iam_role_arn"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := readyWarehouse("42")
			tc.mutate(w)
			got := missingCatalogInputs(w, tc.fallback)
			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("missingCatalogInputs = %v, want exactly [%s]", got, tc.want)
			}
		})
	}

	// The env-configured region covers a row that predates s3_region
	// being populated; nothing else has a fallback.
	t.Run("region falls back to the provisioner's configured region", func(t *testing.T) {
		w := readyWarehouse("42")
		w.S3.Region = ""
		if got := missingCatalogInputs(w, "eu-central-1"); len(got) != 0 {
			t.Fatalf("expected no missing inputs with a fallback region, got %v", got)
		}
	})
}

// --- reconcile path ---

type testProvisionerHarness struct {
	provisioner *TrinoProvisioner
	kube        *kubefake.Clientset
	catalog     *fakeCatalogClient
	bundles     *opa.BundleStore
	store       *fakeTrinoStore
	warehouses  *fakeWarehouseStore
	// passwords is the fake tenant password source: org id -> password.
	// A missing entry resolves to ("", nil) — the duckling-not-ready wait.
	passwords map[string]string
	// passwordErr, when non-nil for an org, fails that org's resolution.
	passwordErr map[string]error
}

const testCellID = "cell-test"

func newTestTrinoProvisioner(t *testing.T, orgs []configstore.TrinoEnabledOrg, warehouses map[string]*configstore.ManagedWarehouse) *testProvisionerHarness {
	t.Helper()
	h := &testProvisionerHarness{
		kube:        kubefake.NewClientset(),
		catalog:     &fakeCatalogClient{},
		bundles:     &opa.BundleStore{},
		store:       &fakeTrinoStore{orgs: orgs},
		warehouses:  &fakeWarehouseStore{rows: warehouses},
		passwords:   map[string]string{},
		passwordErr: map[string]error{},
	}
	// Every org in the fixture gets a password unless the test removes
	// it — the interesting cases are the exceptions, not the norm.
	for _, o := range orgs {
		h.passwords[o.OrgID] = "pw-" + o.OrgID
	}
	p, err := NewTrinoProvisioner(TrinoProvisionerOpts{
		Store:             h.store,
		BootstrapSentinel: newFakeSentinel(),
		Warehouses:        h.warehouses,
		TenantPasswords: func(_ context.Context, orgID string) (string, error) {
			if err := h.passwordErr[orgID]; err != nil {
				return "", err
			}
			return h.passwords[orgID], nil
		},
		Kubernetes:    h.kube,
		Namespace:     TrinoCustomerNamespace,
		CellID:        testCellID,
		Catalog:       h.catalog,
		BundleStore:   h.bundles,
		BundleBuilder: opa.NewBuilder(),
		AWSRegion:     "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewTrinoProvisioner: %v", err)
	}
	h.provisioner = p
	return h
}

func (h *testProvisionerHarness) tenantSecret(t *testing.T) map[string][]byte {
	t.Helper()
	sec, err := h.kube.CoreV1().Secrets(TrinoCustomerNamespace).
		Get(context.Background(), TrinoTenantSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get tenant secret: %v", err)
	}
	return sec.Data
}

// fakeSentinel is an in-memory TrinoBootstrapSentinelStore. The real
// provisioner generates + writes the K8s Secrets itself (against the
// kubefake clientset), so the only thing to fake here is the one-bit
// "ever bootstrapped" marker.
type fakeSentinel struct {
	mu           sync.Mutex
	bootstrapped map[string]bool
	failRead     error // injectable: simulate a transient sentinel read error
}

func newFakeSentinel() *fakeSentinel {
	return &fakeSentinel{bootstrapped: map[string]bool{}}
}

func (f *fakeSentinel) IsTrinoClusterBootstrapped(_ context.Context, namespace string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failRead != nil {
		return false, f.failRead
	}
	return f.bootstrapped[namespace], nil
}

func (f *fakeSentinel) MarkTrinoClusterBootstrapped(_ context.Context, namespace string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.bootstrapped[namespace] = true
	return nil
}

func TestReconcile_CreatesCatalogProjectsSecretsAndConfigMap(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", Tier: "free", CellID: testCellID, RootPasswordHash: "$2a$10$hash42"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{"42": readyWarehouse("42")})

	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Catalog issued via REST, with the DuckLake property set.
	props, ok := h.catalog.created[TrinoCatalogName("42")]
	if !ok {
		t.Fatalf("expected CREATE CATALOG for %s, got %+v", TrinoCatalogName("42"), h.catalog.created)
	}
	want := map[string]string{
		"connector.name":                             "ducklake",
		"ducklake.metadata.connection-url":           "jdbc:postgresql://shard-001-pooler.cnpg-shards.svc.cluster.local:5432/mdstore_42?sslmode=disable",
		"ducklake.metadata.connection-user":          "mdstore_42",
		"ducklake.metadata.connection-password-file": DefaultTrinoTenantSecretMountPath + "/42",
		"ducklake.data-path":                         "s3://posthog-duckling-42-mw-dev/",
		"fs.s3.enabled":                              "true",
		"s3.region":                                  "us-east-1",
		"s3.auth-type":                               "IAM_ROLE",
		"s3.iam-role":                                "arn:aws:iam::123456789012:role/duckling-42",
		"s3.max-connections":                         "50",
	}
	if len(props) != len(want) {
		t.Errorf("catalog property count = %d, want %d: %+v", len(props), len(want), props)
	}
	for k, v := range want {
		if props[k] != v {
			t.Errorf("catalog property %q = %q, want %q", k, props[k], v)
		}
	}
	// The old Iceberg/Lakekeeper shape must be gone, and so must the
	// property-name spelling that Trino rejects.
	for _, gone := range []string{"iceberg.catalog.type", "iceberg.rest-catalog.uri", "fs.native-s3.enabled"} {
		if _, leak := props[gone]; leak {
			t.Errorf("unexpected legacy property %q in %v", gone, props)
		}
	}

	// Tenant password Secret.
	if got := string(h.tenantSecret(t)["42"]); got != "pw-42" {
		t.Errorf("tenant secret key 42 = %q, want %q", got, "pw-42")
	}

	// Auth Secret.
	sec, err := h.kube.CoreV1().Secrets(TrinoCustomerNamespace).Get(context.Background(), TrinoAuthSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get auth secret: %v", err)
	}
	if !strings.Contains(string(sec.Data[TrinoAuthSecretKeyPasswordDB]), "42:$2a$10$hash42") {
		t.Errorf("password.db missing 42 entry: %q", sec.Data[TrinoAuthSecretKeyPasswordDB])
	}
	if !strings.Contains(string(sec.Data[TrinoAuthSecretKeyGroupDB]), "org_42:42") {
		t.Errorf("group.db missing org_42 entry: %q", sec.Data[TrinoAuthSecretKeyGroupDB])
	}

	// ConfigMap.
	cm, err := h.kube.CoreV1().ConfigMaps(TrinoCustomerNamespace).Get(context.Background(), TrinoResourceGroupsConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get resource-groups configmap: %v", err)
	}
	if !strings.Contains(cm.Data[TrinoResourceGroupsConfigMapKey], "root.tenants.42") {
		t.Errorf("resource-groups.json missing root.tenants.42 selector: %q", cm.Data[TrinoResourceGroupsConfigMapKey])
	}

	// OPA bundle Set into the store with a non-empty ETag.
	cur, ok := h.bundles.Current()
	if !ok {
		t.Fatal("expected BundleStore to hold a bundle after Reconcile")
	}
	if cur.ETag == "" {
		t.Errorf("expected non-empty ETag on stored bundle")
	}
	if cur.Len() == 0 {
		t.Errorf("expected non-empty bundle bytes")
	}
}

// The whole reason the password lives in a file: Trino logs the CREATE
// CATALOG statement verbatim and shows catalog properties in its web UI,
// and ships them to every worker.
func TestReconcile_CatalogPropertiesCarryNoSecret(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$h"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{"42": readyWarehouse("42")})
	h.passwords["42"] = "super-secret-metadata-password"

	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	props := h.catalog.created[TrinoCatalogName("42")]
	if len(props) == 0 {
		t.Fatal("expected the catalog to be created")
	}
	for k, v := range props {
		if strings.Contains(v, "super-secret-metadata-password") {
			t.Fatalf("catalog property %q leaks the metadata-store password: %q", k, v)
		}
	}
	// The connector's password-carrying property must not be used at all
	// — only the file indirection.
	if _, leak := props["ducklake.metadata.connection-password"]; leak {
		t.Fatal("ducklake.metadata.connection-password must never be set; use connection-password-file")
	}
	if got := props["ducklake.metadata.connection-password-file"]; got != DefaultTrinoTenantSecretMountPath+"/42" {
		t.Fatalf("connection-password-file = %q, want the mounted tenant-secret path", got)
	}
	// The password does land in the Secret — the file has to exist.
	if got := string(h.tenantSecret(t)["42"]); got != "super-secret-metadata-password" {
		t.Fatalf("tenant secret key 42 = %q, want the password", got)
	}
}

// The tenant Secret is authoritative, not additive: a disabled org's
// password must stop being mounted into the Trino pods.
func TestReconcile_TenantSecretDropsDisabledOrgs(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$h"},
		{OrgID: "43", CellID: testCellID, RootPasswordHash: "$2a$10$h"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{
		"42": readyWarehouse("42"),
		"43": readyWarehouse("43"),
	})
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (1): %v", err)
	}
	data := h.tenantSecret(t)
	if len(data) != 2 || string(data["42"]) == "" || string(data["43"]) == "" {
		t.Fatalf("expected both orgs projected, got %v", keysOf(data))
	}

	// 43 opts out.
	h.store.mu.Lock()
	h.store.orgs = orgs[:1]
	h.store.mu.Unlock()

	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (2): %v", err)
	}
	data = h.tenantSecret(t)
	if _, still := data["43"]; still {
		t.Errorf("disabled org 43's password must be removed from the tenant Secret, got keys %v", keysOf(data))
	}
	if string(data["42"]) != "pw-42" {
		t.Errorf("still-enabled org 42 must keep its password, got %q", data["42"])
	}
	// And its catalog is dropped.
	if !contains(h.catalog.dropped, TrinoCatalogName("43")) {
		t.Errorf("expected %s to be dropped, got %v", TrinoCatalogName("43"), h.catalog.dropped)
	}
}

func TestReconcile_SkipsCatalogWhenTenantPasswordNotReady(t *testing.T) {
	// Trino-enabled, warehouse ready, but the duckling hasn't published a
	// metadata credential yet — no catalog, and no key on the Secret. The
	// auth files are still projected so the password file stays
	// consistent regardless of catalog readiness.
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$hash"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{"42": readyWarehouse("42")})
	delete(h.passwords, "42")

	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(h.catalog.created) != 0 {
		t.Errorf("expected no catalog creates, got %+v", h.catalog.created)
	}
	if _, present := h.tenantSecret(t)["42"]; present {
		t.Errorf("expected no tenant secret key for an org with no published password")
	}
	st, ok := h.store.lastState("42")
	if !ok || st.State != configstore.ManagedWarehouseStateProvisioning {
		t.Errorf("expected provisioning state, got %+v", st)
	}
	if !strings.Contains(st.StatusMessage, "duckling") {
		t.Errorf("status_message should name the missing credential, got %q", st.StatusMessage)
	}

	sec, err := h.kube.CoreV1().Secrets(TrinoCustomerNamespace).Get(context.Background(), TrinoAuthSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get auth secret: %v", err)
	}
	if !strings.Contains(string(sec.Data[TrinoAuthSecretKeyPasswordDB]), "42:") {
		t.Errorf("expected 42 entry in password.db, got %q", sec.Data[TrinoAuthSecretKeyPasswordDB])
	}
}

func TestReconcile_SkipsCatalogWhenWarehouseNotReady(t *testing.T) {
	cases := []struct {
		name       string
		warehouses map[string]*configstore.ManagedWarehouse
		wantMsg    string
	}{
		{
			name:       "no warehouse row",
			warehouses: map[string]*configstore.ManagedWarehouse{},
			wantMsg:    "no managed warehouse row",
		},
		{
			name: "incomplete connection block",
			warehouses: func() map[string]*configstore.ManagedWarehouse {
				w := readyWarehouse("42")
				w.MetadataStore.Endpoint = ""
				w.S3.Bucket = ""
				return map[string]*configstore.ManagedWarehouse{"42": w}
			}(),
			wantMsg: "metadata_store_endpoint",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			orgs := []configstore.TrinoEnabledOrg{{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$h"}}
			h := newTestTrinoProvisioner(t, orgs, tc.warehouses)

			if err := h.provisioner.Reconcile(context.Background()); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if len(h.catalog.created) != 0 {
				t.Errorf("expected no catalog creates, got %+v", h.catalog.created)
			}
			st, ok := h.store.lastState("42")
			if !ok || st.State != configstore.ManagedWarehouseStateProvisioning {
				t.Fatalf("expected provisioning state, got %+v", st)
			}
			if !strings.Contains(st.StatusMessage, tc.wantMsg) {
				t.Errorf("status_message = %q, want substring %q", st.StatusMessage, tc.wantMsg)
			}
		})
	}
}

func TestReconcile_TenantPasswordErrorFailsOnlyThatOrg(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$h"},
		{OrgID: "43", CellID: testCellID, RootPasswordHash: "$2a$10$h"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{
		"42": readyWarehouse("42"),
		"43": readyWarehouse("43"),
	})
	h.passwordErr["42"] = errors.New("secret read forbidden")

	// A per-org failure surfaces on the returned error but must NOT stop
	// the healthy org from being provisioned.
	if err := h.provisioner.Reconcile(context.Background()); err == nil {
		t.Fatal("expected Reconcile to surface the per-org password failure")
	}
	if _, ok := h.catalog.created[TrinoCatalogName("43")]; !ok {
		t.Errorf("healthy org 43 must still get its catalog, got %+v", h.catalog.created)
	}
	if _, ok := h.catalog.created[TrinoCatalogName("42")]; ok {
		t.Errorf("failing org 42 must not get a catalog")
	}
	if st, _ := h.store.lastState("42"); st.State != configstore.ManagedWarehouseStateFailed {
		t.Errorf("42 state = %q, want failed (%q)", st.State, st.StatusMessage)
	}
	if st, _ := h.store.lastState("43"); st.State != configstore.ManagedWarehouseStateReady {
		t.Errorf("43 state = %q, want ready (%q)", st.State, st.StatusMessage)
	}
	// The healthy org's password still landed on the Secret; the failing
	// org's key is absent rather than stale.
	data := h.tenantSecret(t)
	if string(data["43"]) != "pw-43" {
		t.Errorf("expected 43's password on the Secret, got %q", data["43"])
	}
	if _, present := data["42"]; present {
		t.Errorf("expected no key for the failing org")
	}
}

func TestReconcile_DropsStaleCatalogs(t *testing.T) {
	// org_99 exists in Trino but is not in the enabled list → should get
	// DROP. system, jmx, and a hand-made catalog survive.
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$h"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{"42": readyWarehouse("42")})
	h.catalog.existing = []string{"system", "jmx", "maintenance_ducklake", "org_99"}

	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !contains(h.catalog.dropped, "org_99") {
		t.Errorf("expected org_99 in dropped, got %v", h.catalog.dropped)
	}
	for _, survivor := range []string{"system", "jmx", "maintenance_ducklake"} {
		if contains(h.catalog.dropped, survivor) {
			t.Errorf("%s dropped — the provisioner must only touch managed names: %v", survivor, h.catalog.dropped)
		}
	}
}

// A momentarily unresolvable password must never drop a working catalog
// out from under a tenant's running queries.
func TestReconcile_PendingOrgKeepsItsExistingCatalog(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$h"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{"42": readyWarehouse("42")})
	h.catalog.existing = []string{TrinoCatalogName("42")}
	delete(h.passwords, "42")

	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(h.catalog.dropped) != 0 {
		t.Errorf("expected no drops for a still-enabled org, got %v", h.catalog.dropped)
	}
	// But it is NOT reported ready — an existing catalog whose password
	// file is missing is a broken catalog.
	if st, _ := h.store.lastState("42"); st.State != configstore.ManagedWarehouseStateProvisioning {
		t.Errorf("state = %q, want provisioning", st.State)
	}
}

func TestReconcile_IsIdempotentWhenCatalogExists(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$h"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{"42": readyWarehouse("42")})
	// Pretend the catalog already exists.
	h.catalog.existing = []string{TrinoCatalogName("42")}

	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := h.catalog.created[TrinoCatalogName("42")]; ok {
		t.Errorf("expected no CREATE CATALOG when catalog already exists, got %+v", h.catalog.created)
	}
	if st, _ := h.store.lastState("42"); st.State != configstore.ManagedWarehouseStateReady {
		t.Errorf("state = %q, want ready", st.State)
	}
}

func TestReconcile_SecretUpdateIsIdempotent(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$h"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{"42": readyWarehouse("42")})

	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (1): %v", err)
	}
	// Second tick — same input, same Secrets. Should not error.
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (2): %v", err)
	}
	sec, err := h.kube.CoreV1().Secrets(TrinoCustomerNamespace).Get(context.Background(), TrinoAuthSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if !strings.Contains(string(sec.Data[TrinoAuthSecretKeyPasswordDB]), "42:") {
		t.Errorf("expected 42 entry persisted across reconciles")
	}
	if string(h.tenantSecret(t)["42"]) != "pw-42" {
		t.Errorf("expected tenant password persisted across reconciles")
	}
}

// A projection failure must SKIP the catalog step entirely. The killer
// case is the auth Secret: a coordinator that just lost its password.db
// keys 401s every catalog REST call we make, and we'd report that as a
// misleading "catalog reconcile failed" masking the real problem.
func TestReconcile_ProjectionFailureSkipsCatalogStep(t *testing.T) {
	cases := []struct {
		name    string
		break_  func(*testProvisionerHarness)
		wantMsg string
	}{
		{
			name: "auth secret write fails",
			break_: func(h *testProvisionerHarness) {
				h.kube.PrependReactor("update", "secrets", failSecretNamed(TrinoAuthSecretName))
				h.kube.PrependReactor("create", "secrets", failSecretNamed(TrinoAuthSecretName))
			},
			wantMsg: "reconcile auth secret",
		},
		{
			name: "resource-groups configmap write fails",
			break_: func(h *testProvisionerHarness) {
				h.kube.PrependReactor("create", "configmaps", failAlways())
				h.kube.PrependReactor("update", "configmaps", failAlways())
			},
			wantMsg: "reconcile resource groups",
		},
		{
			// Doubly protected: besides the globalErr gate, a failed
			// tenant-Secret write returns an EMPTY projection, so even a
			// catalog step that did run would find no org with a
			// projected password. Breaking the gate alone does not make
			// this subcase fail — the other two are its tripwire.
			name: "tenant secret write fails",
			break_: func(h *testProvisionerHarness) {
				h.kube.PrependReactor("update", "secrets", failSecretNamed(TrinoTenantSecretName))
				h.kube.PrependReactor("create", "secrets", failSecretNamed(TrinoTenantSecretName))
			},
			wantMsg: "reconcile tenant secrets",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			orgs := []configstore.TrinoEnabledOrg{
				{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$h"},
			}
			h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{"42": readyWarehouse("42")})
			// Bootstrap first so the cluster-secret step (which also
			// writes Secrets) isn't the thing that fails.
			if _, err := h.provisioner.Bootstrap(context.Background()); err != nil {
				t.Fatalf("Bootstrap: %v", err)
			}
			tc.break_(h)

			err := h.provisioner.Reconcile(context.Background())
			if err == nil {
				t.Fatal("expected Reconcile to return the projection error")
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("error = %v, want substring %q", err, tc.wantMsg)
			}
			// THE assertion: no catalog REST work happened at all.
			if len(h.catalog.created) != 0 {
				t.Errorf("catalog step must be skipped when a projection failed; created %+v", h.catalog.created)
			}
			if len(h.catalog.dropped) != 0 {
				t.Errorf("catalog step must be skipped when a projection failed; dropped %v", h.catalog.dropped)
			}
			// And the org is attributed the projection error, not a
			// bogus catalog outcome.
			st, ok := h.store.lastState("42")
			if !ok {
				t.Fatal("expected a per-org state write")
			}
			if st.State != configstore.ManagedWarehouseStateFailed {
				t.Errorf("state = %q, want failed", st.State)
			}
			if !strings.HasPrefix(st.StatusMessage, "projection:") {
				t.Errorf("status_message = %q, want a projection: prefix", st.StatusMessage)
			}
		})
	}
}

func TestReconcile_StateTransitions(t *testing.T) {
	// Three orgs covering the non-Failed outcomes plus catalog-create
	// failure:
	//   42: ready         (warehouse ready, catalog created OK)
	//   43: provisioning  (no warehouse row yet)
	//   44: failed        (catalog create errors)
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$h", Tier: "free"},
		{OrgID: "43", CellID: testCellID, RootPasswordHash: "$2a$10$h", Tier: "free"},
		{OrgID: "44", CellID: testCellID, RootPasswordHash: "$2a$10$h", Tier: "free"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{
		"42": readyWarehouse("42"),
		// 43 deliberately missing — warehouse not provisioned yet.
		"44": readyWarehouse("44"),
	})
	// Make CREATE CATALOG fail (43 already short-circuits via the
	// warehouse-not-ready branch).
	h.catalog.createErr = errors.New("trino: 503 service unavailable")

	// Reconcile returns a non-nil error because some org-level steps
	// failed; we still expect per-org state to be written for everyone.
	_ = h.provisioner.Reconcile(context.Background())

	check := func(orgID string, wantState configstore.ManagedWarehouseProvisioningState, wantMsgSubstr string) {
		t.Helper()
		st, ok := h.store.lastState(orgID)
		if !ok {
			t.Errorf("no state written for %s", orgID)
			return
		}
		if st.State != wantState {
			t.Errorf("%s: state = %q, want %q (msg=%q)", orgID, st.State, wantState, st.StatusMessage)
		}
		if wantMsgSubstr != "" && !strings.Contains(st.StatusMessage, wantMsgSubstr) {
			t.Errorf("%s: status_message = %q, want substring %q", orgID, st.StatusMessage, wantMsgSubstr)
		}
	}
	// The fake returns createErr to every call, so both orgs with ready
	// warehouses fail.
	check("42", configstore.ManagedWarehouseStateFailed, "catalog: create catalog")
	check("43", configstore.ManagedWarehouseStateProvisioning, "no managed warehouse row")
	check("44", configstore.ManagedWarehouseStateFailed, "catalog: create catalog")
	if st, _ := h.store.lastState("42"); st.FailedAt == nil || st.FailedAt.IsZero() {
		t.Errorf("expected FailedAt stamped on the transition into failed")
	}

	// Recovery: clear the catalog error and rerun. 42 and 44 should
	// land in Ready; 43 stays Provisioning until its warehouse exists.
	h.catalog.createErr = nil
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (recovery): %v", err)
	}
	check("42", configstore.ManagedWarehouseStateReady, "")
	check("43", configstore.ManagedWarehouseStateProvisioning, "no managed warehouse row")
	check("44", configstore.ManagedWarehouseStateReady, "")

	// ReadyAt stamped and the stale failed_at cleared on the transition.
	st, _ := h.store.lastState("42")
	if st.ReadyAt == nil {
		t.Errorf("expected ReadyAt on 42 after recovery")
	}
	if st.FailedAt == nil || !st.FailedAt.IsZero() {
		t.Errorf("expected failed_at cleared (zero-time pointer) on the transition into ready, got %v", st.FailedAt)
	}
}

// --- cell awareness ---

func TestReconcile_ClaimsUnassignedOrgsIntoThisCell(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: "", RootPasswordHash: "$2a$10$h"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{"42": readyWarehouse("42")})

	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := h.store.cells["42"]; got != testCellID {
		t.Errorf("expected org 42 claimed into %q, got %q", testCellID, got)
	}
	if _, ok := h.catalog.created[TrinoCatalogName("42")]; !ok {
		t.Errorf("a newly claimed org must be reconciled in the same tick, got %+v", h.catalog.created)
	}

	// Second tick: the row is already stamped, so no re-claim.
	before := len(h.store.claimLog)
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (2): %v", err)
	}
	if len(h.store.claimLog) != before {
		t.Errorf("an already-claimed org must not be re-claimed, log=%v", h.store.claimLog)
	}
}

func TestReconcile_IgnoresOrgsOwnedByAnotherCell(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: testCellID, RootPasswordHash: "$2a$10$hash42"},
		{OrgID: "99", CellID: "cell-elsewhere", RootPasswordHash: "$2a$10$hash99"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{
		"42": readyWarehouse("42"),
		"99": readyWarehouse("99"),
	})
	// The other cell's catalog is NOT on this coordinator; make sure we
	// don't create it here either.
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := h.catalog.created[TrinoCatalogName("99")]; ok {
		t.Errorf("must not create a catalog for another cell's org")
	}
	if _, present := h.tenantSecret(t)["99"]; present {
		t.Errorf("must not project another cell's tenant password")
	}
	if _, ok := h.store.lastState("99"); ok {
		t.Errorf("must not write state for another cell's org (the owning cell owns that column)")
	}
	// The password file / group file only carry this cell's tenants.
	sec, err := h.kube.CoreV1().Secrets(TrinoCustomerNamespace).Get(context.Background(), TrinoAuthSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get auth secret: %v", err)
	}
	if strings.Contains(string(sec.Data[TrinoAuthSecretKeyPasswordDB]), "99:") {
		t.Errorf("password.db leaked another cell's org: %q", sec.Data[TrinoAuthSecretKeyPasswordDB])
	}
	// And our own org is unaffected.
	if _, ok := h.catalog.created[TrinoCatalogName("42")]; !ok {
		t.Errorf("this cell's org must still be provisioned, got %+v", h.catalog.created)
	}
}

func TestReconcile_FailedClaimDefersTheOrg(t *testing.T) {
	// A claim that doesn't land means ownership is unrecorded; projecting
	// the tenant anyway risks two cells serving it.
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", CellID: "", RootPasswordHash: "$2a$10$h"},
	}
	h := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouse{"42": readyWarehouse("42")})
	h.store.cellErr = errors.New("db unavailable")

	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(h.catalog.created) != 0 {
		t.Errorf("an unclaimable org must not be projected, got %+v", h.catalog.created)
	}
	if _, present := h.tenantSecret(t)["42"]; present {
		t.Errorf("an unclaimable org must not get a tenant secret key")
	}
}

func TestNewTrinoProvisioner_DefaultsCellAndMountPath(t *testing.T) {
	p, err := NewTrinoProvisioner(baseTestOpts())
	if err != nil {
		t.Fatalf("NewTrinoProvisioner: %v", err)
	}
	if p.CellID() != configstore.DefaultTrinoCellID {
		t.Errorf("CellID() = %q, want the default %q", p.CellID(), configstore.DefaultTrinoCellID)
	}
	if got := p.tenantPasswordFilePath("42"); got != DefaultTrinoTenantSecretMountPath+"/42" {
		t.Errorf("tenantPasswordFilePath = %q, want the default mount path", got)
	}

	// A trailing slash on the configured mount path must not double up.
	opts := baseTestOpts()
	opts.TenantSecretMountPath = "/mnt/trino-secrets/"
	p, err = NewTrinoProvisioner(opts)
	if err != nil {
		t.Fatalf("NewTrinoProvisioner: %v", err)
	}
	if got := p.tenantPasswordFilePath("42"); got != "/mnt/trino-secrets/42" {
		t.Errorf("tenantPasswordFilePath = %q, want /mnt/trino-secrets/42", got)
	}
}

func TestRenderWithClauseDeterministic(t *testing.T) {
	a := renderWithClause(map[string]string{"k1": "v1", "k2": "v2"})
	b := renderWithClause(map[string]string{"k2": "v2", "k1": "v1"})
	if a != b {
		t.Errorf("renderWithClause not deterministic:\n a=%q\n b=%q", a, b)
	}
}

func TestRenderWithClauseEscapesQuotes(t *testing.T) {
	got := renderWithClause(map[string]string{"k": "v'with'quotes"})
	if !strings.Contains(got, "v''with''quotes") {
		t.Errorf("expected SQL-escaped quotes, got %q", got)
	}
}

func baseTestOpts() TrinoProvisionerOpts {
	return TrinoProvisionerOpts{
		Store:             &fakeTrinoStore{},
		BootstrapSentinel: newFakeSentinel(),
		Warehouses:        &fakeWarehouseStore{},
		TenantPasswords:   func(context.Context, string) (string, error) { return "", nil },
		Kubernetes:        kubefake.NewClientset(),
		Catalog:           &fakeCatalogClient{},
		BundleStore:       &opa.BundleStore{},
		BundleBuilder:     opa.NewBuilder(),
	}
}

func TestNewTrinoProvisioner_RequiresAllDeps(t *testing.T) {
	// Missing each required field → constructor error. Partial wiring
	// would otherwise cause a silent reconcile no-op.
	if _, err := NewTrinoProvisioner(baseTestOpts()); err != nil {
		t.Fatalf("expected baseline to succeed, got %v", err)
	}
	for _, f := range []func(o *TrinoProvisionerOpts){
		func(o *TrinoProvisionerOpts) { o.Store = nil },
		func(o *TrinoProvisionerOpts) { o.BootstrapSentinel = nil },
		func(o *TrinoProvisionerOpts) { o.Warehouses = nil },
		func(o *TrinoProvisionerOpts) { o.TenantPasswords = nil },
		func(o *TrinoProvisionerOpts) { o.Kubernetes = nil },
		func(o *TrinoProvisionerOpts) { o.Catalog = nil },
		func(o *TrinoProvisionerOpts) { o.BundleStore = nil },
		func(o *TrinoProvisionerOpts) { o.BundleBuilder = nil },
	} {
		o := baseTestOpts()
		f(&o)
		if _, err := NewTrinoProvisioner(o); err == nil {
			t.Errorf("expected error with missing dep, got nil for %+v", o)
		}
	}
}

// --- helpers ---

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// failSecretNamed returns a kubefake reactor that fails writes to ONE
// Secret by name, leaving the provisioner's other Secret writes working.
// That isolation is what makes the globalErr-gate assertions meaningful:
// the test breaks exactly one projection step and asserts the catalog
// step is skipped, rather than breaking everything at once.
func failSecretNamed(name string) k8stesting.ReactionFunc {
	return func(action k8stesting.Action) (bool, runtime.Object, error) {
		getter, ok := action.(interface{ GetObject() runtime.Object })
		if !ok {
			return false, nil, nil
		}
		sec, ok := getter.GetObject().(*corev1.Secret)
		if !ok || sec.Name != name {
			return false, nil, nil
		}
		return true, nil, errors.New("simulated apiserver failure writing secret " + name)
	}
}

// failAlways fails every action it is registered for.
func failAlways() k8stesting.ReactionFunc {
	return func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("simulated apiserver failure")
	}
}
