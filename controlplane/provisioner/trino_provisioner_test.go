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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

// --- fakes ---

type fakeTrinoStore struct {
	mu     sync.Mutex
	orgs   []configstore.TrinoEnabledOrg
	states map[string]configstore.TrinoStateUpdate // captured per-org state writes
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

func (s *fakeTrinoStore) lastState(orgID string) (configstore.TrinoStateUpdate, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.states[orgID]
	return v, ok
}

type fakeIcebergStore struct {
	rows map[string]*configstore.ManagedWarehouseIceberg
}

func (s *fakeIcebergStore) GetManagedWarehouseIceberg(orgID string) (*configstore.ManagedWarehouseIceberg, error) {
	row, ok := s.rows[orgID]
	if !ok {
		return nil, nil
	}
	cp := *row
	return &cp, nil
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

	// Password file: <org_name>:<bcrypt hash> (numeric-shaped names used
	// here are just compact DNS-1123 fixtures, not team_ids).
	wantPw := "42:$2a$10$hash42\n43:$2a$10$hash43\n"
	if pw != wantPw {
		t.Errorf("password.db mismatch:\n got=%q\nwant=%q", pw, wantPw)
	}

	// Group file: <group_name>:<comma-separated users> — group FIRST.
	wantGrp := "org_42:42\norg_43:43\n"
	if grp != wantGrp {
		t.Errorf("group.db mismatch:\n got=%q\nwant=%q", grp, wantGrp)
	}
}

func TestBuildTrinoAuthFiles_DeterministicOrder(t *testing.T) {
	// Same input, same output — critical so re-runs of the reconcile loop
	// don't churn the Secret on every tick.
	in1 := []configstore.TrinoEnabledOrg{
		{OrgID: "a", RootPasswordHash: "h1"},
		{OrgID: "b", RootPasswordHash: "h2"},
	}
	in2 := []configstore.TrinoEnabledOrg{
		{OrgID: "a", RootPasswordHash: "h1"},
		{OrgID: "b", RootPasswordHash: "h2"},
	}
	pw1, grp1 := BuildTrinoAuthFiles(in1, "")
	pw2, grp2 := BuildTrinoAuthFiles(in2, "")
	if pw1 != pw2 || grp1 != grp2 {
		t.Fatalf("non-deterministic auth file output:\n pw1=%q grp1=%q\n pw2=%q grp2=%q", pw1, grp1, pw2, grp2)
	}
}

func TestBuildTrinoAuthFiles_SkipsEmptyHashes(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", RootPasswordHash: "$2a$10$ok"},
		{OrgID: "43", RootPasswordHash: ""},
		{OrgID: "", RootPasswordHash: "$2a$10$nope"},
	}
	pw, grp := BuildTrinoAuthFiles(orgs, "")
	if pw != "42:$2a$10$ok\n" {
		t.Errorf("password.db = %q, want only the 42 entry", pw)
	}
	if grp != "org_42:42\n" {
		t.Errorf("group.db = %q, want only the org_42 entry", grp)
	}
}

func TestBuildTrinoAuthFiles_Empty(t *testing.T) {
	pw, grp := BuildTrinoAuthFiles(nil, "")
	if pw != "" || grp != "" {
		t.Errorf("expected empty strings on empty input, got pw=%q grp=%q", pw, grp)
	}
}

func TestBuildTrinoAuthFiles_IncludesAdminWhenHashProvided(t *testing.T) {
	// Admin entry is prepended unconditionally when a hash is supplied,
	// even with zero orgs. Required so opa.is_admin (username AND
	// admin-group conjunction) holds for the provisioner's catalog
	// management calls regardless of customer-org population.
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", RootPasswordHash: "$2a$10$h"},
	}
	pw, grp := BuildTrinoAuthFiles(orgs, "$2a$10$admin")

	wantPw := opa.AdminPrincipal + ":$2a$10$admin\n42:$2a$10$h\n"
	if pw != wantPw {
		t.Errorf("password.db with admin: got=%q want=%q", pw, wantPw)
	}
	wantGrp := opa.AdminGroup + ":" + opa.AdminPrincipal + "\norg_42:42\n"
	if grp != wantGrp {
		t.Errorf("group.db with admin: got=%q want=%q", grp, wantGrp)
	}

	// And with zero orgs — admin still present so catalog management
	// works before any customer has been provisioned.
	pwEmpty, grpEmpty := BuildTrinoAuthFiles(nil, "$2a$10$admin")
	if pwEmpty != opa.AdminPrincipal+":$2a$10$admin\n" {
		t.Errorf("empty-orgs password.db: got=%q", pwEmpty)
	}
	if grpEmpty != opa.AdminGroup+":"+opa.AdminPrincipal+"\n" {
		t.Errorf("empty-orgs group.db: got=%q", grpEmpty)
	}
}

func TestTrinoCatalogName(t *testing.T) {
	cases := map[string]string{
		"42":           "org_42_iceberg",
		"acme":         "org_acme_iceberg",
		"Acme-Corp":    "org_acme_corp_iceberg",
		"42-numbers":   "org_42_numbers_iceberg",
		"with.dot":     "org_with_dot_iceberg",
	}
	for in, want := range cases {
		if got := TrinoCatalogName(in); got != want {
			t.Errorf("TrinoCatalogName(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestTrinoCatalogNameMatchesManagedNamePattern closes the Go side of
// the Go ↔ Rego naming contract. The OPA policy authorizes admin
// catalog management on names matching opa.ManagedCatalogPattern. Every
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
	// ducklingOrgIDPattern in provisioning/api.go), e.g. "42", "acme",
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
		"with-dash",   // sanitize → with_dash
		"with.dot",    // sanitize → with_dot
		"Mixed-Case",  // sanitize → mixed_case
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
	// admin authority can't accidentally cover them. These mirror the
	// non-managed names asserted in policy_test.go's adversarial suite.
	negative := []string{
		"system",
		"jmx",
		"iceberg_org_42",
		"org_42",      // missing _iceberg suffix
		"ORG_42_iceberg", // uppercase prefix
		"org_42_iceberg_extra", // trailing junk
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
	// resource-group manager before reaching OPA.
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

// --- reconcile path ---

func newTestTrinoProvisioner(t *testing.T, orgs []configstore.TrinoEnabledOrg, ic map[string]*configstore.ManagedWarehouseIceberg) (*TrinoProvisioner, *kubefake.Clientset, *fakeCatalogClient, *opa.BundleStore, *fakeTrinoStore) {
	t.Helper()
	kc := kubefake.NewClientset()
	catalog := &fakeCatalogClient{}
	bundleStore := &opa.BundleStore{}
	trinoStore := &fakeTrinoStore{orgs: orgs}
	p, err := NewTrinoProvisioner(TrinoProvisionerOpts{
		Store:             trinoStore,
		BootstrapSentinel: newFakeSentinel(),
		IcebergStore:      &fakeIcebergStore{rows: ic},
		Kubernetes:        kc,
		Namespace:         TrinoCustomerNamespace,
		Catalog:           catalog,
		BundleStore:       bundleStore,
		BundleBuilder:     opa.NewBuilder(),
		IAMAccountID:      "123456789012",
		AWSRegion:         "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewTrinoProvisioner: %v", err)
	}
	return p, kc, catalog, bundleStore, trinoStore
}

// fakeSentinel is an in-memory TrinoBootstrapSentinelStore. The real
// provisioner now generates + writes the K8s Secrets itself (against the
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

func TestReconcile_CreatesCatalogProjectsSecretAndConfigMap(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", Tier: "free", RootPasswordHash: "$2a$10$hash42"},
	}
	ic := map[string]*configstore.ManagedWarehouseIceberg{
		"42": {
			LakekeeperEndpoint:  "http://lakekeeper-42.lakekeeper.svc:8181/catalog",
			LakekeeperWarehouse: "org-42",
			Region:              "us-east-1",
		},
	}
	p, kc, catalog, bundleStore, _ := newTestTrinoProvisioner(t, orgs, ic)

	if err := p.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Catalog issued via REST.
	if _, ok := catalog.created[TrinoCatalogName("42")]; !ok {
		t.Errorf("expected CREATE CATALOG for org_42_iceberg, got %+v", catalog.created)
	}
	// Role ARN goes under s3.iam-role (Trino's current property name,
	// matching the maintenance chart) and includes the per-org
	// duckling-<orgid>.
	props := catalog.created[TrinoCatalogName("42")]
	if !strings.Contains(props["s3.iam-role"], "duckling-42") {
		t.Errorf("expected duckling-42 role ARN under s3.iam-role, got %q", props["s3.iam-role"])
	}
	if props["fs.s3.enabled"] != "true" {
		t.Errorf("expected fs.s3.enabled=true, got %q", props["fs.s3.enabled"])
	}
	// Old property names must NOT be emitted — they're silently ignored
	// or rejected by current Trino.
	if _, leak := props["iceberg.s3.aws-role-arn"]; leak {
		t.Errorf("unexpected legacy property iceberg.s3.aws-role-arn in %v", props)
	}

	// Auth Secret.
	sec, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Get(context.Background(), TrinoAuthSecretName, metav1.GetOptions{})
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
	cm, err := kc.CoreV1().ConfigMaps(TrinoCustomerNamespace).Get(context.Background(), TrinoResourceGroupsConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get resource-groups configmap: %v", err)
	}
	if !strings.Contains(cm.Data[TrinoResourceGroupsConfigMapKey], "root.tenants.42") {
		t.Errorf("resource-groups.json missing root.tenants.42 selector: %q", cm.Data[TrinoResourceGroupsConfigMapKey])
	}

	// OPA bundle Set into the store with a non-empty ETag.
	cur, ok := bundleStore.Current()
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

func TestReconcile_SkipsCatalogWhenIcebergNotReady(t *testing.T) {
	// Trino-enabled but Iceberg row missing or endpoint empty — skip
	// catalog creation, but still project auth files for the row.
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", RootPasswordHash: "$2a$10$hash"},
	}
	p, kc, catalog, _, _ := newTestTrinoProvisioner(t, orgs, map[string]*configstore.ManagedWarehouseIceberg{
		// no row for 42
	})
	if err := p.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(catalog.created) != 0 {
		t.Errorf("expected no catalog creates, got %+v", catalog.created)
	}
	// Auth Secret still projected — that's important so password file
	// is consistent regardless of catalog readiness.
	sec, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Get(context.Background(), TrinoAuthSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get auth secret: %v", err)
	}
	if !strings.Contains(string(sec.Data[TrinoAuthSecretKeyPasswordDB]), "42:") {
		t.Errorf("expected 42 entry in password.db, got %q", sec.Data[TrinoAuthSecretKeyPasswordDB])
	}
}

func TestReconcile_DropsStaleCatalogs(t *testing.T) {
	// org_99_iceberg exists in Trino but is not in the enabled list →
	// should get DROP. system + jmx survive.
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", RootPasswordHash: "$2a$10$h"},
	}
	ic := map[string]*configstore.ManagedWarehouseIceberg{
		"42": {LakekeeperEndpoint: "http://lk-42:8181/catalog", LakekeeperWarehouse: "org-42"},
	}
	p, _, catalog, _, _ := newTestTrinoProvisioner(t, orgs, ic)
	catalog.existing = []string{"system", "jmx", "org_99_iceberg"}

	if err := p.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	wantDropped := "org_99_iceberg"
	found := false
	for _, d := range catalog.dropped {
		if d == wantDropped {
			found = true
		}
		if d == "system" || d == "jmx" {
			t.Errorf("system/jmx dropped — must only touch org_*_iceberg: %v", catalog.dropped)
		}
	}
	if !found {
		t.Errorf("expected %q in dropped, got %v", wantDropped, catalog.dropped)
	}
}

func TestReconcile_IsIdempotentWhenCatalogExists(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", RootPasswordHash: "$2a$10$h"},
	}
	ic := map[string]*configstore.ManagedWarehouseIceberg{
		"42": {LakekeeperEndpoint: "http://lk:8181/catalog", LakekeeperWarehouse: "org-42"},
	}
	p, _, catalog, _, _ := newTestTrinoProvisioner(t, orgs, ic)
	// Pretend the catalog already exists.
	catalog.existing = []string{TrinoCatalogName("42")}

	if err := p.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := catalog.created[TrinoCatalogName("42")]; ok {
		t.Errorf("expected no CREATE CATALOG when catalog already exists, got %+v", catalog.created)
	}
}

func TestReconcile_SecretUpdateIsIdempotent(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", RootPasswordHash: "$2a$10$h"},
	}
	p, kc, _, _, _ := newTestTrinoProvisioner(t, orgs, nil)

	if err := p.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (1): %v", err)
	}
	// Second tick — same input, same Secret. Should not error.
	if err := p.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (2): %v", err)
	}
	sec, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Get(context.Background(), TrinoAuthSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if !strings.Contains(string(sec.Data[TrinoAuthSecretKeyPasswordDB]), "42:") {
		t.Errorf("expected 42 entry persisted across reconciles")
	}
}

func TestReconcile_StateTransitions(t *testing.T) {
	// Three orgs covering the three non-Failed outcomes plus a fourth
	// for catalog-create failure:
	//   42: ready (Iceberg ready, catalog created OK)
	//   43: provisioning (Iceberg not ready yet — no row)
	//   44: failed     (catalog create errors)
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "42", RootPasswordHash: "$2a$10$h", Tier: "free"},
		{OrgID: "43", RootPasswordHash: "$2a$10$h", Tier: "free"},
		{OrgID: "44", RootPasswordHash: "$2a$10$h", Tier: "free"},
	}
	ic := map[string]*configstore.ManagedWarehouseIceberg{
		"42": {LakekeeperEndpoint: "http://lk-42:8181/catalog", LakekeeperWarehouse: "org-42"},
		// 43 deliberately missing — Iceberg not ready.
		"44": {LakekeeperEndpoint: "http://lk-44:8181/catalog", LakekeeperWarehouse: "org-44"},
	}
	p, _, catalog, _, trinoStore := newTestTrinoProvisioner(t, orgs, ic)
	// Make 44's CREATE CATALOG fail (org-43 already short-circuits via
	// the iceberg-not-ready branch).
	catalog.createErr = errors.New("trino: 503 service unavailable")

	// Reconcile returns a non-nil error because some org-level steps
	// failed; we still expect per-org state to be written for everyone.
	_ = p.Reconcile(context.Background())

	check := func(orgID string, wantState configstore.ManagedWarehouseProvisioningState, wantMsgSubstr string) {
		t.Helper()
		st, ok := trinoStore.lastState(orgID)
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
	// 44 hit createErr — fails-closed before 42 can attempt; both end
	// up Failed because createErr is sticky on the fake. Document the
	// fake's behavior in the assertion: 42's catalog create also
	// errored (the fake returns the same error to every call).
	check("42", configstore.ManagedWarehouseStateFailed, "catalog: create catalog")
	check("43", configstore.ManagedWarehouseStateProvisioning, "waiting for iceberg")
	check("44", configstore.ManagedWarehouseStateFailed, "catalog: create catalog")

	// Recovery: clear the catalog error and rerun. 42 and 44 should
	// land in Ready; 43 stays Provisioning until Iceberg is wired up.
	catalog.createErr = nil
	if err := p.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile (recovery): %v", err)
	}
	check("42", configstore.ManagedWarehouseStateReady, "")
	check("43", configstore.ManagedWarehouseStateProvisioning, "waiting for iceberg")
	check("44", configstore.ManagedWarehouseStateReady, "")

	// Verify ReadyAt was set for the orgs that reached Ready.
	if st, _ := trinoStore.lastState("42"); st.ReadyAt == nil {
		t.Errorf("expected ReadyAt on 42 after recovery")
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

func TestNewTrinoProvisioner_RequiresAllDeps(t *testing.T) {
	// Missing each required field → constructor error.
	base := TrinoProvisionerOpts{
		Store:             &fakeTrinoStore{},
		BootstrapSentinel: newFakeSentinel(),
		IcebergStore:      &fakeIcebergStore{},
		Kubernetes:        kubefake.NewClientset(),
		Catalog:           &fakeCatalogClient{},
		BundleStore:       &opa.BundleStore{},
		BundleBuilder:     opa.NewBuilder(),
	}
	if _, err := NewTrinoProvisioner(base); err != nil {
		t.Fatalf("expected baseline to succeed, got %v", err)
	}
	for _, f := range []func(o *TrinoProvisionerOpts){
		func(o *TrinoProvisionerOpts) { o.Store = nil },
		func(o *TrinoProvisionerOpts) { o.BootstrapSentinel = nil },
		func(o *TrinoProvisionerOpts) { o.IcebergStore = nil },
		func(o *TrinoProvisionerOpts) { o.Kubernetes = nil },
		func(o *TrinoProvisionerOpts) { o.Catalog = nil },
		func(o *TrinoProvisionerOpts) { o.BundleStore = nil },
		func(o *TrinoProvisionerOpts) { o.BundleBuilder = nil },
	} {
		o := base
		f(&o)
		if _, err := NewTrinoProvisioner(o); err == nil {
			t.Errorf("expected error with missing dep, got nil for %+v", o)
		}
	}
}
