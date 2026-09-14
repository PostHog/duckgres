//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type managedIntentFixture struct {
	TrinoCellLifecycleStore
	intent         bool
	sequence       int64
	claims, clears int
}

type managedLifecycleFixture struct {
	managedIntentFixture
	store       *fakeTrinoStore
	blocked     bool
	freeze      *configstore.TrinoCellFreeze
	admitted    []configstore.TrinoEnabledOrg
	certificate *configstore.TrinoCellCertificate
	finishes    int
}

func (s *managedLifecycleFixture) BeginTrinoCellReconcile(_ context.Context, cell, owner string) (*configstore.TrinoCellLease, bool, error) {
	return &configstore.TrinoCellLease{CellID: cell, Owner: owner, AdmissionEpoch: 1, IntentSequence: s.sequence}, !s.blocked, nil
}
func (s *managedLifecycleFixture) GetTrinoCellLifecycle(context.Context, string) (*configstore.TrinoCellLifecycleStatus, error) {
	return &configstore.TrinoCellLifecycleStatus{AdmissionEpoch: 1, Freeze: s.freeze}, nil
}
func (s *managedLifecycleFixture) FinishTrinoCellReconcile(context.Context, configstore.TrinoCellLease) error {
	if s.intent {
		return configstore.ErrTrinoCellConflict
	}
	s.finishes++
	return nil
}
func (s *managedLifecycleFixture) UpdateManagedTrinoState(_ context.Context, _ configstore.TrinoCellLease, org string, update configstore.TrinoStateUpdate) (bool, error) {
	if s.freeze != nil && update.State == configstore.ManagedWarehouseStateReady {
		return false, nil
	}
	return true, s.store.UpdateTrinoState(org, update)
}
func (s *managedLifecycleFixture) ListAdmittedTrinoOrgs(context.Context, string) ([]configstore.TrinoEnabledOrg, error) {
	return s.admitted, nil
}
func (s *managedLifecycleFixture) CertifyTrinoCellTarget(_ context.Context, _ configstore.TrinoCellLease, _ string, certificate configstore.TrinoCellCertificate) error {
	s.certificate = &certificate
	return nil
}

type managedInventoryFixture struct {
	*fakeCatalogClient
	states map[string]string
	reads  int
}

func (c *managedInventoryFixture) CatalogStates(context.Context) (map[string]string, error) {
	c.reads++
	return c.states, nil
}

func managedHarness(t *testing.T) (*testProvisionerHarness, *managedLifecycleFixture, *fakeCatalogClient) {
	t.Helper()
	h := trinoReadinessHarness(t)
	h.provisioner.cellID = "registered:cell-test"
	h.provisioner.explicitAssignmentOnly = true
	for i := range h.store.orgs {
		h.store.orgs[i].CellID = h.provisioner.cellID
	}
	s := &managedLifecycleFixture{store: h.store}
	green := &fakeCatalogClient{}
	opts := &TrinoManagedCatalogOpts{Store: s, CatalogClients: []TrinoCatalogClient{h.catalog, green}, Active: func(context.Context) (*TrinoManagedBackend, error) {
		return &TrinoManagedBackend{Name: "group-blue", Catalog: h.catalog}, nil
	}, Target: func(context.Context, *configstore.TrinoCellFreeze) (*TrinoManagedBackend, error) { return nil, nil }, TargetProcess: func(context.Context, string) (string, string, error) { return "node", "abcde", nil }}
	if err := h.provisioner.ConfigureManagedCatalogs(opts); err != nil {
		t.Fatal(err)
	}
	return h, s, green
}

func TestManagedPausedAndFollowerPreserveOPAWithoutDDL(t *testing.T) {
	for _, paused := range []bool{true, false} {
		h, s, green := managedHarness(t)
		h.provisioner.managed.Paused = paused
		s.blocked = !paused
		if err := h.provisioner.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(h.catalog.created) != 0 || len(green.created) != 0 || len(h.store.states) != 0 || len(h.builder.last) == 0 {
			t.Fatal("paused/follower path mutated catalogs or lost local OPA refresh")
		}
	}
}

func TestManagedColdStandbyAndRouteChange(t *testing.T) {
	h, s, green := managedHarness(t)
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(h.catalog.created) != 2 || len(green.created) != 0 || s.intent {
		t.Fatal("active-only initial provisioning failed")
	}
	green.existing = append([]string(nil), h.catalog.existing...)
	h.store.orgs = append(h.store.orgs, configstore.TrinoEnabledOrg{OrgID: "tenant-c", DatabaseName: "tenant-c", CellID: h.provisioner.cellID, RootPasswordHash: "hash-c"})
	h.ducklings["tenant-c"] = readyDuckling("tenant-c")
	h.warehouses.rows["tenant-c"] = readyWarehouse("tenant-c")
	h.provisioner.managed.Active = func(context.Context) (*TrinoManagedBackend, error) {
		return &TrinoManagedBackend{Name: "group-green", Catalog: green}, nil
	}
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(green.created) != 1 || green.created["org_tenant_c"] == nil || len(h.catalog.created) != 2 || s.intent {
		t.Fatal("cutover replayed existing catalogs or provisioned the old source")
	}
}

func TestManagedProjectionLagDoesNotSubmitDDL(t *testing.T) {
	h, s, _ := managedHarness(t)
	lag := true
	h.provisioner.secretReadiness = &fakeTrinoSecretReadiness{check: func(context.Context, string, string, map[string][]byte, []TrinoNode) (map[string]string, error) {
		if lag {
			return map[string]string{"tenant-a": "not mounted"}, nil
		}
		return nil, nil
	}}
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.claims != 0 || s.intent || len(h.catalog.created) != 0 {
		t.Fatal("projection lag claimed or submitted DDL")
	}
	lag = false
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.claims != 2 || s.intent {
		t.Fatal("mounted credentials did not allow next reconcile")
	}
}

func TestManagedFrozenCertificateIncludesPreviousAdmissions(t *testing.T) {
	h, s, _ := managedHarness(t)
	s.freeze = &configstore.TrinoCellFreeze{OperationID: "operation", TargetBackend: "group-green", Stable: true, AdmissionEpoch: 1}
	s.admitted = []configstore.TrinoEnabledOrg{{OrgID: "tenant-a", DatabaseName: "tenant-a", State: configstore.ManagedWarehouseStateFailed}}
	target := &managedInventoryFixture{fakeCatalogClient: &fakeCatalogClient{}, states: map[string]string{}}
	h.provisioner.managed.Target = func(context.Context, *configstore.TrinoCellFreeze) (*TrinoManagedBackend, error) {
		return &TrinoManagedBackend{Name: "group-green", Catalog: target}, nil
	}
	if err := h.provisioner.Reconcile(context.Background()); err == nil || s.certificate != nil {
		t.Fatal("previously admitted missing catalog was omitted")
	}
	target.states["org_tenant_a"] = "OPERATIONAL"
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.certificate == nil || s.certificate.AdmittedCount != 1 || s.claims != 0 || len(target.created) != 0 {
		t.Fatal("frozen bulk certificate replayed DDL or omitted admission")
	}
}

func TestManagedTenThousandExistingCatalogsIssueNoDDL(t *testing.T) {
	h, s, _ := managedHarness(t)
	orgs := make([]configstore.TrinoEnabledOrg, 0, 10000)
	data := make(map[string][]byte, 10000)
	projected := make(map[string]bool, 10000)
	for i := range 10000 {
		name := fmt.Sprintf("tenant-%d", i)
		orgs = append(orgs, configstore.TrinoEnabledOrg{OrgID: name, DatabaseName: name})
		data[name] = []byte("fixture-password")
		projected[name] = true
		h.catalog.existing = append(h.catalog.existing, TrinoCatalogName(name))
	}
	outcomes, err := h.provisioner.managedCatalogs(context.Background(), configstore.TrinoCellLease{}, orgs, tenantSecretProjection{data: data, projected: projected})
	if err != nil || len(outcomes) != 10000 || s.claims != 0 || len(h.catalog.created) != 0 {
		t.Fatalf("existing catalogs caused DDL or failed: %v", err)
	}
}

func TestManagedHeldOwnerStillRefreshesPasswordProjection(t *testing.T) {
	h, s, _ := managedHarness(t)
	s.blocked = true
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.store.orgs[0].RootPasswordHash = "replacement-hash"
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	secret, err := h.kube.CoreV1().Secrets(h.provisioner.namespace).Get(context.Background(), TrinoAuthSecretName, metav1.GetOptions{})
	if err != nil || !strings.Contains(string(secret.Data[TrinoAuthSecretKeyPasswordDB]), "replacement-hash") {
		t.Fatal("held catalog owner blocked password reset projection")
	}
	if s.claims != 0 || len(h.catalog.created) != 0 || len(h.store.states) != 0 {
		t.Fatal("projection-only replica acquired catalog/admission authority")
	}
}

func TestManagedReadySurvivesBackendFailureButNotSharedInputFailure(t *testing.T) {
	h, _, _ := managedHarness(t)
	h.store.orgs[0].State = configstore.ManagedWarehouseStateReady
	h.catalog.nodesErr = errors.New("coordinator unavailable")
	if err := h.provisioner.Reconcile(context.Background()); err == nil {
		t.Fatal("backend failure was hidden")
	}
	if h.store.states["tenant-a"].State != configstore.ManagedWarehouseStateReady {
		t.Fatal("backend failure revoked durable admission")
	}
	h.catalog.nodesErr = nil
	h.passwordErr["tenant-a"] = errors.New("credential projection failed")
	_ = h.provisioner.Reconcile(context.Background())
	if h.store.states["tenant-a"].State != configstore.ManagedWarehouseStateFailed {
		t.Fatal("shared input failure was hidden as backend health")
	}
}

func TestManagedFrozenCertificationRejectsUnusableTarget(t *testing.T) {
	for _, mode := range []string{"failed_catalog", "missing_credential", "process_changed"} {
		t.Run(mode, func(t *testing.T) {
			h, s, _ := managedHarness(t)
			s.freeze = &configstore.TrinoCellFreeze{OperationID: "operation", TargetBackend: "group-green", Stable: true, AdmissionEpoch: 1}
			s.admitted = []configstore.TrinoEnabledOrg{{OrgID: "tenant-a", DatabaseName: "tenant-a", State: configstore.ManagedWarehouseStateReady}}
			target := &managedInventoryFixture{fakeCatalogClient: &fakeCatalogClient{}, states: map[string]string{"org_tenant_a": "OPERATIONAL"}}
			h.provisioner.managed.Target = func(context.Context, *configstore.TrinoCellFreeze) (*TrinoManagedBackend, error) {
				return &TrinoManagedBackend{Name: "group-green", Catalog: target}, nil
			}
			if mode == "failed_catalog" {
				target.states["org_tenant_a"] = "FAILING"
			}
			if mode == "missing_credential" {
				delete(h.ducklings, "tenant-a")
			}
			calls := 0
			if mode == "process_changed" {
				h.provisioner.managed.TargetProcess = func(context.Context, string) (string, string, error) {
					calls++
					return fmt.Sprintf("node-%d", calls), "abcde", nil
				}
			}
			if err := h.provisioner.Reconcile(context.Background()); err == nil || s.certificate != nil || s.claims != 0 {
				t.Fatal("unusable target was certified or mutated")
			}
		})
	}
}

func TestManagedFrozenNewWarehouseRemainsUnadmitted(t *testing.T) {
	h, s, _ := managedHarness(t)
	s.freeze = &configstore.TrinoCellFreeze{OperationID: "operation", TargetBackend: "group-green", Stable: true, AdmissionEpoch: 1}
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(h.store.states) != 0 || s.claims != 0 || len(h.catalog.created) != 0 {
		t.Fatal("frozen pending warehouse gained admission or catalog")
	}
}

type managedCredentialFixture struct {
	*fakeCatalogClient
	username, password string
}

func (c *managedCredentialFixture) SetCredentials(username, password string) {
	c.username, c.password = username, password
}

func TestManagedStoppedClientReceivesCredentials(t *testing.T) {
	h, _, _ := managedHarness(t)
	green := &managedCredentialFixture{fakeCatalogClient: &fakeCatalogClient{}}
	h.provisioner.managed.CatalogClients[1] = green
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if green.username == "" || green.password == "" || len(green.created) != 0 {
		t.Fatal("stopped client missed credentials or received DDL")
	}
}

func TestManagedConfigurationRejectsUnfencedWriters(t *testing.T) {
	h, _, _ := managedHarness(t)
	opts := h.provisioner.managed
	h.provisioner.explicitAssignmentOnly = false
	if err := h.provisioner.ConfigureManagedCatalogs(opts); err == nil {
		t.Fatal("unassigned claiming allowed in managed mode")
	}
	h.provisioner.explicitAssignmentOnly = true
	h.provisioner.additionalCatalogs = []TrinoCatalogClient{h.catalog}
	if err := h.provisioner.ConfigureManagedCatalogs(opts); err == nil {
		t.Fatal("static extra writer allowed in managed mode")
	}
	h.provisioner.additionalCatalogs = nil
	copy := *opts
	copy.Store = nil
	if err := h.provisioner.ConfigureManagedCatalogs(&copy); err == nil {
		t.Fatal("managed mode silently omitted lifecycle store")
	}
}

func (s *managedIntentFixture) SetTrinoCellIntent(_ context.Context, _ configstore.TrinoCellLease, intent configstore.TrinoCatalogIntent) error {
	if s.intent || intent.Sequence != s.sequence+1 {
		return configstore.ErrTrinoCellConflict
	}
	s.intent, s.sequence = true, intent.Sequence
	s.claims++
	return nil
}

func (s *managedIntentFixture) ClearTrinoCellIntent(context.Context, configstore.TrinoCellLease, string) error {
	s.intent = false
	s.clears++
	return nil
}

type managedDDLFixture struct {
	TrinoCatalogClient
	err   error
	calls int
}

func (c *managedDDLFixture) CreateCatalog(context.Context, string, map[string]string) error {
	c.calls++
	return c.err
}
func (c *managedDDLFixture) DropCatalog(context.Context, string) error { c.calls++; return c.err }

func TestManagedDDLIntentPrecedesSubmissionAndHoldsUnknown(t *testing.T) {
	store := &managedIntentFixture{}
	upstream := &managedDDLFixture{err: errors.New("transport disconnected")}
	client := &trinoManagedCatalogClient{TrinoCatalogClient: upstream, store: store, backend: "group-blue"}
	if err := client.CreateCatalog(context.Background(), "org_example", nil); err == nil {
		t.Fatal("unknown outcome reported success")
	}
	if !store.intent || store.claims != 1 || store.clears != 0 || upstream.calls != 1 {
		t.Fatal("uncertain submission did not retain durable intent")
	}
	_ = client.DropCatalog(context.Background(), "org_example")
	if upstream.calls != 1 {
		t.Fatal("unknown intent allowed another remote write")
	}
}

func TestManagedDDLTerminalOutcomesClearIntent(t *testing.T) {
	for _, outcome := range []error{nil, &trinoCatalogTerminalError{}} {
		store := &managedIntentFixture{}
		upstream := &managedDDLFixture{err: outcome}
		client := &trinoManagedCatalogClient{TrinoCatalogClient: upstream, store: store, backend: "group-blue"}
		_ = client.CreateCatalog(context.Background(), "org_example", nil)
		_ = client.DropCatalog(context.Background(), "org_example")
		if store.intent || store.claims != 2 || store.clears != 2 || upstream.calls != 2 {
			t.Fatal("confirmed terminal outcome did not allow next fenced write")
		}
	}
}
