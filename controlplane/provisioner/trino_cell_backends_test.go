//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTrinoCellsKeepTenantProjectionsIsolatedAndHydrateStartedGreen(t *testing.T) {
	orgs := []configstore.TrinoEnabledOrg{
		{OrgID: "tenant-a", DatabaseName: "tenant-a", CellID: testCellID, RootPasswordHash: "hash-a"},
		{OrgID: "tenant-b", DatabaseName: "tenant-b", CellID: "registered:cell-test", RootPasswordHash: "hash-b"},
	}
	warehouses := map[string]*configstore.ManagedWarehouse{"tenant-a": readyWarehouse("tenant-a"), "tenant-b": readyWarehouse("tenant-b")}
	legacy := newTestTrinoProvisioner(t, orgs, warehouses)
	cell := newTestTrinoProvisioner(t, orgs, warehouses)
	cell.provisioner.namespace = "trino-second"
	cell.provisioner.cellID = "registered:cell-test"
	cell.provisioner.explicitAssignmentOnly = true
	cell.provisioner.store = legacy.store
	cell.provisioner.kubernetes = legacy.kube
	for i, h := range []*testProvisionerHarness{legacy, cell} {
		if err := h.provisioner.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		own, other := orgs[i], orgs[1-i]
		auth, err := legacy.kube.CoreV1().Secrets(h.provisioner.namespace).Get(context.Background(), TrinoAuthSecretName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		passwords := string(auth.Data[TrinoAuthSecretKeyPasswordDB])
		if !strings.Contains(passwords, own.DatabaseName+":") || strings.Contains(passwords, other.DatabaseName+":") {
			t.Fatal("cell authentication contains the wrong tenant")
		}
		tenantSecret, err := legacy.kube.CoreV1().Secrets(h.provisioner.namespace).Get(context.Background(), TrinoTenantSecretName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(tenantSecret.Data) != 1 || len(tenantSecret.Data[own.OrgID]) == 0 {
			t.Fatal("metadata credentials crossed cells")
		}
		if h.builder.last[TrinoGroupName(own.DatabaseName)] == nil || h.builder.last[TrinoGroupName(other.DatabaseName)] != nil {
			t.Fatal("OPA authorization crossed cells")
		}
		if len(h.catalog.created) != 1 || h.catalog.created[TrinoCatalogName(own.DatabaseName)] == nil {
			t.Fatal("catalog ownership crossed cells")
		}
	}
	green := &fakeCatalogClient{createErr: trinoSecretMountLagError("tenant-b")}
	cell.provisioner.additionalCatalogs = []TrinoCatalogClient{green}
	if err := cell.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state, _ := legacy.store.lastState("tenant-b"); state.State != configstore.ManagedWarehouseStateProvisioning {
		t.Fatal("blue success concealed starting green's pending catalog")
	}
	green.createErr = nil
	if err := cell.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if state, _ := legacy.store.lastState("tenant-b"); state.State != configstore.ManagedWarehouseStateReady {
		t.Fatal("green did not hydrate to ready")
	}
	if state, _ := legacy.store.lastState("tenant-a"); state.State != configstore.ManagedWarehouseStateReady {
		t.Fatal("starting green changed legacy readiness")
	}
}

type blockedCatalog struct{ fakeCatalogClient }

func (c *blockedCatalog) ListCatalogs(ctx context.Context) ([]string, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestTrinoBackendTimeoutDoesNotStarveSibling(t *testing.T) {
	org := configstore.TrinoEnabledOrg{OrgID: "tenant", DatabaseName: "tenant", CellID: testCellID, RootPasswordHash: "hash"}
	h := newTestTrinoProvisioner(t, []configstore.TrinoEnabledOrg{org}, map[string]*configstore.ManagedWarehouse{"tenant": readyWarehouse("tenant")})
	h.provisioner.catalog = &blockedCatalog{}
	h.provisioner.catalogTimeout = time.Millisecond
	h.provisioner.additionalCatalogs = []TrinoCatalogClient{h.catalog}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.provisioner.Reconcile(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wanted backend timeout, got %v", err)
	}
	if len(h.catalog.created) != 1 {
		t.Fatal("timed-out backend starved healthy sibling")
	}
	if state, _ := h.store.lastState("tenant"); state.State != configstore.ManagedWarehouseStateFailed {
		t.Fatal("healthy sibling hid failed backend")
	}
}

func TestTrinoRegisteredCellNeverClaimsUnassignedTenants(t *testing.T) {
	org := configstore.TrinoEnabledOrg{OrgID: "tenant", DatabaseName: "tenant", RootPasswordHash: "hash"}
	h := newTestTrinoProvisioner(t, []configstore.TrinoEnabledOrg{org}, map[string]*configstore.ManagedWarehouse{"tenant": readyWarehouse("tenant")})
	h.provisioner.explicitAssignmentOnly = true
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(h.store.claimLog) != 0 || len(h.catalog.created) != 0 {
		t.Fatal("registered cell stole default placement")
	}
}

type lostClaimStore struct{ *fakeTrinoStore }

func (s lostClaimStore) AssignTrinoCell(string, string) error        { return nil }
func (s lostClaimStore) ClaimTrinoCell(string, string) (bool, error) { return false, nil }

func TestTrinoLostClaimCannotProjectTenant(t *testing.T) {
	org := configstore.TrinoEnabledOrg{OrgID: "tenant", DatabaseName: "tenant", RootPasswordHash: "hash"}
	h := newTestTrinoProvisioner(t, []configstore.TrinoEnabledOrg{org}, map[string]*configstore.ManagedWarehouse{"tenant": readyWarehouse("tenant")})
	h.provisioner.store = lostClaimStore{h.store}
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(h.catalog.created) != 0 {
		t.Fatal("a lost assignment race projected a tenant into the losing cell")
	}
	if _, ok := h.store.lastState("tenant"); ok {
		t.Fatal("a lost assignment race changed tenant status")
	}
}

func TestTrinoCellBackendReadinessAndIndependentCatalogSets(t *testing.T) {
	org := configstore.TrinoEnabledOrg{OrgID: "tenant", DatabaseName: "tenant", CellID: testCellID, RootPasswordHash: "hash"}
	h := newTestTrinoProvisioner(t, []configstore.TrinoEnabledOrg{org}, map[string]*configstore.ManagedWarehouse{"tenant": readyWarehouse("tenant")})
	second := &fakeCatalogClient{}
	h.provisioner.additionalCatalogs = []TrinoCatalogClient{second}
	h.catalog.existing = []string{"org_tenant", "org_old_blue"}
	second.existing = []string{"org_old_green"}
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(h.catalog.created) != 0 || len(second.created) != 1 {
		t.Fatal("a catalog present on blue must still be created on running green")
	}
	if len(h.catalog.dropped) != 1 || len(second.dropped) != 1 {
		t.Fatal("each running backend must clean its own stale catalog set")
	}
	second.listErr = errors.New("backend unavailable")
	if err := h.provisioner.Reconcile(context.Background()); err == nil {
		t.Fatal("a failed running backend must fail reconciliation")
	}
	state, ok := h.store.lastState("tenant")
	if !ok || state.State != configstore.ManagedWarehouseStateFailed {
		t.Fatalf("blue success hid green failure: %+v", state)
	}
}

func TestTrinoCellInternalSecretsAreReadOnlyReferences(t *testing.T) {
	p, kc, _ := newClusterSecretsTestProvisioner(t)
	p.existingInternalSecrets = []string{"blue-internal", "green-internal"}
	for _, name := range p.existingInternalSecrets {
		_, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Create(context.Background(), &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: TrinoCustomerNamespace},
			Data:       map[string][]byte{TrinoInternalCommunicationSecretKey: []byte("existing-" + name)},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	kc.ClearActions()
	if _, err := p.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, action := range kc.Actions() {
		if action.GetVerb() == "update" || action.GetVerb() == "patch" || action.GetVerb() == "delete" {
			if named, ok := action.(interface{ GetName() string }); ok && (named.GetName() == "blue-internal" || named.GetName() == "green-internal") {
				t.Fatal("bootstrap modified a chart-owned internal secret")
			}
		}
	}
	for _, name := range p.existingInternalSecrets {
		secret := getSecret(t, kc, name)
		if secret.Immutable != nil || string(secret.Data[TrinoInternalCommunicationSecretKey]) != "existing-"+name {
			t.Fatal("bootstrap changed the existing secret")
		}
	}
	if _, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Get(context.Background(), TrinoInternalCommunicationSecretName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("new cell unexpectedly created the legacy internal secret")
	}
	if err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Delete(context.Background(), "green-internal", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Bootstrap(context.Background()); err == nil {
		t.Fatal("missing stopped-backend internal secret must fail without regeneration")
	}
	if _, err := kc.CoreV1().Secrets(TrinoCustomerNamespace).Get(context.Background(), "green-internal", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("bootstrap regenerated a missing chart-owned secret")
	}
}
