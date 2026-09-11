//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/controlplane/provisioner/opa"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

type fleetCatalog struct {
	called chan struct{}
	block  bool
}

func (c *fleetCatalog) ListCatalogs(ctx context.Context) ([]string, error) {
	close(c.called)
	if c.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, nil
}
func (c *fleetCatalog) CreateCatalog(context.Context, string, map[string]string) error { return nil }
func (c *fleetCatalog) AlterCatalog(context.Context, string, map[string]string) error  { return nil }
func (c *fleetCatalog) DropCatalog(context.Context, string) error                      { return nil }

func TestTrinoFleetSlowCellDoesNotBlockSibling(t *testing.T) {
	store := &fleetBootstrapStore{initialized: map[string]bool{}}
	kc := kubefake.NewClientset()
	catalogs := []*fleetCatalog{{called: make(chan struct{}), block: true}, {called: make(chan struct{})}}
	var fleet trinoFleet
	for i, namespace := range []string{"legacy", "registered"} {
		p, err := provisioner.NewTrinoProvisioner(provisioner.TrinoProvisionerOpts{
			Store: store, BootstrapSentinel: store, Warehouses: store, Kubernetes: kc,
			Ducklings: func(context.Context, string) (*provisioner.DucklingStatus, error) { return nil, nil },
			Namespace: namespace, CellID: namespace, Catalog: catalogs[i], BundleStore: &opa.BundleStore{}, BundleBuilder: opa.NewBuilder(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Bootstrap(context.Background()); err != nil {
			t.Fatal(err)
		}
		fleet = append(fleet, &trinoWiring{Provisioner: p, Cell: trinoCell{ID: namespace}})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- fleet.Reconcile(ctx) }()
	select {
	case <-catalogs[1].called:
	case <-time.After(10 * time.Second):
		t.Fatal("blocked cell prevented its sibling from reconciling")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("fleet did not cancel")
	}
}

type fleetBootstrapStore struct{ initialized map[string]bool }

func (s *fleetBootstrapStore) ListTrinoEnabledOrgs() ([]configstore.TrinoEnabledOrg, error) {
	return nil, nil
}
func (s *fleetBootstrapStore) UpdateTrinoState(string, configstore.TrinoStateUpdate) error {
	return nil
}
func (s *fleetBootstrapStore) ClaimTrinoCell(string, string) (bool, error) { return false, nil }
func (s *fleetBootstrapStore) GetManagedWarehouseForTrino(string) (*configstore.ManagedWarehouse, error) {
	return nil, nil
}
func (s *fleetBootstrapStore) IsTrinoClusterBootstrapped(_ context.Context, namespace string) (bool, error) {
	return s.initialized[namespace], nil
}
func (s *fleetBootstrapStore) MarkTrinoClusterBootstrapped(_ context.Context, namespace string) error {
	s.initialized[namespace] = true
	return nil
}

func TestTrinoFleetBootstrapSeparatesBundleTokensAndLegacyPath(t *testing.T) {
	t.Setenv(envTrinoFilesystemCacheEnabled, "false")
	store := &fleetBootstrapStore{initialized: map[string]bool{}}
	kc := kubefake.NewClientset()
	registered, err := parseTrinoCellRegistry([]byte(testTrinoRegistryJSON))
	if err != nil {
		t.Fatal(err)
	}
	entry := registered[0]
	for _, backend := range entry.Backends {
		_, err := kc.CoreV1().Secrets(entry.Namespace).Create(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: backend.InternalSecretName}, Data: map[string][]byte{"shared-secret": []byte("chart-managed-" + backend.ID)}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}
	cells := []trinoCell{
		{ID: "cell-001", Namespace: "legacy", CoordinatorURL: "https://legacy.example.test"},
		{ID: "registered:cell-test", PublicID: "cell-test", Namespace: entry.Namespace, ClientURL: entry.ClientURL, CoordinatorURL: entry.Backends[0].CoordinatorURL, Backends: entry.Backends},
	}
	engine := gin.New()
	var wires []*trinoWiring
	var tokens []string
	for _, cell := range cells {
		wire, err := buildTrinoCellWiring(store, kc, func(context.Context, string) (*provisioner.DucklingStatus, error) { return nil, nil }, cell)
		if err != nil {
			t.Fatal(err)
		}
		wire.BundleStore.Set(opa.NewBundle([]byte(cell.ID)))
		engine.Any(wire.bundlePath(), gin.WrapH(wire.BundleHandler))
		secret, err := kc.CoreV1().Secrets(cell.Namespace).Get(context.Background(), provisioner.TrinoOPABundleTokenSecretName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, string(secret.Data["token"]))
		wires = append(wires, wire)
		if wire.Provisioner.CellID() != cell.ID {
			t.Fatal("bootstrap changed ownership")
		}
		if len(wire.Observers) != 1 {
			t.Fatal("stopped backend acquired a live observer")
		}
	}
	if wires[0].bundlePath() != "/bundles/trino" || wires[1].bundlePath() != "/bundles/trino/cell-test" {
		t.Fatal("bundle paths changed")
	}
	if tokens[0] == tokens[1] {
		t.Fatal("cells share a bundle token")
	}
	for target, wire := range wires {
		for owner, token := range tokens {
			request := httptest.NewRequest(http.MethodGet, wire.bundlePath(), nil)
			request.Header.Set("Authorization", "Bearer "+token)
			response := httptest.NewRecorder()
			engine.ServeHTTP(response, request)
			want := http.StatusUnauthorized
			if owner == target {
				want = http.StatusOK
			}
			if response.Code != want {
				t.Fatalf("bundle %d token %d: status %d, want %d", target, owner, response.Code, want)
			}
		}
	}
	if len(store.initialized) != 2 {
		t.Fatal("bootstrap sentinels share state")
	}
}
