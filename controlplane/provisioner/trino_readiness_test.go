//go:build kubernetes

package provisioner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakeTrinoSecretReadiness struct {
	check func(context.Context, string, string, map[string][]byte, []TrinoNode) (map[string]string, error)
	calls int
}

func (f *fakeTrinoSecretReadiness) Check(ctx context.Context, namespace, path string, expected map[string][]byte, nodes []TrinoNode) (map[string]string, error) {
	f.calls++
	if f.check != nil {
		return f.check(ctx, namespace, path, expected, nodes)
	}
	return nil, nil
}

func readyTrinoNodes() []TrinoNode {
	return []TrinoNode{
		{ID: "coordinator", URI: "http://192.0.2.10:8080", Coordinator: true, State: "active"},
		{ID: "worker", URI: "http://192.0.2.11:8080", State: "active"},
	}
}

func trinoReadinessHarness(t *testing.T) *testProvisionerHarness {
	t.Helper()
	return newTestTrinoProvisioner(t, []configstore.TrinoEnabledOrg{
		{OrgID: "tenant-a", DatabaseName: "tenant-a", CellID: testCellID, RootPasswordHash: "hash-a"},
		{OrgID: "tenant-b", DatabaseName: "tenant-b", CellID: testCellID, RootPasswordHash: "hash-b"},
	}, map[string]*configstore.ManagedWarehouse{"tenant-a": readyWarehouse("tenant-a"), "tenant-b": readyWarehouse("tenant-b")})
}

func assertTrinoReadinessState(t *testing.T, h *testProvisionerHarness, org string, want configstore.ManagedWarehouseProvisioningState) configstore.TrinoStateUpdate {
	t.Helper()
	state, ok := h.store.lastState(org)
	if !ok || state.State != want {
		t.Fatalf("%s: got %+v, want %s", org, state, want)
	}
	return state
}

func TestTrinoReadinessWaitsForWorkerProjectionAfterCatalogCreation(t *testing.T) {
	h := trinoReadinessHarness(t)
	lagging := true
	checker := &fakeTrinoSecretReadiness{check: func(_ context.Context, namespace, path string, expected map[string][]byte, nodes []TrinoNode) (map[string]string, error) {
		if namespace != TrinoCustomerNamespace || path != DefaultTrinoTenantSecretMountPath || len(nodes) != 2 {
			t.Fatal("readiness did not check the serving nodes in this cell")
		}
		if string(expected["tenant-b"]) != h.ducklings["tenant-b"].MetadataStore.Password {
			t.Fatal("readiness did not receive the password projected in this reconcile")
		}
		if lagging {
			return map[string]string{"tenant-b": "waiting for tenant password projection on worker"}, nil
		}
		return nil, nil
	}}
	h.provisioner.secretReadiness = checker
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(h.catalog.created) != 2 {
		t.Fatal("coordinator catalog creation did not succeed in the reproduction")
	}
	assertTrinoReadinessState(t, h, "tenant-a", configstore.ManagedWarehouseStateReady)
	state := assertTrinoReadinessState(t, h, "tenant-b", configstore.ManagedWarehouseStateProvisioning)
	if state.ReadyAt != nil || state.FailedAt != nil || !strings.Contains(state.StatusMessage, "password projection") {
		t.Fatalf("ordinary projection lag must remain pending: %+v", state)
	}
	lagging = false
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTrinoReadinessState(t, h, "tenant-b", configstore.ManagedWarehouseStateReady)
	if checker.calls != 2 {
		t.Fatal("an existing coordinator catalog bypassed worker readiness")
	}
}

func TestTrinoReadinessIncludesEveryRunningBackend(t *testing.T) {
	h := trinoReadinessHarness(t)
	greenNodes := readyTrinoNodes()
	greenNodes[1].ID = "green-worker"
	greenNodes[1].URI = "http://192.0.2.21:8080"
	h.provisioner.additionalCatalogs = []TrinoCatalogClient{&fakeCatalogClient{nodes: greenNodes}}
	checker := &fakeTrinoSecretReadiness{check: func(_ context.Context, _, _ string, _ map[string][]byte, nodes []TrinoNode) (map[string]string, error) {
		for _, node := range nodes {
			if node.ID == "green-worker" {
				return map[string]string{"tenant-b": "waiting for green worker projection"}, nil
			}
		}
		return nil, nil
	}}
	h.provisioner.secretReadiness = checker
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTrinoReadinessState(t, h, "tenant-a", configstore.ManagedWarehouseStateReady)
	assertTrinoReadinessState(t, h, "tenant-b", configstore.ManagedWarehouseStateProvisioning)
	if checker.calls != 2 {
		t.Fatal("not every running backend was checked")
	}
}

func TestTrinoReadinessRequiresActiveCoordinatorAndWorker(t *testing.T) {
	for _, nodes := range [][]TrinoNode{nil, readyTrinoNodes()[:1], readyTrinoNodes()[1:]} {
		h := trinoReadinessHarness(t)
		h.catalog.nodes = append([]TrinoNode{}, nodes...)
		checker := &fakeTrinoSecretReadiness{}
		h.provisioner.secretReadiness = checker
		if err := h.provisioner.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertTrinoReadinessState(t, h, "tenant-a", configstore.ManagedWarehouseStateProvisioning)
		if checker.calls != 0 {
			t.Fatal("incomplete topology must not produce a projection acknowledgment")
		}
	}
}

func TestTrinoReadinessRechecksMembershipAndExistingReadyTenants(t *testing.T) {
	h := trinoReadinessHarness(t)
	checker := &fakeTrinoSecretReadiness{}
	h.provisioner.secretReadiness = checker
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTrinoReadinessState(t, h, "tenant-a", configstore.ManagedWarehouseStateReady)
	checker.check = func(_ context.Context, _, _ string, _ map[string][]byte, _ []TrinoNode) (map[string]string, error) {
		h.catalog.nodes = append(readyTrinoNodes(), TrinoNode{ID: "new-worker", URI: "http://192.0.2.12:8080", State: "active"})
		return nil, nil
	}
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTrinoReadinessState(t, h, "tenant-a", configstore.ManagedWarehouseStateProvisioning)
	checker.check = nil
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTrinoReadinessState(t, h, "tenant-a", configstore.ManagedWarehouseStateReady)
}

func TestTrinoReadinessDistinguishesReplacementFromInventoryReordering(t *testing.T) {
	for _, test := range []struct {
		name    string
		after   []TrinoNode
		want    configstore.ManagedWarehouseProvisioningState
		message string
	}{
		{
			name: "same length replacement",
			after: []TrinoNode{
				readyTrinoNodes()[0],
				{ID: "replacement-worker", URI: "http://192.0.2.12:8080", State: "active"},
			},
			want:    configstore.ManagedWarehouseStateProvisioning,
			message: "membership changed",
		},
		{
			name:  "same members in different order",
			after: []TrinoNode{readyTrinoNodes()[1], readyTrinoNodes()[0]},
			want:  configstore.ManagedWarehouseStateReady,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := trinoReadinessHarness(t)
			h.catalog.nodes = readyTrinoNodes()
			h.provisioner.secretReadiness = &fakeTrinoSecretReadiness{check: func(_ context.Context, _, _ string, _ map[string][]byte, _ []TrinoNode) (map[string]string, error) {
				h.catalog.nodes = test.after
				return nil, nil
			}}
			if err := h.provisioner.Reconcile(context.Background()); err != nil {
				t.Fatal(err)
			}
			for _, org := range []string{"tenant-a", "tenant-b"} {
				state := assertTrinoReadinessState(t, h, org, test.want)
				if !strings.Contains(state.StatusMessage, test.message) {
					t.Fatalf("membership transition lost its reason: %+v", state)
				}
			}
		})
	}
}

func TestTrinoReadinessAcknowledgmentPreservesCatalogFailureAndPending(t *testing.T) {
	failure := errors.New("catalog configuration rejected")
	for _, test := range []struct {
		name       string
		catalogErr error
		want       configstore.ManagedWarehouseProvisioningState
		message    string
	}{
		{"failed catalog", failure, configstore.ManagedWarehouseStateFailed, failure.Error()},
		{"pending coordinator projection", trinoSecretMountLagError("tenant-b"), configstore.ManagedWarehouseStateProvisioning, "waiting for the tenant password file"},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := trinoReadinessHarness(t)
			// One existing catalog can pass worker observation while the other
			// tenant's catalog creation is still pending or has failed.
			h.catalog.existing = []string{TrinoCatalogName("tenant-a")}
			h.catalog.createErr = test.catalogErr
			checker := &fakeTrinoSecretReadiness{check: func(_ context.Context, _, _ string, expected map[string][]byte, _ []TrinoNode) (map[string]string, error) {
				if len(expected) != 1 || len(expected["tenant-a"]) == 0 {
					t.Fatalf("unfinished catalog must not be included in a successful readiness acknowledgment: %v", len(expected))
				}
				return nil, nil
			}}
			h.provisioner.secretReadiness = checker
			err := h.provisioner.Reconcile(context.Background())
			if test.want == configstore.ManagedWarehouseStateFailed {
				if !errors.Is(err, failure) {
					t.Fatalf("catalog failure was lost: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if checker.calls != 1 {
				t.Fatal("test did not observe successful worker readiness for the existing catalog")
			}
			state := assertTrinoReadinessState(t, h, "tenant-b", test.want)
			if !strings.Contains(state.StatusMessage, test.message) || state.ReadyAt != nil {
				t.Fatalf("readiness acknowledgment overwrote the original catalog outcome: %+v", state)
			}
		})
	}
}

func TestTrinoReadinessErrorsCannotMarkTenantReady(t *testing.T) {
	for _, stage := range []string{"node inventory", "projection check"} {
		t.Run(stage, func(t *testing.T) {
			h := trinoReadinessHarness(t)
			failure := errors.New("readiness unavailable")
			checker := &fakeTrinoSecretReadiness{}
			h.provisioner.secretReadiness = checker
			if stage == "node inventory" {
				h.catalog.nodesErr = failure
			} else {
				checker.check = func(context.Context, string, string, map[string][]byte, []TrinoNode) (map[string]string, error) {
					return nil, failure
				}
			}
			if err := h.provisioner.Reconcile(context.Background()); !errors.Is(err, failure) {
				t.Fatalf("expected readiness error, got %v", err)
			}
			assertTrinoReadinessState(t, h, "tenant-a", configstore.ManagedWarehouseStateFailed)
		})
	}
}

func TestTrinoReadinessIsBoundedByBackendDeadline(t *testing.T) {
	h := trinoReadinessHarness(t)
	if _, err := h.provisioner.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.provisioner.catalogTimeout = 10 * time.Millisecond
	h.provisioner.secretReadiness = &fakeTrinoSecretReadiness{check: func(ctx context.Context, _, _ string, _ map[string][]byte, _ []TrinoNode) (map[string]string, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	if err := h.provisioner.Reconcile(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected bounded readiness failure, got %v", err)
	}
	assertTrinoReadinessState(t, h, "tenant-a", configstore.ManagedWarehouseStateFailed)
}
