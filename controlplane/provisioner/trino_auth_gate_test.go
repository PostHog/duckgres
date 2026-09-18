//go:build kubernetes

package provisioner

import (
	"context"
	"github.com/posthog/duckgres/controlplane/configstore"
	"testing"
)

type fakeTrinoAuthenticationReadiness struct {
	ready    bool
	expected map[string][]byte
}

func (f *fakeTrinoAuthenticationReadiness) SetExpected(expected map[string][]byte) {
	f.expected = expected
}
func (f *fakeTrinoAuthenticationReadiness) Check(context.Context, string, string, []TrinoNode) (bool, error) {
	return f.ready, nil
}

func TestTrinoReadinessWaitsForCoordinatorAuthentication(t *testing.T) {
	h := trinoReadinessHarness(t)
	auth := &fakeTrinoAuthenticationReadiness{}
	h.provisioner.authReadiness = auth
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(h.catalog.created) != 2 {
		t.Fatal("catalog creation must have succeeded")
	}
	assertTrinoReadinessState(t, h, "tenant-a", configstore.ManagedWarehouseStateProvisioning)
	if len(auth.expected[TrinoAuthSecretKeyPasswordDB]) == 0 || len(auth.expected[TrinoAuthSecretKeyGroupDB]) == 0 {
		t.Fatal("authentication gate did not receive projected files")
	}
	auth.ready = true
	if err := h.provisioner.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertTrinoReadinessState(t, h, "tenant-a", configstore.ManagedWarehouseStateReady)
}
