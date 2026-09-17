//go:build linux || darwin

package configstore_test

import (
	"context"
	"github.com/posthog/duckgres/controlplane/configstore"
	"testing"
)

func TestTrinoHoglakeInitializationSurvivesDisablePostgres(t *testing.T) {
	store := newIsolatedConfigStore(t)
	seedTrinoOrg(t, store, "tenant")
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	if initialized, err := store.GetTrinoHoglakeInitialized(context.Background(), "tenant"); err != nil || initialized {
		t.Fatalf("initial marker %v %v", initialized, err)
	}
	for i := 0; i < 2; i++ {
		if err := store.MarkTrinoHoglakeInitialized(context.Background(), "tenant"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.DisableTrino("tenant"); err != nil {
		t.Fatal(err)
	}
	if initialized, err := store.GetTrinoHoglakeInitialized(context.Background(), "tenant"); err != nil || !initialized {
		t.Fatalf("disabled marker %v %v", initialized, err)
	}
	if err := store.EnableTrino("tenant", configstore.TrinoSettings{}); err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListTrinoEnabledOrgs()
	if err != nil || len(rows) != 1 || !rows[0].HoglakeInitialized {
		t.Fatalf("re-enabled listing %+v %v", rows, err)
	}
	if err = store.MarkTrinoHoglakeInitialized(context.Background(), "missing"); err == nil {
		t.Fatal("marked missing tenant")
	}
}
