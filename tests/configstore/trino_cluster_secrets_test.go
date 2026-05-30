//go:build linux || darwin

package configstore_test

import (
	"context"
	"sync"
	"testing"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoClusterBootstrapSentinel_RoundTrip(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	const ns = "trino-customer"

	// Fresh: not bootstrapped.
	got, err := store.IsTrinoClusterBootstrapped(ctx, ns)
	if err != nil {
		t.Fatalf("IsTrinoClusterBootstrapped (fresh): %v", err)
	}
	if got {
		t.Fatal("expected not-bootstrapped on a fresh store")
	}

	// Mark, then it reads back true.
	if err := store.MarkTrinoClusterBootstrapped(ctx, ns); err != nil {
		t.Fatalf("MarkTrinoClusterBootstrapped: %v", err)
	}
	got, err = store.IsTrinoClusterBootstrapped(ctx, ns)
	if err != nil {
		t.Fatalf("IsTrinoClusterBootstrapped (after mark): %v", err)
	}
	if !got {
		t.Fatal("expected bootstrapped=true after Mark")
	}
}

func TestTrinoClusterBootstrapSentinel_MarkIsIdempotent(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	const ns = "trino-customer"

	if err := store.MarkTrinoClusterBootstrapped(ctx, ns); err != nil {
		t.Fatalf("first mark: %v", err)
	}
	// A second mark must not error (ON CONFLICT DO NOTHING) and must not
	// disturb the existing row — concurrent replicas both finishing
	// their first reconcile rely on this.
	if err := store.MarkTrinoClusterBootstrapped(ctx, ns); err != nil {
		t.Fatalf("second mark (should be idempotent): %v", err)
	}
	got, err := store.IsTrinoClusterBootstrapped(ctx, ns)
	if err != nil {
		t.Fatalf("IsTrinoClusterBootstrapped: %v", err)
	}
	if !got {
		t.Fatal("expected bootstrapped=true after two marks")
	}
}

func TestTrinoClusterBootstrapSentinel_PerNamespace(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()

	if err := store.MarkTrinoClusterBootstrapped(ctx, "trino-customer"); err != nil {
		t.Fatalf("mark trino-customer: %v", err)
	}
	// A different namespace is independently not-bootstrapped.
	got, err := store.IsTrinoClusterBootstrapped(ctx, "trino-customer-dev")
	if err != nil {
		t.Fatalf("IsTrinoClusterBootstrapped (other ns): %v", err)
	}
	if got {
		t.Fatal("a different namespace must not read as bootstrapped")
	}
}

func TestTrinoClusterBootstrapSentinel_ConcurrentMarkConverges(t *testing.T) {
	store := newIsolatedConfigStore(t)
	ctx := context.Background()
	const ns = "trino-customer"

	const replicas = 20
	stores := make([]*cpconfigstore.ConfigStore, replicas)
	stores[0] = store
	for i := 1; i < replicas; i++ {
		stores[i] = newConfigStoreOnSameSchema(t, store)
	}

	errs := make([]error, replicas)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			errs[idx] = stores[idx].MarkTrinoClusterBootstrapped(ctx, ns)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("replica %d mark failed: %v", i, err)
		}
	}
	got, err := store.IsTrinoClusterBootstrapped(ctx, ns)
	if err != nil {
		t.Fatalf("IsTrinoClusterBootstrapped: %v", err)
	}
	if !got {
		t.Fatal("expected bootstrapped=true after concurrent marks")
	}
}
