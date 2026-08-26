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
	// N control-plane replicas finishing their first reconcile at once.
	// The sentinel has no lock of its own — ON CONFLICT DO NOTHING is the
	// whole concurrency story — so this asserts nobody errors and the bit
	// ends up set exactly once.
	const replicas = 20
	_, connStr := newIsolatedConfigStoreSchema(t)
	ctx := context.Background()
	const ns = "trino-customer"

	stores := make([]*cpconfigstore.ConfigStore, replicas)
	for i := range stores {
		store, err := cpconfigStoreNew(connStr)
		if err != nil {
			t.Fatalf("new config store %d: %v", i, err)
		}
		sqlDB, err := store.DB().DB()
		if err != nil {
			t.Fatalf("store %d sql db: %v", i, err)
		}
		t.Cleanup(func() { _ = sqlDB.Close() })
		stores[i] = store
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
	got, err := stores[0].IsTrinoClusterBootstrapped(ctx, ns)
	if err != nil {
		t.Fatalf("IsTrinoClusterBootstrapped: %v", err)
	}
	if !got {
		t.Fatal("expected bootstrapped=true after concurrent marks")
	}

	// Exactly one row: a second Mark must never insert a duplicate or
	// move the original BootstrappedAt.
	var rows int64
	if err := stores[0].DB().Table("duckgres_trino_cluster_bootstrap").Where("namespace = ?", ns).Count(&rows).Error; err != nil {
		t.Fatalf("count sentinel rows: %v", err)
	}
	if rows != 1 {
		t.Fatalf("expected exactly 1 sentinel row for %q, got %d", ns, rows)
	}
}
