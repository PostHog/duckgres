//go:build kubernetes

package controlplane

import (
	"context"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOrgRouterReconcileWarmCapacityUsesExplicitSharedWarmTarget(t *testing.T) {
	sharedPool, _ := newTestK8sPool(t, 10)
	sharedPool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		sharedPool.mu.Lock()
		defer sharedPool.mu.Unlock()
		sharedPool.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		return nil
	}
	tr := &OrgRouter{
		sharedPool: sharedPool,
		globalCfg: ControlPlaneConfig{
			K8s: K8sConfig{
				SharedWarmTarget: 4,
			},
		},
	}

	snap := &configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{
			"analytics": {Name: "analytics"},
			"billing":   {Name: "billing"},
		},
	}

	tr.reconcileWarmCapacity(snap)

	if got := sharedPool.minWorkers; got != 4 {
		t.Fatalf("expected shared warm target 4, got %d", got)
	}
}

func TestOrgRouterHandleConfigChangeRefreshesRuntimeOnlyUpdates(t *testing.T) {
	sharedPool, _ := newTestK8sPool(t, 10)
	pool := NewOrgReservedPool(sharedPool, "analytics", 2, nil)

	oldTC := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "old-metadata.internal",
			},
		},
	}
	newTC := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "new-metadata.internal",
			},
		},
	}

	tr := &OrgRouter{
		orgs: map[string]*OrgStack{
			"analytics": {
				Config: oldTC,
				Pool:   pool,
			},
		},
		baseCfg:   K8sWorkerPoolConfig{MaxWorkers: 2},
		globalCfg: ControlPlaneConfig{},
	}

	tr.HandleConfigChange(
		&configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": oldTC}},
		&configstore.Snapshot{Orgs: map[string]*configstore.OrgConfig{"analytics": newTC}},
	)

	if got := tr.orgs["analytics"].Config.Warehouse.MetadataStore.Endpoint; got != "new-metadata.internal" {
		t.Fatalf("expected runtime-only update to refresh stack config, got %q", got)
	}
}

func TestOrgRouterCreateOrgStackActivatesUsingLatestSnapshotThroughSharedWorkerActivator(t *testing.T) {
	sharedPool, cs := newTestK8sPool(t, 10)
	sharedPool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	sharedPool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		sharedPool.mu.Lock()
		sharedPool.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		sharedPool.mu.Unlock()
		return nil
	}

	for name, value := range map[string]string{
		"analytics-metadata-old": "old-password",
		"analytics-metadata-new": "new-password",
	} {
		_, err := cs.CoreV1().Secrets("tenant-a").Create(context.Background(), newStringSecret("tenant-a", name, "dsn", value), metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("create secret %s: %v", name, err)
		}
	}

	oldOrg := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint:     "old-metadata.internal",
				Port:         5432,
				Username:     "ducklake_user",
				DatabaseName: "ducklake_metadata",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Namespace: "tenant-a",
				Name:      "analytics-metadata-old",
				Key:       "dsn",
			},
		},
	}
	newOrg := &configstore.OrgConfig{
		Name: "analytics",
		Users: map[string]string{
			"bob": "ignored",
		},
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint:     "new-metadata.internal",
				Port:         5432,
				Username:     "ducklake_user",
				DatabaseName: "ducklake_metadata",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Namespace: "tenant-a",
				Name:      "analytics-metadata-new",
				Key:       "dsn",
			},
		},
	}

	store := newTestConfigStoreWithSnapshot(&configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{
			"analytics": oldOrg,
		},
	})

	tr := &OrgRouter{
		orgs:        make(map[string]*OrgStack),
		configStore: store,
		baseCfg:     K8sWorkerPoolConfig{MaxWorkers: 2},
		sharedPool:  sharedPool,
		globalCfg:   ControlPlaneConfig{},
	}

	var captured TenantActivationPayload
	sharedPool.activateTenantFunc = func(ctx context.Context, worker *ManagedWorker, payload TenantActivationPayload) error {
		captured = payload
		return nil
	}

	stack, err := tr.createOrgStack(oldOrg)
	if err != nil {
		t.Fatalf("createOrgStack: %v", err)
	}

	setTestConfigStoreSnapshot(store, &configstore.Snapshot{
		Orgs: map[string]*configstore.OrgConfig{
			"analytics": newOrg,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	worker, err := stack.Pool.AcquireWorker(ctx)
	if err != nil {
		t.Fatalf("AcquireWorker: %v", err)
	}
	if got := worker.SharedState().Lifecycle; got != WorkerLifecycleHot {
		t.Fatalf("expected hot lifecycle after activation, got %q", got)
	}
	if captured.OrgID != "analytics" {
		t.Fatalf("expected org analytics, got %q", captured.OrgID)
	}
	if len(captured.Usernames) != 1 || captured.Usernames[0] != "bob" {
		t.Fatalf("expected latest usernames from router snapshot, got %#v", captured.Usernames)
	}
	if got := captured.DuckLake.MetadataStore; got != "postgres:host=new-metadata.internal port=5432 user=ducklake_user password=new-password dbname=ducklake_metadata" {
		t.Fatalf("expected latest warehouse runtime from router snapshot, got %q", got)
	}
	if captured.LeaseExpiresAt.Before(time.Now()) {
		t.Fatalf("expected lease expiry to be set, got %v", captured.LeaseExpiresAt)
	}
}

func newStringSecret(namespace, name, key, value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		StringData: map[string]string{
			key: value,
		},
	}
}

func newTestConfigStoreWithSnapshot(snapshot *configstore.Snapshot) *configstore.ConfigStore {
	store := &configstore.ConfigStore{}
	setTestConfigStoreSnapshot(store, snapshot)
	return store
}

func setTestConfigStoreSnapshot(store *configstore.ConfigStore, snapshot *configstore.Snapshot) {
	field := reflect.ValueOf(store).Elem().FieldByName("snapshot")
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(snapshot))
}
