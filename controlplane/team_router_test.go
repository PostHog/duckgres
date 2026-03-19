//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakeTeamConfigStore struct {
	snap *configstore.Snapshot
}

func (s *fakeTeamConfigStore) Snapshot() *configstore.Snapshot {
	return s.snap
}

func (s *fakeTeamConfigStore) TeamForUser(username string) string {
	if s.snap == nil {
		return ""
	}
	return s.snap.UserTeam[username]
}

type fakeWorkerPool struct {
	mu              sync.Mutex
	setMaxWorkers   []int
	spawnMinWorkers []int
	healthChecks    int
	shutdowns       int
}

func (p *fakeWorkerPool) AcquireWorker(ctx context.Context) (*ManagedWorker, error) {
	return nil, fmt.Errorf("unexpected AcquireWorker call")
}

func (p *fakeWorkerPool) ReleaseWorker(id int) {}

func (p *fakeWorkerPool) RetireWorker(id int) {}

func (p *fakeWorkerPool) RetireWorkerIfNoSessions(id int) bool { return false }

func (p *fakeWorkerPool) Worker(id int) (*ManagedWorker, bool) { return nil, false }

func (p *fakeWorkerPool) SpawnMinWorkers(count int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.spawnMinWorkers = append(p.spawnMinWorkers, count)
	return nil
}

func (p *fakeWorkerPool) HealthCheckLoop(ctx context.Context, interval time.Duration, onCrash WorkerCrashHandler, onProgress ProgressHandler) {
	p.mu.Lock()
	p.healthChecks++
	p.mu.Unlock()
}

func (p *fakeWorkerPool) SetMaxWorkers(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.setMaxWorkers = append(p.setMaxWorkers, n)
}

func (p *fakeWorkerPool) ShutdownAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.shutdowns++
}

type fakePoolFactory struct {
	mu    sync.Mutex
	pools []*fakeWorkerPool
	cfgs  []K8sWorkerPoolConfig
	err   error
}

func (f *fakePoolFactory) create(cfg K8sWorkerPoolConfig) (WorkerPool, error) {
	f.mu.Lock()
	err := f.err
	f.err = nil
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}

	pool := &fakeWorkerPool{}

	f.mu.Lock()
	f.pools = append(f.pools, pool)
	f.cfgs = append(f.cfgs, cfg)
	f.mu.Unlock()

	return pool, nil
}

func (f *fakePoolFactory) lastPool() *fakeWorkerPool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pools) == 0 {
		return nil
	}
	return f.pools[len(f.pools)-1]
}

func (f *fakePoolFactory) poolCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pools)
}

func (f *fakePoolFactory) lastCfg() K8sWorkerPoolConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.cfgs) == 0 {
		return K8sWorkerPoolConfig{}
	}
	return f.cfgs[len(f.cfgs)-1]
}

func (f *fakePoolFactory) failNext(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func installFakePoolFactory(t *testing.T) *fakePoolFactory {
	t.Helper()

	oldFactory := k8sPoolFactory
	factory := &fakePoolFactory{}
	RegisterK8sPoolFactory(factory.create)
	t.Cleanup(func() {
		k8sPoolFactory = oldFactory
	})
	return factory
}

func testSnapshot(team *configstore.TeamConfig) *configstore.Snapshot {
	return &configstore.Snapshot{
		Teams: map[string]*configstore.TeamConfig{
			team.Name: team,
		},
		UserTeam:     map[string]string{},
		UserPassword: map[string]string{},
	}
}

func testTeamConfig(name string, maxWorkers int, warehouse *configstore.ManagedWarehouseConfig) *configstore.TeamConfig {
	return &configstore.TeamConfig{
		Name:         name,
		MaxWorkers:   maxWorkers,
		MinWorkers:   0,
		MemoryBudget: "8GB",
		IdleTimeoutS: 0,
		Users:        map[string]string{},
		Warehouse:    warehouse,
	}
}

func testWarehouse(endpoint string) *configstore.ManagedWarehouseConfig {
	return &configstore.ManagedWarehouseConfig{
		TeamName: "analytics",
		WarehouseDatabase: configstore.ManagedWarehouseDatabase{
			Region:       "us-east-1",
			Endpoint:     endpoint,
			Port:         5432,
			DatabaseName: "analytics_wh",
			Username:     "warehouse_user",
		},
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:         "dedicated_rds",
			Engine:       "postgres",
			Region:       "us-east-1",
			Endpoint:     "metadata.internal",
			Port:         5432,
			DatabaseName: "ducklake_metadata",
			Username:     "ducklake",
		},
		S3: configstore.ManagedWarehouseS3{
			Provider:   "aws",
			Region:     "us-east-1",
			Bucket:     "analytics-bucket",
			PathPrefix: "analytics",
		},
		WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
			Namespace:          "team-analytics",
			ServiceAccountName: "analytics-worker",
			IAMRoleARN:         "arn:aws:iam::123456789012:role/analytics-worker",
		},
		WarehouseDatabaseCredentials: configstore.SecretRef{
			Namespace: "team-analytics",
			Name:      "analytics-warehouse-db",
			Key:       "password",
		},
		MetadataStoreCredentials: configstore.SecretRef{
			Namespace: "team-analytics",
			Name:      "analytics-metadata",
			Key:       "password",
		},
		S3Credentials: configstore.SecretRef{
			Namespace: "team-analytics",
			Name:      "analytics-s3",
			Key:       "credentials",
		},
		RuntimeConfig: configstore.SecretRef{
			Namespace: "team-analytics",
			Name:      "analytics-runtime",
			Key:       "duckgres.yaml",
		},
		State:                  configstore.ManagedWarehouseStateReady,
		WarehouseDatabaseState: configstore.ManagedWarehouseStateReady,
		MetadataStoreState:     configstore.ManagedWarehouseStateReady,
		S3State:                configstore.ManagedWarehouseStateReady,
		IdentityState:          configstore.ManagedWarehouseStateReady,
		SecretsState:           configstore.ManagedWarehouseStateReady,
	}
}

func TestTeamRouter_HandleConfigChange_RecreatesStackOnWarehouseChange(t *testing.T) {
	factory := installFakePoolFactory(t)

	baseCfg := K8sWorkerPoolConfig{
		Namespace:      "shared-workers",
		CPID:           "cp-1",
		WorkerImage:    "duckgres:test",
		WorkerPort:     8816,
		SecretName:     "worker-token",
		ConfigMap:      "shared-config",
		MaxWorkers:     4,
		IdleTimeout:    5 * time.Minute,
		ConfigPath:     "/etc/duckgres/duckgres.yaml",
		ServiceAccount: "default",
		MemoryBudget:   8 << 30,
	}
	globalCfg := ControlPlaneConfig{HealthCheckInterval: 2 * time.Second}
	store := &fakeTeamConfigStore{
		snap: testSnapshot(testTeamConfig("analytics", 4, testWarehouse("warehouse-a.internal"))),
	}

	router, err := NewTeamRouter(store, baseCfg, globalCfg, nil)
	if err != nil {
		t.Fatalf("NewTeamRouter: %v", err)
	}
	if got := factory.poolCount(); got != 1 {
		t.Fatalf("expected one initial pool, got %d", got)
	}
	if cfg := factory.lastCfg(); cfg.Namespace != "team-analytics" || cfg.ServiceAccount != "analytics-worker" ||
		cfg.ConfigSecretName != "analytics-runtime" || cfg.ConfigSecretKey != "duckgres.yaml" ||
		cfg.ConfigMap != "" || cfg.ConfigPath != teamRuntimeConfigPath {
		t.Fatalf("expected runtime overrides to be applied, got %#v", cfg)
	}

	oldSnap := store.snap
	newSnap := testSnapshot(testTeamConfig("analytics", 4, testWarehouse("warehouse-b.internal")))
	router.HandleConfigChange(oldSnap, newSnap)

	if got := factory.poolCount(); got != 2 {
		t.Fatalf("expected a replacement pool after warehouse change, got %d", got)
	}
	oldPool := factory.pools[0]
	if got := oldPool.shutdowns; got != 1 {
		t.Fatalf("expected old pool to be shut down once, got %d", got)
	}
	stacks := router.AllStacks()
	stack, ok := stacks["analytics"]
	if !ok {
		t.Fatal("expected analytics team stack to exist after reconcile")
	}
	if stack.Pool == oldPool {
		t.Fatal("expected team stack to use a replacement pool after warehouse change")
	}
}

func TestTeamRouter_HandleConfigChange_UpdatesMaxWorkersInPlace(t *testing.T) {
	factory := installFakePoolFactory(t)

	baseCfg := K8sWorkerPoolConfig{
		Namespace:      "shared-workers",
		CPID:           "cp-1",
		WorkerImage:    "duckgres:test",
		WorkerPort:     8816,
		SecretName:     "worker-token",
		MaxWorkers:     4,
		IdleTimeout:    5 * time.Minute,
		ConfigPath:     "/etc/duckgres/duckgres.yaml",
		ServiceAccount: "default",
		MemoryBudget:   8 << 30,
	}
	globalCfg := ControlPlaneConfig{HealthCheckInterval: 2 * time.Second}
	store := &fakeTeamConfigStore{
		snap: testSnapshot(testTeamConfig("analytics", 4, testWarehouse("warehouse-a.internal"))),
	}

	router, err := NewTeamRouter(store, baseCfg, globalCfg, nil)
	if err != nil {
		t.Fatalf("NewTeamRouter: %v", err)
	}
	if got := factory.poolCount(); got != 1 {
		t.Fatalf("expected one initial pool, got %d", got)
	}

	oldSnap := store.snap
	newTeam := testTeamConfig("analytics", 8, testWarehouse("warehouse-a.internal"))
	newSnap := testSnapshot(newTeam)
	router.HandleConfigChange(oldSnap, newSnap)

	if got := factory.poolCount(); got != 1 {
		t.Fatalf("expected max-worker change to reuse existing pool, got %d pools", got)
	}
	pool := factory.lastPool()
	if got := len(pool.setMaxWorkers); got != 1 {
		t.Fatalf("expected one SetMaxWorkers call, got %d", got)
	}
	if got := pool.setMaxWorkers[0]; got != 8 {
		t.Fatalf("expected SetMaxWorkers(8), got %d", got)
	}
	if got := pool.shutdowns; got != 0 {
		t.Fatalf("expected existing pool to stay running, got %d shutdowns", got)
	}
	stacks := router.AllStacks()
	stack, ok := stacks["analytics"]
	if !ok {
		t.Fatal("expected analytics team stack to exist after reconcile")
	}
	if stack.Pool != pool {
		t.Fatal("expected analytics team stack to keep the same pool for max-worker-only changes")
	}
	if stack.Config.MaxWorkers != 8 {
		t.Fatalf("expected stack config to update max workers to 8, got %d", stack.Config.MaxWorkers)
	}
}

func TestTeamRouter_HandleConfigChange_RecreatesStackOnMemoryBudgetChange(t *testing.T) {
	factory := installFakePoolFactory(t)

	baseCfg := K8sWorkerPoolConfig{
		Namespace:      "shared-workers",
		CPID:           "cp-1",
		WorkerImage:    "duckgres:test",
		WorkerPort:     8816,
		SecretName:     "worker-token",
		MaxWorkers:     4,
		IdleTimeout:    5 * time.Minute,
		ConfigPath:     "/etc/duckgres/duckgres.yaml",
		ServiceAccount: "default",
		MemoryBudget:   8 << 30,
	}
	globalCfg := ControlPlaneConfig{HealthCheckInterval: 2 * time.Second}
	team := testTeamConfig("analytics", 4, testWarehouse("warehouse-a.internal"))
	team.MemoryBudget = "8GB"
	store := &fakeTeamConfigStore{
		snap: testSnapshot(team),
	}

	router, err := NewTeamRouter(store, baseCfg, globalCfg, nil)
	if err != nil {
		t.Fatalf("NewTeamRouter: %v", err)
	}
	if got := factory.poolCount(); got != 1 {
		t.Fatalf("expected one initial pool, got %d", got)
	}

	oldPool := factory.lastPool()
	oldSnap := store.snap
	newTeam := testTeamConfig("analytics", 4, testWarehouse("warehouse-a.internal"))
	newTeam.MemoryBudget = "16GB"
	newSnap := testSnapshot(newTeam)

	router.HandleConfigChange(oldSnap, newSnap)

	if got := factory.poolCount(); got != 2 {
		t.Fatalf("expected memory-budget change to create a replacement pool, got %d pools", got)
	}
	if got := oldPool.shutdowns; got != 1 {
		t.Fatalf("expected old pool to be shut down once, got %d", got)
	}
	if got := len(oldPool.setMaxWorkers); got != 0 {
		t.Fatalf("expected no in-place SetMaxWorkers calls on replaced pool, got %d", got)
	}

	stack, ok := router.AllStacks()["analytics"]
	if !ok {
		t.Fatal("expected analytics team stack to exist after reconcile")
	}
	if stack.Pool == oldPool {
		t.Fatal("expected analytics team stack to use a replacement pool after memory-budget change")
	}
	if stack.Config.MemoryBudget != "16GB" {
		t.Fatalf("expected stack config memory budget to update to 16GB, got %q", stack.Config.MemoryBudget)
	}
	if cfg := factory.lastCfg(); cfg.MemoryBudget != 16<<30 {
		t.Fatalf("expected replacement pool memory budget 16GiB, got %d", cfg.MemoryBudget)
	}
}

func TestTeamRouter_HandleConfigChange_IgnoresWarehouseStatusOnlyChanges(t *testing.T) {
	factory := installFakePoolFactory(t)

	baseCfg := K8sWorkerPoolConfig{
		Namespace:      "shared-workers",
		CPID:           "cp-1",
		WorkerImage:    "duckgres:test",
		WorkerPort:     8816,
		SecretName:     "worker-token",
		MaxWorkers:     4,
		IdleTimeout:    5 * time.Minute,
		ConfigPath:     "/etc/duckgres/duckgres.yaml",
		ServiceAccount: "default",
		MemoryBudget:   8 << 30,
	}
	globalCfg := ControlPlaneConfig{HealthCheckInterval: 2 * time.Second}
	store := &fakeTeamConfigStore{
		snap: testSnapshot(testTeamConfig("analytics", 4, testWarehouse("warehouse-a.internal"))),
	}

	router, err := NewTeamRouter(store, baseCfg, globalCfg, nil)
	if err != nil {
		t.Fatalf("NewTeamRouter: %v", err)
	}

	oldSnap := store.snap
	updatedWarehouse := testWarehouse("warehouse-a.internal")
	updatedWarehouse.State = configstore.ManagedWarehouseStateProvisioning
	updatedWarehouse.WarehouseDatabaseStatusMessage = "reconciling"
	updatedWarehouse.MetadataStoreStatusMessage = "still working"
	newSnap := testSnapshot(testTeamConfig("analytics", 4, updatedWarehouse))

	router.HandleConfigChange(oldSnap, newSnap)

	if got := factory.poolCount(); got != 1 {
		t.Fatalf("expected status-only warehouse changes to keep existing pool, got %d pools", got)
	}
	pool := factory.lastPool()
	if got := pool.shutdowns; got != 0 {
		t.Fatalf("expected existing pool to stay running, got %d shutdowns", got)
	}
}

func TestTeamRouter_HandleConfigChange_KeepsExistingStackWhenReplacementFails(t *testing.T) {
	factory := installFakePoolFactory(t)

	baseCfg := K8sWorkerPoolConfig{
		Namespace:      "shared-workers",
		CPID:           "cp-1",
		WorkerImage:    "duckgres:test",
		WorkerPort:     8816,
		SecretName:     "worker-token",
		MaxWorkers:     4,
		IdleTimeout:    5 * time.Minute,
		ConfigPath:     "/etc/duckgres/duckgres.yaml",
		ServiceAccount: "default",
		MemoryBudget:   8 << 30,
	}
	globalCfg := ControlPlaneConfig{HealthCheckInterval: 2 * time.Second}
	store := &fakeTeamConfigStore{
		snap: testSnapshot(testTeamConfig("analytics", 4, testWarehouse("warehouse-a.internal"))),
	}

	router, err := NewTeamRouter(store, baseCfg, globalCfg, nil)
	if err != nil {
		t.Fatalf("NewTeamRouter: %v", err)
	}

	originalPool := factory.lastPool()
	factory.failNext(fmt.Errorf("create failed"))

	oldSnap := store.snap
	newSnap := testSnapshot(testTeamConfig("analytics", 4, testWarehouse("warehouse-b.internal")))
	router.HandleConfigChange(oldSnap, newSnap)

	if got := factory.poolCount(); got != 1 {
		t.Fatalf("expected failed replacement to keep original pool count, got %d", got)
	}
	if got := originalPool.shutdowns; got != 0 {
		t.Fatalf("expected original pool to remain running, got %d shutdowns", got)
	}

	stack, ok := router.AllStacks()["analytics"]
	if !ok {
		t.Fatal("expected analytics stack to remain present after failed replacement")
	}
	if stack.Pool != originalPool {
		t.Fatal("expected analytics stack to keep original pool after failed replacement")
	}
}

func TestTeamRouter_HandleConfigChange_KeepsExistingStackWhenNewRuntimeConfigIsMissing(t *testing.T) {
	factory := installFakePoolFactory(t)

	baseCfg := K8sWorkerPoolConfig{
		Namespace:      "shared-workers",
		CPID:           "cp-1",
		WorkerImage:    "duckgres:test",
		WorkerPort:     8816,
		SecretName:     "worker-token",
		ConfigMap:      "shared-config",
		MaxWorkers:     4,
		IdleTimeout:    5 * time.Minute,
		ConfigPath:     "/etc/duckgres/duckgres.yaml",
		ServiceAccount: "default",
		MemoryBudget:   8 << 30,
	}
	globalCfg := ControlPlaneConfig{HealthCheckInterval: 2 * time.Second}
	store := &fakeTeamConfigStore{
		snap: testSnapshot(testTeamConfig("analytics", 4, testWarehouse("warehouse-a.internal"))),
	}

	router, err := NewTeamRouter(store, baseCfg, globalCfg, nil)
	if err != nil {
		t.Fatalf("NewTeamRouter: %v", err)
	}

	originalPool := factory.lastPool()
	oldSnap := store.snap
	brokenWarehouse := testWarehouse("warehouse-b.internal")
	brokenWarehouse.RuntimeConfig = configstore.SecretRef{}
	newSnap := testSnapshot(testTeamConfig("analytics", 4, brokenWarehouse))

	router.HandleConfigChange(oldSnap, newSnap)

	if got := factory.poolCount(); got != 1 {
		t.Fatalf("expected invalid runtime replacement to keep original pool count, got %d", got)
	}
	if got := originalPool.shutdowns; got != 0 {
		t.Fatalf("expected original pool to remain running, got %d shutdowns", got)
	}

	stack, ok := router.AllStacks()["analytics"]
	if !ok {
		t.Fatal("expected analytics stack to remain present after invalid replacement")
	}
	if stack.Pool != originalPool {
		t.Fatal("expected analytics stack to keep original pool when runtime_config is missing")
	}
}

func TestNewTeamRouter_SkipsManagedWarehouseTeamsMissingRuntimeConfig(t *testing.T) {
	factory := installFakePoolFactory(t)

	baseCfg := K8sWorkerPoolConfig{
		Namespace:      "shared-workers",
		CPID:           "cp-1",
		WorkerImage:    "duckgres:test",
		WorkerPort:     8816,
		SecretName:     "worker-token",
		MaxWorkers:     4,
		IdleTimeout:    5 * time.Minute,
		ConfigPath:     "/etc/duckgres/duckgres.yaml",
		ServiceAccount: "default",
		MemoryBudget:   8 << 30,
	}
	globalCfg := ControlPlaneConfig{HealthCheckInterval: 2 * time.Second}
	brokenWarehouse := testWarehouse("warehouse-a.internal")
	brokenWarehouse.RuntimeConfig = configstore.SecretRef{}
	store := &fakeTeamConfigStore{
		snap: testSnapshot(testTeamConfig("analytics", 4, brokenWarehouse)),
	}

	router, err := NewTeamRouter(store, baseCfg, globalCfg, nil)
	if err != nil {
		t.Fatalf("NewTeamRouter: %v", err)
	}
	if got := factory.poolCount(); got != 0 {
		t.Fatalf("expected no pool to be created for incomplete managed warehouse, got %d", got)
	}
	if stacks := router.AllStacks(); len(stacks) != 0 {
		t.Fatalf("expected no team stacks for incomplete managed warehouse, got %d", len(stacks))
	}
}

func TestTeamRouter_HandleConfigChange_IgnoresDuckLakeSingletonChanges(t *testing.T) {
	factory := installFakePoolFactory(t)

	baseCfg := K8sWorkerPoolConfig{
		Namespace:      "shared-workers",
		CPID:           "cp-1",
		WorkerImage:    "duckgres:test",
		WorkerPort:     8816,
		SecretName:     "worker-token",
		MaxWorkers:     4,
		IdleTimeout:    5 * time.Minute,
		ConfigPath:     "/etc/duckgres/duckgres.yaml",
		ServiceAccount: "default",
		MemoryBudget:   8 << 30,
	}
	globalCfg := ControlPlaneConfig{HealthCheckInterval: 2 * time.Second}
	store := &fakeTeamConfigStore{
		snap: testSnapshot(testTeamConfig("analytics", 4, testWarehouse("warehouse-a.internal"))),
	}

	router, err := NewTeamRouter(store, baseCfg, globalCfg, nil)
	if err != nil {
		t.Fatalf("NewTeamRouter: %v", err)
	}

	oldSnap := store.snap
	newSnap := testSnapshot(testTeamConfig("analytics", 4, testWarehouse("warehouse-a.internal")))
	newSnap.DuckLake = configstore.DuckLakeConfig{
		ID:            1,
		ObjectStore:   "s3://legacy/fallback/",
		MetadataStore: "postgres:host=legacy.example port=5432 user=ducklake password=ducklake dbname=ducklake",
	}

	router.HandleConfigChange(oldSnap, newSnap)

	if got := factory.poolCount(); got != 1 {
		t.Fatalf("expected ducklake singleton changes to keep existing pool, got %d pools", got)
	}
	pool := factory.lastPool()
	if got := pool.shutdowns; got != 0 {
		t.Fatalf("expected existing pool to stay running, got %d shutdowns", got)
	}
}
