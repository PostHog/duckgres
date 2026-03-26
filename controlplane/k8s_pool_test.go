//go:build kubernetes

package controlplane

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type captureRuntimeWorkerStore struct {
	mu      sync.Mutex
	records []configstore.WorkerRecord
}

func (s *captureRuntimeWorkerStore) UpsertWorkerRecord(record *configstore.WorkerRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, *record)
	return nil
}

func (s *captureRuntimeWorkerStore) snapshot() []configstore.WorkerRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]configstore.WorkerRecord, len(s.records))
	copy(out, s.records)
	return out
}

func newTestK8sPool(t *testing.T, maxWorkers int) (*K8sWorkerPool, *fake.Clientset) {
	t.Helper()
	cs := fake.NewSimpleClientset()

	// Create the CP pod so resolveCPUID works
	_, err := cs.CoreV1().Pods("default").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cp",
			Namespace: "default",
			UID:       "cp-uid-123",
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pool := &K8sWorkerPool{
		workers:     make(map[int]*ManagedWorker),
		maxWorkers:  maxWorkers,
		idleTimeout: 5 * time.Minute,
		shutdownCh:  make(chan struct{}),
		stopInform:  make(chan struct{}),
		clientset:   cs,
		namespace:   "default",
		cpID:        "test-cp",
		cpInstanceID:"cp-uid-123:boot-abc",
		cpUID:       "cp-uid-123",
		workerImage: "duckgres:test",
		workerPort:  8816,
		secretName:  "test-secret",
		spawnSem:    make(chan struct{}, 1),
	}

	return pool, cs
}

func TestK8sPool_EnsureBearerTokenSecret_CreatesNew(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)

	err := pool.ensureBearerTokenSecret(context.Background())
	if err != nil {
		t.Fatalf("ensureBearerTokenSecret failed: %v", err)
	}

	// Verify the secret exists
	secret, err := cs.CoreV1().Secrets("default").Get(context.Background(), "test-secret", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("secret not found: %v", err)
	}
	token, ok := secret.Data["bearer-token"]
	if !ok || len(token) == 0 {
		t.Fatal("secret missing bearer-token key or empty")
	}
}

func TestK8sPool_EnsureBearerTokenSecret_ExistingIsPreserved(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)

	// Pre-create the secret
	_, err := cs.CoreV1().Secrets("default").Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "test-secret", Namespace: "default"},
		Data:       map[string][]byte{"bearer-token": []byte("existing-token")},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	err = pool.ensureBearerTokenSecret(context.Background())
	if err != nil {
		t.Fatalf("ensureBearerTokenSecret failed: %v", err)
	}

	// Verify the original token is preserved
	secret, err := cs.CoreV1().Secrets("default").Get(context.Background(), "test-secret", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["bearer-token"]) != "existing-token" {
		t.Fatalf("token was modified: %s", secret.Data["bearer-token"])
	}
}

func TestK8sPool_ReadBearerToken(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)

	_, err := cs.CoreV1().Secrets("default").Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "test-secret", Namespace: "default"},
		Data:       map[string][]byte{"bearer-token": []byte("my-token-123")},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	token, err := pool.readBearerToken(context.Background())
	if err != nil {
		t.Fatalf("readBearerToken failed: %v", err)
	}
	if token != "my-token-123" {
		t.Fatalf("unexpected token: %s", token)
	}
}

func TestK8sPool_WorkerLookup(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	done := make(chan struct{})
	pool.workers[42] = &ManagedWorker{ID: 42, done: done}

	w, ok := pool.Worker(42)
	if !ok || w.ID != 42 {
		t.Fatalf("Worker(42) returned ok=%v, id=%d", ok, w.ID)
	}

	_, ok = pool.Worker(99)
	if ok {
		t.Fatal("Worker(99) should not exist")
	}
}

func TestK8sPool_ReleaseWorker(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	done := make(chan struct{})
	pool.workers[1] = &ManagedWorker{ID: 1, activeSessions: 2, done: done}

	pool.ReleaseWorker(1)

	w := pool.workers[1]
	if w.activeSessions != 1 {
		t.Fatalf("expected 1 active session, got %d", w.activeSessions)
	}
	if w.lastUsed.IsZero() {
		t.Fatal("lastUsed should be set")
	}
}

func TestK8sPool_RetireWorker(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	done := make(chan struct{})
	pool.workers[1] = &ManagedWorker{ID: 1, done: done}

	pool.RetireWorker(1)

	// Give the goroutine time to run
	time.Sleep(100 * time.Millisecond)

	_, ok := pool.Worker(1)
	if ok {
		t.Fatal("worker should be removed from pool after retire")
	}
}

func TestK8sPool_RetireWorkerIfNoSessions_WithSessions(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	done := make(chan struct{})
	pool.workers[1] = &ManagedWorker{ID: 1, activeSessions: 2, done: done}

	retired := pool.RetireWorkerIfNoSessions(1)
	if retired {
		t.Fatal("should not retire worker with 1 remaining session")
	}

	w := pool.workers[1]
	if w.activeSessions != 1 {
		t.Fatalf("expected 1 active session after decrement, got %d", w.activeSessions)
	}
}

func TestK8sPool_RetireWorkerIfNoSessions_LastSession(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	done := make(chan struct{})
	pool.workers[1] = &ManagedWorker{ID: 1, activeSessions: 1, done: done}

	retired := pool.RetireWorkerIfNoSessions(1)
	if !retired {
		t.Fatal("should retire worker with 0 remaining sessions")
	}

	time.Sleep(100 * time.Millisecond)
	_, ok := pool.Worker(1)
	if ok {
		t.Fatal("worker should be removed after retiring")
	}
}

func TestK8sPoolActivateReservedWorkerTransitionsToHot(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	worker := &ManagedWorker{ID: 7, done: make(chan struct{})}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			OrgID:          "analytics",
			LeaseExpiresAt: time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("SetSharedState: %v", err)
	}
	pool.workers[worker.ID] = worker
	pool.activateTenantFunc = func(ctx context.Context, got *ManagedWorker, payload TenantActivationPayload) error {
		if got.ID != worker.ID {
			t.Fatalf("expected worker %d, got %d", worker.ID, got.ID)
		}
		if payload.OrgID != "analytics" {
			t.Fatalf("expected analytics payload, got %#v", payload)
		}
		return nil
	}

	err := pool.ActivateReservedWorker(context.Background(), worker, TenantActivationPayload{
		OrgID:     "analytics",
		Usernames: []string{"alice"},
	})
	if err != nil {
		t.Fatalf("ActivateReservedWorker: %v", err)
	}

	if got := worker.SharedState().Lifecycle; got != WorkerLifecycleHot {
		t.Fatalf("expected hot lifecycle, got %q", got)
	}
}

func TestK8sPoolActivateReservedWorkerRetiresOnFailure(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	worker := &ManagedWorker{ID: 8, done: make(chan struct{})}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			OrgID:          "analytics",
			LeaseExpiresAt: time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("SetSharedState: %v", err)
	}
	pool.workers[worker.ID] = worker
	pool.activateTenantFunc = func(ctx context.Context, got *ManagedWorker, payload TenantActivationPayload) error {
		return context.DeadlineExceeded
	}

	err := pool.ActivateReservedWorker(context.Background(), worker, TenantActivationPayload{
		OrgID:     "analytics",
		Usernames: []string{"alice"},
	})
	if err == nil {
		t.Fatal("expected activation failure")
	}

	time.Sleep(100 * time.Millisecond)
	if _, ok := pool.Worker(worker.ID); ok {
		t.Fatal("expected failed activation to retire worker")
	}
}

func TestK8sPoolReserveSharedWorkerSkipsUnhealthyIdleWorker(t *testing.T) {
	pool, _ := newTestK8sPool(t, 2)
	stale := &ManagedWorker{ID: 1, done: make(chan struct{})}
	if err := stale.SetSharedState(SharedWorkerState{Lifecycle: WorkerLifecycleIdle}); err != nil {
		t.Fatalf("SetSharedState(stale): %v", err)
	}
	pool.workers[stale.ID] = stale

	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		pool.mu.Lock()
		defer pool.mu.Unlock()
		worker := &ManagedWorker{ID: id, done: make(chan struct{})}
		if err := worker.SetSharedState(SharedWorkerState{Lifecycle: WorkerLifecycleIdle}); err != nil {
			return err
		}
		pool.workers[id] = worker
		return nil
	}

	checks := 0
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		checks++
		if worker.ID == stale.ID {
			return context.DeadlineExceeded
		}
		return nil
	}

	got, err := pool.ReserveSharedWorker(context.Background(), &WorkerAssignment{
		OrgID:          "analytics",
		LeaseExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("ReserveSharedWorker: %v", err)
	}
	if got.ID == stale.ID {
		t.Fatalf("expected stale worker to be skipped, got %d", got.ID)
	}
	if checks == 0 {
		t.Fatal("expected liveness recheck before reservation")
	}
	if _, ok := pool.Worker(stale.ID); ok {
		t.Fatal("expected stale worker to be retired")
	}
	if got.SharedState().Assignment == nil || got.SharedState().Assignment.OrgID != "analytics" {
		t.Fatalf("expected returned worker reserved for analytics, got %#v", got.SharedState().Assignment)
	}
}

func TestK8sPool_CleanDeadWorkers(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	alive := make(chan struct{})
	dead := make(chan struct{})
	close(dead) // simulate a dead worker

	pool.workers[1] = &ManagedWorker{ID: 1, done: alive}
	pool.workers[2] = &ManagedWorker{ID: 2, done: dead}

	pool.cleanDeadWorkersLocked()

	if _, ok := pool.workers[1]; !ok {
		t.Fatal("alive worker should still exist")
	}
	if _, ok := pool.workers[2]; ok {
		t.Fatal("dead worker should be cleaned")
	}
}

func TestK8sPool_FindIdleWorker(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	done := make(chan struct{})
	pool.workers[1] = &ManagedWorker{ID: 1, activeSessions: 1, done: done}
	pool.workers[2] = &ManagedWorker{ID: 2, activeSessions: 0, done: done}

	idle := pool.findIdleWorkerLocked()
	if idle == nil || idle.ID != 2 {
		t.Fatalf("expected idle worker 2, got %v", idle)
	}
}

func TestK8sPool_LeastLoadedWorker(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	done := make(chan struct{})
	pool.workers[1] = &ManagedWorker{ID: 1, activeSessions: 5, done: done}
	pool.workers[2] = &ManagedWorker{ID: 2, activeSessions: 2, done: done}
	pool.workers[3] = &ManagedWorker{ID: 3, activeSessions: 3, done: done}

	w := pool.leastLoadedWorkerLocked()
	if w == nil || w.ID != 2 {
		t.Fatalf("expected least loaded worker 2, got %v", w)
	}
}

func TestK8sPool_LiveWorkerCount(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	alive := make(chan struct{})
	dead := make(chan struct{})
	close(dead)

	pool.workers[1] = &ManagedWorker{ID: 1, done: alive}
	pool.workers[2] = &ManagedWorker{ID: 2, done: dead}
	pool.workers[3] = &ManagedWorker{ID: 3, done: alive}
	pool.spawning = 1

	count := pool.liveWorkerCountLocked()
	if count != 3 { // 2 alive + 1 spawning
		t.Fatalf("expected 3 live workers, got %d", count)
	}
}

func TestK8sPoolSpawnMinWorkersTracksWarmCapacityAndSpawnsMissingWorkers(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.workers[41] = &ManagedWorker{ID: 41, done: make(chan struct{})}

	var spawned []int
	var spawnedMu sync.Mutex
	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		spawnedMu.Lock()
		spawned = append(spawned, id)
		spawnedMu.Unlock()
		pool.mu.Lock()
		pool.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		pool.mu.Unlock()
		return nil
	}

	if err := pool.SpawnMinWorkers(3); err != nil {
		t.Fatalf("SpawnMinWorkers: %v", err)
	}

	if pool.minWorkers != 3 {
		t.Fatalf("expected minWorkers to track warm capacity target 3, got %d", pool.minWorkers)
	}
	if len(spawned) != 2 {
		t.Fatalf("expected SpawnMinWorkers to spawn 2 missing workers, got %d", len(spawned))
	}
}

func TestK8sPoolSpawnMinWorkersCountsOnlyNeutralIdleWorkersAsWarmCapacity(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	for _, id := range []int{41, 42} {
		worker := &ManagedWorker{ID: id, done: make(chan struct{})}
		if err := worker.SetSharedState(SharedWorkerState{
			Lifecycle: WorkerLifecycleReserved,
			Assignment: &WorkerAssignment{
				OrgID:          "analytics",
				LeaseExpiresAt: time.Now().Add(time.Hour),
			},
		}); err != nil {
			t.Fatalf("SetSharedState(reserved %d): %v", id, err)
		}
		pool.workers[id] = worker
	}

	var spawned []int
	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		spawned = append(spawned, id)
		pool.mu.Lock()
		pool.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		pool.mu.Unlock()
		return nil
	}

	if err := pool.SpawnMinWorkers(2); err != nil {
		t.Fatalf("SpawnMinWorkers: %v", err)
	}

	if len(spawned) != 2 {
		t.Fatalf("expected SpawnMinWorkers to spawn 2 neutral warm workers, got %d", len(spawned))
	}
}

func TestK8sPoolFindIdleWorkerSkipsReservedSharedWorker(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	reserved := &ManagedWorker{ID: 1, done: make(chan struct{})}
	if err := reserved.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			OrgID:          "analytics",
			LeaseExpiresAt: time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("SetSharedState(reserved): %v", err)
	}

	idle := &ManagedWorker{ID: 2, done: make(chan struct{})}
	pool.workers[reserved.ID] = reserved
	pool.workers[idle.ID] = idle

	got := pool.findIdleWorkerLocked()
	if got == nil || got.ID != idle.ID {
		t.Fatalf("expected idle worker %d, got %#v", idle.ID, got)
	}
}

func TestK8sPoolReserveSharedWorkerReservesIdleWorkerAndReplenishesWarmCapacity(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.minWorkers = 1
	store := &captureRuntimeWorkerStore{}
	pool.runtimeStore = store
	idle := &ManagedWorker{ID: 7, done: make(chan struct{})}
	pool.workers[idle.ID] = idle
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		if worker != idle {
			t.Fatalf("expected liveness check for idle worker %d, got %#v", idle.ID, worker)
		}
		return nil
	}

	replacementSpawned := make(chan int, 1)
	pool.spawnWarmWorkerBackgroundFunc = func(id int) {
		replacementSpawned <- id
		pool.mu.Lock()
		if pool.spawning > 0 {
			pool.spawning--
		}
		pool.mu.Unlock()
	}

	leaseExpiry := time.Date(2026, time.March, 20, 16, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	worker, err := pool.ReserveSharedWorker(ctx, &WorkerAssignment{
		OrgID:          "analytics",
		LeaseExpiresAt: leaseExpiry,
	})
	if err != nil {
		t.Fatalf("ReserveSharedWorker: %v", err)
	}
	if worker.ID != idle.ID {
		t.Fatalf("expected worker %d, got %d", idle.ID, worker.ID)
	}

	state := worker.SharedState()
	if state.Lifecycle != WorkerLifecycleReserved {
		t.Fatalf("expected reserved lifecycle, got %q", state.Lifecycle)
	}
	if state.Assignment == nil || state.Assignment.OrgID != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
	if !state.Assignment.LeaseExpiresAt.Equal(leaseExpiry) {
		t.Fatalf("expected lease expiry %v, got %v", leaseExpiry, state.Assignment.LeaseExpiresAt)
	}
	if worker.ownerEpoch != 1 {
		t.Fatalf("expected owner epoch 1 after reservation, got %d", worker.ownerEpoch)
	}

	records := store.snapshot()
	if len(records) == 0 {
		t.Fatal("expected reservation to persist a worker record")
	}
	last := records[len(records)-1]
	if last.WorkerID != worker.ID {
		t.Fatalf("expected worker_id %d, got %d", worker.ID, last.WorkerID)
	}
	if last.State != configstore.WorkerStateReserved {
		t.Fatalf("expected reserved worker record, got %q", last.State)
	}
	if last.OwnerCPInstanceID != pool.cpInstanceID {
		t.Fatalf("expected owner_cp_instance_id %q, got %q", pool.cpInstanceID, last.OwnerCPInstanceID)
	}
	if last.OwnerEpoch != 1 {
		t.Fatalf("expected owner_epoch 1, got %d", last.OwnerEpoch)
	}
	if last.OrgID != "analytics" {
		t.Fatalf("expected org_id analytics, got %q", last.OrgID)
	}
	if !last.LeaseExpiresAt.Equal(leaseExpiry) {
		t.Fatalf("expected lease expiry %v, got %v", leaseExpiry, last.LeaseExpiresAt)
	}

	select {
	case <-replacementSpawned:
	case <-time.After(time.Second):
		t.Fatal("expected reserve to trigger warm-pool replenishment")
	}
}

func TestK8sPoolActivateReservedWorkerPersistsActivatingThenHotWorkerRecord(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{}
	pool.runtimeStore = store
	worker := &ManagedWorker{ID: 9, done: make(chan struct{}), ownerEpoch: 4}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			OrgID:          "analytics",
			LeaseExpiresAt: time.Date(2026, time.March, 20, 17, 0, 0, 0, time.UTC),
		},
	}); err != nil {
		t.Fatalf("SetSharedState: %v", err)
	}
	pool.workers[worker.ID] = worker
	pool.activateTenantFunc = func(ctx context.Context, got *ManagedWorker, payload TenantActivationPayload) error {
		return nil
	}

	if err := pool.ActivateReservedWorker(context.Background(), worker, TenantActivationPayload{
		OrgID:          "analytics",
		LeaseExpiresAt: worker.SharedState().Assignment.LeaseExpiresAt,
	}); err != nil {
		t.Fatalf("ActivateReservedWorker: %v", err)
	}

	records := store.snapshot()
	if len(records) != 2 {
		t.Fatalf("expected 2 persisted records, got %d", len(records))
	}
	if records[0].State != configstore.WorkerStateActivating {
		t.Fatalf("expected activating record first, got %q", records[0].State)
	}
	if records[1].State != configstore.WorkerStateHot {
		t.Fatalf("expected hot record second, got %q", records[1].State)
	}
	for i, record := range records {
		if record.OwnerEpoch != 4 {
			t.Fatalf("record %d expected owner epoch 4, got %d", i, record.OwnerEpoch)
		}
		if record.OwnerCPInstanceID != pool.cpInstanceID {
			t.Fatalf("record %d expected owner_cp_instance_id %q, got %q", i, pool.cpInstanceID, record.OwnerCPInstanceID)
		}
		if record.OrgID != "analytics" {
			t.Fatalf("record %d expected org_id analytics, got %q", i, record.OrgID)
		}
	}
}

func TestK8sPoolRetireWorkerPersistsRetiredWorkerRecord(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{}
	pool.runtimeStore = store
	worker := &ManagedWorker{ID: 5, done: make(chan struct{}), ownerEpoch: 2}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleHot,
		Assignment: &WorkerAssignment{
			OrgID:          "analytics",
			LeaseExpiresAt: time.Date(2026, time.March, 20, 18, 0, 0, 0, time.UTC),
		},
	}); err != nil {
		t.Fatalf("SetSharedState: %v", err)
	}
	pool.workers[worker.ID] = worker

	pool.RetireWorker(worker.ID)

	records := store.snapshot()
	if len(records) == 0 {
		t.Fatal("expected retirement to persist a worker record")
	}
	last := records[len(records)-1]
	if last.State != configstore.WorkerStateRetired {
		t.Fatalf("expected retired worker record, got %q", last.State)
	}
	if last.OwnerEpoch != 2 {
		t.Fatalf("expected owner epoch 2, got %d", last.OwnerEpoch)
	}
	if last.OwnerCPInstanceID != pool.cpInstanceID {
		t.Fatalf("expected owner_cp_instance_id %q, got %q", pool.cpInstanceID, last.OwnerCPInstanceID)
	}
	if last.OrgID != "analytics" {
		t.Fatalf("expected org_id analytics, got %q", last.OrgID)
	}
	if last.RetireReason != RetireReasonNormal {
		t.Fatalf("expected retire reason %q, got %q", RetireReasonNormal, last.RetireReason)
	}
}

func TestK8sPoolHealthCheckLoopReplenishesWarmCapacityAfterIdleWorkerCrash(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.minWorkers = 1

	worker := &ManagedWorker{ID: 7, done: make(chan struct{})}
	pool.workers[worker.ID] = worker

	replacementSpawned := make(chan int, 1)
	pool.spawnWarmWorkerBackgroundFunc = func(id int) {
		replacementSpawned <- id
		pool.mu.Lock()
		if pool.spawning > 0 {
			pool.spawning--
		}
		pool.mu.Unlock()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.HealthCheckLoop(ctx, time.Millisecond, nil, nil)

	close(worker.done)

	select {
	case <-replacementSpawned:
	case <-time.After(time.Second):
		t.Fatal("expected idle worker crash to trigger warm-pool replenishment")
	}
}

func TestK8sPoolReserveSharedWorkerSpawnsWhenPoolIsCold(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}
	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		pool.mu.Lock()
		pool.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		pool.mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	worker, err := pool.ReserveSharedWorker(ctx, &WorkerAssignment{
		OrgID:          "billing",
		LeaseExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("ReserveSharedWorker: %v", err)
	}
	if worker == nil {
		t.Fatal("expected reserved worker")
	}
	if got := worker.SharedState().Lifecycle; got != WorkerLifecycleReserved {
		t.Fatalf("expected reserved lifecycle after cold start, got %q", got)
	}
}

func TestK8sPoolIdleReaperSkipsReservedSharedWorker(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.idleTimeout = time.Millisecond

	reserved := &ManagedWorker{
		ID:       1,
		lastUsed: time.Now().Add(-time.Hour),
		done:     make(chan struct{}),
	}
	if err := reserved.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			OrgID:          "analytics",
			LeaseExpiresAt: time.Now().Add(time.Hour),
		},
	}); err != nil {
		t.Fatalf("SetSharedState(reserved): %v", err)
	}
	idle := &ManagedWorker{
		ID:       2,
		lastUsed: time.Now().Add(-time.Hour),
		done:     make(chan struct{}),
	}
	pool.workers[reserved.ID] = reserved
	pool.workers[idle.ID] = idle

	pool.reapIdleWorkers()

	if _, ok := pool.workers[reserved.ID]; !ok {
		t.Fatal("reserved worker should not be reaped")
	}
	if _, ok := pool.workers[idle.ID]; ok {
		t.Fatal("idle worker should be reaped")
	}
}

func TestK8sPool_SpawnWorkerCreatesCorrectPod(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	pool.configMap = "my-config"
	var createdWorkerPod *corev1.Pod
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		pod, ok := createAction.GetObject().(*corev1.Pod)
		if !ok {
			return false, nil, nil
		}
		if pod.Labels["app"] == "duckgres-worker" {
			createdWorkerPod = pod.DeepCopy()
		}
		return false, nil, nil
	})

	// Create the bearer token secret
	_, err := cs.CoreV1().Secrets("default").Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "test-secret", Namespace: "default"},
		Data:       map[string][]byte{"bearer-token": []byte("test-token")},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// SpawnWorker will fail at the gRPC connection step since there's no
	// real pod running, but we can verify the pod was created correctly.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = pool.SpawnWorker(ctx, 0)

	pods, err := cs.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{
		LabelSelector: "duckgres/control-plane=test-cp",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Find the worker pod (may have been deleted on spawn failure, check actions)
	found := false
	for _, pod := range pods.Items {
		if pod.Labels["duckgres/worker-id"] == "0" {
			found = true
			assertSpawnedWorkerPod(t, &pod)
			break
		}
	}

	if !found {
		if createdWorkerPod == nil {
			t.Fatal("expected worker pod create to be attempted before cleanup")
		}
		assertSpawnedWorkerPod(t, createdWorkerPod)
	}
}

func assertSpawnedWorkerPod(t *testing.T, pod *corev1.Pod) {
	t.Helper()

	if pod.Labels["duckgres/worker-id"] != "0" {
		t.Fatalf("expected worker-id label 0, got %q", pod.Labels["duckgres/worker-id"])
	}
	if pod.Labels["app"] != "duckgres-worker" {
		t.Fatalf("expected app=duckgres-worker label, got %s", pod.Labels["app"])
	}
	if pod.Labels["duckgres/control-plane"] != "test-cp" {
		t.Fatalf("expected control-plane label test-cp, got %s", pod.Labels["duckgres/control-plane"])
	}
	if pod.Labels["duckgres/cp-instance-id"] != "cp-uid-123:boot-abc" {
		t.Fatalf("expected cp-instance-id label cp-uid-123:boot-abc, got %s", pod.Labels["duckgres/cp-instance-id"])
	}
	if pod.Labels["duckgres/owner-epoch"] != "0" {
		t.Fatalf("expected owner-epoch label 0, got %s", pod.Labels["duckgres/owner-epoch"])
	}
	if _, ok := pod.Labels["duckgres/org"]; ok {
		t.Fatalf("expected shared warm worker startup to stay org-neutral, got labels %#v", pod.Labels)
	}

	if len(pod.OwnerReferences) != 0 {
		t.Fatalf("expected no owner references, got %d", len(pod.OwnerReferences))
	}

	if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.RunAsNonRoot == nil || !*pod.Spec.SecurityContext.RunAsNonRoot {
		t.Fatal("expected runAsNonRoot=true")
	}

	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(pod.Spec.Containers))
	}
	c := pod.Spec.Containers[0]
	if c.Image != "duckgres:test" {
		t.Fatalf("expected image duckgres:test, got %s", c.Image)
	}

	foundEnv := false
	foundSharedWarmWorkerEnv := false
	for _, env := range c.Env {
		if env.Name == "DUCKGRES_DUCKDB_TOKEN" && env.ValueFrom != nil &&
			env.ValueFrom.SecretKeyRef != nil &&
			env.ValueFrom.SecretKeyRef.Name == "test-secret" {
			foundEnv = true
		}
		if env.Name == "DUCKGRES_SHARED_WARM_WORKER" && env.Value == "true" {
			foundSharedWarmWorkerEnv = true
		}
	}
	if !foundEnv {
		t.Fatal("bearer token env var not found or incorrect")
	}
	if !foundSharedWarmWorkerEnv {
		t.Fatal("expected shared warm worker startup env to be present")
	}

	if len(pod.Spec.Volumes) == 0 {
		t.Fatal("expected configmap volume")
	}
}

func TestK8sPool_ShutdownAll(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)

	// Add some workers
	for i := 0; i < 3; i++ {
		done := make(chan struct{})
		pool.workers[i] = &ManagedWorker{ID: i, done: done}

		// Create corresponding pods
		_, _ = cs.CoreV1().Pods("default").Create(context.Background(), &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "duckgres-worker-test-cp-" + strconv.Itoa(i),
				Namespace: "default",
				Labels: map[string]string{
					"duckgres/control-plane": "test-cp",
					"duckgres/worker-id":     strconv.Itoa(i),
				},
			},
		}, metav1.CreateOptions{})
	}

	pool.ShutdownAll()

	pool.mu.RLock()
	count := len(pool.workers)
	pool.mu.RUnlock()
	if count != 0 {
		t.Fatalf("expected 0 workers after shutdown, got %d", count)
	}
}

func TestK8sPool_OnPodTerminated(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	done := make(chan struct{})
	pool.workers[5] = &ManagedWorker{ID: 5, done: done}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"duckgres/worker-id": "5",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodFailed},
	}

	pool.onPodTerminated(pod)

	// Verify done channel was closed
	select {
	case <-done:
		// Good
	default:
		t.Fatal("done channel should be closed after pod termination")
	}
}

func TestK8sPool_IdleReaper(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	pool.idleTimeout = 1 * time.Millisecond // Very short for testing

	done := make(chan struct{})
	pool.workers[1] = &ManagedWorker{
		ID:             1,
		activeSessions: 0,
		lastUsed:       time.Now().Add(-1 * time.Hour), // Idle for a long time
		done:           done,
	}

	// Create corresponding pod
	_, _ = cs.CoreV1().Pods("default").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "duckgres-worker-test-cp-1",
			Namespace: "default",
		},
	}, metav1.CreateOptions{})

	pool.reapIdleWorkers()

	// Give goroutine time to retire
	time.Sleep(100 * time.Millisecond)

	_, ok := pool.Worker(1)
	if ok {
		t.Fatal("idle worker should have been reaped")
	}
}

func TestWorkerResources_BothSet(t *testing.T) {
	pool := &K8sWorkerPool{
		workerCPURequest:    "500m",
		workerMemoryRequest: "2Gi",
	}
	res := pool.workerResources()
	if res.Requests == nil {
		t.Fatal("expected requests to be set")
	}
	cpu := res.Requests[corev1.ResourceCPU]
	if cpu.String() != "500m" {
		t.Fatalf("expected CPU request 500m, got %s", cpu.String())
	}
	mem := res.Requests[corev1.ResourceMemory]
	if mem.String() != "2Gi" {
		t.Fatalf("expected memory request 2Gi, got %s", mem.String())
	}
	if res.Limits != nil {
		t.Fatal("expected no limits to be set")
	}
}

func TestWorkerResources_CPUOnly(t *testing.T) {
	pool := &K8sWorkerPool{
		workerCPURequest: "1",
	}
	res := pool.workerResources()
	if _, ok := res.Requests[corev1.ResourceCPU]; !ok {
		t.Fatal("expected CPU request")
	}
	if _, ok := res.Requests[corev1.ResourceMemory]; ok {
		t.Fatal("expected no memory request")
	}
}

func TestWorkerResources_MemoryOnly(t *testing.T) {
	pool := &K8sWorkerPool{
		workerMemoryRequest: "4Gi",
	}
	res := pool.workerResources()
	if _, ok := res.Requests[corev1.ResourceMemory]; !ok {
		t.Fatal("expected memory request")
	}
	if _, ok := res.Requests[corev1.ResourceCPU]; ok {
		t.Fatal("expected no CPU request")
	}
}

func TestWorkerResources_NeitherSet(t *testing.T) {
	pool := &K8sWorkerPool{}
	res := pool.workerResources()
	if res.Requests != nil {
		t.Fatal("expected empty requests (BestEffort)")
	}
	if res.Limits != nil {
		t.Fatal("expected empty limits")
	}
}

func TestWorkerScheduling_NodeSelectorAndToleration(t *testing.T) {
	pool := &K8sWorkerPool{
		workerNodeSelector:  map[string]string{"posthog.com/duckgres-workers": "duckgres-workers"},
		workerTolerationKey: "posthog.com/duckgres-workers",
	}

	// Build a minimal pod to test scheduling additions
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			NodeSelector: pool.workerNodeSelector,
		},
	}

	// Verify nodeSelector
	if pod.Spec.NodeSelector == nil {
		t.Fatal("expected nodeSelector to be set")
	}
	if pod.Spec.NodeSelector["posthog.com/duckgres-workers"] != "duckgres-workers" {
		t.Fatalf("unexpected nodeSelector: %v", pod.Spec.NodeSelector)
	}

	// Verify toleration construction
	if pool.workerTolerationKey == "" {
		t.Fatal("expected tolerationKey to be set")
	}
	toleration := corev1.Toleration{
		Key:    pool.workerTolerationKey,
		Effect: corev1.TaintEffectNoSchedule,
	}
	if toleration.Key != "posthog.com/duckgres-workers" {
		t.Fatalf("unexpected toleration key: %s", toleration.Key)
	}
	if toleration.Effect != corev1.TaintEffectNoSchedule {
		t.Fatalf("expected NoSchedule effect, got %s", toleration.Effect)
	}
}

func TestWorkerScheduling_NoSelectorOrToleration(t *testing.T) {
	pool := &K8sWorkerPool{}

	if pool.workerNodeSelector != nil {
		t.Fatal("expected nil nodeSelector by default")
	}
	if pool.workerTolerationKey != "" {
		t.Fatal("expected empty tolerationKey by default")
	}
}

func TestParseNodeSelector(t *testing.T) {
	// Valid JSON
	m := parseNodeSelector(`{"posthog.com/pool":"workers"}`)
	if m == nil || m["posthog.com/pool"] != "workers" {
		t.Fatalf("expected parsed selector, got %v", m)
	}

	// Empty string
	if parseNodeSelector("") != nil {
		t.Fatal("expected nil for empty string")
	}

	// Invalid JSON
	if parseNodeSelector("not-json") != nil {
		t.Fatal("expected nil for invalid JSON")
	}
}
