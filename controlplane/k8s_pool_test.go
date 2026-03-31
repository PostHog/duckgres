//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type captureRuntimeWorkerStore struct {
	mu                    sync.Mutex
	records               []configstore.WorkerRecord
	claimed               *configstore.WorkerRecord
	claimErr              error
	claimCalls            int
	claimOwnerCPID        string
	claimOrgID            string
	claimMaxOrgWorkers    int
	spawned               *configstore.WorkerRecord
	spawnErr              error
	spawnCalls            int
	spawnOwnerCPID        string
	spawnOrgID            string
	spawnOwnerEpoch       int64
	spawnPodNamePrefix    string
	spawnMaxOrgWorkers    int
	spawnMaxGlobalWorks   int
	neutralSpawned        *configstore.WorkerRecord
	neutralSpawnErr       error
	neutralSpawnCalls     int
	neutralSpawnOwnerCPID string
	neutralSpawnPodPrefix string
	neutralSpawnTarget    int
	neutralSpawnMaxGlobal  int
	hotIdleClaimResult     *configstore.WorkerRecord
	hotIdleClaimCPID       string
	hotIdleClaimOrgID      string
	takenOver             *configstore.WorkerRecord
	takeOverErr           error
	takeOverWorkerID      int
	takeOverOwnerCPID     string
	takeOverOrgID         string
	takeOverExpectedEpoch int64
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

func (s *captureRuntimeWorkerStore) ClaimIdleWorker(ownerCPInstanceID, orgID string, maxOrgWorkers int) (*configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++
	s.claimOwnerCPID = ownerCPInstanceID
	s.claimOrgID = orgID
	s.claimMaxOrgWorkers = maxOrgWorkers
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	if s.claimed == nil {
		return nil, nil
	}
	claimed := *s.claimed
	return &claimed, nil
}

func (s *captureRuntimeWorkerStore) ClaimHotIdleWorker(ownerCPInstanceID, orgID string) (*configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hotIdleClaimCPID = ownerCPInstanceID
	s.hotIdleClaimOrgID = orgID
	if s.hotIdleClaimResult != nil {
		r := *s.hotIdleClaimResult
		return &r, nil
	}
	return nil, nil
}

func (s *captureRuntimeWorkerStore) CreateSpawningWorkerSlot(ownerCPInstanceID, orgID string, ownerEpoch int64, podNamePrefix string, maxOrgWorkers, maxGlobalWorkers int) (*configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spawnCalls++
	s.spawnOwnerCPID = ownerCPInstanceID
	s.spawnOrgID = orgID
	s.spawnOwnerEpoch = ownerEpoch
	s.spawnPodNamePrefix = podNamePrefix
	s.spawnMaxOrgWorkers = maxOrgWorkers
	s.spawnMaxGlobalWorks = maxGlobalWorkers
	if s.spawnErr != nil {
		return nil, s.spawnErr
	}
	if s.spawned == nil {
		return nil, nil
	}
	spawned := *s.spawned
	return &spawned, nil
}

func (s *captureRuntimeWorkerStore) CreateNeutralWarmWorkerSlot(ownerCPInstanceID, podNamePrefix string, targetWarmWorkers, maxGlobalWorkers int) (*configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.neutralSpawnCalls++
	s.neutralSpawnOwnerCPID = ownerCPInstanceID
	s.neutralSpawnPodPrefix = podNamePrefix
	s.neutralSpawnTarget = targetWarmWorkers
	s.neutralSpawnMaxGlobal = maxGlobalWorkers
	if s.neutralSpawnErr != nil {
		return nil, s.neutralSpawnErr
	}
	if s.neutralSpawned == nil {
		return nil, nil
	}
	spawned := *s.neutralSpawned
	return &spawned, nil
}

func (s *captureRuntimeWorkerStore) GetWorkerRecord(workerID int) (*configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed != nil && s.claimed.WorkerID == workerID {
		record := *s.claimed
		return &record, nil
	}
	if s.spawned != nil && s.spawned.WorkerID == workerID {
		record := *s.spawned
		return &record, nil
	}
	if s.takenOver != nil && s.takenOver.WorkerID == workerID {
		record := *s.takenOver
		return &record, nil
	}
	return nil, nil
}

func (s *captureRuntimeWorkerStore) TakeOverWorker(workerID int, ownerCPInstanceID, orgID string, expectedOwnerEpoch int64) (*configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.takeOverWorkerID = workerID
	s.takeOverOwnerCPID = ownerCPInstanceID
	s.takeOverOrgID = orgID
	s.takeOverExpectedEpoch = expectedOwnerEpoch
	if s.takeOverErr != nil {
		return nil, s.takeOverErr
	}
	if s.takenOver == nil {
		return nil, nil
	}
	record := *s.takenOver
	return &record, nil
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
		workers:      make(map[int]*ManagedWorker),
		maxWorkers:   maxWorkers,
		idleTimeout:  5 * time.Minute,
		shutdownCh:   make(chan struct{}),
		stopInform:   make(chan struct{}),
		clientset:    cs,
		namespace:    "default",
		cpID:         "test-cp",
		cpInstanceID: "cp-uid-123:boot-abc",
		cpUID:        "cp-uid-123",
		workerImage:  "duckgres:test",
		workerPort:   8816,
		secretName:   "test-secret",
		spawnSem:     make(chan struct{}, 1),
	}

	return pool, cs
}

func TestK8sPool_EnsureWorkerRPCSecret_CreatesNew(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)

	secretName, err := pool.ensureWorkerRPCSecret(context.Background(), "duckgres-worker-test-cp-0")
	if err != nil {
		t.Fatalf("ensureWorkerRPCSecret failed: %v", err)
	}

	// Verify the secret exists
	secret, err := cs.CoreV1().Secrets("default").Get(context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("secret not found: %v", err)
	}
	token, ok := secret.Data["bearer-token"]
	if !ok || len(token) == 0 {
		t.Fatal("secret missing bearer-token key or empty")
	}
	if len(secret.Data["tls.crt"]) == 0 {
		t.Fatal("secret missing tls.crt key or empty")
	}
	if len(secret.Data["tls.key"]) == 0 {
		t.Fatal("secret missing tls.key key or empty")
	}
}

func TestK8sPool_EnsureWorkerRPCSecret_DefaultsToPerWorkerPrefix(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	pool.secretName = ""

	secretName, err := pool.ensureWorkerRPCSecret(context.Background(), "duckgres-worker-test-cp-0")
	if err != nil {
		t.Fatalf("ensureWorkerRPCSecret failed: %v", err)
	}
	if secretName != "duckgres-worker-token-duckgres-worker-test-cp-0" {
		t.Fatalf("unexpected worker RPC secret name: %q", secretName)
	}

	secret, err := cs.CoreV1().Secrets("default").Get(context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("worker RPC secret not found: %v", err)
	}
	if _, ok := secret.Data["bearer-token"]; !ok {
		t.Fatal("worker RPC secret missing bearer-token key")
	}
}

func TestK8sPool_EnsureWorkerRPCSecret_ExistingIsPreserved(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	secretName := "test-secret-duckgres-worker-test-cp-0"

	// Pre-create the secret
	_, err := cs.CoreV1().Secrets("default").Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: "default"},
		Data:       map[string][]byte{"bearer-token": []byte("existing-token")},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	gotSecretName, err := pool.ensureWorkerRPCSecret(context.Background(), "duckgres-worker-test-cp-0")
	if err != nil {
		t.Fatalf("ensureWorkerRPCSecret failed: %v", err)
	}
	if gotSecretName != secretName {
		t.Fatalf("expected secret name %q, got %q", secretName, gotSecretName)
	}

	// Verify the original token is preserved
	secret, err := cs.CoreV1().Secrets("default").Get(context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(secret.Data["bearer-token"]) != "existing-token" {
		t.Fatalf("token was modified: %s", secret.Data["bearer-token"])
	}
}

func TestK8sPool_EnsureWorkerRPCSecret_UsesDistinctCredentialsPerWorker(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	firstSecretName, err := pool.ensureWorkerRPCSecret(context.Background(), "duckgres-worker-test-cp-0")
	if err != nil {
		t.Fatalf("ensureWorkerRPCSecret first worker failed: %v", err)
	}
	secondSecretName, err := pool.ensureWorkerRPCSecret(context.Background(), "duckgres-worker-test-cp-1")
	if err != nil {
		t.Fatalf("ensureWorkerRPCSecret second worker failed: %v", err)
	}

	firstToken, firstCert, err := pool.readWorkerRPCSecurity(context.Background(), "duckgres-worker-test-cp-0")
	if err != nil {
		t.Fatalf("readWorkerRPCSecurity first worker failed: %v", err)
	}
	secondToken, secondCert, err := pool.readWorkerRPCSecurity(context.Background(), "duckgres-worker-test-cp-1")
	if err != nil {
		t.Fatalf("readWorkerRPCSecurity second worker failed: %v", err)
	}

	if firstSecretName == secondSecretName {
		t.Fatal("expected distinct worker RPC secret names")
	}
	if firstToken == secondToken {
		t.Fatal("expected distinct worker RPC bearer tokens")
	}
	if string(firstCert) == string(secondCert) {
		t.Fatal("expected distinct worker RPC certificates")
	}
}

func TestK8sPool_ReadWorkerRPCSecurity(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	secretName := "test-secret-duckgres-worker-test-cp-0"

	_, err := cs.CoreV1().Secrets("default").Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: "default"},
		Data: map[string][]byte{
			"bearer-token": []byte("my-token-123"),
			"tls.crt":      []byte("my-cert"),
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	token, certPEM, err := pool.readWorkerRPCSecurity(context.Background(), "duckgres-worker-test-cp-0")
	if err != nil {
		t.Fatalf("readWorkerRPCSecurity failed: %v", err)
	}
	if token != "my-token-123" {
		t.Fatalf("unexpected token: %s", token)
	}
	if string(certPEM) != "my-cert" {
		t.Fatalf("unexpected cert: %s", certPEM)
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
			OrgID: "analytics",
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
			OrgID: "analytics",
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

func TestK8sPoolReserveClaimedWorkerUnlocksPoolOnTransitionError(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	worker := &ManagedWorker{ID: 9, done: make(chan struct{})}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleHot,
		Assignment: &WorkerAssignment{
			OrgID: "analytics",
		},
	}); err != nil {
		t.Fatalf("SetSharedState: %v", err)
	}
	pool.workers[worker.ID] = worker

	_, err := pool.reserveClaimedWorker(context.Background(), &configstore.WorkerRecord{
		WorkerID:          worker.ID,
		OwnerCPInstanceID: "cp-2:boot-b",
		OwnerEpoch:        7,
		State:             configstore.WorkerStateReserved,
	}, &WorkerAssignment{
		OrgID: "billing",
	})
	if err == nil {
		t.Fatal("expected transition error")
	}

	locked := make(chan struct{})
	go func() {
		pool.mu.Lock()
		pool.mu.Unlock()
		close(locked)
	}()

	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("expected reserveClaimedWorker to unlock pool mutex on error")
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
		OrgID: "analytics",
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
				OrgID: "analytics",
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
			OrgID: "analytics",
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

func TestK8sPoolReserveSharedWorkerReservesIdleWorkerWithoutLocalReplenishmentInRuntimeMode(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	worker, err := pool.ReserveSharedWorker(ctx, &WorkerAssignment{
		OrgID: "analytics",
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

	select {
	case id := <-replacementSpawned:
		t.Fatalf("did not expect local warm-pool replenishment in runtime mode, got background spawn %d", id)
	default:
	}
}

func TestK8sPoolReserveSharedWorkerClaimsRuntimeWorkerAndAdoptsPod(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	pool.minWorkers = 0
	store := &captureRuntimeWorkerStore{
		claimed: &configstore.WorkerRecord{
			WorkerID:          21,
			PodName:           "duckgres-worker-other-cp-21",
			State:             configstore.WorkerStateReserved,
			OrgID:             "analytics",
			OwnerCPInstanceID: pool.cpInstanceID,
			OwnerEpoch:        3,
		},
	}
	pool.runtimeStore = store

	_, err := cs.CoreV1().Pods("default").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "duckgres-worker-other-cp-21",
			Namespace: "default",
			Labels: map[string]string{
				"duckgres/control-plane": "other-cp",
				"duckgres/worker-id":     "21",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.21",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create adopted worker pod: %v", err)
	}

	var connectedPodName string
	var connectedPodIP string
	pool.connectWorkerFunc = func(ctx context.Context, podName, podIP, bearerToken string) (*flightsql.Client, error) {
		connectedPodName = podName
		connectedPodIP = podIP
		return nil, nil
	}
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		if worker == nil {
			t.Fatal("expected claimed worker for liveness check")
		}
		if worker.ID != 21 {
			t.Fatalf("expected claimed worker id 21, got %d", worker.ID)
		}
		return nil
	}
	_, err = cs.CoreV1().Secrets("default").Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "test-secret-duckgres-worker-other-cp-21", Namespace: "default"},
		Data: map[string][]byte{
			"bearer-token": []byte("worker-21-token"),
			"tls.crt":      []byte("worker-21-cert"),
			"tls.key":      []byte("worker-21-key"),
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create adopted worker RPC secret: %v", err)
	}

	worker, err := pool.ReserveSharedWorker(context.Background(), &WorkerAssignment{
		OrgID: "analytics",
	})
	if err != nil {
		t.Fatalf("ReserveSharedWorker: %v", err)
	}
	if worker.ID != 21 {
		t.Fatalf("expected claimed worker 21, got %d", worker.ID)
	}
	if worker.PodName() != "duckgres-worker-other-cp-21" {
		t.Fatalf("expected tracked pod name duckgres-worker-other-cp-21, got %q", worker.PodName())
	}
	if worker.OwnerEpoch() != 3 {
		t.Fatalf("expected claimed owner epoch 3, got %d", worker.OwnerEpoch())
	}
	if worker.OwnerCPInstanceID() != pool.cpInstanceID {
		t.Fatalf("expected owner cp instance id %q, got %q", pool.cpInstanceID, worker.OwnerCPInstanceID())
	}
	if connectedPodName != "duckgres-worker-other-cp-21" || connectedPodIP != "10.0.0.21" {
		t.Fatalf("expected connection to claimed pod, got name=%q ip=%q", connectedPodName, connectedPodIP)
	}
	if store.claimCalls != 1 {
		t.Fatalf("expected one claim call, got %d", store.claimCalls)
	}
	if store.claimOwnerCPID != pool.cpInstanceID {
		t.Fatalf("expected claim owner cp instance id %q, got %q", pool.cpInstanceID, store.claimOwnerCPID)
	}
	if store.claimOrgID != "analytics" {
		t.Fatalf("expected claim org analytics, got %q", store.claimOrgID)
	}
	if store.claimMaxOrgWorkers != 0 {
		t.Fatalf("expected default max org workers 0, got %d", store.claimMaxOrgWorkers)
	}

	state := worker.SharedState()
	if state.Lifecycle != WorkerLifecycleReserved {
		t.Fatalf("expected reserved lifecycle, got %q", state.Lifecycle)
	}
	if state.Assignment == nil || state.Assignment.OrgID != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
}

func TestK8sPoolReserveSharedWorkerFallsBackWhenRuntimeClaimReturnsNil(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{}
	pool.runtimeStore = store
	idle := &ManagedWorker{ID: 8, done: make(chan struct{})}
	pool.workers[idle.ID] = idle
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return nil
	}

	worker, err := pool.ReserveSharedWorker(context.Background(), &WorkerAssignment{
		OrgID: "analytics",
	})
	if err != nil {
		t.Fatalf("ReserveSharedWorker: %v", err)
	}
	if worker.ID != idle.ID {
		t.Fatalf("expected fallback idle worker %d, got %d", idle.ID, worker.ID)
	}
	if store.claimCalls != 1 {
		t.Fatalf("expected one claim attempt before fallback, got %d", store.claimCalls)
	}
}

func TestK8sPoolReserveSharedWorkerPassesOrgCapToRuntimeClaim(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{}
	pool.runtimeStore = store
	idle := &ManagedWorker{ID: 12, done: make(chan struct{})}
	pool.workers[idle.ID] = idle
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error { return nil }

	_, err := pool.ReserveSharedWorker(context.Background(), &WorkerAssignment{
		OrgID:      "analytics",
		MaxWorkers: 3,
	})
	if err != nil {
		t.Fatalf("ReserveSharedWorker: %v", err)
	}
	if store.claimMaxOrgWorkers != 3 {
		t.Fatalf("expected claim max org workers 3, got %d", store.claimMaxOrgWorkers)
	}
}

func TestK8sPoolClaimSpecificWorkerTakesOverRuntimeWorker(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{
		takenOver: &configstore.WorkerRecord{
			WorkerID:          44,
			PodName:           "duckgres-worker-test-cp-44",
			State:             configstore.WorkerStateReserved,
			OrgID:             "analytics",
			OwnerCPInstanceID: pool.cpInstanceID,
			OwnerEpoch:        8,
		},
	}
	pool.runtimeStore = store
	worker := &ManagedWorker{ID: 44, done: make(chan struct{})}
	pool.workers[worker.ID] = worker
	livenessChecked := false
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		livenessChecked = true
		return nil
	}

	claimed, err := pool.claimSpecificWorker(context.Background(), 44, 7, &WorkerAssignment{
		OrgID:      "analytics",
		MaxWorkers: 3,
	})
	if err != nil {
		t.Fatalf("claimSpecificWorker: %v", err)
	}
	if claimed.ID != 44 {
		t.Fatalf("expected claimed worker 44, got %d", claimed.ID)
	}
	if store.takeOverWorkerID != 44 {
		t.Fatalf("expected takeover worker id 44, got %d", store.takeOverWorkerID)
	}
	if store.takeOverOwnerCPID != pool.cpInstanceID {
		t.Fatalf("expected takeover owner cp id %q, got %q", pool.cpInstanceID, store.takeOverOwnerCPID)
	}
	if store.takeOverOrgID != "analytics" {
		t.Fatalf("expected takeover org analytics, got %q", store.takeOverOrgID)
	}
	if store.takeOverExpectedEpoch != 7 {
		t.Fatalf("expected takeover expected epoch 7, got %d", store.takeOverExpectedEpoch)
	}
	if claimed.OwnerEpoch() != 8 {
		t.Fatalf("expected owner epoch 8, got %d", claimed.OwnerEpoch())
	}
	state := claimed.SharedState()
	if state.Lifecycle != WorkerLifecycleReserved {
		t.Fatalf("expected reserved lifecycle, got %q", state.Lifecycle)
	}
	if state.Assignment == nil || state.Assignment.OrgID != "analytics" {
		t.Fatalf("expected analytics assignment, got %#v", state.Assignment)
	}
	if !livenessChecked {
		t.Fatal("expected claimSpecificWorker to recheck worker liveness")
	}
}

func TestK8sPoolClaimSpecificWorkerReturnsEpochMismatchError(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{
		takeOverErr: configstore.ErrWorkerOwnerEpochMismatch,
	}
	pool.runtimeStore = store

	claimed, err := pool.claimSpecificWorker(context.Background(), 44, 7, &WorkerAssignment{
		OrgID:      "analytics",
		MaxWorkers: 3,
	})
	if err == nil {
		t.Fatal("expected stale takeover to return an error")
	}
	if !errors.Is(err, configstore.ErrWorkerOwnerEpochMismatch) {
		t.Fatalf("expected ErrWorkerOwnerEpochMismatch, got %v", err)
	}
	if claimed != nil {
		t.Fatalf("expected no claimed worker, got %#v", claimed)
	}
}

func TestK8sPoolClaimSpecificWorkerRetiresUnhealthyWorker(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{
		takenOver: &configstore.WorkerRecord{
			WorkerID:          44,
			PodName:           "duckgres-worker-test-cp-44",
			State:             configstore.WorkerStateReserved,
			OrgID:             "analytics",
			OwnerCPInstanceID: pool.cpInstanceID,
			OwnerEpoch:        8,
		},
	}
	pool.runtimeStore = store
	pool.workers[44] = &ManagedWorker{ID: 44, done: make(chan struct{})}
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		return errors.New("dead worker")
	}

	claimed, err := pool.claimSpecificWorker(context.Background(), 44, 7, &WorkerAssignment{
		OrgID:      "analytics",
		MaxWorkers: 3,
	})
	if err == nil {
		t.Fatal("expected unhealthy claimed worker to fail liveness recheck")
	}
	if claimed != nil {
		t.Fatalf("expected no claimed worker, got %#v", claimed)
	}
	if _, ok := pool.Worker(44); ok {
		t.Fatal("expected unhealthy worker to be retired from the pool")
	}
}

func TestK8sPoolReserveSharedWorkerCreatesRuntimeSpawningSlotWhenPoolIsCold(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{
		spawned: &configstore.WorkerRecord{
			WorkerID:          31,
			PodName:           "duckgres-worker-test-cp-31",
			State:             configstore.WorkerStateSpawning,
			OrgID:             "analytics",
			OwnerCPInstanceID: pool.cpInstanceID,
			OwnerEpoch:        1,
		},
	}
	pool.runtimeStore = store
	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		worker := &ManagedWorker{ID: id, podName: "duckgres-worker-test-cp-31", done: make(chan struct{})}
		pool.workers[id] = worker
		return nil
	}
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error {
		if worker == nil || worker.ID != 31 {
			t.Fatalf("expected spawned runtime worker 31, got %#v", worker)
		}
		return nil
	}

	worker, err := pool.ReserveSharedWorker(context.Background(), &WorkerAssignment{
		OrgID:      "analytics",
		MaxWorkers: 2,
	})
	if err != nil {
		t.Fatalf("ReserveSharedWorker: %v", err)
	}
	if worker.ID != 31 {
		t.Fatalf("expected spawned worker 31, got %d", worker.ID)
	}
	if store.spawnCalls != 1 {
		t.Fatalf("expected one spawning slot allocation, got %d", store.spawnCalls)
	}
	if store.spawnOwnerCPID != pool.cpInstanceID {
		t.Fatalf("expected spawn owner cp-instance %q, got %q", pool.cpInstanceID, store.spawnOwnerCPID)
	}
	if store.spawnOrgID != "analytics" {
		t.Fatalf("expected spawn org analytics, got %q", store.spawnOrgID)
	}
	if store.spawnOwnerEpoch != 1 {
		t.Fatalf("expected spawn owner epoch 1, got %d", store.spawnOwnerEpoch)
	}
	if store.spawnPodNamePrefix != "duckgres-worker-test-cp" {
		t.Fatalf("expected pod name prefix duckgres-worker-test-cp, got %q", store.spawnPodNamePrefix)
	}
	if store.spawnMaxOrgWorkers != 2 {
		t.Fatalf("expected max org workers 2, got %d", store.spawnMaxOrgWorkers)
	}
	if store.spawnMaxGlobalWorks != 5 {
		t.Fatalf("expected max global workers 5, got %d", store.spawnMaxGlobalWorks)
	}
	if worker.OwnerEpoch() != 1 {
		t.Fatalf("expected owner epoch 1, got %d", worker.OwnerEpoch())
	}
	if worker.SharedState().Lifecycle != WorkerLifecycleReserved {
		t.Fatalf("expected reserved lifecycle, got %q", worker.SharedState().Lifecycle)
	}
}

func TestK8sPoolSpawnWarmWorkerAllocatesRuntimeSlotWhenIDZero(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{
		neutralSpawned: &configstore.WorkerRecord{
			WorkerID:          41,
			PodName:           "duckgres-worker-test-cp-41",
			State:             configstore.WorkerStateSpawning,
			OwnerCPInstanceID: pool.cpInstanceID,
		},
	}
	pool.runtimeStore = store

	var spawnedID int
	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		spawnedID = id
		return nil
	}

	if err := pool.spawnWarmWorker(context.Background(), 0); err != nil {
		t.Fatalf("spawnWarmWorker: %v", err)
	}
	if spawnedID != 41 {
		t.Fatalf("expected runtime-allocated worker id 41, got %d", spawnedID)
	}
	if store.neutralSpawnCalls != 1 {
		t.Fatalf("expected one runtime neutral spawn slot allocation, got %d", store.neutralSpawnCalls)
	}
	if store.neutralSpawnPodPrefix != "duckgres-worker-test-cp" {
		t.Fatalf("expected pod name prefix duckgres-worker-test-cp, got %q", store.neutralSpawnPodPrefix)
	}
}

func TestK8sPoolSpawnMinWorkersUsesRuntimeSlots(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{}
	pool.runtimeStore = store

	var mu sync.Mutex
	var slots int
	store.neutralSpawned = &configstore.WorkerRecord{
		WorkerID:          51,
		PodName:           "duckgres-worker-test-cp-51",
		State:             configstore.WorkerStateSpawning,
		OwnerCPInstanceID: pool.cpInstanceID,
	}
	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		mu.Lock()
		defer mu.Unlock()
		slots++
		if slots == 1 {
			if id != 51 {
				t.Fatalf("expected first runtime worker id 51, got %d", id)
			}
			store.neutralSpawned = &configstore.WorkerRecord{
				WorkerID:          52,
				PodName:           "duckgres-worker-test-cp-52",
				State:             configstore.WorkerStateSpawning,
				OwnerCPInstanceID: pool.cpInstanceID,
			}
			return nil
		}
		if id != 52 {
			t.Fatalf("expected second runtime worker id 52, got %d", id)
		}
		return nil
	}

	if err := pool.SpawnMinWorkers(2); err != nil {
		t.Fatalf("SpawnMinWorkers: %v", err)
	}
	if store.neutralSpawnCalls != 2 {
		t.Fatalf("expected two runtime neutral spawn slot allocations, got %d", store.neutralSpawnCalls)
	}
	if store.neutralSpawnTarget != 2 {
		t.Fatalf("expected neutral warm target 2, got %d", store.neutralSpawnTarget)
	}
}

func TestK8sPoolActivateReservedWorkerPersistsActivatingThenHotWorkerRecord(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{}
	pool.runtimeStore = store
	worker := &ManagedWorker{ID: 9, done: make(chan struct{}), ownerEpoch: 4}
	worker.SetOwnerCPInstanceID(pool.cpInstanceID)
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			OrgID: "analytics",
		},
	}); err != nil {
		t.Fatalf("SetSharedState: %v", err)
	}
	pool.workers[worker.ID] = worker
	pool.activateTenantFunc = func(ctx context.Context, got *ManagedWorker, payload TenantActivationPayload) error {
		return nil
	}

	if err := pool.ActivateReservedWorker(context.Background(), worker, TenantActivationPayload{
		OrgID: "analytics",
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
	worker.SetOwnerCPInstanceID(pool.cpInstanceID)
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleHot,
		Assignment: &WorkerAssignment{
			OrgID: "analytics",
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
		OrgID: "billing",
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
			OrgID: "analytics",
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

	_, err := cs.CoreV1().ConfigMaps("default").Create(context.Background(), &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "my-config", Namespace: "default"},
		Data: map[string]string{
			"duckgres.yaml": "data_dir: /data\nextensions:\n  - ducklake\n",
		},
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
	if pod.Labels["duckgres/cp-instance-id"] != "cp-uid-123-boot-abc" {
		t.Fatalf("expected cp-instance-id label cp-uid-123-boot-abc, got %s", pod.Labels["duckgres/cp-instance-id"])
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
	if pod.Spec.ServiceAccountName != "duckgres-worker" {
		t.Fatalf("expected neutral worker service account duckgres-worker, got %q", pod.Spec.ServiceAccountName)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("expected automountServiceAccountToken=false for shared warm worker pods")
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
	foundTLSCertEnv := false
	foundTLSKeyEnv := false
	for _, env := range c.Env {
		if env.Name == "DUCKGRES_DUCKDB_TOKEN" && env.ValueFrom != nil &&
			env.ValueFrom.SecretKeyRef != nil &&
			env.ValueFrom.SecretKeyRef.Name == "test-secret-duckgres-worker-test-cp-0" {
			foundEnv = true
		}
		if env.Name == "DUCKGRES_SHARED_WARM_WORKER" && env.Value == "true" {
			foundSharedWarmWorkerEnv = true
		}
		if env.Name == "DUCKGRES_CERT" && env.Value == "/etc/duckgres/worker-rpc/tls.crt" {
			foundTLSCertEnv = true
		}
		if env.Name == "DUCKGRES_KEY" && env.Value == "/etc/duckgres/worker-rpc/tls.key" {
			foundTLSKeyEnv = true
		}
	}
	if !foundEnv {
		t.Fatal("bearer token env var not found or incorrect")
	}
	if !foundSharedWarmWorkerEnv {
		t.Fatal("expected shared warm worker startup env to be present")
	}
	if !foundTLSCertEnv || !foundTLSKeyEnv {
		t.Fatal("expected worker RPC TLS env vars to be present")
	}

	if len(pod.Spec.Volumes) == 0 {
		t.Fatal("expected configmap volume")
	}
	foundWorkerRPCSecret := false
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == "worker-rpc-tls" && volume.Secret != nil &&
			volume.Secret.SecretName == "test-secret-duckgres-worker-test-cp-0" {
			foundWorkerRPCSecret = true
		}
	}
	if !foundWorkerRPCSecret {
		t.Fatal("expected worker RPC volume to reference per-worker secret")
	}
}

func TestK8sPool_RetireWorkerDeletesWorkerRPCSecret(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	worker := &ManagedWorker{ID: 1, podName: "duckgres-worker-test-cp-1", done: make(chan struct{})}
	pool.workers[1] = worker

	_, err := cs.CoreV1().Secrets("default").Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "test-secret-duckgres-worker-test-cp-1", Namespace: "default"},
		Data:       map[string][]byte{"bearer-token": []byte("test-token"), "tls.crt": []byte("test-cert"), "tls.key": []byte("test-key")},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pool.RetireWorker(1)

	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := cs.CoreV1().Secrets("default").Get(context.Background(), "test-secret-duckgres-worker-test-cp-1", metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			break
		}
		if err != nil {
			t.Fatalf("get worker rpc secret: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("expected worker RPC secret to be deleted on retire")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestValidateSharedStartupConfigRejectsTenantRuntimeFields(t *testing.T) {
	err := validateSharedStartupConfig([]byte(`
data_dir: /data
users:
  postgres: postgres
ducklake:
  object_store: s3://tenant-a/private/
`))
	if err == nil {
		t.Fatal("expected tenant runtime fields to be rejected in shared startup config")
	}
}

func TestControlPlaneIDLabelValue_StaysKubernetesSafe(t *testing.T) {
	t.Parallel()

	label := controlPlaneIDLabelValue("duckgres-control-plane-7fb9dd69c6-dcgzw:14cd8dd9eb353e609c7a4387a594a418")
	if len(label) > 63 {
		t.Fatalf("expected label length <= 63, got %d (%q)", len(label), label)
	}
	matched, err := regexp.MatchString(`^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$`, label)
	if err != nil {
		t.Fatalf("failed to compile label regex: %v", err)
	}
	if !matched {
		t.Fatalf("expected Kubernetes-safe label, got %q", label)
	}
	if label == "duckgres-control-plane-7fb9dd69c6-dcgzw:14cd8dd9eb353e609c7a4387a594a418" {
		t.Fatalf("expected sanitized label, got original %q", label)
	}
}

func TestValidateSharedWorkerConfigRejectsTenantRuntimeFields(t *testing.T) {
	err := validateSharedWorkerConfig([]byte(`
data_dir: /data
extensions:
  - ducklake
ducklake:
  object_store: s3://tenant-a/private/
`))
	if err == nil {
		t.Fatal("expected tenant runtime fields to be rejected")
	}
}

func TestK8sPool_ShutdownAll(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)

	// Add some workers
	for i := 0; i < 3; i++ {
		done := make(chan struct{})
		pool.workers[i] = &ManagedWorker{ID: i, podName: "duckgres-worker-test-cp-" + strconv.Itoa(i), done: done}

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

func TestK8sPoolRetireWorkerUsesTrackedPodName(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)

	done := make(chan struct{})
	worker := &ManagedWorker{
		ID:      11,
		podName: "duckgres-worker-other-cp-11",
		done:    done,
	}
	pool.workers[worker.ID] = worker

	var deletedPodName string
	cs.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleteAction, ok := action.(k8stesting.DeleteAction)
		if !ok {
			return false, nil, nil
		}
		deletedPodName = deleteAction.GetName()
		return false, nil, nil
	})

	pool.RetireWorker(worker.ID)
	time.Sleep(100 * time.Millisecond)

	if deletedPodName != "duckgres-worker-other-cp-11" {
		t.Fatalf("expected retire to delete tracked pod name duckgres-worker-other-cp-11, got %q", deletedPodName)
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
