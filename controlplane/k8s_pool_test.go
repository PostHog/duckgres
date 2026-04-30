//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
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
	mu                               sync.Mutex
	records                          []configstore.WorkerRecord
	claimed                          *configstore.WorkerRecord
	claimErr                         error
	claimCalls                       int
	claimOwnerCPID                   string
	claimOrgID                       string
	claimImage                       string
	claimMaxOrgWorkers               int
	spawned                          *configstore.WorkerRecord
	spawnErr                         error
	spawnCalls                       int
	spawnOwnerCPID                   string
	spawnOrgID                       string
	spawnImage                       string
	spawnOwnerEpoch                  int64
	spawnPodNamePrefix               string
	spawnMaxOrgWorkers               int
	spawnMaxGlobalWorks              int
	neutralSpawned                   *configstore.WorkerRecord
	neutralSpawnedFunc               func() *configstore.WorkerRecord
	neutralSpawnErr                  error
	neutralSpawnCalls                int
	neutralSpawnOwnerCPID            string
	neutralSpawnPodPrefix            string
	neutralSpawnImage                string
	neutralSpawnTarget               int
	neutralSpawnMaxGlobal            int
	hotIdleClaimResult               *configstore.WorkerRecord
	hotIdleClaimCPID                 string
	hotIdleClaimOrgID                string
	takenOver                        *configstore.WorkerRecord
	takeOverErr                      error
	takeOverWorkerID                 int
	takeOverOwnerCPID                string
	takeOverOrgID                    string
	takeOverExpectedEpoch            int64
	retireIdleCalls                  int
	retireIdleCalledIDs              []int
	retireIdleCalledReasons          []string
	retireIdleErr                    error
	retireIdleMisses                 map[int]bool
	retireIdleOrHotIdleCalls         int
	retireIdleOrHotIdleCalledIDs     []int
	retireIdleOrHotIdleCalledReasons []string
	retireIdleOrHotIdleErr           error
	retireIdleOrHotIdleMisses        map[int]bool
	retireOrphanCalls                int
	retireOrphanCalledIDs            []int
	retireOrphanCalledReasons        []string
	retireOrphanErr                  error
	preloadedRecords                 map[int]*configstore.WorkerRecord
	getRecordErrIDs                  map[int]error
	markDrainingCalls                int
	markDrainingCalledIDs            []int
	markDrainingCalledCPs            []string
	markDrainingMisses               map[int]bool
	markDrainingErr                  error
	retireDrainingCalls              int
	retireDrainingCalledIDs          []int
	retireDrainingReasons            []string
	retireDrainingMisses             map[int]bool
	retireDrainingErr                error
	// events records a unified, ordered timeline of state transitions on
	// this store so tests can assert happens-before relationships (e.g.
	// that pod-delete occurs between markDraining and retireDraining).
	events []string
}

func (s *captureRuntimeWorkerStore) recordEvent(evt string) {
	s.events = append(s.events, evt)
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

func (s *captureRuntimeWorkerStore) ClaimIdleWorker(ownerCPInstanceID, orgID, image string, maxOrgWorkers int) (*configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++
	s.claimOwnerCPID = ownerCPInstanceID
	s.claimOrgID = orgID
	s.claimImage = image
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

func (s *captureRuntimeWorkerStore) CreateSpawningWorkerSlot(ownerCPInstanceID, orgID, image string, ownerEpoch int64, podNamePrefix string, maxOrgWorkers, maxGlobalWorkers int) (*configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spawnCalls++
	s.spawnOwnerCPID = ownerCPInstanceID
	s.spawnOrgID = orgID
	s.spawnImage = image
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
	spawned.OwnerEpoch = ownerEpoch
	return &spawned, nil
}

func (s *captureRuntimeWorkerStore) CreateNeutralWarmWorkerSlot(ownerCPInstanceID, podNamePrefix, image string, targetWarmWorkers, maxGlobalWorkers int) (*configstore.WorkerRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.neutralSpawnCalls++
	s.neutralSpawnOwnerCPID = ownerCPInstanceID
	s.neutralSpawnPodPrefix = podNamePrefix
	s.neutralSpawnImage = image
	s.neutralSpawnTarget = targetWarmWorkers
	s.neutralSpawnMaxGlobal = maxGlobalWorkers
	if s.neutralSpawnErr != nil {
		return nil, s.neutralSpawnErr
	}
	if s.neutralSpawnedFunc != nil {
		rec := s.neutralSpawnedFunc()
		if rec == nil {
			return nil, nil
		}
		copy := *rec
		return &copy, nil
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
	if err, ok := s.getRecordErrIDs[workerID]; ok {
		return nil, err
	}
	if rec, ok := s.preloadedRecords[workerID]; ok {
		if rec == nil {
			return nil, nil
		}
		record := *rec
		return &record, nil
	}
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

func (s *captureRuntimeWorkerStore) RetireIdleWorker(workerID int, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retireIdleCalls++
	s.retireIdleCalledIDs = append(s.retireIdleCalledIDs, workerID)
	s.retireIdleCalledReasons = append(s.retireIdleCalledReasons, reason)
	if s.retireIdleErr != nil {
		return false, s.retireIdleErr
	}
	if s.retireIdleMisses[workerID] {
		return false, nil
	}
	return true, nil
}

func (s *captureRuntimeWorkerStore) RetireIdleOrHotIdleWorker(workerID int, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retireIdleOrHotIdleCalls++
	s.retireIdleOrHotIdleCalledIDs = append(s.retireIdleOrHotIdleCalledIDs, workerID)
	s.retireIdleOrHotIdleCalledReasons = append(s.retireIdleOrHotIdleCalledReasons, reason)
	if s.retireIdleOrHotIdleErr != nil {
		return false, s.retireIdleOrHotIdleErr
	}
	if s.retireIdleOrHotIdleMisses[workerID] {
		return false, nil
	}
	return true, nil
}

func (s *captureRuntimeWorkerStore) RetireOrphanWorker(workerID int, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retireOrphanCalls++
	s.retireOrphanCalledIDs = append(s.retireOrphanCalledIDs, workerID)
	s.retireOrphanCalledReasons = append(s.retireOrphanCalledReasons, reason)
	if s.retireOrphanErr != nil {
		return false, s.retireOrphanErr
	}
	return true, nil
}

func (s *captureRuntimeWorkerStore) MarkWorkerDraining(workerID int, ownerCPInstanceID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markDrainingCalls++
	s.markDrainingCalledIDs = append(s.markDrainingCalledIDs, workerID)
	s.markDrainingCalledCPs = append(s.markDrainingCalledCPs, ownerCPInstanceID)
	s.recordEvent(fmt.Sprintf("draining:%d", workerID))
	if s.markDrainingErr != nil {
		return false, s.markDrainingErr
	}
	if s.markDrainingMisses[workerID] {
		return false, nil
	}
	return true, nil
}

func (s *captureRuntimeWorkerStore) RetireDrainingWorker(workerID int, reason string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retireDrainingCalls++
	s.retireDrainingCalledIDs = append(s.retireDrainingCalledIDs, workerID)
	s.retireDrainingReasons = append(s.retireDrainingReasons, reason)
	s.recordEvent(fmt.Sprintf("retired:%d", workerID))
	if s.retireDrainingErr != nil {
		return false, s.retireDrainingErr
	}
	if s.retireDrainingMisses[workerID] {
		return false, nil
	}
	return true, nil
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
		workers:       make(map[int]*ManagedWorker),
		maxWorkers:    maxWorkers,
		idleTimeout:   5 * time.Minute,
		shutdownCh:    make(chan struct{}),
		stopInform:    make(chan struct{}),
		clientset:     cs,
		namespace:     "default",
		cpID:          "test-cp",
		cpInstanceID:  "cp-uid-123:boot-abc",
		cpUID:         "cp-uid-123",
		workerImage:   "duckgres:test",
		workerPort:    8816,
		secretName:    "test-secret",
		spawnSem:      make(chan struct{}, 1),
		retireSem:     make(chan struct{}, 5),
		nodeFirstSeen: make(map[string]time.Time),
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
		return
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
		return
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
			return nil
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
		return
	}
	if claimed != nil {
		t.Fatalf("expected no claimed worker, got %#v", claimed)
	}
	if _, ok := pool.Worker(44); ok {
		t.Fatal("expected unhealthy worker to be retired from the pool")
	}
}

func TestK8sPoolHotIdleMismatchedImageCorrectlyHandled(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	// Setup a hot-idle worker with "v1" image
	w := &ManagedWorker{ID: 7, done: make(chan struct{})}
	if err := w.SetSharedState(SharedWorkerState{
		Lifecycle:  WorkerLifecycleHot,
		Assignment: &WorkerAssignment{OrgID: "analytics", Image: "duckgres:v1"},
	}); err != nil {
		t.Fatalf("SetSharedState: %v", err)
	}
	pool.workers[7] = w

	store := &captureRuntimeWorkerStore{
		hotIdleClaimResult: &configstore.WorkerRecord{
			WorkerID: 7,
			Image:    "duckgres:v1",
		},
		spawned: &configstore.WorkerRecord{
			WorkerID: 42,
			PodName:  "test-cp-worker-42",
			Image:    "duckgres:v2",
		},
	}
	pool.runtimeStore = store

	// Org requests "v2" image
	assignment := &WorkerAssignment{
		OrgID: "analytics",
		Image: "duckgres:v2",
	}

	// Mock spawnWarmWorker since we don't have real pods
	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error { return nil }
	// Mock health check since we don't have real worker binaries
	pool.healthCheckFunc = func(ctx context.Context, worker *ManagedWorker) error { return nil }
	// Mock connection since we don't have real pods
	pool.connectWorkerFunc = func(ctx context.Context, podName, podIP, bearerToken string) (*flightsql.Client, error) {
		return nil, nil
	}

	// Provide a mock secret for the spawned pod (worker 42)
	podName := "test-cp-worker-42"
	secretName := pool.workerRPCSecretName(podName)

	// Minimal valid PEM blocks for a self-signed cert/key to satisfy parsing.
	certPEM := []byte("-----BEGIN CERTIFICATE-----\nMIICojCCAYqgAwIBAgIQI6v5m9mN6L3Xv8O5/0u/2zANBgkqhkiG9w0BAQsFADAV\nMRMwEQYDVQQDEwpkdWNra2dyZXMwHhcNMjYwNDI3MDEwODIyWhcNMjYwNTI3MDEw\nODIyWjAVMRMwEQYDVQQDEwpkdWNra2dyZXMwggEiMA0GCSqGSIb3DQEBAQUAA4IB\nDwAwggEKAoIBAQC8u9+9\n-----END CERTIFICATE-----\n")
	keyPEM := []byte("-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEAvLvf\n-----END RSA PRIVATE KEY-----\n")

	_, _ = pool.clientset.CoreV1().Secrets(pool.namespace).Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName},
		Data: map[string][]byte{
			"bearer-token":      []byte("secret"),
			"tls.crt":           certPEM,
			"tls.key":           keyPEM,
			"worker-rpc-ca.crt": certPEM,
		},
	}, metav1.CreateOptions{})

	// Provide mock pod
	_, _ = pool.clientset.CoreV1().Pods(pool.namespace).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   podName,
			Labels: map[string]string{"duckgres/worker-id": "42"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.42",
		},
	}, metav1.CreateOptions{})

	got, err := pool.ReserveSharedWorker(context.Background(), assignment)
	if err != nil {
		t.Fatalf("ReserveSharedWorker: %v", err)
	}

	if got.ID != 42 {
		t.Fatalf("expected to spawn new worker 42, got %d (v1 worker was incorrectly reclaimed)", got.ID)
	}

	// Verify v1 worker was retired
	if store.retireIdleOrHotIdleCalls != 1 || store.retireIdleOrHotIdleCalledIDs[0] != 7 {
		t.Fatalf("expected mismatched hot-idle worker 7 to be retired, got calls=%d ids=%v", store.retireIdleOrHotIdleCalls, store.retireIdleOrHotIdleCalledIDs)
	}
	if store.retireIdleOrHotIdleCalledReasons[0] != RetireReasonMismatchedVersion {
		t.Fatalf("expected reason %q, got %q", RetireReasonMismatchedVersion, store.retireIdleOrHotIdleCalledReasons[0])
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
	if store.spawnPodNamePrefix != "test-cp-worker" {
		t.Fatalf("expected pod name prefix test-cp-worker, got %q", store.spawnPodNamePrefix)
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

	if err := pool.spawnWarmWorker(context.Background(), 0, pool.workerImage); err != nil {
		t.Fatalf("spawnWarmWorker: %v", err)
	}
	if spawnedID != 41 {
		t.Fatalf("expected runtime-allocated worker id 41, got %d", spawnedID)
	}
	if store.neutralSpawnCalls != 1 {
		t.Fatalf("expected one runtime neutral spawn slot allocation, got %d", store.neutralSpawnCalls)
	}
	if store.neutralSpawnPodPrefix != "test-cp-worker" {
		t.Fatalf("expected pod name prefix test-cp-worker, got %q", store.neutralSpawnPodPrefix)
	}
}

func TestK8sPoolSpawnMinWorkersUsesRuntimeSlots(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	store := &captureRuntimeWorkerStore{}
	pool.runtimeStore = store

	// Return different records on successive CreateNeutralWarmWorkerSlot calls.
	neutralRecords := []*configstore.WorkerRecord{
		{
			WorkerID:          51,
			PodName:           "duckgres-worker-test-cp-51",
			State:             configstore.WorkerStateSpawning,
			OwnerCPInstanceID: pool.cpInstanceID,
		},
		{
			WorkerID:          52,
			PodName:           "duckgres-worker-test-cp-52",
			State:             configstore.WorkerStateSpawning,
			OwnerCPInstanceID: pool.cpInstanceID,
		},
	}
	var neutralIdx int
	store.neutralSpawnedFunc = func() *configstore.WorkerRecord {
		idx := neutralIdx
		neutralIdx++
		if idx < len(neutralRecords) {
			return neutralRecords[idx]
		}
		return nil
	}

	var mu sync.Mutex
	spawnedIDs := map[int]bool{}
	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		mu.Lock()
		defer mu.Unlock()
		spawnedIDs[id] = true
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
	if !spawnedIDs[51] || !spawnedIDs[52] {
		t.Fatalf("expected worker ids 51 and 52 to be spawned, got %v", spawnedIDs)
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

// TestK8sPoolWorkerRecordForIdleStampsOwnerCPInstanceID guards against the
// warm-pool churn loop. workerRecordFor used to clear OwnerCPInstanceID
// whenever state==Idle, which left every freshly-spawned warm worker
// matching ListOrphanedWorkers case (2) (NULLIF(owner_cp_instance_id, ”) IS
// NULL AND last_heartbeat_at <= before) the moment it crossed the orphan
// grace. The janitor then retired it, the warm pool replenished, and the
// loop repeated indefinitely. Stamping warm workers with the creating CP's
// id makes case (1) handle them via the existing CP heartbeat instead.
func TestK8sPoolWorkerRecordForIdleStampsOwnerCPInstanceID(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	// worker == nil branch: spawn path before the in-memory ManagedWorker
	// exists. Used by the warm-slot creation flow.
	rec := pool.workerRecordFor(11, nil, 0, configstore.WorkerStateIdle, "", nil)
	if rec.OwnerCPInstanceID != pool.cpInstanceID {
		t.Fatalf("worker==nil idle: expected OwnerCPInstanceID %q, got %q", pool.cpInstanceID, rec.OwnerCPInstanceID)
	}

	// worker != nil branch with the worker already stamped: ManagedWorker
	// owner is preserved.
	w := &ManagedWorker{ID: 12, done: make(chan struct{})}
	w.SetOwnerCPInstanceID(pool.cpInstanceID)
	rec = pool.workerRecordFor(w.ID, w, 0, configstore.WorkerStateIdle, "", nil)
	if rec.OwnerCPInstanceID != pool.cpInstanceID {
		t.Fatalf("worker!=nil idle: expected OwnerCPInstanceID %q, got %q", pool.cpInstanceID, rec.OwnerCPInstanceID)
	}
	if rec.OrgID != "" {
		t.Fatalf("idle workers must have empty OrgID, got %q", rec.OrgID)
	}

	// worker != nil branch with the worker not yet stamped (e.g. the spawn
	// path's transition into Idle before SetOwnerCPInstanceID has run):
	// fall back to this CP's id rather than persisting an empty owner.
	w2 := &ManagedWorker{ID: 13, done: make(chan struct{})}
	rec = pool.workerRecordFor(w2.ID, w2, 0, configstore.WorkerStateIdle, "", nil)
	if rec.OwnerCPInstanceID != pool.cpInstanceID {
		t.Fatalf("worker!=nil unstamped idle: expected OwnerCPInstanceID %q, got %q", pool.cpInstanceID, rec.OwnerCPInstanceID)
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
		return
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
	_ = pool.SpawnWorker(ctx, 0, pool.workerImage)

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
			return
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
			env.ValueFrom.SecretKeyRef.Name == "test-secret-test-cp-worker-0" {
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
			volume.Secret.SecretName == "test-secret-test-cp-worker-0" {
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
		return
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
		return
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
	// Guaranteed QoS: limits == requests
	if res.Limits == nil {
		t.Fatal("expected limits to be set (Guaranteed QoS)")
	}
	cpuLimit := res.Limits[corev1.ResourceCPU]
	if cpuLimit.String() != "500m" {
		t.Fatalf("expected CPU limit 500m, got %s", cpuLimit.String())
	}
	memLimit := res.Limits[corev1.ResourceMemory]
	if memLimit.String() != "2Gi" {
		t.Fatalf("expected memory limit 2Gi, got %s", memLimit.String())
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
	if _, ok := res.Limits[corev1.ResourceCPU]; !ok {
		t.Fatal("expected CPU limit (Guaranteed QoS)")
	}
	if _, ok := res.Limits[corev1.ResourceMemory]; ok {
		t.Fatal("expected no memory limit")
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
	if _, ok := res.Limits[corev1.ResourceMemory]; !ok {
		t.Fatal("expected memory limit (Guaranteed QoS)")
	}
	if _, ok := res.Limits[corev1.ResourceCPU]; ok {
		t.Fatal("expected no CPU limit")
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

func TestSetWorkerResources(t *testing.T) {
	pool := &K8sWorkerPool{
		workerCPURequest:    "500m",
		workerMemoryRequest: "2Gi",
	}
	pool.SetWorkerResources("46", "360Gi")

	res := pool.workerResources()
	cpu := res.Requests[corev1.ResourceCPU]
	if cpu.String() != "46" {
		t.Fatalf("expected CPU request 46, got %s", cpu.String())
	}
	mem := res.Requests[corev1.ResourceMemory]
	if mem.String() != "360Gi" {
		t.Fatalf("expected memory request 360Gi, got %s", mem.String())
	}
	cpuLimit := res.Limits[corev1.ResourceCPU]
	if cpuLimit.String() != "46" {
		t.Fatalf("expected CPU limit 46, got %s", cpuLimit.String())
	}
	memLimit := res.Limits[corev1.ResourceMemory]
	if memLimit.String() != "360Gi" {
		t.Fatalf("expected memory limit 360Gi, got %s", memLimit.String())
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

// mismatchVersionTestPool builds a pool whose cpID looks like a Deployment
// pod ("duckgres-<rshash>-<podhash>") so trimK8sPodHashSuffix yields a
// comparable version prefix.
func mismatchVersionTestPool(t *testing.T, cpID string, store RuntimeWorkerStore) (*K8sWorkerPool, *fake.Clientset) {
	t.Helper()
	cs := fake.NewClientset()
	pool := &K8sWorkerPool{
		workers:      make(map[int]*ManagedWorker),
		shutdownCh:   make(chan struct{}),
		stopInform:   make(chan struct{}),
		clientset:    cs,
		namespace:    "default",
		cpID:         cpID,
		cpInstanceID: cpID + "-boot",
		runtimeStore: store,
		retireSem:    make(chan struct{}, 5),
	}
	return pool, cs
}

func createMismatchWorkerPod(t *testing.T, cs *fake.Clientset, name, controlPlaneLabel, workerIDLabel string) {
	t.Helper()
	createMismatchWorkerPodWithImage(t, cs, name, controlPlaneLabel, workerIDLabel, "")
}

func createMismatchWorkerPodWithImage(t *testing.T, cs *fake.Clientset, name, controlPlaneLabel, workerIDLabel, image string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				"app":                    "duckgres-worker",
				"duckgres/control-plane": controlPlaneLabel,
				"duckgres/worker-id":     workerIDLabel,
			},
		},
	}
	if image != "" {
		pod.Spec.Containers = []corev1.Container{{
			Name:  "duckdb-worker",
			Image: image,
		}}
	}
	_, err := cs.CoreV1().Pods("default").Create(context.Background(), &corev1.Pod{
		ObjectMeta: pod.ObjectMeta,
		Spec:       pod.Spec,
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create pod %q: %v", name, err)
	}
}

func TestRetireOneMismatchedVersionWorker_RetiresOlderVersionIdleWorker(t *testing.T) {
	store := &captureRuntimeWorkerStore{}
	pool, cs := mismatchVersionTestPool(t, "duckgres-newhash-aaaaa", store)
	createMismatchWorkerPod(t, cs, "duckgres-oldhash-worker-7", "duckgres-oldhash-zzzzz", "7")

	if !pool.RetireOneMismatchedVersionWorker(context.Background()) {
		t.Fatal("expected reaper to retire one mismatched worker")
	}
	if store.retireIdleOrHotIdleCalls != 1 || len(store.retireIdleOrHotIdleCalledIDs) != 1 || store.retireIdleOrHotIdleCalledIDs[0] != 7 {
		t.Fatalf("expected one RetireIdleOrHotIdleWorker(7) call, got calls=%d ids=%v", store.retireIdleOrHotIdleCalls, store.retireIdleOrHotIdleCalledIDs)
	}
	if reason := store.retireIdleOrHotIdleCalledReasons[0]; reason != RetireReasonMismatchedVersion {
		t.Fatalf("expected reason %q, got %q", RetireReasonMismatchedVersion, reason)
	}
	pods, _ := cs.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{})
	if len(pods.Items) != 0 {
		t.Fatalf("expected pod to be deleted, got %d remaining", len(pods.Items))
	}
}

func TestRetireOneMismatchedVersionWorker_RetiresHotIdleWorker(t *testing.T) {
	// hot-idle workers have handled a session but are currently idle. They
	// are safe to reap during a rolling update.
	store := &captureRuntimeWorkerStore{}
	pool, cs := mismatchVersionTestPool(t, "duckgres-new-aaaaa", store)
	createMismatchWorkerPod(t, cs, "duckgres-old-worker-9", "duckgres-old-zzzzz", "9")

	if !pool.RetireOneMismatchedVersionWorker(context.Background()) {
		t.Fatal("expected reaper to retire the mismatched hot-idle worker")
	}
	if store.retireIdleOrHotIdleCalls != 1 || store.retireIdleOrHotIdleCalledIDs[0] != 9 {
		t.Fatalf("expected RetireIdleOrHotIdleWorker(9), got calls=%d ids=%v", store.retireIdleOrHotIdleCalls, store.retireIdleOrHotIdleCalledIDs)
	}
	if !podExists(t, cs, "duckgres-old-worker-9") {
		// good, pod deleted
	} else {
		t.Fatal("expected hot-idle pod to be deleted")
	}
}

func TestRetireOneMismatchedVersionWorker_UsesRuntimeOrgForSharedAssignedWorker(t *testing.T) {
	store := &captureRuntimeWorkerStore{
		preloadedRecords: map[int]*configstore.WorkerRecord{
			42: {
				WorkerID: 42,
				OrgID:    "org-1",
				Image:    "posthog/duckgres:old",
				State:    configstore.WorkerStateHotIdle,
			},
		},
	}
	pool, cs := mismatchVersionTestPool(t, "duckgres-current-aaaaa", store)
	pool.workerImage = "posthog/duckgres:stable"
	pool.resolveOrgConfig = func(orgID string) (*configstore.OrgConfig, error) {
		if orgID != "org-1" {
			t.Fatalf("unexpected org lookup %q", orgID)
		}
		return &configstore.OrgConfig{
			Name: "org-1",
			Warehouse: &configstore.ManagedWarehouseConfig{
				Image: "posthog/duckgres:new",
			},
		}, nil
	}
	createMismatchWorkerPodWithImage(t, cs, "duckgres-worker-42", "duckgres-current-bbbbb", "42", "posthog/duckgres:old")

	if !pool.RetireOneMismatchedVersionWorker(context.Background()) {
		t.Fatal("expected reaper to retire shared assigned worker with tenant image mismatch")
	}
	if store.retireIdleOrHotIdleCalls != 1 || store.retireIdleOrHotIdleCalledIDs[0] != 42 {
		t.Fatalf("expected RetireIdleOrHotIdleWorker(42), got calls=%d ids=%v", store.retireIdleOrHotIdleCalls, store.retireIdleOrHotIdleCalledIDs)
	}
	if podExists(t, cs, "duckgres-worker-42") {
		t.Fatal("expected mismatched tenant worker pod to be deleted")
	}
}

func TestRetireOneMismatchedVersionWorker_LeavesSameVersionWorkersAlone(t *testing.T) {
	store := &captureRuntimeWorkerStore{}
	pool, cs := mismatchVersionTestPool(t, "duckgres-samehash-aaaaa", store)
	createMismatchWorkerPod(t, cs, "duckgres-samehash-worker-1", "duckgres-samehash-bbbbb", "1")
	createMismatchWorkerPod(t, cs, "duckgres-samehash-worker-2", "duckgres-samehash-ccccc", "2")

	if pool.RetireOneMismatchedVersionWorker(context.Background()) {
		t.Fatal("expected reaper to find nothing to retire when all workers share the version")
	}
	if store.retireIdleCalls != 0 {
		t.Fatalf("expected no retirement calls, got %d", store.retireIdleCalls)
	}
	pods, _ := cs.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{})
	if len(pods.Items) != 2 {
		t.Fatalf("expected both pods to survive, got %d", len(pods.Items))
	}
}

func TestRetireOneMismatchedVersionWorker_RetiresOnePerCall(t *testing.T) {
	store := &captureRuntimeWorkerStore{}
	pool, cs := mismatchVersionTestPool(t, "duckgres-new-aaaaa", store)
	createMismatchWorkerPod(t, cs, "duckgres-old-worker-10", "duckgres-old-xxxxx", "10")
	createMismatchWorkerPod(t, cs, "duckgres-old-worker-11", "duckgres-old-yyyyy", "11")
	createMismatchWorkerPod(t, cs, "duckgres-old-worker-12", "duckgres-old-zzzzz", "12")

	// Each call should retire at most one pod so replenishment can refill the
	// slot with a new-version pod between ticks.
	for i := 0; i < 3; i++ {
		if !pool.RetireOneMismatchedVersionWorker(context.Background()) {
			t.Fatalf("expected a retirement on call %d", i+1)
		}
	}
	if pool.RetireOneMismatchedVersionWorker(context.Background()) {
		t.Fatal("expected no more retirements after all mismatched pods removed")
	}
	if store.retireIdleOrHotIdleCalls != 3 {
		t.Fatalf("expected exactly 3 retirement attempts, got %d", store.retireIdleOrHotIdleCalls)
	}
}

func TestRetireOneMismatchedVersionWorker_SkipsWhenNotIdle(t *testing.T) {
	// RetireIdleOrHotIdleWorker returns false when the row is no longer idle (busy,
	// reserved, etc.). The reaper must skip those pods and leave
	// them running — it will try again on the next tick.
	store := &captureRuntimeWorkerStore{
		retireIdleOrHotIdleMisses: map[int]bool{5: true},
	}
	pool, cs := mismatchVersionTestPool(t, "duckgres-new-aaaaa", store)
	createMismatchWorkerPod(t, cs, "duckgres-old-worker-5", "duckgres-old-zzzzz", "5")
	createMismatchWorkerPod(t, cs, "duckgres-old-worker-6", "duckgres-old-zzzzz", "6")

	if !pool.RetireOneMismatchedVersionWorker(context.Background()) {
		t.Fatal("expected reaper to skip busy worker 5 and retire worker 6")
	}
	remaining := map[string]bool{}
	pods, _ := cs.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{})
	for _, p := range pods.Items {
		remaining[p.Name] = true
	}
	if !remaining["duckgres-old-worker-5"] {
		t.Fatal("expected busy worker 5 pod to survive (atomic CAS returned false)")
	}
	if remaining["duckgres-old-worker-6"] {
		t.Fatal("expected idle worker 6 pod to be deleted")
	}
}

func TestRetireOneMismatchedVersionWorker_NoopWhenCPIDHasNoHashSuffix(t *testing.T) {
	// Standalone/StatefulSet deployments won't have a ReplicaSet hash in the
	// CP pod name, so version comparison isn't meaningful. The reaper must be
	// a no-op rather than retiring everything it can't parse.
	store := &captureRuntimeWorkerStore{}
	pool, cs := mismatchVersionTestPool(t, "some-bare-hostname", store)
	createMismatchWorkerPod(t, cs, "stray-worker-1", "duckgres-somehash-aaaaa", "1")

	if pool.RetireOneMismatchedVersionWorker(context.Background()) {
		t.Fatal("expected no-op when cpID has no pod-hash suffix")
	}
	if store.retireIdleCalls != 0 {
		t.Fatalf("expected no retirement calls, got %d", store.retireIdleCalls)
	}
}

// --- Stranded-pod reconciler tests ---
//
// cleanupOrphanedWorkerPods closes a gap left by ShutdownAll: the CP marks the
// worker row terminal (retired/lost) in the DB before issuing the K8s pod
// delete, and the delete is fire-and-forget. If the delete fails (API hiccup,
// CP SIGKILL'd mid-shutdown), the pod survives forever because:
//   - ListOrphanedWorkers excludes terminal states, so orphan cleanup ignores it
//   - Bare worker pods have no owner reference, so nothing else reaps them
// These tests pin the expected behavior of the K8s-label-based reconciler.

// strandedReconcilerPool wires a K8sWorkerPool with a fake clientset and store
// for reconciler tests. Ownership labels aren't checked by the reconciler, so
// we keep the setup minimal.
func strandedReconcilerPool(t *testing.T, store RuntimeWorkerStore) (*K8sWorkerPool, *fake.Clientset) {
	t.Helper()
	cs := fake.NewClientset()
	pool := &K8sWorkerPool{
		workers:      make(map[int]*ManagedWorker),
		shutdownCh:   make(chan struct{}),
		stopInform:   make(chan struct{}),
		clientset:    cs,
		namespace:    "default",
		cpID:         "duckgres-new-aaaaa",
		cpInstanceID: "duckgres-new-aaaaa-boot",
		runtimeStore: store,
		retireSem:    make(chan struct{}, 5),
	}
	return pool, cs
}

func createStrandedWorkerPod(t *testing.T, cs *fake.Clientset, name, workerIDLabel string, age time.Duration) {
	t.Helper()
	_, err := cs.CoreV1().Pods("default").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-age)),
			Labels: map[string]string{
				"app":                "duckgres-worker",
				"duckgres/worker-id": workerIDLabel,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create pod %q: %v", name, err)
	}
}

func podExists(t *testing.T, cs *fake.Clientset, name string) bool {
	t.Helper()
	_, err := cs.CoreV1().Pods("default").Get(context.Background(), name, metav1.GetOptions{})
	if err == nil {
		return true
	}
	if k8serrors.IsNotFound(err) {
		return false
	}
	t.Fatalf("unexpected error fetching pod %q: %v", name, err)
	return false
}

func TestCleanupOrphanedWorkerPods_DeletesPodWhenDBStateRetired(t *testing.T) {
	// This is the exact prod scenario: worker's DB row is state=retired (a
	// previous CP marked it during ShutdownAll) but the K8s pod survived
	// because the delete failed or was interrupted. The reconciler must catch
	// this and delete the pod.
	store := &captureRuntimeWorkerStore{
		preloadedRecords: map[int]*configstore.WorkerRecord{
			31758: {WorkerID: 31758, State: configstore.WorkerStateRetired},
		},
	}
	pool, cs := strandedReconcilerPool(t, store)
	createStrandedWorkerPod(t, cs, "duckgres-old-worker-31758", "31758", 10*time.Minute)

	deleted := pool.cleanupOrphanedWorkerPods(context.Background(), 2*time.Minute)
	if deleted != 1 {
		t.Fatalf("expected 1 pod deleted, got %d", deleted)
	}
	if podExists(t, cs, "duckgres-old-worker-31758") {
		t.Fatal("expected stranded pod to be deleted")
	}
}

func TestCleanupOrphanedWorkerPods_DeletesPodWhenDBStateLost(t *testing.T) {
	// lost is the DB state assigned when a worker is retired with reason=crash
	// (see markWorkerRetiredLocked). These pods are also terminal-in-DB and
	// must be reconciled.
	store := &captureRuntimeWorkerStore{
		preloadedRecords: map[int]*configstore.WorkerRecord{
			42: {WorkerID: 42, State: configstore.WorkerStateLost},
		},
	}
	pool, cs := strandedReconcilerPool(t, store)
	createStrandedWorkerPod(t, cs, "duckgres-lost-worker-42", "42", 10*time.Minute)

	if deleted := pool.cleanupOrphanedWorkerPods(context.Background(), 2*time.Minute); deleted != 1 {
		t.Fatalf("expected 1 pod deleted, got %d", deleted)
	}
	if podExists(t, cs, "duckgres-lost-worker-42") {
		t.Fatal("expected lost-state pod to be deleted")
	}
}

func TestCleanupOrphanedWorkerPods_DeletesPodWhenDBRecordMissing(t *testing.T) {
	// No DB row exists at all for this worker-id: fully orphaned pod, likely
	// from a worker row that was purged while the pod kept running. Treat it
	// the same as a terminal-state pod.
	store := &captureRuntimeWorkerStore{}
	pool, cs := strandedReconcilerPool(t, store)
	createStrandedWorkerPod(t, cs, "duckgres-ghost-worker-99", "99", 10*time.Minute)

	if deleted := pool.cleanupOrphanedWorkerPods(context.Background(), 2*time.Minute); deleted != 1 {
		t.Fatalf("expected 1 pod deleted, got %d", deleted)
	}
	if podExists(t, cs, "duckgres-ghost-worker-99") {
		t.Fatal("expected ghost pod with no DB row to be deleted")
	}
}

func TestCleanupOrphanedWorkerPods_LeavesLivePodAlone(t *testing.T) {
	// Workers in any non-terminal state (idle, reserved, activating, hot,
	// hot_idle, spawning, draining) are part of the normal lifecycle — the
	// reconciler must not disturb them. This test covers the common live
	// state (idle). Other states follow the same code path.
	store := &captureRuntimeWorkerStore{
		preloadedRecords: map[int]*configstore.WorkerRecord{
			7: {WorkerID: 7, State: configstore.WorkerStateIdle},
		},
	}
	pool, cs := strandedReconcilerPool(t, store)
	createStrandedWorkerPod(t, cs, "duckgres-live-worker-7", "7", 10*time.Minute)

	if deleted := pool.cleanupOrphanedWorkerPods(context.Background(), 2*time.Minute); deleted != 0 {
		t.Fatalf("expected no pods deleted for live worker, got %d", deleted)
	}
	if !podExists(t, cs, "duckgres-live-worker-7") {
		t.Fatal("expected idle worker pod to survive reconciliation")
	}
}

func TestCleanupOrphanedWorkerPods_SkipsYoungPod(t *testing.T) {
	// Spawning workers create the pod BEFORE inserting the DB row. Without a
	// grace window on pod age, the reconciler would delete freshly-spawned
	// pods in the ~100ms race window between pod creation and DB upsert.
	store := &captureRuntimeWorkerStore{} // no record yet — newborn
	pool, cs := strandedReconcilerPool(t, store)
	createStrandedWorkerPod(t, cs, "duckgres-newborn-worker-11", "11", 30*time.Second)

	if deleted := pool.cleanupOrphanedWorkerPods(context.Background(), 2*time.Minute); deleted != 0 {
		t.Fatalf("expected young pod to be skipped, got deleted=%d", deleted)
	}
	if !podExists(t, cs, "duckgres-newborn-worker-11") {
		t.Fatal("expected newborn pod to survive (under grace window)")
	}
}

func TestCleanupOrphanedWorkerPods_TreatsNotFoundAsSuccess(t *testing.T) {
	// If two CPs both become leader for a moment during a split-brain, or the
	// pod was evicted by kubelet between our List and Delete, the delete will
	// return NotFound. The reconciler must treat that as success.
	store := &captureRuntimeWorkerStore{
		preloadedRecords: map[int]*configstore.WorkerRecord{
			50: {WorkerID: 50, State: configstore.WorkerStateRetired},
			51: {WorkerID: 51, State: configstore.WorkerStateRetired},
		},
	}
	pool, cs := strandedReconcilerPool(t, store)
	createStrandedWorkerPod(t, cs, "duckgres-gone-worker-50", "50", 10*time.Minute)
	createStrandedWorkerPod(t, cs, "duckgres-stale-worker-51", "51", 10*time.Minute)

	// Make the DELETE for worker 50 return NotFound (simulating race).
	cs.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		da := action.(k8stesting.DeleteAction)
		if da.GetName() == "duckgres-gone-worker-50" {
			return true, nil, k8serrors.NewNotFound(corev1.Resource("pods"), da.GetName())
		}
		return false, nil, nil
	})

	if deleted := pool.cleanupOrphanedWorkerPods(context.Background(), 2*time.Minute); deleted != 2 {
		t.Fatalf("expected 2 pods deleted (NotFound counts as success), got %d", deleted)
	}
	if podExists(t, cs, "duckgres-stale-worker-51") {
		t.Fatal("expected worker 51's pod to be deleted")
	}
}

func TestCleanupOrphanedWorkerPods_IgnoresNonWorkerPods(t *testing.T) {
	// Only pods carrying the duckgres-worker app label should be considered.
	// This guards against accidentally reaping CP pods or other workloads
	// that happened to be scheduled into the duckgres namespace.
	store := &captureRuntimeWorkerStore{}
	pool, cs := strandedReconcilerPool(t, store)
	// Non-worker pod (missing app=duckgres-worker label) — must be ignored.
	_, err := cs.CoreV1().Pods("default").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "some-other-pod",
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
			Labels:            map[string]string{"app": "something-else"},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create other pod: %v", err)
	}

	if deleted := pool.cleanupOrphanedWorkerPods(context.Background(), 2*time.Minute); deleted != 0 {
		t.Fatalf("expected no deletions, got %d", deleted)
	}
	if !podExists(t, cs, "some-other-pod") {
		t.Fatal("non-worker pod must survive reconciliation")
	}
}

// --- ShutdownAll draining-chain tests ---
//
// ShutdownAll is called when the CP pod receives SIGTERM from Kubernetes. The
// old implementation marked each worker retired in the DB and then fire-and-
// forget deleted the pod — on delete failure the DB row moved on but the pod
// survived forever (ListOrphanedWorkers excludes terminal states). These
// tests pin the new 3-step chain:
//
//   1. MarkWorkerDraining: atomic CAS idle/hot_idle/... → draining. Fences
//      the worker against claims by other CPs (their claim queries match
//      state=idle/hot_idle, which no longer applies).
//   2. K8s pod delete.
//   3. RetireDrainingWorker: atomic CAS draining → retired. Only reached on
//      successful pod-delete — so on delete failure the row stays in
//      draining, where ListOrphanedWorkers picks it up once the CP's
//      heartbeat expires, or cleanupOrphanedWorkerPods handles it by pod
//      label regardless of DB state.

func shutdownTestPool(t *testing.T, store *captureRuntimeWorkerStore) (*K8sWorkerPool, *fake.Clientset) {
	t.Helper()
	pool, cs := newTestK8sPool(t, 5)
	pool.runtimeStore = store
	// Intercept pod deletions so the test can assert that Delete is invoked
	// strictly between MarkWorkerDraining and RetireDrainingWorker.
	cs.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		da := action.(k8stesting.DeleteAction)
		store.mu.Lock()
		store.recordEvent(fmt.Sprintf("delete:%s", da.GetName()))
		store.mu.Unlock()
		return false, nil, nil // fall through so the fake actually removes the pod
	})
	return pool, cs
}

func addShutdownWorker(t *testing.T, p *K8sWorkerPool, cs *fake.Clientset, id int) *ManagedWorker {
	t.Helper()
	w := &ManagedWorker{
		ID:      id,
		podName: fmt.Sprintf("worker-%d", id),
		done:    make(chan struct{}),
	}
	p.workers[id] = w
	_, err := cs.CoreV1().Pods(p.namespace).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      w.podName,
			Namespace: p.namespace,
			Labels:    map[string]string{"app": "duckgres-worker", "duckgres/worker-id": strconv.Itoa(id)},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create pod %q: %v", w.podName, err)
	}
	return w
}

func TestShutdownAll_UsesDrainingChainPerWorker(t *testing.T) {
	// Per worker: MarkWorkerDraining → Delete pod → RetireDrainingWorker.
	store := &captureRuntimeWorkerStore{}
	pool, cs := shutdownTestPool(t, store)
	addShutdownWorker(t, pool, cs, 1)
	addShutdownWorker(t, pool, cs, 2)

	pool.ShutdownAll()

	if store.markDrainingCalls != 2 {
		t.Fatalf("expected 2 MarkWorkerDraining calls, got %d", store.markDrainingCalls)
	}
	if store.retireDrainingCalls != 2 {
		t.Fatalf("expected 2 RetireDrainingWorker calls, got %d", store.retireDrainingCalls)
	}
	for _, reason := range store.retireDrainingReasons {
		if reason != RetireReasonShutdown {
			t.Fatalf("expected retire reason=%q, got %q", RetireReasonShutdown, reason)
		}
	}
	for _, name := range []string{"worker-1", "worker-2"} {
		if podExists(t, cs, name) {
			t.Fatalf("expected pod %q to be deleted", name)
		}
	}
}

func TestShutdownAll_DrainBeforeDeleteBeforeRetire(t *testing.T) {
	// Enforces the happens-before chain for a single worker: the SQL CAS to
	// draining must complete before the K8s delete, and the K8s delete must
	// complete before the SQL CAS to retired. If the order were swapped,
	// another CP could claim the worker mid-delete (delete → claim → fail),
	// or a crash between delete and retire would leave a stranded pod that
	// the orphan sweep can't see (excludes terminal states).
	store := &captureRuntimeWorkerStore{}
	pool, cs := shutdownTestPool(t, store)
	addShutdownWorker(t, pool, cs, 42)

	pool.ShutdownAll()

	wantSuffix := []string{"draining:42", "delete:worker-42", "retired:42"}
	if len(store.events) < len(wantSuffix) {
		t.Fatalf("expected at least %d events, got %d: %v", len(wantSuffix), len(store.events), store.events)
	}
	for i, want := range wantSuffix {
		if store.events[i] != want {
			t.Fatalf("event[%d] = %q, want %q (full events: %v)", i, store.events[i], want, store.events)
		}
	}
}

func TestShutdownAll_SkipsWorkerWhenMarkDrainingCASMisses(t *testing.T) {
	// MarkWorkerDraining returns false when the row is already terminal (e.g.
	// the worker was retired on another path between list and CAS) or owned
	// by a different CP. In that case there's nothing to drain, so we must
	// neither delete the pod nor call RetireDrainingWorker (which would
	// transition from a state that isn't draining, never matching).
	store := &captureRuntimeWorkerStore{
		markDrainingMisses: map[int]bool{99: true},
	}
	pool, cs := shutdownTestPool(t, store)
	addShutdownWorker(t, pool, cs, 99)
	addShutdownWorker(t, pool, cs, 100)

	pool.ShutdownAll()

	// Worker 99 should be skipped entirely after the CAS miss: no pod delete,
	// no RetireDrainingWorker call. Worker 100 should proceed normally.
	for _, event := range store.events {
		if event == "delete:worker-99" {
			t.Fatal("expected no pod delete for worker 99 (MarkDraining CAS missed)")
		}
	}
	for _, id := range store.retireDrainingCalledIDs {
		if id == 99 {
			t.Fatal("expected no RetireDrainingWorker for worker 99 after CAS miss")
		}
	}
	if !podExists(t, cs, "worker-99") {
		t.Fatal("expected pod worker-99 to survive — its DB row wasn't owned by us")
	}
	if podExists(t, cs, "worker-100") {
		t.Fatal("expected worker-100 pod to be deleted")
	}
}

func TestShutdownAll_LeavesInDrainingWhenPodDeleteFails(t *testing.T) {
	// On pod-delete failure the worker row stays in draining. That's the
	// signal for recovery paths:
	//   - Once this CP's heartbeat expires, ListOrphanedWorkers picks up
	//     draining rows owned by expired CPs and retires them.
	//   - cleanupOrphanedWorkerPods sees the pod by label and deletes it
	//     regardless of DB state.
	// What we must NOT do is call RetireDrainingWorker, since that would
	// clear the signal and let a stranded pod linger indefinitely.
	store := &captureRuntimeWorkerStore{}
	pool, cs := shutdownTestPool(t, store)
	addShutdownWorker(t, pool, cs, 7)
	// Make the Delete fail with a non-NotFound error.
	cs.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		da := action.(k8stesting.DeleteAction)
		if da.GetName() == "worker-7" {
			return true, nil, errors.New("api server timeout")
		}
		return false, nil, nil
	})

	pool.ShutdownAll()

	if store.markDrainingCalls != 1 {
		t.Fatalf("expected 1 MarkDraining call, got %d", store.markDrainingCalls)
	}
	if store.retireDrainingCalls != 0 {
		t.Fatalf("expected no RetireDrainingWorker call after delete failure, got %d", store.retireDrainingCalls)
	}
}

// TestShutdownAll_SparesWorkersWithActiveSessions: a CP receiving SIGTERM
// must not pod-delete a worker that's mid-query. Today the chain runs for
// every owned worker regardless of session count, which collapses every
// in-flight query at the moment ShutdownAll fires (the failure mode the
// production incident hit on a 15-minute drain wall).
//
// After the fix, busy workers (activeSessions > 0) are skipped — left in
// hot/serving state, owned by the dying CP. Customer Flight clients can
// reconnect via session token; the orphan janitor's flight-session JOIN
// (Layer 3) prevents peer CPs from retiring them while a session record
// is still active. Idle workers (activeSessions == 0) drain normally.
func TestShutdownAll_SparesWorkersWithActiveSessions(t *testing.T) {
	store := &captureRuntimeWorkerStore{}
	pool, cs := shutdownTestPool(t, store)

	busy := addShutdownWorker(t, pool, cs, 1)
	busy.activeSessions = 2

	addShutdownWorker(t, pool, cs, 2) // idle (activeSessions == 0)

	pool.ShutdownAll()

	// Busy worker: pod survives, no DB transitions on its row.
	if !podExists(t, cs, "worker-1") {
		t.Fatal("worker-1 has active sessions; ShutdownAll must not delete its pod")
	}
	for _, id := range store.markDrainingCalledIDs {
		if id == 1 {
			t.Fatalf("MarkWorkerDraining called for busy worker 1; ShutdownAll must skip it (calls=%v)", store.markDrainingCalledIDs)
		}
	}
	for _, id := range store.retireDrainingCalledIDs {
		if id == 1 {
			t.Fatalf("RetireDrainingWorker called for busy worker 1; ShutdownAll must skip it (calls=%v)", store.retireDrainingCalledIDs)
		}
	}

	// Idle worker: drained as before.
	if podExists(t, cs, "worker-2") {
		t.Fatal("worker-2 is idle; ShutdownAll should have deleted its pod")
	}
}

func TestShutdownAll_TreatsPodNotFoundAsDeleteSuccess(t *testing.T) {
	// NotFound means another actor already removed the pod (node eviction,
	// a racing CP during split-brain, manual kubectl delete). The state
	// machine effectively reached "pod gone", so we should proceed to the
	// final retire CAS rather than leaving the worker pinned in draining.
	store := &captureRuntimeWorkerStore{}
	pool, cs := shutdownTestPool(t, store)
	addShutdownWorker(t, pool, cs, 8)
	cs.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		da := action.(k8stesting.DeleteAction)
		if da.GetName() == "worker-8" {
			return true, nil, k8serrors.NewNotFound(corev1.Resource("pods"), da.GetName())
		}
		return false, nil, nil
	})

	pool.ShutdownAll()

	if store.retireDrainingCalls != 1 {
		t.Fatalf("expected RetireDrainingWorker even when delete returned NotFound, got calls=%d", store.retireDrainingCalls)
	}
}

// --- Node-age-aware scheduling tests ---
//
// These cover the two places the pool uses `nodeFirstSeen`: picking the next
// idle worker to claim (oldest node preferred) and picking which excess idle
// worker to reap (newest node preferred). Ordering must be deterministic so
// query sessions land on cache-warm nodes and Karpenter can consolidate the
// newest nodes first.

// addIdleWorker inserts a ready warm-idle worker on nodeName with a matching
// nodeFirstSeen entry. idleFor controls how far in the past lastUsed is —
// must exceed idleTimeout for the reaper to consider it.
func addIdleWorker(t *testing.T, p *K8sWorkerPool, id int, nodeName string, nodeSeenAt time.Time, idleFor time.Duration) {
	t.Helper()
	w := &ManagedWorker{
		ID:       id,
		podName:  fmt.Sprintf("worker-%d", id),
		nodeName: nodeName,
		done:     make(chan struct{}),
		lastUsed: time.Now().Add(-idleFor),
	}
	p.workers[id] = w
	if _, ok := p.nodeFirstSeen[nodeName]; !ok {
		p.nodeFirstSeen[nodeName] = nodeSeenAt
	}
}

func TestStampNodeFirstSeenLockedOnlySetsOnce(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)

	pool.stampNodeFirstSeenLocked("node-a")
	first := pool.nodeFirstSeen["node-a"]

	// Sleep long enough that the Now() reading would differ if we overwrote.
	time.Sleep(2 * time.Millisecond)
	pool.stampNodeFirstSeenLocked("node-a")
	if pool.nodeFirstSeen["node-a"] != first {
		t.Errorf("nodeFirstSeen overwritten on repeat stamp; want stable %v, got %v", first, pool.nodeFirstSeen["node-a"])
	}
}

func TestStampNodeFirstSeenLockedIgnoresEmptyNodeName(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.stampNodeFirstSeenLocked("")
	if len(pool.nodeFirstSeen) != 0 {
		t.Errorf("empty nodeName should not be recorded, got %v", pool.nodeFirstSeen)
	}
}

func TestPruneNodeFirstSeenLockedRemovesOnlyOrphanedEntries(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	now := time.Now()
	addIdleWorker(t, pool, 1, "node-a", now.Add(-1*time.Hour), time.Hour)
	addIdleWorker(t, pool, 2, "node-a", now.Add(-1*time.Hour), time.Hour)
	addIdleWorker(t, pool, 3, "node-b", now.Add(-10*time.Minute), time.Hour)

	// Remove one worker on node-a — node-a should stay because worker 2 remains.
	delete(pool.workers, 1)
	pool.pruneNodeFirstSeenLocked("node-a")
	if _, ok := pool.nodeFirstSeen["node-a"]; !ok {
		t.Error("node-a pruned while worker still references it")
	}

	// Remove the last worker on node-a — entry should go.
	delete(pool.workers, 2)
	pool.pruneNodeFirstSeenLocked("node-a")
	if _, ok := pool.nodeFirstSeen["node-a"]; ok {
		t.Error("node-a not pruned after last worker removed")
	}
	// node-b untouched.
	if _, ok := pool.nodeFirstSeen["node-b"]; !ok {
		t.Error("pruning node-a should leave node-b alone")
	}
}

func TestFindIdleWorkerLockedPrefersOldestNode(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	now := time.Now()
	addIdleWorker(t, pool, 1, "node-young", now.Add(-1*time.Minute), time.Hour)
	addIdleWorker(t, pool, 2, "node-old", now.Add(-1*time.Hour), time.Hour)
	addIdleWorker(t, pool, 3, "node-mid", now.Add(-10*time.Minute), time.Hour)

	chosen := pool.findIdleWorkerLocked()
	if chosen == nil {
		t.Fatal("expected to find an idle worker")
	}
	if chosen.nodeName != "node-old" {
		t.Errorf("claim picked %q, want node-old (longest-lived cache)", chosen.nodeName)
	}
}

func TestFindIdleWorkerLockedUnknownNodeSortsLast(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	now := time.Now()
	// Worker on a known old node + worker with no node info (e.g. race between
	// spawn and the informer). The known-old node must win.
	addIdleWorker(t, pool, 1, "node-old", now.Add(-1*time.Hour), time.Hour)
	w := &ManagedWorker{ID: 2, podName: "worker-2", done: make(chan struct{}), lastUsed: time.Now().Add(-time.Hour)}
	pool.workers[2] = w

	chosen := pool.findIdleWorkerLocked()
	if chosen == nil || chosen.ID != 1 {
		t.Errorf("expected worker on node-old (id=1), got %+v", chosen)
	}
}

func TestReapIdleWorkersEvictsNewestNodeFirst(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.minWorkers = 1
	pool.idleTimeout = 5 * time.Minute

	now := time.Now()
	// 3 idle workers, 3 different nodes. minWorkers=1 so 2 get reaped.
	// Expect: node-youngest + node-mid reaped (newest-first), node-old survives.
	addIdleWorker(t, pool, 1, "node-old", now.Add(-1*time.Hour), time.Hour)
	addIdleWorker(t, pool, 2, "node-youngest", now.Add(-1*time.Minute), time.Hour)
	addIdleWorker(t, pool, 3, "node-mid", now.Add(-10*time.Minute), time.Hour)

	// Stub retireWorkerPod so the reaper doesn't try to talk to k8s.
	var retired []string
	pool.retireSem = make(chan struct{}, 5)
	origClient := pool.clientset
	_ = origClient
	// Monkey-patch via clientset fake — easier: just mark workers retired and observe.
	// Call the reap logic directly; spot-check deletions via p.workers.

	pool.reapIdleWorkers()

	// Drain any retire goroutines by giving them a chance (they just run fake k8s).
	// We only assert on map state, which reapIdleWorkers mutates under the lock.

	if _, ok := pool.workers[1]; !ok {
		t.Error("worker on node-old was reaped; expected to survive (oldest node)")
	}
	if _, ok := pool.workers[2]; ok {
		t.Error("worker on node-youngest not reaped; expected to be evicted first")
	}
	if _, ok := pool.workers[3]; ok {
		t.Error("worker on node-mid not reaped; should have been second eviction")
	}
	if _, ok := pool.nodeFirstSeen["node-youngest"]; ok {
		t.Error("nodeFirstSeen entry for node-youngest not pruned after last worker reaped")
	}
	if _, ok := pool.nodeFirstSeen["node-mid"]; ok {
		t.Error("nodeFirstSeen entry for node-mid not pruned after last worker reaped")
	}
	if _, ok := pool.nodeFirstSeen["node-old"]; !ok {
		t.Error("nodeFirstSeen entry for node-old incorrectly pruned (worker still alive)")
	}
	_ = retired
}

func TestReapIdleWorkersStopsAtMinWorkers(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.minWorkers = 2
	pool.idleTimeout = 5 * time.Minute

	now := time.Now()
	addIdleWorker(t, pool, 1, "node-a", now.Add(-1*time.Hour), time.Hour)
	addIdleWorker(t, pool, 2, "node-b", now.Add(-45*time.Minute), time.Hour)
	addIdleWorker(t, pool, 3, "node-c", now.Add(-30*time.Minute), time.Hour)

	pool.reapIdleWorkers()

	// 3 idle - 2 minWorkers = 1 should be reaped (the newest, node-c).
	if len(pool.workers) != 2 {
		t.Fatalf("expected 2 workers after reap, got %d", len(pool.workers))
	}
	if _, ok := pool.workers[3]; ok {
		t.Error("expected worker on node-c (newest) to be reaped")
	}
}

func TestReapIdleWorkersSkipsWorkersWithinIdleTimeout(t *testing.T) {
	pool, _ := newTestK8sPool(t, 5)
	pool.minWorkers = 0
	pool.idleTimeout = 5 * time.Minute

	now := time.Now()
	// Idle for only 1 minute — well under idleTimeout.
	addIdleWorker(t, pool, 1, "node-a", now.Add(-1*time.Hour), 1*time.Minute)
	addIdleWorker(t, pool, 2, "node-b", now.Add(-30*time.Minute), 1*time.Minute)

	pool.reapIdleWorkers()

	if len(pool.workers) != 2 {
		t.Errorf("expected both workers to survive (under idleTimeout); got %d remaining", len(pool.workers))
	}
}
