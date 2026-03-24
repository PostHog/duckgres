//go:build kubernetes

package controlplane

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

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
	idle := &ManagedWorker{ID: 7, done: make(chan struct{})}
	pool.workers[idle.ID] = idle

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
	worker, err := pool.ReserveSharedWorker(context.Background(), &WorkerAssignment{
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

	select {
	case <-replacementSpawned:
	case <-time.After(time.Second):
		t.Fatal("expected reserve to trigger warm-pool replenishment")
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
	pool.spawnWarmWorkerFunc = func(ctx context.Context, id int) error {
		pool.mu.Lock()
		pool.workers[id] = &ManagedWorker{ID: id, done: make(chan struct{})}
		pool.mu.Unlock()
		return nil
	}

	worker, err := pool.ReserveSharedWorker(context.Background(), &WorkerAssignment{
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
	if _, ok := pod.Labels["duckgres/org"]; ok {
		t.Fatalf("expected shared warm worker startup to stay org-neutral, got labels %#v", pod.Labels)
	}

	if len(pod.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(pod.OwnerReferences))
	}
	if pod.OwnerReferences[0].Name != "test-cp" {
		t.Fatalf("expected owner ref to test-cp, got %s", pod.OwnerReferences[0].Name)
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
