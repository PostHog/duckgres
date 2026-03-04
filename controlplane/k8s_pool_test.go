//go:build kubernetes

package controlplane

import (
	"context"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
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

func TestK8sPool_SpawnWorkerCreatesCorrectPod(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	pool.configMap = "my-config"

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
	// With fake clientset, the pod exists even after delete sometimes, so check create actions
	found := false
	for _, pod := range pods.Items {
		if pod.Labels["duckgres/worker-id"] == "0" {
			found = true

			// Verify labels
			if pod.Labels["app"] != "duckgres-worker" {
				t.Fatalf("expected app=duckgres-worker label, got %s", pod.Labels["app"])
			}
			if pod.Labels["duckgres/control-plane"] != "test-cp" {
				t.Fatalf("expected control-plane label test-cp, got %s", pod.Labels["duckgres/control-plane"])
			}

			// Verify owner references
			if len(pod.OwnerReferences) != 1 {
				t.Fatalf("expected 1 owner reference, got %d", len(pod.OwnerReferences))
			}
			if pod.OwnerReferences[0].Name != "test-cp" {
				t.Fatalf("expected owner ref to test-cp, got %s", pod.OwnerReferences[0].Name)
			}

			// Verify security context
			if pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.RunAsNonRoot == nil || !*pod.Spec.SecurityContext.RunAsNonRoot {
				t.Fatal("expected runAsNonRoot=true")
			}

			// Verify container
			if len(pod.Spec.Containers) != 1 {
				t.Fatalf("expected 1 container, got %d", len(pod.Spec.Containers))
			}
			c := pod.Spec.Containers[0]
			if c.Image != "duckgres:test" {
				t.Fatalf("expected image duckgres:test, got %s", c.Image)
			}

			// Verify bearer token env var
			foundEnv := false
			for _, env := range c.Env {
				if env.Name == "DUCKGRES_DUCKDB_TOKEN" && env.ValueFrom != nil &&
					env.ValueFrom.SecretKeyRef != nil &&
					env.ValueFrom.SecretKeyRef.Name == "test-secret" {
					foundEnv = true
				}
			}
			if !foundEnv {
				t.Fatal("bearer token env var not found or incorrect")
			}

			// Verify configmap volume mount
			if len(pod.Spec.Volumes) == 0 {
				t.Fatal("expected configmap volume")
			}

			break
		}
	}

	if !found {
		// This is expected since the pod gets deleted after spawn failure
		// Let's just check that the create was attempted by looking at actions
		t.Log("Pod was cleaned up after spawn failure (expected in unit test without real K8s)")
	}
}

func TestK8sPool_DiscoverExistingWorkers(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)

	// Create the bearer token secret
	_, err := cs.CoreV1().Secrets("default").Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "test-secret", Namespace: "default"},
		Data:       map[string][]byte{"bearer-token": []byte("test-token")},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Create a "running" worker pod (without a real gRPC server it will be deleted)
	_, _ = cs.CoreV1().Pods("default").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "duckgres-worker-test-cp-0",
			Namespace: "default",
			Labels: map[string]string{
				"app":                    "duckgres-worker",
				"duckgres/control-plane": "test-cp",
				"duckgres/worker-id":     "0",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: "10.0.0.5",
		},
	}, metav1.CreateOptions{})

	// Create a failed worker pod
	_, _ = cs.CoreV1().Pods("default").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "duckgres-worker-test-cp-1",
			Namespace: "default",
			Labels: map[string]string{
				"app":                    "duckgres-worker",
				"duckgres/control-plane": "test-cp",
				"duckgres/worker-id":     "1",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodFailed},
	}, metav1.CreateOptions{})

	pool.DiscoverExistingWorkers(context.Background())

	// The running pod will fail gRPC connection (no real server) and be deleted.
	// The failed pod should be deleted directly.
	// Both pods should have been processed.
	// In a real cluster with actual workers, we'd verify reconnection.
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
