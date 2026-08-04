//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// emulateGenerateName makes the fake clientset assign unique names from
// metav1.GenerateName on pod create, as the real API server does (the fake
// otherwise leaves Name empty, so multiple GenerateName creates collide).
func emulateGenerateName(cs *fake.Clientset) {
	var n int
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod, ok := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		if ok && pod.Name == "" && pod.GenerateName != "" {
			n++
			pod.Name = fmt.Sprintf("%s%d", pod.GenerateName, n)
		}
		return false, nil, nil // fall through to the default tracker with the mutated object
	})
}

// fakeSpawnLogStore satisfies both RuntimeWorkerStore (embedding a nil
// interface would panic, so it stubs the methods) and workerSpawnLogStore.
type fakeSpawnLogStore struct {
	RuntimeWorkerStore // nil; none of its methods are exercised by headroom

	stats    configstore.HeadroomSpawnStats
	statsErr error
	recorded []recordedSpawn
	pruned   []time.Time
}

type recordedSpawn struct {
	orgID     string
	cpuMillis int64
	memBytes  int64
}

func (f *fakeSpawnLogStore) RecordWorkerSpawn(orgID string, cpuMillis, memBytes int64) error {
	f.recorded = append(f.recorded, recordedSpawn{orgID, cpuMillis, memBytes})
	return nil
}

func (f *fakeSpawnLogStore) HeadroomSpawnStats(_, _, _ time.Duration) (configstore.HeadroomSpawnStats, error) {
	return f.stats, f.statsErr
}

func (f *fakeSpawnLogStore) PruneWorkerSpawnLog(olderThan time.Time) (int64, error) {
	f.pruned = append(f.pruned, olderThan)
	return 0, nil
}

func mkWorkerPod(name, cpu, mem string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{"app": "duckgres-worker"}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "duckdb-worker",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(mem),
			}},
		}}},
	}
}

func listHeadroomPods(t *testing.T, cs *fake.Clientset) []corev1.Pod {
	t.Helper()
	pods, err := cs.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{LabelSelector: "app=" + headroomPodLabel})
	if err != nil {
		t.Fatalf("list placeholders: %v", err)
	}
	return pods.Items
}

func newHeadroomPool(cs *fake.Clientset, store RuntimeWorkerStore) *K8sWorkerPool {
	return &K8sWorkerPool{
		clientset:                    cs,
		namespace:                    "default",
		cpID:                         "duckgres-cp-abcde",
		placeholderPriorityClassName: "duckgres-headroom",
		runtimeStore:                 store,
	}
}

func TestPlaceholderPodGenerateName(t *testing.T) {
	// "duckgres-placeholder-<cp-replicaset-hash>-<apiserver-rand>": the RS hash
	// identifies the owning CP build; the CP pod's own random suffix is NOT
	// embedded (it was pure noise in the old
	// duckgres-headroom-<full-cp-pod-name>-<rand> shape).
	cs := fake.NewSimpleClientset()
	p := &K8sWorkerPool{
		clientset: cs,
		namespace: "default",
		cpID:      "duckgres-5ff4d85bdb-zshgq",
	}
	if err := p.createPlaceholderPod(context.Background(), resource.MustParse("1"), resource.MustParse("2Gi")); err != nil {
		t.Fatalf("createPlaceholderPod: %v", err)
	}
	pods, err := cs.CoreV1().Pods("default").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(pods.Items) != 1 {
		t.Fatalf("expected 1 placeholder pod, got %d (err=%v)", len(pods.Items), err)
	}
	pod := pods.Items[0]
	if pod.GenerateName != "duckgres-placeholder-5ff4d85bdb-" {
		t.Fatalf("GenerateName = %q, want duckgres-placeholder-5ff4d85bdb-", pod.GenerateName)
	}
	// Selector continuity: the app label keeps the legacy value so pods created
	// under the old name stay managed across a rollout.
	if pod.Labels["app"] != "duckgres-headroom" {
		t.Fatalf("app label = %q, want duckgres-headroom", pod.Labels["app"])
	}
}

// The slot count follows the peak spawn burst and the slot size follows the
// largest recently spawned shape — the two dynamic signals.
func TestReconcileHeadroomFollowsSpawnBurst(t *testing.T) {
	cs := fake.NewSimpleClientset(mkWorkerPod("w1", "15", "120Gi"))
	emulateGenerateName(cs)
	store := &fakeSpawnLogStore{stats: configstore.HeadroomSpawnStats{
		PeakBurst:    3,
		MaxCPUMillis: 15000,
		MaxMemBytes:  120 << 30,
	}}
	p := newHeadroomPool(cs, store)

	p.reconcileHeadroom(context.Background())

	pods := listHeadroomPods(t, cs)
	if len(pods) != 3 {
		t.Fatalf("expected 3 placeholders for peak burst 3, got %d", len(pods))
	}
	req := pods[0].Spec.Containers[0].Resources.Requests
	if req.Cpu().MilliValue() != 15000 || req.Memory().Value() != 120<<30 {
		t.Fatalf("expected spawn-sized placeholder 15/120Gi, got %s/%s", req.Cpu(), req.Memory())
	}
	if len(store.pruned) != 1 {
		t.Fatalf("expected one spawn-log prune per reconcile, got %d", len(store.pruned))
	}
}

// An idle pool (no recent spawns) keeps exactly the floor: one warm slot,
// never zero while enabled, sized from the live fleet when the spawn window
// is empty.
func TestReconcileHeadroomIdleFloorsAtOne(t *testing.T) {
	cs := fake.NewSimpleClientset(mkWorkerPod("w1", "15", "120Gi"))
	emulateGenerateName(cs)
	store := &fakeSpawnLogStore{} // zero stats: no spawns in either window
	p := newHeadroomPool(cs, store)

	p.reconcileHeadroom(context.Background())

	pods := listHeadroomPods(t, cs)
	if len(pods) != 1 {
		t.Fatalf("idle pool: expected the floor of 1 placeholder, got %d", len(pods))
	}
	req := pods[0].Spec.Containers[0].Resources.Requests
	if req.Cpu().MilliValue() != 15000 || req.Memory().Value() != 120<<30 {
		t.Fatalf("expected live-fleet fallback size 15/120Gi, got %s/%s", req.Cpu(), req.Memory())
	}
}

// With no spawns AND no live workers (fresh cluster), the slot shape falls
// back to the pool's configured worker request.
func TestReconcileHeadroomFreshClusterFallsBackToDefaultShape(t *testing.T) {
	cs := fake.NewSimpleClientset()
	emulateGenerateName(cs)
	p := newHeadroomPool(cs, &fakeSpawnLogStore{})
	p.workerCPURequest = "8"
	p.workerMemoryRequest = "16Gi"

	p.reconcileHeadroom(context.Background())

	pods := listHeadroomPods(t, cs)
	if len(pods) != 1 {
		t.Fatalf("expected 1 placeholder, got %d", len(pods))
	}
	req := pods[0].Spec.Containers[0].Resources.Requests
	if req.Cpu().MilliValue() != 8000 || req.Memory().Value() != 16<<30 {
		t.Fatalf("expected configured fallback 8/16Gi, got %s/%s", req.Cpu(), req.Memory())
	}
}

// The fleet-relative cap bounds a spawn storm: peak burst 50 with 20 live
// workers is capped at ceil(25% × 20) = 5 — fleet size is the CEILING, never
// the target, so a big fleet alone (peak 0) still gets only the floor.
func TestReconcileHeadroomCapBoundsSpawnStorm(t *testing.T) {
	objs := []runtime.Object{}
	for i := 0; i < 20; i++ {
		objs = append(objs, mkWorkerPod(fmt.Sprintf("w%d", i), "15", "120Gi"))
	}
	cs := fake.NewSimpleClientset(objs...)
	emulateGenerateName(cs)
	store := &fakeSpawnLogStore{stats: configstore.HeadroomSpawnStats{PeakBurst: 50, MaxCPUMillis: 15000, MaxMemBytes: 120 << 30}}
	p := newHeadroomPool(cs, store)

	p.reconcileHeadroom(context.Background())

	if got := len(listHeadroomPods(t, cs)); got != 5 {
		t.Fatalf("spawn storm: expected cap ceil(25%% of 20)=5 placeholders, got %d", got)
	}

	// Ceiling-not-target: a large idle fleet with zero recent spawns keeps
	// only the floor (the idle-worker-leak amplification regression).
	cs2 := fake.NewSimpleClientset(objs...)
	emulateGenerateName(cs2)
	p2 := newHeadroomPool(cs2, &fakeSpawnLogStore{})
	p2.reconcileHeadroom(context.Background())
	if got := len(listHeadroomPods(t, cs2)); got != 1 {
		t.Fatalf("big idle fleet: expected floor of 1 placeholder, got %d", got)
	}
}

// A small fleet still gets the cap floor, so a burst can raise slots to 4
// even when 25% of the fleet would allow fewer.
func TestReconcileHeadroomCapFloorAllowsSmallFleetBurst(t *testing.T) {
	cs := fake.NewSimpleClientset(mkWorkerPod("w1", "15", "120Gi"))
	emulateGenerateName(cs)
	store := &fakeSpawnLogStore{stats: configstore.HeadroomSpawnStats{PeakBurst: 9, MaxCPUMillis: 15000, MaxMemBytes: 120 << 30}}
	p := newHeadroomPool(cs, store)

	p.reconcileHeadroom(context.Background())

	if got := len(listHeadroomPods(t, cs)); got != headroomCapFloor {
		t.Fatalf("expected cap floor %d placeholders, got %d", headroomCapFloor, got)
	}
}

// Scale-down is lazy: above-target placeholders are removed one per
// headroomScaleDownDelay, not all at once, and only after the target has
// stayed lower for the delay.
func TestReconcileHeadroomScaleDownIsLazy(t *testing.T) {
	cs := fake.NewSimpleClientset(mkWorkerPod("w1", "15", "120Gi"))
	emulateGenerateName(cs)
	store := &fakeSpawnLogStore{stats: configstore.HeadroomSpawnStats{PeakBurst: 3, MaxCPUMillis: 15000, MaxMemBytes: 120 << 30}}
	p := newHeadroomPool(cs, store)
	p.reconcileHeadroom(context.Background())
	if got := len(listHeadroomPods(t, cs)); got != 3 {
		t.Fatalf("setup: expected 3 placeholders, got %d", got)
	}

	// Burst ages out of the window: target drops to the floor.
	store.stats = configstore.HeadroomSpawnStats{MaxCPUMillis: 15000, MaxMemBytes: 120 << 30}

	// First tick below target only starts the timer — nothing deleted.
	p.reconcileHeadroom(context.Background())
	if got := len(listHeadroomPods(t, cs)); got != 3 {
		t.Fatalf("scale-down timer start: expected 3 placeholders still, got %d", got)
	}

	// Delay elapsed: exactly one slot removed per reconcile.
	p.headroomDownSince = time.Now().Add(-headroomScaleDownDelay)
	p.reconcileHeadroom(context.Background())
	if got := len(listHeadroomPods(t, cs)); got != 2 {
		t.Fatalf("after one delay: expected 2 placeholders, got %d", got)
	}
	p.headroomDownSince = time.Now().Add(-headroomScaleDownDelay)
	p.reconcileHeadroom(context.Background())
	if got := len(listHeadroomPods(t, cs)); got != 1 {
		t.Fatalf("after two delays: expected the floor of 1, got %d", got)
	}

	// At the floor the timer resets and nothing more is deleted.
	p.reconcileHeadroom(context.Background())
	if got := len(listHeadroomPods(t, cs)); got != 1 {
		t.Fatalf("at floor: expected 1 placeholder, got %d", got)
	}
	if !p.headroomDownSince.IsZero() {
		t.Fatal("expected scale-down timer to reset at target")
	}
}

// Placeholders whose shape drifted beyond the tolerance are replaced with the
// new shape in the same tick (capacity refresh — no scale-down hysteresis).
func TestReconcileHeadroomReplacesShapeDriftedPlaceholders(t *testing.T) {
	cs := fake.NewSimpleClientset(mkWorkerPod("w1", "15", "120Gi"))
	emulateGenerateName(cs)
	store := &fakeSpawnLogStore{stats: configstore.HeadroomSpawnStats{PeakBurst: 2, MaxCPUMillis: 8000, MaxMemBytes: 16 << 30}}
	p := newHeadroomPool(cs, store)
	p.reconcileHeadroom(context.Background())
	if got := len(listHeadroomPods(t, cs)); got != 2 {
		t.Fatalf("setup: expected 2 placeholders, got %d", got)
	}

	// Workers grew: the spawn-window max is now 15/120Gi (>20% drift).
	store.stats = configstore.HeadroomSpawnStats{PeakBurst: 2, MaxCPUMillis: 15000, MaxMemBytes: 120 << 30}
	p.reconcileHeadroom(context.Background())

	pods := listHeadroomPods(t, cs)
	if len(pods) != 2 {
		t.Fatalf("expected 2 placeholders after replacement, got %d", len(pods))
	}
	for _, pod := range pods {
		req := pod.Spec.Containers[0].Resources.Requests
		if req.Cpu().MilliValue() != 15000 || req.Memory().Value() != 120<<30 {
			t.Fatalf("expected replaced placeholder at 15/120Gi, got %s/%s", req.Cpu(), req.Memory())
		}
	}

	// Small drift (≤ tolerance) does NOT churn pods.
	store.stats = configstore.HeadroomSpawnStats{PeakBurst: 2, MaxCPUMillis: 16000, MaxMemBytes: 125 << 30}
	p.reconcileHeadroom(context.Background())
	pods = listHeadroomPods(t, cs)
	for _, pod := range pods {
		req := pod.Spec.Containers[0].Resources.Requests
		if req.Cpu().MilliValue() != 15000 {
			t.Fatalf("small drift must not replace placeholders, got cpu %s", req.Cpu())
		}
	}
}

// No placeholder PriorityClass configured = headroom disabled: existing
// placeholders converge to zero immediately (there is no other delete path).
func TestReconcileHeadroomDisabledDeletesPlaceholders(t *testing.T) {
	mkPlaceholder := func(name string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "default", Labels: map[string]string{"app": headroomPodLabel},
		}}
	}
	cs := fake.NewSimpleClientset(mkPlaceholder("duckgres-placeholder-x-1"), mkPlaceholder("duckgres-placeholder-x-2"))
	p := &K8sWorkerPool{clientset: cs, namespace: "default"} // no placeholderPriorityClassName

	p.reconcileHeadroom(context.Background())

	if got := len(listHeadroomPods(t, cs)); got != 0 {
		t.Fatalf("expected disabled headroom to delete all placeholders, got %d", got)
	}
}

// A transient spawn-stats error skips the tick without touching pods.
func TestReconcileHeadroomStatsErrorSkipsTick(t *testing.T) {
	cs := fake.NewSimpleClientset(mkWorkerPod("w1", "15", "120Gi"))
	emulateGenerateName(cs)
	store := &fakeSpawnLogStore{stats: configstore.HeadroomSpawnStats{PeakBurst: 2, MaxCPUMillis: 15000, MaxMemBytes: 120 << 30}}
	p := newHeadroomPool(cs, store)
	p.reconcileHeadroom(context.Background())
	if got := len(listHeadroomPods(t, cs)); got != 2 {
		t.Fatalf("setup: expected 2 placeholders, got %d", got)
	}

	store.statsErr = fmt.Errorf("config store down")
	p.reconcileHeadroom(context.Background())
	if got := len(listHeadroomPods(t, cs)); got != 2 {
		t.Fatalf("stats error must not change placeholders, got %d", got)
	}
}

func TestLiveWorkerStats(t *testing.T) {
	// Counts live worker pods and takes the componentwise max of their
	// requests; excludes terminating/terminal pods and non-worker pods
	// (placeholders must never feed their own sizing).
	terminating := mkWorkerPod("w-going", "64", "500Gi")
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	cs := fake.NewSimpleClientset(
		mkWorkerPod("w1", "2", "4Gi"),
		mkWorkerPod("w2", "15", "120Gi"),
		mkWorkerPod("w3", "16", "8Gi"), // cpu max and mem max from different pods
		terminating,
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "ph", Namespace: "default", Labels: map[string]string{"app": headroomPodLabel}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: "pause",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("46"),
					corev1.ResourceMemory: resource.MustParse("360Gi"),
				}},
			}}},
		},
	)
	p := &K8sWorkerPool{clientset: cs, namespace: "default"}
	count, cpu, mem, err := p.liveWorkerStats(context.Background())
	if err != nil {
		t.Fatalf("liveWorkerStats: %v", err)
	}
	if count != 3 {
		t.Fatalf("live worker count = %d, want 3", count)
	}
	if cpu != 16000 {
		t.Fatalf("max cpu = %d millicores, want 16000", cpu)
	}
	if want := int64(120) << 30; mem != want {
		t.Fatalf("max mem = %d, want %d", mem, want)
	}
}

func TestShapeDrifted(t *testing.T) {
	tests := []struct {
		existing, planned int64
		want              bool
	}{
		{15000, 15000, false},
		{15000, 16000, false}, // ~6% under: keep
		{15000, 18000, false}, // exactly 20% over existing→planned; diff 3000 = 20% of planned: keep
		{15000, 20000, true},  // 25% under: replace
		{46000, 15000, true},  // legacy node-sized placeholder vs worker-sized plan: replace
		{0, 15000, true},
		{15000, 0, true},
	}
	for _, tt := range tests {
		if got := shapeDrifted(tt.existing, tt.planned); got != tt.want {
			t.Fatalf("shapeDrifted(%d, %d) = %v, want %v", tt.existing, tt.planned, got, tt.want)
		}
	}
}

// The spawn path logs each pod's shape best-effort for the headroom sizing.
func TestRecordSpawnForHeadroom(t *testing.T) {
	store := &fakeSpawnLogStore{}
	p := &K8sWorkerPool{runtimeStore: store, orgID: "org-a"}
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("15"),
			corev1.ResourceMemory: resource.MustParse("120Gi"),
		}},
	}}}}
	p.recordSpawnForHeadroom(pod)
	if len(store.recorded) != 1 {
		t.Fatalf("expected 1 recorded spawn, got %d", len(store.recorded))
	}
	got := store.recorded[0]
	if got.orgID != "org-a" || got.cpuMillis != 15000 || got.memBytes != 120<<30 {
		t.Fatalf("recorded spawn = %+v, want org-a/15000/%d", got, int64(120)<<30)
	}

	// A store without the spawn-log surface (plain RuntimeWorkerStore) is a
	// silent no-op — never a panic or spawn failure.
	p2 := &K8sWorkerPool{}
	p2.recordSpawnForHeadroom(pod)
}
