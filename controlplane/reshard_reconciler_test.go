//go:build kubernetes

package controlplane

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
)

// fakeReconcilerStore fakes the reconciler's config-store surface.
type fakeReconcilerStore struct {
	ops  map[int64]*configstore.ReshardOperation
	logs []string
}

func (f *fakeReconcilerStore) ListActiveReshardOperations() ([]configstore.ReshardOperation, error) {
	var out []configstore.ReshardOperation
	for _, op := range f.ops {
		if !op.State.Terminal() {
			out = append(out, *op)
		}
	}
	return out, nil
}

func (f *fakeReconcilerStore) GetReshardOperation(id int64) (*configstore.ReshardOperation, error) {
	op, ok := f.ops[id]
	if !ok {
		return nil, context.Canceled // any error
	}
	cp := *op
	return &cp, nil
}

func (f *fakeReconcilerStore) ClaimReshardOperation(id int64, runnerCP string, staleAfter time.Duration) (*configstore.ReshardOperation, error) {
	op, ok := f.ops[id]
	if !ok || op.State.Terminal() {
		return nil, nil
	}
	if op.State == configstore.ReshardStateRunning && op.HeartbeatAt != nil && time.Since(*op.HeartbeatAt) <= staleAfter {
		return nil, nil
	}
	op.State = configstore.ReshardStateRunning
	op.RunnerCP = runnerCP
	op.RunnerEpoch++
	cp := *op
	return &cp, nil
}

func (f *fakeReconcilerStore) FinishReshardOperation(id int64, runnerCP string, epoch int64, state configstore.ReshardState, errMsg string) error {
	op := f.ops[id]
	op.State = state
	op.Error = errMsg
	now := time.Now().UTC()
	op.FinishedAt = &now
	return nil
}

func (f *fakeReconcilerStore) AppendReshardLog(_ int64, level, message string) error {
	f.logs = append(f.logs, level+": "+message)
	return nil
}

func staleRunningOp(id int64) *configstore.ReshardOperation {
	hb := time.Now().UTC().Add(-time.Hour)
	return &configstore.ReshardOperation{
		ID:          id,
		OrgID:       "acme",
		State:       configstore.ReshardStateRunning,
		RunnerCP:    "dead-pod",
		RunnerEpoch: 1,
		HeartbeatAt: &hb,
		CreatedAt:   time.Now().UTC().Add(-2 * time.Hour),
		PasswordURL: "http://192.0.2.9:8080/api/v1/reshards/1/password",
	}
}

func testReconciler(store *fakeReconcilerStore, cs *k8sfake.Clientset) *reshardReconciler {
	r := newReshardReconciler(store, NewReshardPodSpawner(cs, "duckgres", "duckgres-control-plane-abc-xyz", "", ""))
	return r
}

// TestReconcilerRespawnsDeadRunner pins: a running op with a stale heartbeat
// and no live pod gets its runner pod respawned, carrying the persisted
// password URL (so the handoff still works while the stashing replica lives).
func TestReconcilerRespawnsDeadRunner(t *testing.T) {
	store := &fakeReconcilerStore{ops: map[int64]*configstore.ReshardOperation{1: staleRunningOp(1)}}
	cs := k8sfake.NewSimpleClientset(fakeCPPod())
	r := testReconciler(store, cs)

	r.reconcileOnce(context.Background())

	pod, err := cs.CoreV1().Pods("duckgres").Get(context.Background(), "duckgres-reshard-op-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("runner pod not respawned: %v", err)
	}
	env := envByName(pod.Spec.Containers[0].Env)
	if env["DUCKGRES_RESHARD_PASSWORD_URL"].Value != store.ops[1].PasswordURL {
		t.Fatalf("respawned pod missing the persisted password URL: %q", env["DUCKGRES_RESHARD_PASSWORD_URL"].Value)
	}

	// A live pod now exists: the next tick must NOT replace it.
	r.reconcileOnce(context.Background())
	if got := r.respawnAttempts[1]; got != 1 {
		t.Fatalf("respawn attempts = %d after a successful spawn + live pod, want 1", got)
	}
}

// TestReconcilerReplacesWedgedRunningPod covers the failure mode where the
// process remains Running but has stopped heartbeating. Pod phase is not proof
// of liveness: the stale lease must cause a replacement so takeover can occur.
func TestReconcilerReplacesWedgedRunningPod(t *testing.T) {
	op := staleRunningOp(9)
	wedged := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "duckgres-reshard-op-9", Namespace: "duckgres",
			Labels: map[string]string{"app": "duckgres-reshard", "duckgres-op-id": "9"},
			UID:    "wedged-runner",
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	store := &fakeReconcilerStore{ops: map[int64]*configstore.ReshardOperation{9: op}}
	cs := k8sfake.NewSimpleClientset(fakeCPPod(), wedged)
	r := testReconciler(store, cs)

	r.reconcileOnce(context.Background())

	pod, err := cs.CoreV1().Pods("duckgres").Get(context.Background(), wedged.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("replacement runner missing: %v", err)
	}
	if pod.UID == wedged.UID {
		t.Fatal("stale-heartbeat Running pod was left in place")
	}
	if got := r.respawnAttempts[9]; got != 1 {
		t.Fatalf("respawn attempts = %d, want 1", got)
	}
}

// TestReconcilerLeavesHealthyOpsAlone pins: fresh-heartbeat running ops and
// young pending ops are untouched.
func TestReconcilerLeavesHealthyOpsAlone(t *testing.T) {
	now := time.Now().UTC()
	store := &fakeReconcilerStore{ops: map[int64]*configstore.ReshardOperation{
		1: {ID: 1, State: configstore.ReshardStateRunning, HeartbeatAt: &now, CreatedAt: now},
		2: {ID: 2, State: configstore.ReshardStatePending, CreatedAt: now},
	}}
	cs := k8sfake.NewSimpleClientset(fakeCPPod())
	r := testReconciler(store, cs)

	r.reconcileOnce(context.Background())

	pods, _ := cs.CoreV1().Pods("duckgres").List(context.Background(), metav1.ListOptions{LabelSelector: "app=duckgres-reshard"})
	if len(pods.Items) != 0 {
		t.Fatalf("reconciler spawned pods for healthy ops: %d", len(pods.Items))
	}
}

// TestReconcilerRespawnsStalePending pins: a pending op older than the grace
// with no pod (the start handler's spawn was lost) gets a pod.
func TestReconcilerRespawnsStalePending(t *testing.T) {
	store := &fakeReconcilerStore{ops: map[int64]*configstore.ReshardOperation{
		3: {ID: 3, State: configstore.ReshardStatePending, CreatedAt: time.Now().UTC().Add(-time.Hour)},
	}}
	cs := k8sfake.NewSimpleClientset(fakeCPPod())
	r := testReconciler(store, cs)

	r.reconcileOnce(context.Background())

	if _, err := cs.CoreV1().Pods("duckgres").Get(context.Background(), "duckgres-reshard-op-3", metav1.GetOptions{}); err != nil {
		t.Fatalf("pod not spawned for stale pending op: %v", err)
	}
}

// TestReconcilerRetryBoundForceFails pins the bound: after maxRespawnAttempts
// interventions the op is force-failed (claimed + finished) with the clear
// operator-facing error, and the counter is dropped.
func TestReconcilerRetryBoundForceFails(t *testing.T) {
	store := &fakeReconcilerStore{ops: map[int64]*configstore.ReshardOperation{1: staleRunningOp(1)}}
	// No CP pod in the fake cluster → every spawn fails (own-pod read error).
	cs := k8sfake.NewSimpleClientset()
	r := testReconciler(store, cs)

	for i := 0; i < r.maxRespawnAttempts; i++ {
		r.reconcileOnce(context.Background())
		if store.ops[1].State != configstore.ReshardStateRunning {
			t.Fatalf("op force-failed too early (attempt %d)", i+1)
		}
	}
	r.reconcileOnce(context.Background())
	if store.ops[1].State != configstore.ReshardStateFailed {
		t.Fatalf("op state = %s after exhausted respawns, want failed", store.ops[1].State)
	}
	if !strings.Contains(store.ops[1].Error, "giving up") {
		t.Fatalf("force-fail error = %q", store.ops[1].Error)
	}
	if _, tracked := r.respawnAttempts[1]; tracked {
		t.Fatal("respawn counter not dropped after force-fail")
	}
}

// TestReconcilerReapsTerminalPods pins the cleanup half: a pod whose op is
// terminal is deleted once it exited (Succeeded/Failed) or after the terminal
// grace; a pod of an ACTIVE op is never reaped.
func TestReconcilerReapsTerminalPods(t *testing.T) {
	finishedLongAgo := time.Now().UTC().Add(-time.Hour)
	justFinished := time.Now().UTC()
	store := &fakeReconcilerStore{ops: map[int64]*configstore.ReshardOperation{
		// exited pod, terminal op → reap
		1: {ID: 1, State: configstore.ReshardStateSucceeded, FinishedAt: &justFinished},
		// still-Running pod, terminal for > grace → reap
		2: {ID: 2, State: configstore.ReshardStateFailed, FinishedAt: &finishedLongAgo},
		// still-Running pod, JUST terminal → leave (exits on its own)
		3: {ID: 3, State: configstore.ReshardStateCancelled, FinishedAt: &justFinished},
		// active op with fresh heartbeat → leave
		4: staleRunningOp(4),
	}}
	hb := time.Now().UTC()
	store.ops[4].HeartbeatAt = &hb

	mkPod := func(id string, phase corev1.PodPhase) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "duckgres-reshard-op-" + id,
				Namespace: "duckgres",
				Labels:    map[string]string{"app": "duckgres-reshard", "duckgres-op-id": id},
			},
			Status: corev1.PodStatus{Phase: phase},
		}
	}
	cs := k8sfake.NewSimpleClientset(
		fakeCPPod(),
		mkPod("1", corev1.PodSucceeded),
		mkPod("2", corev1.PodRunning),
		mkPod("3", corev1.PodRunning),
		mkPod("4", corev1.PodRunning),
	)
	r := testReconciler(store, cs)

	r.reconcileOnce(context.Background())

	for id, wantGone := range map[string]bool{"1": true, "2": true, "3": false, "4": false} {
		_, err := cs.CoreV1().Pods("duckgres").Get(context.Background(), "duckgres-reshard-op-"+id, metav1.GetOptions{})
		gone := err != nil
		if gone != wantGone {
			t.Errorf("pod op-%s gone=%t, want %t", id, gone, wantGone)
		}
	}
}
