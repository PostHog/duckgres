//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
)

// reshardReconciler is the LEADER-ONLY janitor for reshard runner pods. The
// CP never executes reshard steps in-process anymore — every operation runs in
// its own duckgres-reshard-op-<id> pod — so someone has to notice a runner pod
// that died (node loss, OOM, eviction) or was never created, and reap pods
// whose operation finished. That someone is this loop, attached under the
// janitor leader lease so exactly one CP replica runs it:
//
//   - an op that should be executing but has NO live pod — pending older than
//     a grace (pod create raced/failed), or running with a stale heartbeat
//     (>5m, the runner died) — gets its pod (re)spawned. The new pod re-claims
//     via the standard stale-heartbeat CAS (epoch bump fences the old pod).
//   - after maxRespawnAttempts consecutive respawn failures the op is force-
//     failed with a clear operator-facing error instead of thrashing forever.
//   - a TERMINAL op with a still-existing pod gets the pod deleted (Succeeded/
//     Failed pods immediately; a still-running pod only after a grace, to let
//     a just-finished runner exit on its own).
type reshardReconciler struct {
	store   reshardReconcilerStore
	spawner *ReshardPodSpawner

	interval         time.Duration // reconcile tick (leader loop)
	pendingGrace     time.Duration // how long a pending op may sit before we (re)spawn
	staleAfter       time.Duration // running-op heartbeat staleness (matches the runner's claim CAS)
	terminalPodGrace time.Duration // how long a terminal op's still-running pod is left to exit on its own

	maxRespawnAttempts int
}

// reshardReconcilerStore is the config-store surface the reconciler needs.
type reshardReconcilerStore interface {
	ListActiveReshardOperations() ([]configstore.ReshardOperation, error)
	GetReshardOperation(id int64) (*configstore.ReshardOperation, error)
	IncrementReshardRespawnAttempts(id int64) (int64, error)
	AppendReshardLog(opID int64, level, message string) error
}

func newReshardReconciler(store reshardReconcilerStore, spawner *ReshardPodSpawner) *reshardReconciler {
	return &reshardReconciler{
		store:              store,
		spawner:            spawner,
		interval:           30 * time.Second,
		pendingGrace:       2 * time.Minute,
		staleAfter:         5 * time.Minute,
		terminalPodGrace:   5 * time.Minute,
		maxRespawnAttempts: 3,
	}
}

// Run is the leader loop body (JanitorLeaderManager.AttachLeaderLoop): ticks
// until leadership is lost / ctx done.
func (r *reshardReconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcileOnce(ctx)
		}
	}
}

func (r *reshardReconciler) reconcileOnce(ctx context.Context) {
	if r.spawner == nil {
		return
	}
	ops, err := r.store.ListActiveReshardOperations()
	if err != nil {
		slog.Warn("Reshard reconciler: listing active operations failed.", "error", err)
		return
	}
	active := make(map[int64]struct{}, len(ops))
	stale := 0
	recoveryRequired := 0
	for i := range ops {
		op := &ops[i]
		active[op.ID] = struct{}{}
		if r.opNeedsPod(op) {
			stale++
		}
		if op.RespawnAttempts > int64(r.maxRespawnAttempts) {
			recoveryRequired++
		}
		r.reconcileActiveOp(ctx, op)
	}
	reshardActiveOperationsGauge.Set(float64(len(ops)))
	reshardStaleOperationsGauge.Set(float64(stale))
	reshardRecoveryRequiredGauge.Set(float64(recoveryRequired))
	r.reapTerminalPods(ctx, active)
}

// opNeedsPod reports whether op should have a live runner pod by now: a
// pending op past the spawn grace (the start handler's spawn failed/raced or
// this is an old-style pending row), or a running op whose runner stopped
// heartbeating.
func (r *reshardReconciler) opNeedsPod(op *configstore.ReshardOperation) bool {
	now := time.Now().UTC()
	switch op.State {
	case configstore.ReshardStatePending:
		return now.Sub(op.CreatedAt) > r.pendingGrace
	case configstore.ReshardStateRunning:
		return op.HeartbeatAt == nil || now.Sub(*op.HeartbeatAt) > r.staleAfter
	default:
		return false
	}
}

func (r *reshardReconciler) reconcileActiveOp(ctx context.Context, op *configstore.ReshardOperation) {
	if !r.opNeedsPod(op) {
		return
	}
	pod, err := r.spawner.GetReshardPod(ctx, op.ID)
	if err != nil {
		slog.Warn("Reshard reconciler: reading runner pod failed.", "op", op.ID, "error", err)
		return
	}
	if pod != nil && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
		if spawnedAt, err := time.Parse(time.RFC3339Nano, pod.Annotations[reshardPodSpawnedAtAnnotation]); err == nil && time.Since(spawnedAt) <= r.pendingGrace {
			// A replacement was created recently and needs time to schedule, start,
			// and claim the stale row. Do not replace it again on every tick.
			return
		}
	}
	// Reaching this point means the operation itself is stale. Pod phase is not
	// a liveness signal: a wedged process remains Running forever. Replace even
	// a non-terminal pod; SpawnReshardPod deletes the deterministic old pod
	// before creating its successor. Fresh-heartbeat operations never reach
	// this branch, so healthy runners are left alone.
	if op.RespawnAttempts > int64(r.maxRespawnAttempts) {
		return
	}

	attempt, err := r.store.IncrementReshardRespawnAttempts(op.ID)
	if err != nil {
		slog.Warn("Reshard reconciler: recording durable respawn attempt failed.", "op", op.ID, "error", err)
		return
	}
	if attempt > int64(r.maxRespawnAttempts) {
		if attempt == int64(r.maxRespawnAttempts+1) {
			r.markOpRecoveryRequired(op)
		}
		return
	}
	slog.Info("Reshard reconciler: (re)spawning runner pod.",
		"op", op.ID, "state", op.State, "attempt", attempt, "pod_was", podPhaseOrAbsent(pod))
	_ = r.store.AppendReshardLog(op.ID, "warn",
		fmt.Sprintf("runner pod missing or dead (%s) — respawning %s (attempt %d/%d)",
			podPhaseOrAbsent(pod), ReshardPodName(op.ID), attempt, r.maxRespawnAttempts))
	if err := r.spawner.SpawnReshardPod(ctx, op); err != nil {
		reshardRespawnCounter.WithLabelValues("failed").Inc()
		slog.Warn("Reshard reconciler: respawn failed.", "op", op.ID, "error", err)
		_ = r.store.AppendReshardLog(op.ID, "error", "respawning runner pod failed: "+err.Error())
		return
	}
	reshardRespawnCounter.WithLabelValues("spawned").Inc()
	// A successful spawn resets nothing: the counter counts consecutive
	// reconciler interventions for this op, so a pod that keeps dying still
	// converges to the failure below instead of respawning forever. The
	// counter is dropped once the op leaves the active set.
}

// markOpRecoveryRequired leaves the operation active and therefore keeps the
// connection barrier intact. Terminal-failing without rollback creates an
// unrecoverable state: cancel/re-run cannot operate on a blocked warehouse.
func (r *reshardReconciler) markOpRecoveryRequired(op *configstore.ReshardOperation) {
	msg := fmt.Sprintf("reshard runner pod died/failed to start %d times — automatic respawn paused with the operation ACTIVE and the warehouse safely blocked. "+
		"Repair the %s scheduling/startup failure, reset respawn_attempts to 0, and let the reconciler resume; see docs/runbooks/resharding.md.", r.maxRespawnAttempts, ReshardPodName(op.ID))
	slog.Error("Reshard reconciler: operation requires manual recovery after exhausted respawns.", "op", op.ID)
	_ = r.store.AppendReshardLog(op.ID, "error", msg)
}

// reapTerminalPods deletes runner pods whose operation is no longer active:
// exited pods immediately, still-running ones only after the op has been
// terminal for terminalPodGrace (a just-finished runner exits by itself).
func (r *reshardReconciler) reapTerminalPods(ctx context.Context, active map[int64]struct{}) {
	pods, err := r.spawner.ListReshardPods(ctx)
	if err != nil {
		slog.Warn("Reshard reconciler: listing runner pods failed.", "error", err)
		return
	}
	for i := range pods {
		pod := &pods[i]
		opID, err := strconv.ParseInt(pod.Labels[reshardPodOpIDLabel], 10, 64)
		if err != nil {
			continue
		}
		if _, isActive := active[opID]; isActive {
			continue
		}
		exited := pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
		if !exited && !r.terminalOpOlderThanGrace(opID) {
			continue
		}
		if err := r.spawner.DeleteReshardPod(ctx, opID); err != nil {
			slog.Warn("Reshard reconciler: deleting finished runner pod failed.", "op", opID, "error", err)
			continue
		}
		slog.Info("Reshard reconciler: reaped runner pod of finished operation.", "op", opID, "phase", pod.Status.Phase)
	}
}

// terminalOpOlderThanGrace reports whether opID's operation finished more than
// terminalPodGrace ago (or does not exist at all — an orphan pod).
func (r *reshardReconciler) terminalOpOlderThanGrace(opID int64) bool {
	op, err := r.store.GetReshardOperation(opID)
	if err != nil {
		// Row gone (or unreadable): treat a row-less pod as an orphan only via
		// the exited-pod path; be conservative here.
		return false
	}
	if !op.State.Terminal() || op.FinishedAt == nil {
		return false
	}
	return time.Since(*op.FinishedAt) > r.terminalPodGrace
}

func podPhaseOrAbsent(pod *corev1.Pod) string {
	if pod == nil {
		return "no pod"
	}
	return "pod phase " + string(pod.Status.Phase)
}
