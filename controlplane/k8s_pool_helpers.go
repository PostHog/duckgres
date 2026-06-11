//go:build kubernetes

package controlplane

import (
	stderrors "errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
)

// --- Helpers ---

// allocateWorkerID returns the next worker ID, using the shared generator
// if configured (multi-tenant mode) or the pool's internal counter.
// Must be called with p.mu held.
func (p *K8sWorkerPool) allocateWorkerIDLocked() int {
	if p.workerIDGenerator != nil {
		return p.workerIDGenerator()
	}
	id := p.nextWorkerID
	p.nextWorkerID++
	return id
}

func (p *K8sWorkerPool) allocateBackgroundSpawnIDLocked() int {
	if p.runtimeStore != nil {
		return 0
	}
	return p.allocateWorkerIDLocked()
}

// persistWorkerRecord upserts the record and returns the underlying
// error (including ErrWorkerRecordUpsertFenceMiss when the CP no longer
// owns the lease). Callers that don't care about the result discard it
// with `_ =`; markWorkerRetiredLocked uses it to gate metric emission
// so retire_local samples reflect transitions that actually persisted.
func (p *K8sWorkerPool) persistWorkerRecord(record *configstore.WorkerRecord) error {
	if p.runtimeStore == nil || record == nil {
		return nil
	}
	err := p.runtimeStore.UpsertWorkerRecord(record)
	if err == nil {
		return nil
	}
	// Fence misses are expected when a peer CP has already advanced the
	// worker's lease (terminal state, newer owner_epoch, or different
	// owner at the same epoch). They prove the fence is doing its job —
	// log at Debug so they don't masquerade as persistence failures.
	if stderrors.Is(err, configstore.ErrWorkerRecordUpsertFenceMiss) {
		slog.Debug("Worker runtime upsert fenced by newer lease.", "worker_id", record.WorkerID, "state", record.State, "error", err)
		return err
	}
	slog.Warn("Persisting worker runtime record failed.", "worker_id", record.WorkerID, "state", record.State, "error", err)
	return err
}

func (p *K8sWorkerPool) workerRecordFor(id int, worker *ManagedWorker, ownerEpoch int64, state configstore.WorkerState, retireReason string, activationStartedAt *time.Time) *configstore.WorkerRecord {
	record := &configstore.WorkerRecord{
		WorkerID:          id,
		PodName:           p.podNameForWorker(id),
		Image:             p.workerImage,
		State:             state,
		OwnerCPInstanceID: p.cpInstanceID,
		OwnerEpoch:        ownerEpoch,
		LastHeartbeatAt:   time.Now(),
		RetireReason:      retireReason,
	}
	if activationStartedAt != nil {
		startedAt := *activationStartedAt
		record.ActivationStartedAt = &startedAt
	}
	if worker == nil {
		// OwnerCPInstanceID is already this CP's id from the struct literal
		// above. Stamping idle workers with the creating CP keeps
		// last_heartbeat_at fresh via the CP heartbeat — without it, the
		// orphan reconciler matches case (2) (NULLIF(owner_cp_instance_id,
		// '') IS NULL AND last_heartbeat_at <= before) the moment the row
		// crosses orphanGrace, so idle workers get reaped on a ~30s loop.
		return record
	}
	record.PodName = p.workerPodName(worker)
	if owner := worker.OwnerCPInstanceID(); owner != "" {
		record.OwnerCPInstanceID = owner
	}
	if worker.image != "" {
		record.Image = worker.image
	}
	record.ProfileCPU = worker.profile.CPU
	record.ProfileMemory = worker.profile.Memory
	record.TTLMinutes = int(worker.profile.TTL.Minutes())
	if assignment := worker.SharedState().Assignment; assignment != nil {
		record.OrgID = assignment.OrgID
	}
	if state == configstore.WorkerStateIdle {
		record.OrgID = ""
	}
	return record
}

func (p *K8sWorkerPool) healthCheckPayloadForWorker(worker *ManagedWorker) server.WorkerHealthCheckPayload {
	return p.healthCheckPayloadForLease(p.workerLeaseSnapshot(worker))
}

func (p *K8sWorkerPool) healthCheckPayloadForLease(lease workerLeaseSnapshot) server.WorkerHealthCheckPayload {
	payload := server.WorkerHealthCheckPayload{
		WorkerControlMetadata: server.WorkerControlMetadata{
			WorkerID:     lease.workerID,
			OwnerEpoch:   lease.ownerEpoch,
			CPInstanceID: lease.ownerCPInstanceID,
		},
	}
	return payload
}

// podNameForWorker returns the pod name for a given worker ID,
// including the org ID if set (multi-tenant mode).
func (p *K8sWorkerPool) podNameForWorker(id int) string {
	return fmt.Sprintf("%s-%d", p.workerPodNamePrefix(), id)
}

func (p *K8sWorkerPool) workerPodName(worker *ManagedWorker) string {
	if worker != nil && worker.PodName() != "" {
		return worker.PodName()
	}
	if worker == nil {
		return ""
	}
	return p.podNameForWorker(worker.ID)
}

// workerPodNamePrefix builds the worker pod name prefix:
// "duckgres-worker[-<org>]-<cp-replicaset-hash>". A fixed "duckgres-worker"
// head makes worker pods sort/scan together in `kubectl get pods`; the CP
// ReplicaSet hash identifies which control-plane build spawned the worker
// (useful mid-rollout, when the version-mismatch reaper is churning the
// fleet). Uniqueness across CP replicas rides on worker IDs, which are
// cluster-unique (config-store issued) — the hash is shared by all replicas
// of one Deployment revision, exactly like the old <cp-pod-minus-suffix>
// prefix was.
func (p *K8sWorkerPool) workerPodNamePrefix() string {
	if p.orgID != "" {
		return fmt.Sprintf("duckgres-worker-%s-%s", p.orgID, cpReplicaSetHash(p.cpID))
	}
	return fmt.Sprintf("duckgres-worker-%s", cpReplicaSetHash(p.cpID))
}

// cpReplicaSetHash extracts the Deployment ReplicaSet hash segment from a CP
// pod name ("duckgres-control-plane-6f877c7779-abcde" -> "6f877c7779"). A
// name without a recognizable random pod-hash suffix (local runs, tests) is
// returned whole so the prefix stays unique per CP instance.
func cpReplicaSetHash(cpID string) string {
	base := trimK8sPodHashSuffix(cpID)
	if base == cpID {
		return cpID
	}
	if i := strings.LastIndex(base, "-"); i > 0 {
		return base[i+1:]
	}
	return base
}

// trimK8sPodHashSuffix removes the trailing 5-character random pod-hash
// segment from a K8s deployment pod name (e.g. "duckgres-7b667c7bfd-7745x"
// → "duckgres-7b667c7bfd"). Names that don't end in a plausible pod-hash
// segment are returned unchanged.
func trimK8sPodHashSuffix(name string) string {
	idx := strings.LastIndex(name, "-")
	if idx <= 0 {
		return name
	}
	suffix := name[idx+1:]
	if len(suffix) != 5 {
		return name
	}
	for _, r := range suffix {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return name
		}
	}
	return name[:idx]
}

// workerLogAttrs returns the standard identity attrs for a worker log line:
// worker id, pod name, and the assigned org (when tenant-bound). Every log
// line in a worker's lifecycle should carry these so the full history of one
// worker — or one org's workers — is filterable without joins.
func workerLogAttrs(w *ManagedWorker) []any {
	if w == nil {
		return nil
	}
	attrs := []any{"worker", w.ID}
	if pod := w.PodName(); pod != "" {
		attrs = append(attrs, "worker_pod", pod)
	}
	if a := w.SharedState().Assignment; a != nil && a.OrgID != "" {
		attrs = append(attrs, "org", a.OrgID)
	}
	return attrs
}

// logw returns a logger pre-scoped with the worker's identity attrs (see
// workerLogAttrs), resolved by id. For workers no longer tracked locally it
// still carries the id, so the line stays attributable.
func (p *K8sWorkerPool) logw(id int) *slog.Logger {
	if w, ok := p.Worker(id); ok {
		return slog.With(workerLogAttrs(w)...)
	}
	return slog.With("worker", id)
}
