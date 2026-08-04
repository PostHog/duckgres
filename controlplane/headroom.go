//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// headroomPodLabel marks the low-priority placeholder pods this controller owns.
const headroomPodLabel = "duckgres-headroom"

// Dynamic headroom: the controller keeps N preemptible placeholder pods
// ("slots") of a worker-sized shape, both derived from the worker spawn log
// (duckgres_worker_spawn_log) — no configured count or size. Enabled iff a
// placeholder PriorityClass is configured (placeholders WITHOUT a priority
// below the worker class would never be preempted, so the class is a hard
// prerequisite anyway); disabled pools converge their placeholders to zero.
//
//   - Slot COUNT follows demand: the peak number of spawns in any one
//     headroomBurstBucket of the last headroomBurstWindow. Serial spawns reuse
//     one warm slot; only burst concurrency needs parallel warm capacity.
//     Floored at 1 (never zero while enabled — an idle pool keeps one warm
//     slot so the first spawn after a quiet period isn't fully cold) and
//     capped RELATIVE to the live fleet (headroomCapFleetPct, with
//     headroomCapFloor so a tiny fleet can still absorb a small burst). The
//     fleet-relative cap grows with the deployment — nobody has to bump a
//     constant — while bounding a spawn-storm bug's placeholder cost. Fleet
//     size is only the CEILING, never the target, so an idle-worker leak
//     cannot pull the placeholder count up (the amplification failure of the
//     retired demand-proportional mode).
//   - Slot SIZE follows the largest worker shape actually spawned in the
//     (longer) headroomSizeWindow, componentwise. Client-sized (GUC) workers
//     can exceed any configured maximum, so sizing from observed spawns is
//     honest where a configured shape would drift. A spawn larger than one
//     slot preempts several placeholders and stays partially warm — wrong
//     sizing degrades to Karpenter latency, never to a failure.
//
// Scale-up is immediate; scale-down is lazy (one slot per
// headroomScaleDownDelay once the target has stayed lower) so shape/target
// flapping never thrashes pods.
const (
	headroomBurstWindow    = time.Hour          // count lookback
	headroomBurstBucket    = 5 * time.Minute    // ≈ Karpenter node-provision latency
	headroomSizeWindow     = 7 * 24 * time.Hour // size lookback (and spawn-log retention)
	headroomSlotFloor      = 1                  // never zero while enabled
	headroomCapFloor       = 4                  // minimum ceiling, so small fleets can burst
	headroomCapFleetPct    = 25                 // ceiling = this % of live worker pods
	headroomScaleDownDelay = 10 * time.Minute   // one slot removed per delay while above target
	headroomShapeDriftPct  = 20                 // replace placeholders whose shape drifts more than this
)

// workerSpawnLogStore is the optional spawn-log surface of the runtime store
// (satisfied by *configstore.ConfigStore). Asserted at use sites so fake
// stores in tests — and any store without the table — simply opt out.
type workerSpawnLogStore interface {
	RecordWorkerSpawn(orgID string, cpuMillis, memBytes int64) error
	HeadroomSpawnStats(burstWindow, bucket, sizeWindow time.Duration) (configstore.HeadroomSpawnStats, error)
	PruneWorkerSpawnLog(olderThan time.Time) (int64, error)
}

var (
	headroomSlotsDesiredGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "duckgres_headroom_slots_desired",
		Help: "Placeholder slots the dynamic headroom controller is currently targeting (clamp(peak burst, floor, cap)).",
	})
	headroomSlotsCapGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "duckgres_headroom_slots_cap",
		Help: "Fleet-relative ceiling on headroom slots (max(cap floor, pct of live workers)). Desired pinned at the cap means demand wants more headroom than the fleet ratio allows.",
	})
	headroomPeakBurstGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "duckgres_headroom_peak_spawn_burst",
		Help: "Peak worker spawns in any one burst bucket of the lookback window — the demand signal behind the desired slot count.",
	})
	headroomSlotCPUGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "duckgres_headroom_slot_cpu_millicores",
		Help: "CPU request of one headroom placeholder slot.",
	})
	headroomSlotMemoryGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "duckgres_headroom_slot_memory_bytes",
		Help: "Memory request of one headroom placeholder slot.",
	})
)

// recordSpawnForHeadroom appends this spawn's shape to the spawn log, which
// the leader-only reconcile reads to size the placeholder pool. Best-effort:
// a failure is logged and dropped — it must never fail the spawn (the slot
// count just undercounts by one).
func (p *K8sWorkerPool) recordSpawnForHeadroom(pod *corev1.Pod) {
	store, ok := p.runtimeStore.(workerSpawnLogStore)
	if !ok {
		return
	}
	req := pod.Spec.Containers[0].Resources.Requests
	if err := store.RecordWorkerSpawn(p.orgID, req.Cpu().MilliValue(), req.Memory().Value()); err != nil {
		slog.Warn("Headroom: failed to record worker spawn.", "error", err)
	}
}

// headroomPlan is one reconcile tick's resolved target: how many placeholder
// slots, of what shape, and the inputs behind it (for logging/metrics).
type headroomPlan struct {
	desired   int
	cap       int
	peakBurst int
	cpu       resource.Quantity
	mem       resource.Quantity
}

// reconcileHeadroom drives the placeholder pods toward the dynamic target.
// Leader-gated (called from the janitor, which runs only on the elected CP).
// Always runs (even when headroom is disabled) so that a disabled pool
// converges to ZERO placeholders instead of stranding the ones it created —
// there is no other path that deletes them.
func (p *K8sWorkerPool) reconcileHeadroom(ctx context.Context) {
	enabled := p.placeholderPriorityClassName != ""

	existing, err := p.listPlaceholderPods(ctx)
	if err != nil {
		slog.Warn("Headroom: failed to list placeholder pods.", "error", err)
		return
	}

	if !enabled {
		// Disabled: converge to zero immediately (teardown, not scale-down).
		for _, ph := range existing {
			_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, ph.name, metav1.DeleteOptions{GracePeriodSeconds: int64Ptr(0)})
		}
		if len(existing) > 0 {
			slog.Info("Headroom: disabled, deleted placeholder pods.", "count", len(existing))
		}
		headroomSlotsDesiredGauge.Set(0)
		return
	}

	plan, ok := p.headroomTarget(ctx)
	if !ok {
		return // sizing/demand error already logged; back off, retry next tick
	}
	headroomSlotsDesiredGauge.Set(float64(plan.desired))
	headroomSlotsCapGauge.Set(float64(plan.cap))
	headroomPeakBurstGauge.Set(float64(plan.peakBurst))
	headroomSlotCPUGauge.Set(float64(plan.cpu.MilliValue()))
	headroomSlotMemoryGauge.Set(float64(plan.mem.Value()))

	// Replace placeholders whose shape drifted from the plan: delete them now,
	// the top-up below recreates them at the new shape in the same tick. This
	// is a capacity refresh, not a scale-down — no hysteresis.
	keep := existing[:0]
	replaced := 0
	for _, ph := range existing {
		if shapeDrifted(ph.cpuMillis, plan.cpu.MilliValue()) || shapeDrifted(ph.memBytes, plan.mem.Value()) {
			_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, ph.name, metav1.DeleteOptions{GracePeriodSeconds: int64Ptr(0)})
			replaced++
			continue
		}
		keep = append(keep, ph)
	}
	if replaced > 0 {
		slog.Info("Headroom: replacing shape-drifted placeholder pods.",
			"count", replaced, "cpu", plan.cpu.String(), "memory", plan.mem.String())
	}

	switch {
	case len(keep) < plan.desired:
		// Scale up (or refill after preemption/replacement) immediately.
		for i := len(keep); i < plan.desired; i++ {
			if err := p.createPlaceholderPod(ctx, plan.cpu, plan.mem); err != nil {
				slog.Warn("Headroom: failed to create placeholder pod.", "error", err)
				break // back off; next tick retries
			}
		}
		p.headroomDownSince = time.Time{}
		slog.Info("Headroom: scaled placeholder pods up.",
			"from", len(keep), "to", plan.desired, "peakBurst", plan.peakBurst, "cap", plan.cap)
	case len(keep) > plan.desired:
		// Scale down lazily: one slot per headroomScaleDownDelay while the
		// target stays lower, so a transient dip never thrashes capacity.
		now := time.Now()
		if p.headroomDownSince.IsZero() {
			p.headroomDownSince = now
		} else if now.Sub(p.headroomDownSince) >= headroomScaleDownDelay {
			// Delete the newest placeholder first (stable order by name).
			names := make([]string, len(keep))
			for i, ph := range keep {
				names[i] = ph.name
			}
			sort.Sort(sort.Reverse(sort.StringSlice(names)))
			_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, names[0], metav1.DeleteOptions{GracePeriodSeconds: int64Ptr(0)})
			p.headroomDownSince = now
			slog.Info("Headroom: scaled placeholder pods down by one.",
				"from", len(keep), "target", plan.desired, "peakBurst", plan.peakBurst)
		}
	default:
		p.headroomDownSince = time.Time{}
	}

	// Retention: the spawn log only needs the sizing window. Best-effort.
	if store, ok := p.runtimeStore.(workerSpawnLogStore); ok {
		if _, err := store.PruneWorkerSpawnLog(time.Now().Add(-headroomSizeWindow)); err != nil {
			slog.Warn("Headroom: failed to prune worker spawn log.", "error", err)
		}
	}
}

// shapeDrifted reports whether an existing placeholder's request differs from
// the planned one by more than headroomShapeDriftPct on this axis. The
// tolerance keeps mixed short-lived profiles from churning pods.
func shapeDrifted(existing, planned int64) bool {
	if planned <= 0 {
		return existing > 0
	}
	diff := existing - planned
	if diff < 0 {
		diff = -diff
	}
	return diff*100 > planned*headroomShapeDriftPct
}

// headroomTarget resolves this tick's slot count and shape from the spawn log
// and the live fleet. ok=false means a transient error (already logged); the
// caller skips the tick and retries on the next one.
//
//	desired = clamp(peak spawn burst, headroomSlotFloor, cap)
//	cap     = max(headroomCapFloor, ceil(headroomCapFleetPct% × live workers))
//	shape   = componentwise max spawned in headroomSizeWindow,
//	          falling back per axis to the live fleet's max, then to the
//	          pool's configured/default worker request (fresh empty cluster).
func (p *K8sWorkerPool) headroomTarget(ctx context.Context) (headroomPlan, bool) {
	liveCount, liveMaxCPUMillis, liveMaxMemBytes, err := p.liveWorkerStats(ctx)
	if err != nil {
		slog.Warn("Headroom: failed to inspect live worker pods.", "error", err)
		return headroomPlan{}, false
	}

	var stats configstore.HeadroomSpawnStats
	if store, ok := p.runtimeStore.(workerSpawnLogStore); ok {
		stats, err = store.HeadroomSpawnStats(headroomBurstWindow, headroomBurstBucket, headroomSizeWindow)
		if err != nil {
			slog.Warn("Headroom: failed to read spawn stats.", "error", err)
			return headroomPlan{}, false
		}
	}

	plan := headroomPlan{peakBurst: stats.PeakBurst}
	plan.cap = headroomCapFloor
	if c := ceilDiv(int64(liveCount)*headroomCapFleetPct, 100); c > plan.cap {
		plan.cap = c
	}
	plan.desired = stats.PeakBurst
	if plan.desired < headroomSlotFloor {
		plan.desired = headroomSlotFloor
	}
	if plan.desired > plan.cap {
		plan.desired = plan.cap
	}

	cpuMillis := stats.MaxCPUMillis
	if cpuMillis == 0 {
		cpuMillis = liveMaxCPUMillis
	}
	memBytes := stats.MaxMemBytes
	if memBytes == 0 {
		memBytes = liveMaxMemBytes
	}
	if cpuMillis == 0 {
		q, err := resource.ParseQuantity(firstNonEmpty(p.workerCPURequest, defaultWorkerCPU))
		if err != nil {
			slog.Warn("Headroom: invalid default worker cpu for placeholder sizing.", "value", p.workerCPURequest, "error", err)
			return headroomPlan{}, false
		}
		cpuMillis = q.MilliValue()
	}
	if memBytes == 0 {
		q, err := resource.ParseQuantity(firstNonEmpty(p.workerMemoryRequest, defaultWorkerMemory))
		if err != nil {
			slog.Warn("Headroom: invalid default worker memory for placeholder sizing.", "value", p.workerMemoryRequest, "error", err)
			return headroomPlan{}, false
		}
		memBytes = q.Value()
	}
	plan.cpu = *resource.NewMilliQuantity(cpuMillis, resource.DecimalSI)
	plan.mem = *resource.NewQuantity(memBytes, resource.BinarySI)
	return plan, true
}

func ceilDiv(a, b int64) int {
	if b <= 0 {
		return 0
	}
	if a <= 0 {
		return 0
	}
	return int((a + b - 1) / b)
}

// liveWorkerStats counts live (non-terminating) worker pods in the namespace
// and returns the componentwise max of their per-pod resource requests — the
// fleet-relative cap input and the slot-size fallback when the spawn log is
// empty. Worker pods are never BestEffort (every one carries requests), so
// the shapes are faithful. Listing by label covers all CP replicas' workers,
// which is what the leader-only reconcile needs.
func (p *K8sWorkerPool) liveWorkerStats(ctx context.Context) (count int, maxCPUMillis, maxMemBytes int64, err error) {
	pods, err := p.clientset.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		return 0, 0, 0, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		count++
		var cpuMillis, memBytes int64
		for c := range pod.Spec.Containers {
			req := pod.Spec.Containers[c].Resources.Requests
			cpuMillis += req.Cpu().MilliValue()
			memBytes += req.Memory().Value()
		}
		if cpuMillis > maxCPUMillis {
			maxCPUMillis = cpuMillis
		}
		if memBytes > maxMemBytes {
			maxMemBytes = memBytes
		}
	}
	return count, maxCPUMillis, maxMemBytes, nil
}

// placeholderPod is one live placeholder's identity and shape (for shape-drift
// replacement).
type placeholderPod struct {
	name      string
	cpuMillis int64
	memBytes  int64
}

// listPlaceholderPods returns the live (non-terminating) placeholder pods.
func (p *K8sWorkerPool) listPlaceholderPods(ctx context.Context) ([]placeholderPod, error) {
	pods, err := p.clientset.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + headroomPodLabel,
	})
	if err != nil {
		return nil, err
	}
	out := make([]placeholderPod, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		ph := placeholderPod{name: pod.Name}
		for c := range pod.Spec.Containers {
			req := pod.Spec.Containers[c].Resources.Requests
			ph.cpuMillis += req.Cpu().MilliValue()
			ph.memBytes += req.Memory().Value()
		}
		out = append(out, ph)
	}
	return out, nil
}

func (p *K8sWorkerPool) createPlaceholderPod(ctx context.Context, cpu, mem resource.Quantity) error {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			// GenerateName lets the API server assign a unique suffix per pod —
			// the controller creates N placeholders per reconcile and does not
			// track ids, so a fixed/derived name would collide ("already exists").
			// "duckgres-placeholder-<cp-replicaset-hash>-<rand>": the RS hash
			// identifies the owning CP build without embedding the whole CP pod
			// name (whose own random suffix is noise). The app label stays
			// `duckgres-headroom`, so selectors keep matching pods created
			// under the old name across a rollout.
			GenerateName: fmt.Sprintf("duckgres-placeholder-%s-", cpReplicaSetHash(p.cpID)),
			Namespace:    p.namespace,
			Labels: map[string]string{
				"app":                    headroomPodLabel,
				"duckgres/control-plane": p.cpID,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyAlways,
			PriorityClassName:             p.placeholderPriorityClassName,
			NodeSelector:                  p.workerNodeSelector,
			AutomountServiceAccountToken:  boolPtr(false),
			TerminationGracePeriodSeconds: int64Ptr(0),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: boolPtr(true),
				RunAsUser:    int64Ptr(1000),
			},
			Containers: []corev1.Container{{
				Name:  "pause",
				Image: firstNonEmpty(p.placeholderImage, "registry.k8s.io/pause:3.9"),
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    cpu,
						corev1.ResourceMemory: mem,
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    cpu,
						corev1.ResourceMemory: mem,
					},
				},
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: boolPtr(false),
				},
			}},
		},
	}
	if p.workerTolerationKey != "" {
		tol := corev1.Toleration{Key: p.workerTolerationKey, Effect: corev1.TaintEffectNoSchedule}
		if p.workerTolerationValue != "" {
			tol.Operator = corev1.TolerationOpEqual
			tol.Value = p.workerTolerationValue
		}
		pod.Spec.Tolerations = []corev1.Toleration{tol}
	}
	_, err := p.clientset.CoreV1().Pods(p.namespace).Create(ctx, pod, metav1.CreateOptions{})
	return err
}
