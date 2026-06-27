//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// headroomPodLabel marks the low-priority placeholder pods this controller owns.
const headroomPodLabel = "duckgres-headroom"

// Node-sized placeholder shape for CONSTANT node-headroom (HeadroomNodes>0):
// one placeholder ≈ one worker node, so N placeholders reserve N preemptible
// nodes' worth of warm capacity regardless of current demand. Sized to an r6gd
// worker node's schedulable capacity (matches the historical exclusive-worker
// shape). A real worker (any shape) preempts one placeholder to claim a node;
// the next reconcile recreates it, which makes Karpenter add a node in the
// background. Kept as a constant — the count is the only knob — per the
// "keep headroom small and constant" decision.
const (
	headroomNodeCPU    = "46"
	headroomNodeMemory = "360Gi"
)

// headroomPlaceholdersNeeded returns how many placeholder pods of size
// (phCPUMillis, phMemBytes) are required to hold `percent`% of the CURRENT
// WORKER DEMAND (workerCPUMillis/workerMemBytes — the summed resource requests
// of live worker pods) as preemptible spare capacity. It's the max of the
// CPU-axis and memory-axis counts, so both are satisfied. Pure (no I/O).
//
// Worker demand is the only sane baseline. Sizing against node allocatable —
// even with the placeholders' own share subtracted — still counts the FREE
// space on nodes as "capacity that needs headroom", which is backwards: free
// capacity IS headroom. Observed failure mode: a single over-sized node (one
// 192-CPU consolidation artifact) demanded 11 placeholders at 5% with ZERO
// workers running, and those placeholders then pinned the node alive. Keyed to
// worker demand, the target tracks load by construction: zero workers -> the
// floor; N busy CPUs -> percent of N; node geometry drops out entirely and
// the placeholders can never feed their own target.
//
// Floors at 1 while headroom is enabled: a pure percentage hits zero on an
// idle pool, which would leave no preemptible slot at all and make the first
// spawn after an idle period fully cold — one warm slot is the point of the
// feature.
func headroomPlaceholdersNeeded(workerCPUMillis, workerMemBytes, phCPUMillis, phMemBytes int64, percent int) int {
	if percent <= 0 {
		return 0
	}
	if phCPUMillis <= 0 && phMemBytes <= 0 {
		return 0
	}
	n := 1 // floor: always keep one preemptible slot while enabled
	if phCPUMillis > 0 {
		cpuTarget := workerCPUMillis * int64(percent) / 100
		if c := ceilDiv(cpuTarget, phCPUMillis); c > n {
			n = c
		}
	}
	if phMemBytes > 0 {
		memTarget := workerMemBytes * int64(percent) / 100
		if c := ceilDiv(memTarget, phMemBytes); c > n {
			n = c
		}
	}
	return n
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

// reconcileHeadroom drives the number of low-priority placeholder pods toward
// the headroom target. Leader-gated (called from the janitor, which runs only
// on the elected CP). Always runs (even when headroom is disabled) so that a
// disabled/just-disabled pool converges to ZERO placeholders instead of
// stranding the ones it created — there is no other path that deletes them.
//
// Two modes:
//   - CONSTANT (HeadroomNodes>0, the prod default): keep exactly HeadroomNodes
//     node-sized placeholders, INDEPENDENT of worker demand. This is what
//     bounds headroom cost: the previous demand-proportional mode counted
//     hot_idle (idle-but-alive) workers as demand and provisioned an extra
//     headroomPercent% of full-worker-sized placeholders, so it AMPLIFIED any
//     idle-worker leak ~1.75x. A fixed node count cannot do that.
//   - LEGACY percent (HeadroomPercent>0, HeadroomNodes==0): demand-proportional
//     sizing, retained for small/dev pools.
//
// Placeholders carry a PriorityClass BELOW the worker PriorityClass, so the
// scheduler preempts them to fit a real worker; a worker larger than one
// placeholder preempts as many as it needs. The reconcile recreates them toward
// the target on the next tick, which makes Karpenter add node capacity in the
// background, so headroom self-heals after any spawn.
func (p *K8sWorkerPool) reconcileHeadroom(ctx context.Context) {
	desired, phCPU, phMem, ok := p.headroomTarget(ctx)
	if !ok {
		return // sizing/demand error already logged; back off, retry next tick
	}

	existing, _, err := p.listPlaceholderPods(ctx)
	if err != nil {
		slog.Warn("Headroom: failed to list placeholder pods.", "error", err)
		return
	}

	switch {
	case len(existing) < desired:
		for i := len(existing); i < desired; i++ {
			if err := p.createPlaceholderPod(ctx, phCPU, phMem); err != nil {
				slog.Warn("Headroom: failed to create placeholder pod.", "error", err)
				return // back off; next tick retries
			}
		}
		slog.Info("Headroom: scaled placeholder pods up.", "from", len(existing), "to", desired)
	case len(existing) > desired:
		// Delete the newest placeholders first (stable order by name).
		sort.Slice(existing, func(i, j int) bool { return existing[i] > existing[j] })
		for i := 0; i < len(existing)-desired; i++ {
			_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, existing[i], metav1.DeleteOptions{GracePeriodSeconds: int64Ptr(0)})
		}
		slog.Info("Headroom: scaled placeholder pods down.", "from", len(existing), "to", desired)
	}
}

// headroomTarget resolves the desired placeholder count and per-placeholder
// size for this tick. ok=false means a transient sizing/demand error (already
// logged) and the caller should skip this tick. A desired of 0 with ok=true is
// the disabled state — the caller still lists and deletes any existing
// placeholders so they don't leak.
func (p *K8sWorkerPool) headroomTarget(ctx context.Context) (desired int, phCPU, phMem resource.Quantity, ok bool) {
	if p.headroomNodes > 0 {
		cpu, err := resource.ParseQuantity(headroomNodeCPU)
		if err != nil {
			slog.Warn("Headroom: invalid node cpu for placeholder sizing.", "value", headroomNodeCPU, "error", err)
			return 0, resource.Quantity{}, resource.Quantity{}, false
		}
		mem, err := resource.ParseQuantity(headroomNodeMemory)
		if err != nil {
			slog.Warn("Headroom: invalid node memory for placeholder sizing.", "value", headroomNodeMemory, "error", err)
			return 0, resource.Quantity{}, resource.Quantity{}, false
		}
		return p.headroomNodes, cpu, mem, true
	}

	if p.headroomPercent > 0 {
		cpu, err := resource.ParseQuantity(firstNonEmpty(p.workerCPURequest, defaultWorkerCPU))
		if err != nil {
			slog.Warn("Headroom: invalid default worker cpu for placeholder sizing.", "value", p.workerCPURequest, "error", err)
			return 0, resource.Quantity{}, resource.Quantity{}, false
		}
		mem, err := resource.ParseQuantity(firstNonEmpty(p.workerMemoryRequest, defaultWorkerMemory))
		if err != nil {
			slog.Warn("Headroom: invalid default worker memory for placeholder sizing.", "value", p.workerMemoryRequest, "error", err)
			return 0, resource.Quantity{}, resource.Quantity{}, false
		}
		workerCPU, workerMem, err := p.activeWorkerDemand(ctx)
		if err != nil {
			slog.Warn("Headroom: failed to sum worker demand.", "error", err)
			return 0, resource.Quantity{}, resource.Quantity{}, false
		}
		return headroomPlaceholdersNeeded(workerCPU, workerMem, cpu.MilliValue(), mem.Value(), p.headroomPercent), cpu, mem, true
	}

	// Disabled: converge to zero placeholders.
	return 0, resource.Quantity{}, resource.Quantity{}, true
}

// activeWorkerDemand sums the resource REQUESTS of live (non-terminating)
// worker pods in the namespace — the demand baseline the headroom percentage
// applies to. Worker pods are never BestEffort (every one carries requests),
// so the sum is faithful. Listing by label covers all CP replicas' workers,
// which is what the leader-only reconcile needs.
func (p *K8sWorkerPool) activeWorkerDemand(ctx context.Context) (cpuMillis, memBytes int64, err error) {
	pods, err := p.clientset.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		return 0, 0, err
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for c := range pod.Spec.Containers {
			req := pod.Spec.Containers[c].Resources.Requests
			cpuMillis += req.Cpu().MilliValue()
			memBytes += req.Memory().Value()
		}
	}
	return cpuMillis, memBytes, nil
}

// listPlaceholderPods returns the live placeholder pod names plus how many of
// them are SCHEDULED (assigned to a node). Only scheduled placeholders occupy
// node allocatable, so only they are subtracted from the sizing baseline —
// subtracting Pending ones (created but not yet backed by a Karpenter node)
// would shrink the baseline below reality and make the target flap down while
// capacity is still arriving.
func (p *K8sWorkerPool) listPlaceholderPods(ctx context.Context) (names []string, scheduled int, err error) {
	pods, err := p.clientset.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + headroomPodLabel,
	})
	if err != nil {
		return nil, 0, err
	}
	names = make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		// Skip pods already terminating.
		if pods.Items[i].DeletionTimestamp != nil {
			continue
		}
		names = append(names, pods.Items[i].Name)
		if pods.Items[i].Spec.NodeName != "" {
			scheduled++
		}
	}
	return names, scheduled, nil
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
