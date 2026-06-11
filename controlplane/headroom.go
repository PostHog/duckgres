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

// reconcileHeadroom drives the number of placeholder pods toward the configured
// percentage of CURRENT WORKER DEMAND (summed live worker pod requests).
// Leader-gated (called from the janitor, which runs only on the elected CP).
// A no-op when headroomPercent<=0.
//
// Placeholders are sized to the pool's DEFAULT WORKER SHAPE (WorkerCPURequest/
// WorkerMemoryRequest, else the built-in defaults) — no separate sizing knob:
// one preempted placeholder always frees exactly one default-worker slot, in
// every environment. They carry a PriorityClass BELOW the worker
// PriorityClass, so the scheduler preempts them to fit a real worker; a worker
// larger than one placeholder preempts AS MANY as it needs (standard k8s
// priority preemption). The reconcile then recreates placeholders back toward
// the target on the next tick, which makes Karpenter add node capacity in the
// background. So headroom self-heals after any size of spawn.
func (p *K8sWorkerPool) reconcileHeadroom(ctx context.Context) {
	if p.headroomPercent <= 0 {
		return
	}
	phCPU, err := resource.ParseQuantity(firstNonEmpty(p.workerCPURequest, defaultWorkerCPU))
	if err != nil {
		slog.Warn("Headroom: invalid default worker cpu for placeholder sizing.", "value", p.workerCPURequest, "error", err)
		return
	}
	phMem, err := resource.ParseQuantity(firstNonEmpty(p.workerMemoryRequest, defaultWorkerMemory))
	if err != nil {
		slog.Warn("Headroom: invalid default worker memory for placeholder sizing.", "value", p.workerMemoryRequest, "error", err)
		return
	}

	workerCPU, workerMem, err := p.activeWorkerDemand(ctx)
	if err != nil {
		slog.Warn("Headroom: failed to sum worker demand.", "error", err)
		return
	}

	existing, _, err := p.listPlaceholderPods(ctx)
	if err != nil {
		slog.Warn("Headroom: failed to list placeholder pods.", "error", err)
		return
	}

	desired := headroomPlaceholdersNeeded(workerCPU, workerMem, phCPU.MilliValue(), phMem.Value(), p.headroomPercent)

	switch {
	case len(existing) < desired:
		for i := len(existing); i < desired; i++ {
			if err := p.createPlaceholderPod(ctx, phCPU, phMem); err != nil {
				slog.Warn("Headroom: failed to create placeholder pod.", "error", err)
				return // back off; next tick retries
			}
		}
		slog.Info("Headroom: scaled placeholder pods up.", "from", len(existing), "to", desired, "percent", p.headroomPercent)
	case len(existing) > desired:
		// Delete the newest placeholders first (stable order by name).
		sort.Slice(existing, func(i, j int) bool { return existing[i] > existing[j] })
		for i := 0; i < len(existing)-desired; i++ {
			_ = p.clientset.CoreV1().Pods(p.namespace).Delete(ctx, existing[i], metav1.DeleteOptions{GracePeriodSeconds: int64Ptr(0)})
		}
		slog.Info("Headroom: scaled placeholder pods down.", "from", len(existing), "to", desired, "percent", p.headroomPercent)
	}
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
