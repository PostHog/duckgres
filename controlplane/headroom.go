//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// headroomPodLabel marks the low-priority placeholder pods this controller owns.
const headroomPodLabel = "duckgres-headroom"

// headroomPlaceholdersNeeded returns how many placeholder pods of size
// (phCPUMillis, phMemBytes) are required to hold `percent`% of the worker
// nodepool's NON-PLACEHOLDER allocatable free. It's the max of the count
// needed to cover the CPU headroom and the count needed to cover the memory
// headroom, so both axes are satisfied. Pure (no I/O) for testability.
//
// The capacity held by the already-SCHEDULED placeholders is subtracted
// from allocatable before taking the percentage. Without the subtraction the
// target is self-referential and RATCHETS: placeholders pin nodes (a node
// hosting one is never "Empty", so WhenEmpty consolidation can't reclaim it),
// pinned nodes inflate allocatable, inflated allocatable raises the target,
// which creates more placeholders — observed holding ~57% of the nodepool at
// a configured 20%. Sizing against the non-placeholder baseline makes the
// target track real (worker) usage: as workers finish, the target falls, the
// reconciler deletes the excess, emptied nodes consolidate, and allocatable
// follows it down.
//
// Floors at 1 while headroom is enabled: a pure percentage hits zero on an
// idle pool, which would leave no preemptible slot at all and make the first
// spawn after an idle period fully cold — one warm slot is the point of the
// feature.
func headroomPlaceholdersNeeded(allocCPUMillis, allocMemBytes, phCPUMillis, phMemBytes int64, percent, scheduledPlaceholders int) int {
	if percent <= 0 {
		return 0
	}
	if phCPUMillis <= 0 && phMemBytes <= 0 {
		return 0
	}
	baseCPU := allocCPUMillis - int64(scheduledPlaceholders)*phCPUMillis
	baseMem := allocMemBytes - int64(scheduledPlaceholders)*phMemBytes
	n := 1 // floor: always keep one preemptible slot while enabled
	if phCPUMillis > 0 {
		cpuTarget := baseCPU * int64(percent) / 100
		if c := ceilDiv(cpuTarget, phCPUMillis); c > n {
			n = c
		}
	}
	if phMemBytes > 0 {
		memTarget := baseMem * int64(percent) / 100
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
// percentage of worker-nodepool allocatable. Leader-gated (called from the
// janitor, which runs only on the elected CP). A no-op when headroomPercent<=0.
//
// Placeholders are default-worker-sized and carry a PriorityClass BELOW the
// worker PriorityClass, so the scheduler preempts them to fit a real worker. A
// worker larger than one placeholder preempts AS MANY placeholders as it needs
// (standard k8s priority preemption) — e.g. a 32-core worker evicts ~4 of the
// 8-core placeholders. The reconcile then recreates placeholders back toward the
// target on the next tick, which makes Karpenter add node capacity in the
// background. So headroom self-heals after any size of spawn.
func (p *K8sWorkerPool) reconcileHeadroom(ctx context.Context) {
	if p.headroomPercent <= 0 {
		return
	}
	phCPU, err := resource.ParseQuantity(firstNonEmpty(p.placeholderCPU, defaultWorkerCPU))
	if err != nil {
		slog.Warn("Headroom: invalid placeholder cpu.", "value", p.placeholderCPU, "error", err)
		return
	}
	phMem, err := resource.ParseQuantity(firstNonEmpty(p.placeholderMemory, defaultWorkerMemory))
	if err != nil {
		slog.Warn("Headroom: invalid placeholder memory.", "value", p.placeholderMemory, "error", err)
		return
	}

	allocCPU, allocMem, err := p.workerNodepoolAllocatable(ctx)
	if err != nil {
		slog.Warn("Headroom: failed to read worker nodepool allocatable.", "error", err)
		return
	}

	existing, scheduled, err := p.listPlaceholderPods(ctx)
	if err != nil {
		slog.Warn("Headroom: failed to list placeholder pods.", "error", err)
		return
	}

	desired := headroomPlaceholdersNeeded(allocCPU, allocMem, phCPU.MilliValue(), phMem.Value(), p.headroomPercent, scheduled)

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

// workerNodepoolAllocatable sums allocatable CPU (millicores) and memory (bytes)
// across Ready nodes matching the worker nodeSelector. An empty selector means
// all nodes (callers should configure a selector in multi-nodepool clusters).
func (p *K8sWorkerPool) workerNodepoolAllocatable(ctx context.Context) (cpuMillis, memBytes int64, err error) {
	opts := metav1.ListOptions{}
	if sel := labelSelectorString(p.workerNodeSelector); sel != "" {
		opts.LabelSelector = sel
	}
	nodes, err := p.clientset.CoreV1().Nodes().List(ctx, opts)
	if err != nil {
		return 0, 0, err
	}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if !nodeIsReady(n) || n.Spec.Unschedulable {
			continue
		}
		cpuMillis += n.Status.Allocatable.Cpu().MilliValue()
		memBytes += n.Status.Allocatable.Memory().Value()
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

func nodeIsReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// labelSelectorString renders a node selector map as a comma-joined equality
// selector ("k1=v1,k2=v2"), deterministically ordered.
func labelSelectorString(sel map[string]string) string {
	if len(sel) == 0 {
		return ""
	}
	keys := make([]string, 0, len(sel))
	for k := range sel {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+sel[k])
	}
	return strings.Join(parts, ",")
}
