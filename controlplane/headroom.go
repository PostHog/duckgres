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
// nodepool's allocatable (allocCPUMillis, allocMemBytes) free. It's the max of
// the count needed to cover the CPU headroom and the count needed to cover the
// memory headroom, so both axes are satisfied. Pure (no I/O) for testability.
func headroomPlaceholdersNeeded(allocCPUMillis, allocMemBytes, phCPUMillis, phMemBytes int64, percent int) int {
	if percent <= 0 {
		return 0
	}
	if phCPUMillis <= 0 && phMemBytes <= 0 {
		return 0
	}
	n := 0
	if phCPUMillis > 0 {
		cpuTarget := allocCPUMillis * int64(percent) / 100
		if c := ceilDiv(cpuTarget, phCPUMillis); c > n {
			n = c
		}
	}
	if phMemBytes > 0 {
		memTarget := allocMemBytes * int64(percent) / 100
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

	desired := headroomPlaceholdersNeeded(allocCPU, allocMem, phCPU.MilliValue(), phMem.Value(), p.headroomPercent)

	existing, err := p.listPlaceholderPods(ctx)
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

func (p *K8sWorkerPool) listPlaceholderPods(ctx context.Context) ([]string, error) {
	pods, err := p.clientset.CoreV1().Pods(p.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + headroomPodLabel,
	})
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(pods.Items))
	for i := range pods.Items {
		// Skip pods already terminating.
		if pods.Items[i].DeletionTimestamp != nil {
			continue
		}
		names = append(names, pods.Items[i].Name)
	}
	return names, nil
}

func (p *K8sWorkerPool) createPlaceholderPod(ctx context.Context, cpu, mem resource.Quantity) error {
	name := fmt.Sprintf("%s-%s-%d", headroomPodLabel, p.cpID, p.allocateBackgroundSpawnID())
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: p.namespace,
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

// allocateBackgroundSpawnID returns a process-unique id (shares the worker id
// space; only used to make placeholder pod names unique).
func (p *K8sWorkerPool) allocateBackgroundSpawnID() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.allocateBackgroundSpawnIDLocked()
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
