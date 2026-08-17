//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"
)

func TestK8sPoolReservedSpawnAddsPlacementLabelAndAffinityBeforeCreate(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	pool.workerOrgAffinityEnabled = true
	pool.workerOrgAffinityWeight = 37
	pool.workerNodeSelector = map[string]string{"example.com/pool": "workers"}
	pool.workerTolerationKey = "example.com/dedicated"
	pool.workerTolerationValue = "workers"
	pool.workerPriorityClassName = "worker-priority"

	var created *corev1.Pod
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		pod, ok := create.GetObject().(*corev1.Pod)
		if !ok || pod.Labels["app"] != "duckgres-worker" {
			return false, nil, nil
		}
		created = pod.DeepCopy()
		return true, nil, errors.New("stop after capture")
	})

	_, err := pool.spawnReservedWorkerForSlot(context.Background(), 41, &WorkerAssignment{
		OrgID: "org-a",
		Image: "duckgres:test",
	})
	if err == nil {
		t.Fatal("reserved spawn unexpectedly succeeded")
	}
	if created == nil {
		t.Fatal("worker pod was not submitted to Kubernetes")
	}

	if got := created.Labels[placementOrgLabelKey]; got != "org-a" {
		t.Fatalf("placement label = %q, want org-a", got)
	}
	if _, ok := created.Labels[activeOrgLabelKey]; ok {
		t.Fatalf("new worker must not treat placement metadata as active-org authorization: %#v", created.Labels)
	}

	if created.Spec.Affinity == nil || created.Spec.Affinity.PodAffinity == nil {
		t.Fatal("expected pod affinity on an org-bound worker")
	}
	terms := created.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 1 {
		t.Fatalf("preferred pod-affinity terms = %d, want 1", len(terms))
	}
	term := terms[0]
	if term.Weight != 37 {
		t.Fatalf("affinity weight = %d, want 37", term.Weight)
	}
	if term.PodAffinityTerm.TopologyKey != "kubernetes.io/hostname" {
		t.Fatalf("topology key = %q", term.PodAffinityTerm.TopologyKey)
	}
	if !reflect.DeepEqual(term.PodAffinityTerm.Namespaces, []string{"default"}) {
		t.Fatalf("affinity namespaces = %#v, want worker namespace", term.PodAffinityTerm.Namespaces)
	}
	if !reflect.DeepEqual(term.PodAffinityTerm.LabelSelector.MatchLabels, map[string]string{placementOrgLabelKey: "org-a"}) {
		t.Fatalf("affinity selector = %#v", term.PodAffinityTerm.LabelSelector)
	}
	if created.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		t.Fatal("placement must never add required pod affinity")
	}

	if got := created.Spec.NodeSelector["example.com/pool"]; got != "workers" {
		t.Fatalf("node selector = %#v", created.Spec.NodeSelector)
	}
	if len(created.Spec.Tolerations) != 1 || created.Spec.Tolerations[0].Key != "example.com/dedicated" {
		t.Fatalf("tolerations = %#v", created.Spec.Tolerations)
	}
	if created.Spec.PriorityClassName != "worker-priority" {
		t.Fatalf("priority class = %q", created.Spec.PriorityClassName)
	}
	if cpu := created.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]; cpu.Cmp(resource.MustParse(defaultWorkerCPU)) != 0 {
		t.Fatalf("cpu request = %s, want %s", cpu.String(), defaultWorkerCPU)
	}
}

func TestK8sPoolReservedSpawnWithoutTrustedOrgHasNoPlacementMetadataOrAffinity(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)
	pool.workerOrgAffinityEnabled = true

	var created *corev1.Pod
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		create, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		pod, ok := create.GetObject().(*corev1.Pod)
		if !ok || pod.Labels["app"] != "duckgres-worker" {
			return false, nil, nil
		}
		created = pod.DeepCopy()
		return true, nil, errors.New("stop after capture")
	})

	_, _ = pool.spawnReservedWorkerForSlot(context.Background(), 42, &WorkerAssignment{Image: "duckgres:test"})
	if created == nil {
		t.Fatal("worker pod was not submitted to Kubernetes")
	}
	if _, ok := created.Labels[placementOrgLabelKey]; ok {
		t.Fatalf("worker without a trusted org received placement metadata: %#v", created.Labels)
	}
	if created.Spec.Affinity != nil {
		t.Fatalf("worker without a trusted org received affinity: %#v", created.Spec.Affinity)
	}
}

func TestAddWorkerOrgPlacementAffinityMergesAndDisabledIsNoop(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Affinity: &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{},
		PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{Weight: 7}},
		},
		PodAffinity: &corev1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{Weight: 11}},
		},
	}}}
	p := &K8sWorkerPool{namespace: "workers", workerOrgAffinityEnabled: true, workerOrgAffinityWeight: 73}
	p.addWorkerOrgPlacementAffinity(pod, "org-a")

	if pod.Spec.Affinity.NodeAffinity == nil || pod.Spec.Affinity.PodAntiAffinity == nil {
		t.Fatal("existing affinity fields were overwritten")
	}
	terms := pod.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution
	if len(terms) != 2 || terms[0].Weight != 11 {
		t.Fatalf("existing pod affinity was not preserved: %#v", terms)
	}
	if got := terms[1]; got.Weight != 73 || got.PodAffinityTerm.TopologyKey != "kubernetes.io/hostname" || !reflect.DeepEqual(got.PodAffinityTerm.Namespaces, []string{"workers"}) || !reflect.DeepEqual(got.PodAffinityTerm.LabelSelector.MatchLabels, map[string]string{placementOrgLabelKey: "org-a"}) {
		t.Fatalf("unexpected appended placement affinity: %#v", got)
	}

	disabled := &corev1.Pod{Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}}}
	want := disabled.DeepCopy()
	(&K8sWorkerPool{namespace: "workers", workerOrgAffinityWeight: 73}).addWorkerOrgPlacementAffinity(disabled, "org-a")
	if !reflect.DeepEqual(disabled, want) {
		t.Fatalf("disabled affinity mutated scheduling fields: got %#v, want %#v", disabled.Spec.Affinity, want.Spec.Affinity)
	}
}

func TestReconcilePlacementLabelsUsesTrustedRecordAndIsIdempotent(t *testing.T) {
	const namespace = "default"
	old := metav1.NewTime(time.Now().Add(-3 * time.Minute))
	orgBound := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:              "existing-org-worker",
		Namespace:         namespace,
		CreationTimestamp: old,
		Labels: map[string]string{
			"app":                "duckgres-worker",
			"duckgres/worker-id": "51",
			activeOrgLabelKey:    "network-policy-org",
		},
	}}
	unknown := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:              "unknown-worker",
		Namespace:         namespace,
		CreationTimestamp: old,
		Labels: map[string]string{
			"app": "duckgres-worker", "duckgres/worker-id": "52",
		},
	}}
	pool, cs := newTestK8sPool(t, 5)
	_, _ = cs.CoreV1().Pods(namespace).Create(context.Background(), orgBound, metav1.CreateOptions{})
	_, _ = cs.CoreV1().Pods(namespace).Create(context.Background(), unknown, metav1.CreateOptions{})
	pool.runtimeStore = &captureRuntimeWorkerStore{preloadedRecords: map[int]*configstore.WorkerRecord{
		51: {WorkerID: 51, PodName: orgBound.Name, OrgID: "org-a", State: configstore.WorkerStateHot},
		52: {WorkerID: 52, PodName: unknown.Name, State: configstore.WorkerStateHot},
	}}

	if got := pool.cleanupOrphanedWorkerPods(context.Background(), 2*time.Minute); got != 0 {
		t.Fatalf("reconciliation deleted %d live worker pods", got)
	}
	got, err := cs.CoreV1().Pods(namespace).Get(context.Background(), orgBound.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get org-bound worker: %v", err)
	}
	if got.Labels[placementOrgLabelKey] != "org-a" {
		t.Fatalf("placement label = %q, want org-a", got.Labels[placementOrgLabelKey])
	}
	if got.Labels[activeOrgLabelKey] != "network-policy-org" {
		t.Fatalf("active-org was replaced or weakened: %#v", got.Labels)
	}
	if got, err := cs.CoreV1().Pods(namespace).Get(context.Background(), unknown.Name, metav1.GetOptions{}); err != nil || got.Labels[placementOrgLabelKey] != "" {
		t.Fatalf("untrusted worker received placement label: pod=%#v err=%v", got, err)
	}
	patchesAfterFirstPass := countPodPatches(cs)
	if patchesAfterFirstPass != 1 {
		t.Fatalf("placement patches after first pass = %d, want 1", patchesAfterFirstPass)
	}

	pool.cleanupOrphanedWorkerPods(context.Background(), 2*time.Minute)
	if got := countPodPatches(cs); got != patchesAfterFirstPass {
		t.Fatalf("reconciliation repeated an already-converged patch: %d -> %d", patchesAfterFirstPass, got)
	}
}

func countPodPatches(cs interface{ Actions() []k8stesting.Action }) int {
	count := 0
	for _, action := range cs.Actions() {
		if action.Matches("patch", "pods") {
			count++
		}
	}
	return count
}

func TestPlacementOrgLabelValueIsKubernetesSafe(t *testing.T) {
	label := placementOrgLabelValue("tenant:with/slashes and a very long value that exceeds the Kubernetes label value limit")
	if len(label) > 63 {
		t.Fatalf("placement label exceeds Kubernetes limit: %q", label)
	}
	if label == "" {
		t.Fatal("placement label must remain matchable after sanitization")
	}
}
