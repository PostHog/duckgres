//go:build kubernetes

package controlplane

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestHeadroomPlaceholdersNeeded(t *testing.T) {
	const (
		gi = 1 << 30
	)
	tests := []struct {
		name                            string
		workerCPUMillis, workerMemBytes int64
		phCPUMillis, phMemBytes         int64
		percent                         int
		want                            int
	}{
		{name: "disabled (0%)", workerCPUMillis: 100000, workerMemBytes: 1000 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 0, want: 0},
		// 100 worker cores / 200Gi demand; 15% => 15c/30Gi headroom; placeholder
		// 8c/16Gi. cpu ceil(15/8)=2, mem ceil(30/16)=2 => 2.
		{name: "cpu and mem both 2", workerCPUMillis: 100000, workerMemBytes: 200 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 15, want: 2},
		// memory-bound demand: 100c/2000Gi, 10% => 10c/200Gi; mem ceil(200/16)=13.
		{name: "memory bound", workerCPUMillis: 100000, workerMemBytes: 2000 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 10, want: 13},
		// cpu-bound demand: 1000c/100Gi, 20% => 200c/20Gi; cpu ceil(200/8)=25.
		{name: "cpu bound", workerCPUMillis: 1000000, workerMemBytes: 100 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 20, want: 25},
		// IDLE POOL: zero worker demand => the floor, regardless of how many or
		// how large the nodes currently are. This is the ratchet regression: the
		// old node-allocatable baseline demanded 11 placeholders at 5% off one
		// stray 192-CPU node with ZERO workers running, and those placeholders
		// then pinned the node alive.
		{name: "zero demand -> floor", workerCPUMillis: 0, workerMemBytes: 0, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 25, want: 1},
		// tiny demand rounds up to the floor.
		{name: "rounds up to one", workerCPUMillis: 800, workerMemBytes: 2 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 10, want: 1},
		// placeholder with no size => none (guards a misconfig).
		{name: "no placeholder size", workerCPUMillis: 100000, workerMemBytes: 200 * gi, phCPUMillis: 0, phMemBytes: 0, percent: 25, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := headroomPlaceholdersNeeded(tt.workerCPUMillis, tt.workerMemBytes, tt.phCPUMillis, tt.phMemBytes, tt.percent)
			if got != tt.want {
				t.Fatalf("headroomPlaceholdersNeeded = %d, want %d", got, tt.want)
			}
		})
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

func TestActiveWorkerDemand(t *testing.T) {
	// Sums live worker pod requests; excludes terminating/terminal pods and
	// non-worker pods (placeholders must never feed the demand baseline).
	mk := func(name string, cpu, mem string, labels map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: labels},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: "duckdb-worker",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(cpu),
					corev1.ResourceMemory: resource.MustParse(mem),
				}},
			}}},
		}
	}
	worker := map[string]string{"app": "duckgres-worker"}
	terminating := mk("w-going", "4", "8Gi", worker)
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	cs := fake.NewSimpleClientset(
		mk("w1", "2", "4Gi", worker),
		mk("w2", "750m", "1536Mi", worker),
		terminating,
		mk("ph", "46", "360Gi", map[string]string{"app": "duckgres-headroom"}),
	)
	p := &K8sWorkerPool{clientset: cs, namespace: "default"}
	cpu, mem, err := p.activeWorkerDemand(context.Background())
	if err != nil {
		t.Fatalf("activeWorkerDemand: %v", err)
	}
	if cpu != 2750 {
		t.Fatalf("cpu demand = %d millicores, want 2750", cpu)
	}
	wantMem := int64(4)<<30 + int64(1536)<<20
	if mem != wantMem {
		t.Fatalf("mem demand = %d, want %d", mem, wantMem)
	}
}
