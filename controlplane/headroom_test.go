//go:build kubernetes

package controlplane

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestHeadroomPlaceholdersNeeded(t *testing.T) {
	const (
		gi = 1 << 30
	)
	tests := []struct {
		name                          string
		allocCPUMillis, allocMemBytes int64
		phCPUMillis, phMemBytes       int64
		percent                       int
		scheduled                     int
		want                          int
	}{
		{name: "disabled (0%)", allocCPUMillis: 100000, allocMemBytes: 1000 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 0, want: 0},
		// 100 cores, 200Gi alloc; 15% => 15 cores / 30Gi headroom; placeholder 8c/16Gi.
		// cpu: ceil(15/8)=2 ; mem: ceil(30/16)=2 => 2.
		{name: "cpu and mem both 2", allocCPUMillis: 100000, allocMemBytes: 200 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 15, want: 2},
		// memory-bound: 100c/2000Gi, 10% => 10c/200Gi; cpu ceil(10/8)=2, mem ceil(200/16)=13 => 13.
		{name: "memory bound", allocCPUMillis: 100000, allocMemBytes: 2000 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 10, want: 13},
		// cpu-bound: 1000c/100Gi, 20% => 200c/20Gi; cpu ceil(200/8)=25, mem ceil(20/16)=2 => 25.
		{name: "cpu bound", allocCPUMillis: 1000000, allocMemBytes: 100 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 20, want: 25},
		// tiny cluster, headroom rounds up to 1 placeholder.
		{name: "rounds up to one", allocCPUMillis: 8000, allocMemBytes: 16 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 10, want: 1},
		// zero allocatable: the floor still asks for one placeholder — it goes
		// Pending and pulls the first node, so an idle pool keeps a warm slot.
		{name: "no nodes -> floor", allocCPUMillis: 0, allocMemBytes: 0, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 25, want: 1},
		// placeholder with no size => none (guards a misconfig).
		{name: "no placeholder size", allocCPUMillis: 100000, allocMemBytes: 200 * gi, phCPUMillis: 0, phMemBytes: 0, percent: 25, want: 0},
		// RATCHET REGRESSION: 10 scheduled placeholders (80c/160Gi) pin nodes
		// that inflate allocatable to 200c/400Gi. Sizing must target the
		// non-placeholder baseline (120c/240Gi): 15% => 18c/36Gi => 3 — NOT
		// 15% of the inflated total (=> 4, which would never shrink).
		{name: "scheduled placeholders excluded from baseline", allocCPUMillis: 200000, allocMemBytes: 400 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 15, scheduled: 10, want: 3},
		// Idle pool: allocatable is ONLY placeholder-held capacity => baseline
		// 0 => floor of one warm slot (the old formula self-sustained all 10).
		{name: "idle pool converges to floor", allocCPUMillis: 80000, allocMemBytes: 160 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 20, scheduled: 10, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := headroomPlaceholdersNeeded(tt.allocCPUMillis, tt.allocMemBytes, tt.phCPUMillis, tt.phMemBytes, tt.percent, tt.scheduled)
			if got != tt.want {
				t.Fatalf("headroomPlaceholdersNeeded = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestLabelSelectorStringDeterministic(t *testing.T) {
	got := labelSelectorString(map[string]string{"posthog.com/nodepool": "duckgres-workers", "kubernetes.io/arch": "arm64"})
	want := "kubernetes.io/arch=arm64,posthog.com/nodepool=duckgres-workers"
	if got != want {
		t.Fatalf("labelSelectorString = %q, want %q", got, want)
	}
	if labelSelectorString(nil) != "" {
		t.Fatal("nil selector must render empty")
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
