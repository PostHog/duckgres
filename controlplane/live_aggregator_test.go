//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"sort"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestCPDeploymentPrefix(t *testing.T) {
	cases := map[string]string{
		"duckgres-control-plane-5584b6c998-zcndl": "duckgres-control-plane",
		"duckgres-control-plane-abc-def":          "duckgres-control-plane",
		"foo-bar":                                 "foo-bar", // <=2 segments → unchanged
		"single":                                  "single",
	}
	for in, want := range cases {
		if got := cpDeploymentPrefix(in); got != want {
			t.Errorf("cpDeploymentPrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

func cpPod(name, ip string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "duckgres",
			Labels:    map[string]string{"app.kubernetes.io/name": "duckgres"},
		},
		Status: corev1.PodStatus{Phase: phase, PodIP: ip},
	}
}

// discoverPeerIPs must return only Running CP peers with an IP, excluding self
// by BOTH name and IP, and excluding non-CP / pending / no-IP pods. The
// IP-based self-exclusion is the fix for the cpID != pod-name double-count.
func TestDiscoverPeerIPs(t *testing.T) {
	self := "duckgres-control-plane-rs1-self"
	objs := []runtime.Object{
		cpPod(self, "10.0.0.1", corev1.PodRunning),                                // self by name
		cpPod("duckgres-control-plane-rs1-peerA", "10.0.0.2", corev1.PodRunning),  // real peer
		cpPod("duckgres-control-plane-rs2-peerB", "10.0.0.3", corev1.PodRunning),  // peer in a different replicaset (rollout)
		cpPod("duckgres-control-plane-rs1-pending", "", corev1.PodPending),        // no IP → skip
		cpPod("duckgres-control-plane-rs1-selfip", "10.0.0.1", corev1.PodRunning), // self by IP (cpID != name case) → skip
		cpPod("duckgres-worker-xyz", "10.0.0.9", corev1.PodRunning),               // not a CP → skip
	}
	f := &clusterPeerFetcher{
		clientset:    fake.NewSimpleClientset(objs...),
		namespace:    "duckgres",
		selfPod:      self,
		selfIPs:      map[string]struct{}{"10.0.0.1": {}},
		deployPrefix: "duckgres-control-plane",
	}
	got, err := f.discoverPeerIPs(context.Background())
	if err != nil {
		t.Fatalf("discoverPeerIPs: %v", err)
	}
	sort.Strings(got)
	want := []string{"10.0.0.2", "10.0.0.3"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("discoverPeerIPs = %v, want %v", got, want)
	}
}

func TestFetchPeersReportsDiscoveryFailure(t *testing.T) {
	clientset := fake.NewClientset()
	clientset.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("kubernetes API unavailable")
	})
	f := &clusterPeerFetcher{
		clientset:    clientset,
		namespace:    "duckgres",
		selfPod:      "duckgres-control-plane-rs1-self",
		selfIPs:      map[string]struct{}{"10.0.0.1": {}},
		deployPrefix: "duckgres-control-plane",
	}

	result := f.FetchPeers(context.Background(), "/api/v1/queries")
	if result.DiscoveryComplete {
		t.Fatalf("discovery complete = true, want false after Kubernetes list failure: %+v", result)
	}
	if len(result.Bodies) != 0 || result.Peers != 0 {
		t.Fatalf("failed discovery result = %+v, want no known peers", result)
	}
}
