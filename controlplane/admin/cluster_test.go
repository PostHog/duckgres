//go:build kubernetes

package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

// clusterTestRouter mounts the cluster-topology endpoints on a fresh gin engine
// backed by a fake clientset seeded with objs.
func clusterTestRouter(objs ...runtime.Object) *gin.Engine {
	gin.SetMode(gin.TestMode)
	cs := fake.NewSimpleClientset(objs...)
	e := gin.New()
	registerClusterAPI(e.Group("/api/v1"), cs)
	return e
}

func getJSON(t *testing.T, e *gin.Engine, path string) map[string]any {
	t.Helper()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	e.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200 (%s)", path, w.Code, w.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("GET %s: bad json: %v", path, err)
	}
	return out
}

func TestClusterNodesProjection(t *testing.T) {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ip-10-0-0-1.ec2.internal",
			UID:  "node-uid-1",
			Labels: map[string]string{
				"karpenter.sh/nodepool":            "duckgres-workers",
				"node.kubernetes.io/instance-type": "m5.large",
				"karpenter.sh/capacity-type":       "spot",
			},
		},
		Spec: corev1.NodeSpec{
			Unschedulable: true,
			Taints: []corev1.Taint{
				{Key: "karpenter.sh/disruption", Effect: corev1.TaintEffectNoSchedule},
			},
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("8Gi"),
			},
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}
	e := clusterTestRouter(node)
	body := getJSON(t, e, "/api/v1/cluster/nodes")
	items, _ := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("nodes: got %d items, want 1", len(items))
	}
	n := items[0].(map[string]any)
	meta := n["metadata"].(map[string]any)
	if meta["uid"] != "node-uid-1" {
		t.Errorf("uid = %v, want node-uid-1", meta["uid"])
	}
	labels := meta["labels"].(map[string]any)
	if labels["karpenter.sh/nodepool"] != "duckgres-workers" {
		t.Errorf("nodepool label missing: %v", labels)
	}
	spec := n["spec"].(map[string]any)
	if spec["unschedulable"] != true {
		t.Errorf("unschedulable = %v, want true", spec["unschedulable"])
	}
	taints := spec["taints"].([]any)
	if len(taints) != 1 || taints[0].(map[string]any)["key"] != "karpenter.sh/disruption" {
		t.Errorf("taints wrong: %v", taints)
	}
	status := n["status"].(map[string]any)
	alloc := status["allocatable"].(map[string]any)
	if alloc["cpu"] != "2" || alloc["memory"] != "8Gi" {
		t.Errorf("allocatable wrong: %v", alloc)
	}
	conds := status["conditions"].([]any)
	if len(conds) != 1 || conds[0].(map[string]any)["type"] != "Ready" {
		t.Errorf("conditions wrong: %v", conds)
	}
}

func TestClusterPodsProjection(t *testing.T) {
	prio := int32(-5)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-abc",
			Namespace: "duckgres",
			UID:       "pod-uid-1",
			Annotations: map[string]string{
				"kubernetes.io/config.mirror":                      "yes",
				"kubectl.kubernetes.io/last-applied-configuration": "SHOULD_BE_DROPPED",
			},
			OwnerReferences: []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "worker-6df9"}},
		},
		Spec: corev1.PodSpec{
			NodeName: "ip-10-0-0-1.ec2.internal",
			Priority: &prio,
			Containers: []corev1.Container{{
				Image: "duckgres:latest",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	e := clusterTestRouter(pod)
	body := getJSON(t, e, "/api/v1/cluster/pods")
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("pods: got %d items, want 1", len(items))
	}
	p := items[0].(map[string]any)
	meta := p["metadata"].(map[string]any)
	ann := meta["annotations"].(map[string]any)
	if _, leaked := ann["kubectl.kubernetes.io/last-applied-configuration"]; leaked {
		t.Errorf("bulky annotation leaked into projection: %v", ann)
	}
	if ann["kubernetes.io/config.mirror"] != "yes" {
		t.Errorf("mirror annotation missing: %v", ann)
	}
	owners := meta["ownerReferences"].([]any)
	if owners[0].(map[string]any)["kind"] != "ReplicaSet" {
		t.Errorf("ownerReferences wrong: %v", owners)
	}
	spec := p["spec"].(map[string]any)
	if spec["nodeName"] != "ip-10-0-0-1.ec2.internal" {
		t.Errorf("nodeName wrong: %v", spec["nodeName"])
	}
	if spec["priority"].(float64) != -5 {
		t.Errorf("priority = %v, want -5", spec["priority"])
	}
	reqs := spec["containers"].([]any)[0].(map[string]any)["resources"].(map[string]any)["requests"].(map[string]any)
	if reqs["cpu"] != "500m" || reqs["memory"] != "1Gi" {
		t.Errorf("requests wrong: %v", reqs)
	}
	if p["status"].(map[string]any)["phase"] != "Running" {
		t.Errorf("phase wrong: %v", p["status"])
	}
}

func TestClusterEventsProjection(t *testing.T) {
	ev := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: "ev1", Namespace: "duckgres", UID: "ev-uid-1"},
		Reason:     "FailedScheduling",
		Message:    "0/3 nodes are available",
		Type:       "Warning",
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Name:      "worker-abc",
			Namespace: "duckgres",
		},
	}
	e := clusterTestRouter(ev)
	body := getJSON(t, e, "/api/v1/cluster/events")
	items := body["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("events: got %d items, want 1", len(items))
	}
	evOut := items[0].(map[string]any)
	if evOut["reason"] != "FailedScheduling" {
		t.Errorf("reason wrong: %v", evOut["reason"])
	}
	io := evOut["involvedObject"].(map[string]any)
	if io["kind"] != "Pod" || io["name"] != "worker-abc" {
		t.Errorf("involvedObject wrong: %v", io)
	}
}

// TestClusterNodepoolsAbsentDegradesToEmpty asserts the karpenter passthrough
// returns an empty list (not a 500) when the CRD isn't installed — the fake
// clientset has no karpenter.sh API, so both version probes fail.
func TestClusterNodepoolsAbsentDegradesToEmpty(t *testing.T) {
	e := clusterTestRouter()
	body := getJSON(t, e, "/api/v1/cluster/nodepools")
	items, ok := body["items"].([]any)
	if !ok || len(items) != 0 {
		t.Fatalf("nodepools (karpenter absent): got %v, want empty items list", body)
	}
}
