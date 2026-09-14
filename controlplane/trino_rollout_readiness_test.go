//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestTrinoRolloutReadinessInventoryIncludesTerminatingAndMainImages(t *testing.T) {
	pod := rolloutTestPod("worker", "one")
	old := rolloutTestPod("coordinator", "two")
	now := metav1.Now()
	old.DeletionTimestamp = &now
	old.Finalizers = []string{"example.test/retain"}
	inventory, err := rolloutPodInventory([]corev1.Pod{pod, old})
	if err != nil {
		t.Fatal(err)
	}
	if inventory.Total != 2 || inventory.Terminating != 1 || inventory.Workers != 1 || inventory.ReadyWorkers != 1 || inventory.ReadyCoordinators != 0 || len(inventory.Images) != 1 {
		t.Fatalf("unexpected inventory: %+v", inventory)
	}
	if inventory.Images[0].SpecImage == inventory.Images[0].RuntimeImageID {
		t.Fatal("test requires distinct index and platform digests")
	}
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "opa", Image: "sidecar:latest"})
	if _, err := rolloutPodInventory([]corev1.Pod{pod}); err != nil {
		t.Fatal(err)
	}
	pod.Spec.Containers[0].Image = ""
	if _, err := rolloutPodInventory([]corev1.Pod{pod}); err == nil {
		t.Fatal("missing main image accepted")
	}
}

func TestTrinoRolloutReadinessAuthAndStoppedInventory(t *testing.T) {
	kube := fake.NewSimpleClientset()
	probeCalls := 0
	h := &trinoRolloutReadinessHandler{
		token: "test-capability", kube: kube, slots: map[string]rolloutReadinessSlot{"cell-test/green": {cell: "cell-test", color: "green", namespace: "test", backendName: "cell-test-green"}},
		limit: make(chan struct{}, 1), timeout: time.Second,
		probe: func(context.Context, rolloutReadinessSlot) (*rolloutCoordinatorFacts, error) {
			probeCalls++
			return nil, errors.New("must not probe stopped slot")
		},
	}
	for _, tc := range []struct {
		name   string
		tokens []string
		status int
	}{
		{"missing", nil, 401}, {"wrong", []string{"wrong"}, 401}, {"duplicate", []string{"test-capability", "test-capability"}, 401}, {"valid", []string{"test-capability"}, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/internal/trino/rollout-readiness/cell-test/green", nil)
			for _, token := range tc.tokens {
				r.Header.Add(rolloutCapabilityHeader, token)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			if tc.status == 200 {
				var result rolloutReadinessResponse
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if result.Pods.Total != 0 || result.Coordinator != nil || result.Canary != nil {
					t.Fatal("stopped slot reported ready")
				}
			}
		})
	}
	if probeCalls != 0 {
		t.Fatal("stopped slot triggered coordinator read")
	}
	for _, action := range kube.Actions() {
		if action.GetVerb() != "list" {
			t.Fatal("readiness mutated Kubernetes")
		}
	}
}

func rolloutTestPod(role, name string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test", Labels: map[string]string{"posthog.com/trino-cell": "cell-test", "posthog.com/trino-color": "green", "app.kubernetes.io/component": role, "app.kubernetes.io/name": "trino-test"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "trino-test", Image: "registry.example/trino@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "trino-test", Ready: true, ImageID: "registry.example/trino@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}},
	}
}

func TestTrinoRolloutReadinessFailsClosedOnErrorsAndConcurrency(t *testing.T) {
	coordinator, worker := rolloutTestPod("coordinator", "coordinator"), rolloutTestPod("worker", "worker")
	coordinator.Status.PodIP, worker.Status.PodIP = "192.0.2.10", "192.0.2.11"
	h := &trinoRolloutReadinessHandler{token: "test-capability", kube: fake.NewSimpleClientset(&coordinator, &worker), slots: map[string]rolloutReadinessSlot{"cell-test/green": {cell: "cell-test", color: "green", namespace: "test", backendName: "cell-test-green"}}, limit: make(chan struct{}, 1), timeout: time.Second}
	request := func(path string, ctx context.Context) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
		r.Header.Set(rolloutCapabilityHeader, h.token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	h.probe = func(context.Context, rolloutReadinessSlot) (*rolloutCoordinatorFacts, error) {
		return nil, errors.New("private-password-and-endpoint")
	}
	if w := request(rolloutReadinessPrefix+"cell-test/green", context.Background()); w.Code != 503 || strings.Contains(w.Body.String(), "private-password") {
		t.Fatal("probe error leaked or became success")
	}
	if w := request(rolloutReadinessPrefix+"cell-test/other", context.Background()); w.Code != 404 {
		t.Fatal("unknown slot accepted")
	}
	h.limit <- struct{}{}
	if w := request(rolloutReadinessPrefix+"cell-test/green", context.Background()); w.Code != 503 || !strings.Contains(w.Body.String(), "busy") {
		t.Fatal("concurrency limit ignored")
	}
	<-h.limit
	h.probe = func(context.Context, rolloutReadinessSlot) (*rolloutCoordinatorFacts, error) {
		return &rolloutCoordinatorFacts{NodeID: "node-test", CoordinatorID: "abcde", RegisteredWorkers: 1, members: []rolloutNodeMember{{ip: "192.0.2.10", coordinator: true}, {ip: "192.0.2.11"}}}, nil
	}
	if w := request(rolloutReadinessPrefix+"cell-test/green", context.Background()); w.Code != 200 {
		t.Fatalf("ready probe failed: %s", w.Body.String())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if w := request(rolloutReadinessPrefix+"cell-test/green", ctx); w.Code != 503 {
		t.Fatal("expired observation succeeded")
	}
}

func TestTrinoRolloutReadinessMembershipBindsEveryPodAndRole(t *testing.T) {
	coordinator, worker := rolloutTestPod("coordinator", "coordinator"), rolloutTestPod("worker", "worker")
	coordinator.Status.PodIP, worker.Status.PodIP = "192.0.2.10", "192.0.2.11"
	for _, tc := range []struct {
		name    string
		members []rolloutNodeMember
		valid   bool
	}{
		{"exact", []rolloutNodeMember{{"192.0.2.10", true}, {"192.0.2.11", false}}, true},
		{"wrong slot", []rolloutNodeMember{{"192.0.2.20", true}, {"192.0.2.21", false}}, false},
		{"wrong roles", []rolloutNodeMember{{"192.0.2.10", false}, {"192.0.2.11", true}}, false},
		{"duplicate", []rolloutNodeMember{{"192.0.2.10", true}, {"192.0.2.10", true}}, false},
		{"missing", []rolloutNodeMember{{"192.0.2.10", true}}, false},
		{"extra", []rolloutNodeMember{{"192.0.2.10", true}, {"192.0.2.11", false}, {"192.0.2.12", false}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rolloutMembersMatchPods(tc.members, []corev1.Pod{coordinator, worker}); got != tc.valid {
				t.Fatal("incorrect member-to-pod decision")
			}
		})
	}
}
