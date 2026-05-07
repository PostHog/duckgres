//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// TestStampActiveOrgLabel verifies the post-activation label patch lands
// on the worker pod. The pre-provisioned per-warehouse Cilium policy
// keys on this label, so the patch is what causes the policy to apply.
func TestStampActiveOrgLabel(t *testing.T) {
	const (
		ns      = "duckgres"
		podName = "duckgres-cp-worker-7"
		orgID   = "tenant-alpha"
	)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
			Labels: map[string]string{
				"app":                "duckgres-worker",
				"duckgres/worker-id": "7",
			},
		},
	}
	cs := fake.NewSimpleClientset(pod)
	a := &SharedWorkerActivator{clientset: cs, defaultNamespace: ns}

	a.stampActiveOrgLabel(context.Background(), podName, orgID)

	got, err := cs.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Get pod: %v", err)
	}
	if got.Labels[activeOrgLabelKey] != orgID {
		t.Errorf("label %s: got %q, want %q", activeOrgLabelKey, got.Labels[activeOrgLabelKey], orgID)
	}
	// Existing labels must be preserved (strategic-merge, not replace).
	if got.Labels["duckgres/worker-id"] != "7" {
		t.Errorf("strategic-merge dropped existing label duckgres/worker-id")
	}
}

// TestStampActiveOrgLabelIdempotent verifies that re-stamping the same
// label is a no-op — the credential-refresh path re-enters activation
// for already-running workers, so we must not error or churn the pod.
func TestStampActiveOrgLabelIdempotent(t *testing.T) {
	const (
		ns      = "duckgres"
		podName = "duckgres-cp-worker-7"
		orgID   = "tenant-alpha"
	)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: ns,
			Labels:    map[string]string{activeOrgLabelKey: orgID},
		},
	}
	cs := fake.NewSimpleClientset(pod)
	a := &SharedWorkerActivator{clientset: cs, defaultNamespace: ns}

	a.stampActiveOrgLabel(context.Background(), podName, orgID)
	a.stampActiveOrgLabel(context.Background(), podName, orgID)

	got, _ := cs.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{})
	if got.Labels[activeOrgLabelKey] != orgID {
		t.Errorf("label clobbered after repeated stamping: got %q", got.Labels[activeOrgLabelKey])
	}
}

// TestStampActiveOrgLabelSwallowsErrors verifies the patch is best-effort:
// a Patch failure (e.g. pod was just retired and 404s) must not panic or
// surface back to the activation caller.
func TestStampActiveOrgLabelSwallowsErrors(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("patch", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("simulated apiserver failure")
	})
	a := &SharedWorkerActivator{clientset: cs, defaultNamespace: "duckgres"}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("stampActiveOrgLabel panicked on Patch error: %v", r)
		}
	}()
	a.stampActiveOrgLabel(context.Background(), "duckgres-cp-worker-99", "tenant-x")
}

// TestStampActiveOrgLabelGuardsEmptyInputs covers the trivial guards —
// an empty pod name or org ID must be a silent no-op (no apiserver call,
// no panic). This protects the credential-refresh and re-activation paths
// where the worker may not have a pod handle in unit tests.
func TestStampActiveOrgLabelGuardsEmptyInputs(t *testing.T) {
	cs := fake.NewSimpleClientset()
	called := false
	cs.PrependReactor("patch", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		called = true
		return true, nil, nil
	})
	a := &SharedWorkerActivator{clientset: cs, defaultNamespace: "duckgres"}

	a.stampActiveOrgLabel(context.Background(), "", "tenant-x")
	a.stampActiveOrgLabel(context.Background(), "duckgres-cp-worker-1", "")
	if called {
		t.Error("Patch was called despite empty pod name / org ID guard")
	}
}
