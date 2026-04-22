//go:build k8s_integration

package k8s_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestK8sWorkerAlwaysStampedWithPodAndNode verifies that every worker pod
// spawned by the control plane carries POD_NAME and NODE_NAME env vars
// injected via the Downward API. Those drive the `pod=` / `node=` attrs on
// every log line (slog stampedHandler) and are unconditional — cache on
// or off, they should always be present.
func TestK8sWorkerAlwaysStampedWithPodAndNode(t *testing.T) {
	pod, ok := firstReadyWorkerPod(t)
	if !ok {
		t.Skip("no worker pods present; earlier tests should have spawned at least one")
	}
	env := podContainerEnv(pod, "duckgres")

	if !hasDownwardFieldRef(env, "POD_NAME", "metadata.name") {
		t.Errorf("worker pod %s missing POD_NAME Downward API env var (fieldRef: metadata.name)", pod.Name)
	}
	if !hasDownwardFieldRef(env, "NODE_NAME", "spec.nodeName") {
		t.Errorf("worker pod %s missing NODE_NAME Downward API env var (fieldRef: spec.nodeName)", pod.Name)
	}
}

// TestK8sWorkerCacheEnvWhenEnabled asserts the cache-specific env wiring
// (DUCKGRES_CACHE_ENABLED + NODE_IP) reaches worker pods when the control
// plane runs with DUCKGRES_CACHE_ENABLED=true. Skipped when the CP doesn't
// have cache enabled (e.g., default kind setup) so the test suite stays
// usable in environments without the DaemonSet.
func TestK8sWorkerCacheEnvWhenEnabled(t *testing.T) {
	if !cpHasCacheEnabled(t) {
		t.Skip("control plane does not have DUCKGRES_CACHE_ENABLED=true; skipping cache wiring assertions")
	}

	pod, ok := firstReadyWorkerPod(t)
	if !ok {
		t.Skip("no worker pods present; earlier tests should have spawned at least one")
	}
	env := podContainerEnv(pod, "duckgres")

	if v := envValue(env, "DUCKGRES_CACHE_ENABLED"); v != "true" {
		t.Errorf("worker %s DUCKGRES_CACHE_ENABLED = %q, want %q", pod.Name, v, "true")
	}
	if !hasDownwardFieldRef(env, "NODE_IP", "status.hostIP") {
		t.Errorf("worker %s missing NODE_IP Downward API env var (fieldRef: status.hostIP)", pod.Name)
	}
}

// firstReadyWorkerPod returns any ready worker pod in the namespace, or
// (zero, false) when none exist. Unlike latestWorkerPod, it never calls
// t.Fatal — the caller decides whether to Skip or Fatal.
func firstReadyWorkerPod(t *testing.T) (corev1.Pod, bool) {
	t.Helper()
	pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=duckgres-worker",
	})
	if err != nil {
		t.Fatalf("failed to list worker pods: %v", err)
	}
	for _, pod := range pods.Items {
		if isReadyPod(pod) {
			return pod, true
		}
	}
	return corev1.Pod{}, false
}

// cpHasCacheEnabled reports whether any control-plane pod in the test
// namespace has DUCKGRES_CACHE_ENABLED=true set on the duckgres container.
// Checking pod spec rather than a named Deployment avoids coupling to the
// deployment's resource name, which varies across Helm chart configurations.
func cpHasCacheEnabled(t *testing.T) bool {
	t.Helper()
	pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app=duckgres-control-plane",
	})
	if err != nil {
		t.Fatalf("failed to list control-plane pods: %v", err)
	}
	for _, pod := range pods.Items {
		if envValue(podContainerEnv(pod, "duckgres"), "DUCKGRES_CACHE_ENABLED") == "true" {
			return true
		}
	}
	return false
}

// podContainerEnv returns the env vars for the named container of pod, or
// nil if not found.
func podContainerEnv(pod corev1.Pod, containerName string) []corev1.EnvVar {
	for _, c := range pod.Spec.Containers {
		if c.Name == containerName {
			return c.Env
		}
	}
	return nil
}

// envValue returns the plain string value of env var name, or "" if not
// present or valueFrom-sourced.
func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

// hasDownwardFieldRef checks for an env var named `name` whose value comes
// from the Downward API field `fieldPath`.
func hasDownwardFieldRef(env []corev1.EnvVar, name, fieldPath string) bool {
	for _, e := range env {
		if e.Name != name {
			continue
		}
		if e.ValueFrom == nil || e.ValueFrom.FieldRef == nil {
			return false
		}
		return e.ValueFrom.FieldRef.FieldPath == fieldPath
	}
	return false
}
