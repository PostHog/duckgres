//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"
)

func secretKeyRefEnv(name, secret, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secret},
				Key:                  key,
			},
		},
	}
}

func setCPPodNamedEnv(t *testing.T, pool *K8sWorkerPool, env []corev1.EnvVar, envFrom []corev1.EnvFromSource) {
	t.Helper()
	pod, err := pool.clientset.CoreV1().Pods(pool.namespace).Get(context.Background(), pool.cpID, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CP pod: %v", err)
	}
	pod.Spec.Containers = []corev1.Container{{
		Name:    "controlplane",
		Image:   "example/duckgres-controlplane:test",
		Env:     env,
		EnvFrom: envFrom,
	}}
	if _, err := pool.clientset.CoreV1().Pods(pool.namespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update CP pod: %v", err)
	}
}

func captureSpawnedWorkerEnv(t *testing.T, pool *K8sWorkerPool) []corev1.EnvVar {
	t.Helper()
	cs, ok := pool.clientset.(interface {
		PrependReactor(string, string, k8stesting.ReactionFunc)
	})
	if !ok {
		t.Fatal("clientset does not support PrependReactor")
	}
	var created *corev1.Pod
	cs.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		createAction, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		pod, ok := createAction.GetObject().(*corev1.Pod)
		if !ok || pod.Labels["app"] != "duckgres-worker" {
			return false, nil, nil
		}
		created = pod.DeepCopy()
		return true, nil, k8serrors.NewForbidden(
			schema.GroupResource{Resource: "pods"}, pod.Name, errors.New("stop after capture"),
		)
	})
	if err := pool.spawnWorker(context.Background(), 3, pool.workerImage, WorkerProfile{}, false); err == nil {
		t.Fatal("spawnWorker unexpectedly succeeded")
	}
	if created == nil {
		t.Fatal("worker Pod was not submitted")
	}
	if len(created.Spec.Containers) == 0 {
		t.Fatal("worker Pod has no containers")
	}
	return created.Spec.Containers[0].Env
}

func loadPostHogFromPODNAME(t *testing.T, pool *K8sWorkerPool) {
	t.Helper()
	t.Setenv("POD_NAME", pool.cpID)
	pool.loadPostHogLogEnv(context.Background())
}

// TestWorkerSpawnPostHogEnvIsSecretRef pins the charts contract: a named
// POSTHOG_API_KEY secretKeyRef on the CP pod is copied as a ref. A literal
// value is refused so the token never appears as value: on the worker spec.
func TestWorkerSpawnPostHogEnvIsSecretRef(t *testing.T) {
	t.Run("secretKeyRef is copied with other log knobs", func(t *testing.T) {
		pool, _ := newTestK8sPool(t, 5)
		setCPPodNamedEnv(t, pool, []corev1.EnvVar{
			secretKeyRefEnv("POSTHOG_API_KEY", "duckgres-posthog", "api-key"),
			{Name: "POSTHOG_HOST", Value: "us.i.posthog.com"},
			{Name: "DUCKGRES_POSTHOG_LOG_LEVEL", Value: "warn"},
			{Name: "DUCKGRES_POSTHOG_LOG_INFO_SAMPLE", Value: "0"},
			{Name: "DUCKGRES_POSTHOG_LOG_QUERY_TEXT", Value: "redacted"},
			{Name: "DUCKGRES_IDENTIFIER", Value: "mw-dev"},
			{Name: "ADDITIONAL_POSTHOG_API_KEYS", Value: "phc_must_not_copy"},
			{Name: "POSTHOG_ANALYTICS_API_KEY", Value: "phc_analytics_only"},
		}, nil)
		loadPostHogFromPODNAME(t, pool)

		env := envByName(captureSpawnedWorkerEnv(t, pool))
		got, ok := env["POSTHOG_API_KEY"]
		if !ok {
			t.Fatal("POSTHOG_API_KEY missing on worker")
		}
		if got.Value != "" {
			t.Fatalf("POSTHOG_API_KEY materialized as value %q", got.Value)
		}
		if got.ValueFrom == nil || got.ValueFrom.SecretKeyRef == nil {
			t.Fatalf("POSTHOG_API_KEY is not a secretKeyRef: %+v", got)
		}
		if got.ValueFrom.SecretKeyRef.Name != "duckgres-posthog" || got.ValueFrom.SecretKeyRef.Key != "api-key" {
			t.Fatalf("POSTHOG_API_KEY secretKeyRef = %s/%s", got.ValueFrom.SecretKeyRef.Name, got.ValueFrom.SecretKeyRef.Key)
		}
		if env["POSTHOG_HOST"].Value != "us.i.posthog.com" {
			t.Fatalf("POSTHOG_HOST = %+v", env["POSTHOG_HOST"])
		}
		if env["DUCKGRES_POSTHOG_LOG_LEVEL"].Value != "warn" {
			t.Fatalf("DUCKGRES_POSTHOG_LOG_LEVEL = %+v", env["DUCKGRES_POSTHOG_LOG_LEVEL"])
		}
		if env["DUCKGRES_IDENTIFIER"].Value != "mw-dev" {
			t.Fatalf("DUCKGRES_IDENTIFIER = %+v", env["DUCKGRES_IDENTIFIER"])
		}
		if _, leaked := env["ADDITIONAL_POSTHOG_API_KEYS"]; leaked {
			t.Fatal("ADDITIONAL_POSTHOG_API_KEYS must not be forwarded")
		}
		if _, leaked := env["POSTHOG_ANALYTICS_API_KEY"]; leaked {
			t.Fatal("POSTHOG_ANALYTICS_API_KEY must not be forwarded")
		}
		ns := env["POD_NAMESPACE"]
		if ns.ValueFrom == nil || ns.ValueFrom.FieldRef == nil || ns.ValueFrom.FieldRef.FieldPath != "metadata.namespace" {
			t.Fatalf("POD_NAMESPACE downward API = %+v", ns)
		}
	})

	t.Run("literal value is not copied", func(t *testing.T) {
		pool, _ := newTestK8sPool(t, 5)
		setCPPodNamedEnv(t, pool, []corev1.EnvVar{
			{Name: "POSTHOG_API_KEY", Value: "phc_literal_must_not_appear"},
			{Name: "POSTHOG_HOST", Value: "us.i.posthog.com"},
		}, nil)
		loadPostHogFromPODNAME(t, pool)

		env := envByName(captureSpawnedWorkerEnv(t, pool))
		if got, ok := env["POSTHOG_API_KEY"]; ok {
			t.Fatalf("literal POSTHOG_API_KEY was copied onto the worker: %+v", got)
		}
		if env["POSTHOG_HOST"].Value != "us.i.posthog.com" {
			t.Fatalf("POSTHOG_HOST should still copy: %+v", env["POSTHOG_HOST"])
		}
	})
}

// TestWorkerSpawnOmitsPostHogWhenCPHasOnlyEnvFrom pins that envFrom is
// insufficient: a Pod GET does not resolve those keys, and we must not invent
// a plaintext value: from os.Getenv.
func TestWorkerSpawnOmitsPostHogWhenCPHasOnlyEnvFrom(t *testing.T) {
	t.Setenv("POSTHOG_API_KEY", "phc_from_process_env_must_not_be_invented")
	pool, _ := newTestK8sPool(t, 5)
	setCPPodNamedEnv(t, pool, nil, []corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "duckgres-posthog"},
		},
	}})
	loadPostHogFromPODNAME(t, pool)

	env := envByName(captureSpawnedWorkerEnv(t, pool))
	if got, ok := env["POSTHOG_API_KEY"]; ok {
		t.Fatalf("envFrom-only CP must not invent POSTHOG_API_KEY on the worker: %+v", got)
	}
}

// TestWorkerSpawnSucceedsWhenCPPodGetFails: a missing CP pod (or empty
// POD_NAME) must not fail spawn. Workers just will not export.
func TestWorkerSpawnSucceedsWhenCPPodGetFails(t *testing.T) {
	t.Run("get fails", func(t *testing.T) {
		pool, _ := newTestK8sPool(t, 5)
		t.Setenv("POD_NAME", "no-such-cp-pod")
		pool.loadPostHogLogEnv(context.Background())

		env := envByName(captureSpawnedWorkerEnv(t, pool))
		if got, ok := env["POSTHOG_API_KEY"]; ok {
			t.Fatalf("failed CP Get must omit POSTHOG_API_KEY, got %+v", got)
		}
	})

	t.Run("POD_NAME empty", func(t *testing.T) {
		pool, _ := newTestK8sPool(t, 5)
		if err := os.Unsetenv("POD_NAME"); err != nil {
			t.Fatal(err)
		}
		pool.loadPostHogLogEnv(context.Background())

		env := envByName(captureSpawnedWorkerEnv(t, pool))
		if got, ok := env["POSTHOG_API_KEY"]; ok {
			t.Fatalf("empty POD_NAME must omit POSTHOG_API_KEY, got %+v", got)
		}
	})
}
