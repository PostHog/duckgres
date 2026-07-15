//go:build kubernetes

package controlplane

import (
	"context"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

// fakeCPPod builds a control-plane pod carrying the env/image/SA the spawner
// templates the runner pod from — a mix of literal values and a secretKeyRef,
// plus env NOT on the allowlist that must not leak through.
func fakeCPPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "duckgres-control-plane-abc-xyz", Namespace: "duckgres"},
		Spec: corev1.PodSpec{
			ServiceAccountName: "duckgres-control-plane",
			Containers: []corev1.Container{
				{
					Name:            "controlplane",
					Image:           "example/duckgres-controlplane:test",
					ImagePullPolicy: corev1.PullIfNotPresent,
					Env: []corev1.EnvVar{
						{Name: "DUCKGRES_CONFIG_STORE", Value: "postgres://cs/db"},
						{
							Name: "DUCKGRES_INTERNAL_SECRET",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: "duckgres-tokens"},
									Key:                  "internal-secret",
								},
							},
						},
						{Name: "DUCKGRES_AWS_REGION", Value: "us-east-1"},
						{Name: "DUCKGRES_CONFIG_POLL_INTERVAL", Value: "5s"},
						{Name: "DUCKGRES_K8S_WORKER_IMAGE", Value: "must-not-leak"},
						{Name: "DUCKGRES_USER_SECRET_KEY", Value: "must-not-leak"},
					},
				},
			},
		},
	}
}

func envByName(env []corev1.EnvVar) map[string]corev1.EnvVar {
	m := map[string]corev1.EnvVar{}
	for _, e := range env {
		m[e.Name] = e
	}
	return m
}

// TestSpawnReshardPodSpec pins the runner pod's shape: name/labels, the CP's
// own image + ServiceAccount (no new RBAC), the env allowlist copied VERBATIM
// (secretKeyRef preserved, non-allowlisted CP env NOT leaked), the op id +
// password URL env, restartPolicy Never, generous termination grace, and the
// requests=limits resource shape from the knobs.
func TestSpawnReshardPodSpec(t *testing.T) {
	cs := k8sfake.NewSimpleClientset(fakeCPPod())
	s := NewReshardPodSpawner(cs, "duckgres", "duckgres-control-plane-abc-xyz", "", "")

	op := &configstore.ReshardOperation{
		ID:          42,
		OrgID:       "acme",
		TargetKind:  configstore.MetadataStoreKindExternal,
		PasswordURL: "http://192.0.2.9:8080/api/v1/reshards/42/password",
	}
	if err := s.SpawnReshardPod(context.Background(), op); err != nil {
		t.Fatalf("SpawnReshardPod: %v", err)
	}

	pod, err := cs.CoreV1().Pods("duckgres").Get(context.Background(), "duckgres-reshard-op-42", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("runner pod not created: %v", err)
	}
	if pod.Labels["app"] != "duckgres-reshard" || pod.Labels["duckgres-op-id"] != "42" {
		t.Fatalf("labels = %v", pod.Labels)
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("restartPolicy = %s, want Never", pod.Spec.RestartPolicy)
	}
	if pod.Spec.TerminationGracePeriodSeconds == nil || *pod.Spec.TerminationGracePeriodSeconds != 600 {
		t.Fatalf("terminationGracePeriodSeconds = %v, want 600", pod.Spec.TerminationGracePeriodSeconds)
	}
	if pod.Spec.ServiceAccountName != "duckgres-control-plane" {
		t.Fatalf("serviceAccount = %q, want the CP's own", pod.Spec.ServiceAccountName)
	}
	c := pod.Spec.Containers[0]
	if c.Image != "example/duckgres-controlplane:test" {
		t.Fatalf("image = %q, want the CP's own", c.Image)
	}
	if strings.Join(c.Args, " ") != "--mode reshard-runner" {
		t.Fatalf("args = %v", c.Args)
	}

	env := envByName(c.Env)
	if env["DUCKGRES_RESHARD_OP_ID"].Value != "42" {
		t.Fatalf("DUCKGRES_RESHARD_OP_ID = %q", env["DUCKGRES_RESHARD_OP_ID"].Value)
	}
	if env["DUCKGRES_RESHARD_PASSWORD_URL"].Value != op.PasswordURL {
		t.Fatalf("DUCKGRES_RESHARD_PASSWORD_URL = %q", env["DUCKGRES_RESHARD_PASSWORD_URL"].Value)
	}
	if env["DUCKGRES_CONFIG_STORE"].Value != "postgres://cs/db" {
		t.Fatal("DUCKGRES_CONFIG_STORE not copied")
	}
	// The secretKeyRef must be copied AS A REF — never resolved to a value.
	is := env["DUCKGRES_INTERNAL_SECRET"]
	if is.Value != "" || is.ValueFrom == nil || is.ValueFrom.SecretKeyRef == nil ||
		is.ValueFrom.SecretKeyRef.Name != "duckgres-tokens" {
		t.Fatalf("DUCKGRES_INTERNAL_SECRET not copied as a secretKeyRef: %+v", is)
	}
	for _, name := range []string{"DUCKGRES_K8S_WORKER_IMAGE", "DUCKGRES_USER_SECRET_KEY"} {
		if _, leaked := env[name]; leaked {
			t.Fatalf("non-allowlisted CP env %s leaked into the runner pod", name)
		}
	}

	// Default resource shape 2 CPU / 8Gi, requests=limits.
	cpu := c.Resources.Requests[corev1.ResourceCPU]
	mem := c.Resources.Requests[corev1.ResourceMemory]
	if cpu.String() != "2" || mem.String() != "8Gi" {
		t.Fatalf("default resources = %s/%s, want 2/8Gi", cpu.String(), mem.String())
	}
	if !c.Resources.Limits[corev1.ResourceCPU].Equal(cpu) || !c.Resources.Limits[corev1.ResourceMemory].Equal(mem) {
		t.Fatal("limits != requests")
	}
}

// TestSpawnReshardPodKnobsAndNoPasswordURL pins the env-only resource knobs
// and that a cnpg-target op (no PasswordURL) gets NO password env at all.
func TestSpawnReshardPodKnobsAndNoPasswordURL(t *testing.T) {
	cs := k8sfake.NewSimpleClientset(fakeCPPod())
	s := NewReshardPodSpawner(cs, "duckgres", "duckgres-control-plane-abc-xyz", "4", "16Gi")

	op := &configstore.ReshardOperation{ID: 7, OrgID: "acme", TargetKind: configstore.MetadataStoreKindCnpgShard}
	if err := s.SpawnReshardPod(context.Background(), op); err != nil {
		t.Fatalf("SpawnReshardPod: %v", err)
	}
	pod, err := cs.CoreV1().Pods("duckgres").Get(context.Background(), "duckgres-reshard-op-7", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	c := pod.Spec.Containers[0]
	cpu := c.Resources.Requests[corev1.ResourceCPU]
	mem := c.Resources.Requests[corev1.ResourceMemory]
	if cpu.String() != "4" || mem.String() != "16Gi" {
		t.Fatalf("knob resources = %s/%s, want 4/16Gi", cpu.String(), mem.String())
	}
	if _, has := envByName(c.Env)["DUCKGRES_RESHARD_PASSWORD_URL"]; has {
		t.Fatal("cnpg-target runner pod must carry no password URL env")
	}
}

func TestSpawnReshardPodRejectsInvalidResourceKnobsWithoutPanicking(t *testing.T) {
	cs := k8sfake.NewSimpleClientset(fakeCPPod())
	s := NewReshardPodSpawner(cs, "duckgres", "duckgres-control-plane-abc-xyz", "not-a-cpu", "also-bad")
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("SpawnReshardPod panicked on invalid operator config: %v", recovered)
		}
	}()
	err := s.SpawnReshardPod(context.Background(), &configstore.ReshardOperation{ID: 8})
	if err == nil || !strings.Contains(err.Error(), "invalid reshard pod") {
		t.Fatalf("error = %v, want invalid reshard pod resource error", err)
	}
}

// TestSpawnReshardPodMissingCPPod pins the failure mode when the CP cannot
// read its own pod (the template): a clear error, no pod created.
func TestSpawnReshardPodMissingCPPod(t *testing.T) {
	cs := k8sfake.NewSimpleClientset()
	s := NewReshardPodSpawner(cs, "duckgres", "gone-pod", "", "")
	err := s.SpawnReshardPod(context.Background(), &configstore.ReshardOperation{ID: 1})
	if err == nil || !strings.Contains(err.Error(), "own control-plane pod") {
		t.Fatalf("want own-pod read error, got %v", err)
	}
}

// TestReshardPodLifecycleHelpers pins Get/Delete/List.
func TestReshardPodLifecycleHelpers(t *testing.T) {
	cs := k8sfake.NewSimpleClientset(fakeCPPod())
	s := NewReshardPodSpawner(cs, "duckgres", "duckgres-control-plane-abc-xyz", "", "")
	if err := s.SpawnReshardPod(context.Background(), &configstore.ReshardOperation{ID: 5}); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	pod, err := s.GetReshardPod(context.Background(), 5)
	if err != nil || pod == nil {
		t.Fatalf("get = %v/%v", pod, err)
	}
	pods, err := s.ListReshardPods(context.Background())
	if err != nil || len(pods) != 1 {
		t.Fatalf("list = %d/%v", len(pods), err)
	}
	if err := s.DeleteReshardPod(context.Background(), 5); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if pod, _ := s.GetReshardPod(context.Background(), 5); pod != nil {
		t.Fatal("pod survived delete")
	}
	// Deleting an absent pod is a nil no-op.
	if err := s.DeleteReshardPod(context.Background(), 99); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
}
