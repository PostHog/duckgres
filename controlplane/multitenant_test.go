//go:build kubernetes

package controlplane

import "testing"

func TestBuildMultiTenantBasePoolConfigDoesNotUseSharedWorkerConfigMap(t *testing.T) {
	cfg := ControlPlaneConfig{
		ConfigPath:        "/etc/duckgres/duckgres.yaml",
		WorkerIdleTimeout: 0,
		K8s: K8sConfig{
			WorkerNamespace: "duckgres",
			ControlPlaneID:  "cp-1",
			WorkerImage:     "duckgres:test",
			WorkerPort:      8816,
			WorkerSecret:    "duckgres-worker-token",
			ImagePullPolicy: "IfNotPresent",
			ServiceAccount:  "duckgres-control-plane",
		},
	}

	got := buildMultiTenantBasePoolConfig(cfg, 8<<30, 4)

	if got.ConfigMap != "" {
		t.Fatalf("expected multitenant base pool config to avoid shared worker configmap, got %q", got.ConfigMap)
	}
	if got.WorkerImage != "duckgres:test" {
		t.Fatalf("expected worker image duckgres:test, got %q", got.WorkerImage)
	}
	if got.SecretName != "duckgres-worker-token" {
		t.Fatalf("expected worker secret duckgres-worker-token, got %q", got.SecretName)
	}
}
