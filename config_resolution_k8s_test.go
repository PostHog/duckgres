package main

import "testing"

func TestResolveEffectiveConfigDefaultsK8sWorkerServiceAccountToNeutralWorker(t *testing.T) {
	resolved := resolveEffectiveConfig(nil, configCLIInputs{}, nil, nil)

	if resolved.K8sWorkerServiceAccount != "duckgres-worker" {
		t.Fatalf("expected default K8s worker service account duckgres-worker, got %q", resolved.K8sWorkerServiceAccount)
	}
}
