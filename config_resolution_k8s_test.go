package main

import "testing"

func TestResolveEffectiveConfigDefaultsK8sWorkerServiceAccountToNeutralWorker(t *testing.T) {
	resolved := resolveEffectiveConfig(nil, configCLIInputs{}, nil, nil)

	if resolved.K8sWorkerServiceAccount != "duckgres-worker" {
		t.Fatalf("expected default K8s worker service account duckgres-worker, got %q", resolved.K8sWorkerServiceAccount)
	}
}

func TestResolveEffectiveConfigExposesDuckLakeDefaultSpecVersionForControlPlane(t *testing.T) {
	resolved := resolveEffectiveConfig(nil, configCLIInputs{}, func(key string) string {
		if key == "DUCKGRES_DUCKLAKE_DEFAULT_SPEC_VERSION" {
			return "1.1"
		}
		return ""
	}, nil)

	if resolved.DuckLakeDefaultSpecVersion != "1.1" {
		t.Fatalf("expected DuckLake default spec version 1.1, got %q", resolved.DuckLakeDefaultSpecVersion)
	}
}
