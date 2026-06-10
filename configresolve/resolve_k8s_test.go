package configresolve

import (
	"testing"
)

func TestResolveEffectiveDefaultsK8sWorkerServiceAccountToDefaultWorker(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, nil, nil)

	if resolved.K8sWorkerServiceAccount != "duckgres-worker" {
		t.Fatalf("expected default K8s worker service account duckgres-worker, got %q", resolved.K8sWorkerServiceAccount)
	}
}

func TestResolveEffectiveExposesDuckLakeDefaultSpecVersionForControlPlane(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		if key == "DUCKGRES_DUCKLAKE_DEFAULT_SPEC_VERSION" {
			return "1.1"
		}
		return ""
	}, nil)

	if resolved.DuckLakeDefaultSpecVersion != "1.1" {
		t.Fatalf("expected DuckLake default spec version 1.1, got %q", resolved.DuckLakeDefaultSpecVersion)
	}
}

func TestResolveEffectiveParsesK8sWorkerMaxTTL(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		if key == "DUCKGRES_K8S_WORKER_MAX_TTL" {
			return "1h"
		}
		return ""
	}, nil)

	if resolved.K8sWorkerMaxTTL.String() != "1h0m0s" {
		t.Fatalf("expected K8s worker max TTL 1h, got %s", resolved.K8sWorkerMaxTTL)
	}
}

func TestResolveEffectiveParsesK8sWorkerDefaultTTL(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		if key == "DUCKGRES_K8S_WORKER_DEFAULT_TTL" {
			return "70m"
		}
		return ""
	}, nil)

	if resolved.K8sWorkerDefaultTTL.String() != "1h10m0s" {
		t.Fatalf("expected K8s hot-idle TTL 70m, got %s", resolved.K8sWorkerDefaultTTL)
	}
}

func TestResolveEffectiveRejectsInvalidK8sWorkerDefaultTTL(t *testing.T) {
	var warned []string
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		switch key {
		case "DUCKGRES_K8S_WORKER_DEFAULT_TTL":
			return "soon"
		case "DUCKGRES_K8S_WORKER_MAX_TTL":
			return "-5m"
		}
		return ""
	}, func(msg string) { warned = append(warned, msg) })

	if resolved.K8sWorkerDefaultTTL != 0 {
		t.Fatalf("expected invalid hot-idle TTL to resolve to 0, got %s", resolved.K8sWorkerDefaultTTL)
	}
	if resolved.K8sWorkerMaxTTL != 0 {
		t.Fatalf("expected negative worker max TTL to resolve to 0, got %s", resolved.K8sWorkerMaxTTL)
	}
	if len(warned) != 2 {
		t.Fatalf("expected 2 warnings for invalid TTL values, got %d: %v", len(warned), warned)
	}
}
