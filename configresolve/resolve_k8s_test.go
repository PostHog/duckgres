package configresolve

import (
	"slices"
	"testing"
	"time"
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

func TestResolveEffectiveParsesMetadataHostnameSuffixes(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		switch key {
		case "DUCKGRES_METADATA_HOSTNAME_SUFFIXES":
			return " .md.dev.postwh.com, .md.us.postwh.com, .md.eu.postwh.com "
		case "DUCKGRES_METADATA_PROXY_MAX_CONNECTIONS_PER_ORG":
			return "7"
		}
		return ""
	}, nil)

	want := []string{".md.dev.postwh.com", ".md.us.postwh.com", ".md.eu.postwh.com"}
	if !slices.Equal(resolved.MetadataHostnameSuffixes, want) {
		t.Fatalf("expected metadata hostname suffixes %v, got %v", want, resolved.MetadataHostnameSuffixes)
	}
	if resolved.MetadataProxyMaxConns != 7 {
		t.Fatalf("expected metadata per-org connection cap 7, got %d", resolved.MetadataProxyMaxConns)
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

func TestResolveEffectiveParsesClientIdleTimeoutMax(t *testing.T) {
	var warned []string
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		switch key {
		case "DUCKGRES_CLIENT_IDLE_TIMEOUT_MAX":
			return "15m"
		default:
			return ""
		}
	}, func(msg string) { warned = append(warned, msg) })
	if resolved.Server.ClientIdleTimeoutMax != 15*time.Minute {
		t.Fatalf("ClientIdleTimeoutMax = %s, want 15m", resolved.Server.ClientIdleTimeoutMax)
	}
	if len(warned) != 0 {
		t.Fatalf("unexpected warnings: %v", warned)
	}
}

func TestResolveEffectiveRejectsInvalidClientIdleTimeoutMax(t *testing.T) {
	var warned []string
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		if key == "DUCKGRES_CLIENT_IDLE_TIMEOUT_MAX" {
			return "-1s"
		}
		return ""
	}, func(msg string) { warned = append(warned, msg) })
	if resolved.Server.ClientIdleTimeoutMax != 0 {
		t.Fatalf("ClientIdleTimeoutMax = %s, want disabled", resolved.Server.ClientIdleTimeoutMax)
	}
	if len(warned) != 1 {
		t.Fatalf("warnings = %v, want one invalid-duration warning", warned)
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

func TestExploratoryTierEnvKnobs(t *testing.T) {
	env := map[string]string{
		"DUCKGRES_EXPLORATORY_TIER_ENABLED":  "true",
		"DUCKGRES_EXPLORATORY_WORKER_CPU":    "2",
		"DUCKGRES_EXPLORATORY_WORKER_MEMORY": "4Gi",
		"DUCKGRES_EXPLORATORY_WORKER_TTL":    "48h",
	}
	getenv := func(k string) string { return env[k] }
	r := ResolveEffective(nil, CLIInputs{}, getenv, nil)
	if !r.K8sExploratoryTierEnabled {
		t.Fatal("expected exploratory tier enabled")
	}
	if r.K8sExploratoryWorkerCPU != "2" || r.K8sExploratoryWorkerMemory != "4Gi" {
		t.Fatalf("cpu=%q mem=%q", r.K8sExploratoryWorkerCPU, r.K8sExploratoryWorkerMemory)
	}
	if r.K8sExploratoryWorkerTTL != 48*time.Hour {
		t.Fatalf("ttl=%v", r.K8sExploratoryWorkerTTL)
	}
}

func TestExploratoryTierEnvKnobsInvalid(t *testing.T) {
	var warned []string
	env := map[string]string{
		"DUCKGRES_EXPLORATORY_TIER_ENABLED": "banana",
		"DUCKGRES_EXPLORATORY_WORKER_TTL":   "-5m",
	}
	r := ResolveEffective(nil, CLIInputs{}, func(k string) string { return env[k] }, func(w string) { warned = append(warned, w) })
	if r.K8sExploratoryTierEnabled {
		t.Fatal("invalid bool must leave tier disabled")
	}
	if r.K8sExploratoryWorkerTTL != 0 {
		t.Fatalf("invalid ttl must stay 0 (built-in default applied later), got %v", r.K8sExploratoryWorkerTTL)
	}
	if len(warned) != 2 {
		t.Fatalf("want 2 warnings, got %v", warned)
	}
}
