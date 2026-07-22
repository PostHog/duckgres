package configresolve

import (
	"reflect"
	"testing"
)

// Mirrors resolve_internal_secret_test.go: the discovery secret must keep
// the exact env/CLI/fallback semantics of the internal secret it is scoped
// down from. The two resolution paths are copy-paste twins today; these
// tests keep them that way.

func TestResolveEffectiveDiscoverySecretFromEnvAndCLI(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		if key == "DUCKGRES_DISCOVERY_SECRET" {
			return "env-secret"
		}
		return ""
	}, nil)
	if resolved.DiscoverySecret != "env-secret" {
		t.Fatalf("DiscoverySecret = %q, want env-secret", resolved.DiscoverySecret)
	}

	resolved = ResolveEffective(nil, CLIInputs{
		Set:             map[string]bool{"discovery-secret": true},
		DiscoverySecret: "cli-secret",
	}, func(key string) string {
		if key == "DUCKGRES_DISCOVERY_SECRET" {
			return "env-secret"
		}
		return ""
	}, nil)
	if resolved.DiscoverySecret != "cli-secret" {
		t.Fatalf("DiscoverySecret = %q, want cli-secret (CLI beats env)", resolved.DiscoverySecret)
	}
}

func TestResolveEffectiveDiscoverySecretFallbacksFromEnv(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		if key == "DUCKGRES_DISCOVERY_SECRET_FALLBACKS" {
			return "old1, old2,"
		}
		return ""
	}, nil)

	want := []string{"old1", "old2"}
	if !reflect.DeepEqual(resolved.DiscoverySecretFallbacks, want) {
		t.Fatalf("DiscoverySecretFallbacks = %v, want %v (trimmed, empty entries dropped)",
			resolved.DiscoverySecretFallbacks, want)
	}
}

func TestResolveEffectiveDiscoverySecretFallbacksCLIBeatsEnv(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{
		Set:                      map[string]bool{"discovery-secret-fallbacks": true},
		DiscoverySecretFallbacks: "cli-old",
	}, func(key string) string {
		if key == "DUCKGRES_DISCOVERY_SECRET_FALLBACKS" {
			return "env-old"
		}
		return ""
	}, nil)

	want := []string{"cli-old"}
	if !reflect.DeepEqual(resolved.DiscoverySecretFallbacks, want) {
		t.Fatalf("DiscoverySecretFallbacks = %v, want %v (CLI beats env)",
			resolved.DiscoverySecretFallbacks, want)
	}
}

func TestResolveEffectiveDiscoverySecretFallbacksExplicitEmptyCLIClearsEnv(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{
		Set:                      map[string]bool{"discovery-secret-fallbacks": true},
		DiscoverySecretFallbacks: "",
	}, func(key string) string {
		if key == "DUCKGRES_DISCOVERY_SECRET_FALLBACKS" {
			return "env-old"
		}
		return ""
	}, nil)

	if len(resolved.DiscoverySecretFallbacks) != 0 {
		t.Fatalf("DiscoverySecretFallbacks = %v, want empty (explicit empty flag clears env)",
			resolved.DiscoverySecretFallbacks)
	}
}

func TestResolveEffectiveDiscoverySecretUnsetIsEmpty(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, nil, nil)

	if resolved.DiscoverySecret != "" {
		t.Fatalf("DiscoverySecret = %q, want empty when unset", resolved.DiscoverySecret)
	}
	if resolved.DiscoverySecretFallbacks != nil {
		t.Fatalf("DiscoverySecretFallbacks = %v, want nil when unset",
			resolved.DiscoverySecretFallbacks)
	}
}
