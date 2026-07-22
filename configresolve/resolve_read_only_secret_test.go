package configresolve

import (
	"reflect"
	"testing"
)

// Mirrors resolve_internal_secret_test.go: the read-only secret must keep
// the exact env/CLI/fallback semantics of the internal secret it is scoped
// down from. The two resolution paths are copy-paste twins today; these
// tests keep them that way.

func TestResolveEffectiveReadOnlySecretFromEnvAndCLI(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		if key == "DUCKGRES_READ_ONLY_SECRET" {
			return "env-secret"
		}
		return ""
	}, nil)
	if resolved.ReadOnlySecret != "env-secret" {
		t.Fatalf("ReadOnlySecret = %q, want env-secret", resolved.ReadOnlySecret)
	}

	resolved = ResolveEffective(nil, CLIInputs{
		Set:             map[string]bool{"read-only-secret": true},
		ReadOnlySecret: "cli-secret",
	}, func(key string) string {
		if key == "DUCKGRES_READ_ONLY_SECRET" {
			return "env-secret"
		}
		return ""
	}, nil)
	if resolved.ReadOnlySecret != "cli-secret" {
		t.Fatalf("ReadOnlySecret = %q, want cli-secret (CLI beats env)", resolved.ReadOnlySecret)
	}
}

func TestResolveEffectiveReadOnlySecretFallbacksFromEnv(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		if key == "DUCKGRES_READ_ONLY_SECRET_FALLBACKS" {
			return "old1, old2,"
		}
		return ""
	}, nil)

	want := []string{"old1", "old2"}
	if !reflect.DeepEqual(resolved.ReadOnlySecretFallbacks, want) {
		t.Fatalf("ReadOnlySecretFallbacks = %v, want %v (trimmed, empty entries dropped)",
			resolved.ReadOnlySecretFallbacks, want)
	}
}

func TestResolveEffectiveReadOnlySecretFallbacksCLIBeatsEnv(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{
		Set:                      map[string]bool{"read-only-secret-fallbacks": true},
		ReadOnlySecretFallbacks: "cli-old",
	}, func(key string) string {
		if key == "DUCKGRES_READ_ONLY_SECRET_FALLBACKS" {
			return "env-old"
		}
		return ""
	}, nil)

	want := []string{"cli-old"}
	if !reflect.DeepEqual(resolved.ReadOnlySecretFallbacks, want) {
		t.Fatalf("ReadOnlySecretFallbacks = %v, want %v (CLI beats env)",
			resolved.ReadOnlySecretFallbacks, want)
	}
}

func TestResolveEffectiveReadOnlySecretFallbacksExplicitEmptyCLIClearsEnv(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{
		Set:                      map[string]bool{"read-only-secret-fallbacks": true},
		ReadOnlySecretFallbacks: "",
	}, func(key string) string {
		if key == "DUCKGRES_READ_ONLY_SECRET_FALLBACKS" {
			return "env-old"
		}
		return ""
	}, nil)

	if len(resolved.ReadOnlySecretFallbacks) != 0 {
		t.Fatalf("ReadOnlySecretFallbacks = %v, want empty (explicit empty flag clears env)",
			resolved.ReadOnlySecretFallbacks)
	}
}

func TestResolveEffectiveReadOnlySecretUnsetIsEmpty(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, nil, nil)

	if resolved.ReadOnlySecret != "" {
		t.Fatalf("ReadOnlySecret = %q, want empty when unset", resolved.ReadOnlySecret)
	}
	if resolved.ReadOnlySecretFallbacks != nil {
		t.Fatalf("ReadOnlySecretFallbacks = %v, want nil when unset",
			resolved.ReadOnlySecretFallbacks)
	}
}
