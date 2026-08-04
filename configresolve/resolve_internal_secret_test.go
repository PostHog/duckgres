package configresolve

import (
	"reflect"
	"testing"
)

func TestResolveEffectiveInternalSecretFallbacksFromEnv(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string {
		if key == "DUCKGRES_INTERNAL_SECRET_FALLBACKS" {
			return "old1, old2,"
		}
		return ""
	}, nil)

	want := []string{"old1", "old2"}
	if !reflect.DeepEqual(resolved.InternalSecretFallbacks, want) {
		t.Fatalf("InternalSecretFallbacks = %v, want %v (trimmed, empty entries dropped)",
			resolved.InternalSecretFallbacks, want)
	}
}

func TestResolveEffectiveInternalSecretFallbacksCLIBeatsEnv(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{
		Set:                     map[string]bool{"internal-secret-fallbacks": true},
		InternalSecretFallbacks: "cli-old",
	}, func(key string) string {
		if key == "DUCKGRES_INTERNAL_SECRET_FALLBACKS" {
			return "env-old"
		}
		return ""
	}, nil)

	want := []string{"cli-old"}
	if !reflect.DeepEqual(resolved.InternalSecretFallbacks, want) {
		t.Fatalf("InternalSecretFallbacks = %v, want %v (CLI beats env)",
			resolved.InternalSecretFallbacks, want)
	}
}

func TestResolveEffectiveInternalSecretFallbacksExplicitEmptyCLIClearsEnv(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{
		Set:                     map[string]bool{"internal-secret-fallbacks": true},
		InternalSecretFallbacks: "",
	}, func(key string) string {
		if key == "DUCKGRES_INTERNAL_SECRET_FALLBACKS" {
			return "env-old"
		}
		return ""
	}, nil)

	if len(resolved.InternalSecretFallbacks) != 0 {
		t.Fatalf("InternalSecretFallbacks = %v, want empty (explicit empty flag clears env)",
			resolved.InternalSecretFallbacks)
	}
}

func TestResolveEffectiveInternalSecretFallbacksUnsetIsNil(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, nil, nil)

	if resolved.InternalSecretFallbacks != nil {
		t.Fatalf("InternalSecretFallbacks = %v, want nil when unset",
			resolved.InternalSecretFallbacks)
	}
}
