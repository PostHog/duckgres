package configresolve

import (
	"testing"

	"github.com/posthog/duckgres/configloader"
	"gopkg.in/yaml.v3"
)

func TestResolveEffectiveRetainedBindBudgetDefaults(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, nil, nil)

	if got, want := resolved.Server.MaxRetainedBindBytes, int64(64<<20); got != want {
		t.Fatalf("MaxRetainedBindBytes = %d, want %d", got, want)
	}
	if got, want := resolved.Server.MaxOpenPortals, 1024; got != want {
		t.Fatalf("MaxOpenPortals = %d, want %d", got, want)
	}
}

func TestResolveEffectiveRetainedBindBudgetsPrecedence(t *testing.T) {
	var fileCfg configloader.FileConfig
	if err := yaml.Unmarshal([]byte("max_retained_bind_bytes: 1048576\nmax_open_portals: 16\n"), &fileCfg); err != nil {
		t.Fatalf("parse YAML config: %v", err)
	}
	env := map[string]string{
		"DUCKGRES_MAX_RETAINED_BIND_BYTES": "2097152",
		"DUCKGRES_MAX_OPEN_PORTALS":        "32",
	}
	cli := CLIInputs{
		Set: map[string]bool{
			"max-retained-bind-bytes": true,
			"max-open-portals":        true,
		},
		MaxRetainedBindBytes: 3 << 20,
		MaxOpenPortals:       64,
	}

	resolved := ResolveEffective(&fileCfg, cli, func(key string) string { return env[key] }, nil)
	if got, want := resolved.Server.MaxRetainedBindBytes, int64(3<<20); got != want {
		t.Fatalf("MaxRetainedBindBytes = %d, want CLI value %d", got, want)
	}
	if got, want := resolved.Server.MaxOpenPortals, 64; got != want {
		t.Fatalf("MaxOpenPortals = %d, want CLI value %d", got, want)
	}
}

func TestResolveEffectiveRetainedBindBudgetsEnvOverridesFile(t *testing.T) {
	var fileCfg configloader.FileConfig
	if err := yaml.Unmarshal([]byte("max_retained_bind_bytes: 1048576\nmax_open_portals: 16\n"), &fileCfg); err != nil {
		t.Fatalf("parse YAML config: %v", err)
	}
	env := map[string]string{
		"DUCKGRES_MAX_RETAINED_BIND_BYTES": "2097152",
		"DUCKGRES_MAX_OPEN_PORTALS":        "32",
	}

	resolved := ResolveEffective(&fileCfg, CLIInputs{}, func(key string) string { return env[key] }, nil)
	if got, want := resolved.Server.MaxRetainedBindBytes, int64(2<<20); got != want {
		t.Fatalf("MaxRetainedBindBytes = %d, want env value %d", got, want)
	}
	if got, want := resolved.Server.MaxOpenPortals, 32; got != want {
		t.Fatalf("MaxOpenPortals = %d, want env value %d", got, want)
	}
}

func TestResolveEffectiveRetainedBindBudgetsRejectNonPositiveValues(t *testing.T) {
	var fileCfg configloader.FileConfig
	if err := yaml.Unmarshal([]byte("max_retained_bind_bytes: -1\nmax_open_portals: 0\n"), &fileCfg); err != nil {
		t.Fatalf("parse YAML config: %v", err)
	}
	var warnings []string
	resolved := ResolveEffective(&fileCfg, CLIInputs{
		Set: map[string]bool{
			"max-retained-bind-bytes": true,
			"max-open-portals":        true,
		},
		MaxRetainedBindBytes: 0,
		MaxOpenPortals:       -1,
	}, func(key string) string {
		switch key {
		case "DUCKGRES_MAX_RETAINED_BIND_BYTES":
			return "0"
		case "DUCKGRES_MAX_OPEN_PORTALS":
			return "-1"
		default:
			return ""
		}
	}, func(message string) {
		warnings = append(warnings, message)
	})

	if got, want := resolved.Server.MaxRetainedBindBytes, int64(64<<20); got != want {
		t.Fatalf("MaxRetainedBindBytes = %d, want default %d after invalid inputs", got, want)
	}
	if got, want := resolved.Server.MaxOpenPortals, 1024; got != want {
		t.Fatalf("MaxOpenPortals = %d, want default %d after invalid inputs", got, want)
	}
	if len(warnings) != 6 {
		t.Fatalf("warnings = %v, want one for each invalid file/env/CLI value", warnings)
	}
}
