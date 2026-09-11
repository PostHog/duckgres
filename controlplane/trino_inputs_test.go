//go:build kubernetes

package controlplane

import (
	"strings"
	"testing"
)

func TestTrinoFilesystemCacheEnabled(t *testing.T) {
	for _, tc := range []struct {
		name, value   string
		want, wantErr bool
	}{
		{"default", "", false, false},
		{"disabled", "false", false, false},
		{"enabled", "true", true, false},
		{"trimmed", " true ", true, false},
		{"invalid", "enabled", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envTrinoFilesystemCacheEnabled, tc.value)
			got, err := trinoFilesystemCacheEnabled()
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, want error %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("enabled = %v, want %v", got, tc.want)
			}
			if err != nil && !strings.Contains(err.Error(), envTrinoFilesystemCacheEnabled) {
				t.Fatalf("error must identify setting: %v", err)
			}
		})
	}
}

func TestBuildTrinoWiringRejectsInvalidFilesystemCacheSetting(t *testing.T) {
	t.Setenv(envTrinoCoordinatorURL, "https://trino.example.com")
	t.Setenv(envTrinoFilesystemCacheEnabled, "invalid")
	_, err := buildTrinoWiring(nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), envTrinoFilesystemCacheEnabled) {
		t.Fatalf("expected invalid cache setting error before constructing dependencies, got %v", err)
	}
}
