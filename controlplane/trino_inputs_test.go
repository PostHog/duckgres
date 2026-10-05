//go:build kubernetes

package controlplane

import (
	"strings"
	"testing"
)

func TestTrinoFilesystemCacheEnabled(t *testing.T) {
	for _, setting := range []struct {
		env  string
		read func() (bool, error)
	}{
		{envTrinoFilesystemCacheEnabled, trinoFilesystemCacheEnabled},
		{envTrinoHoglakeFilesystemCacheEnabled, trinoHoglakeFilesystemCacheEnabled},
	} {
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
			t.Run(setting.env+"/"+tc.name, func(t *testing.T) {
				t.Setenv(setting.env, tc.value)
				got, err := setting.read()
				if (err != nil) != tc.wantErr {
					t.Fatalf("error = %v, want error %v", err, tc.wantErr)
				}
				if got != tc.want {
					t.Fatalf("enabled = %v, want %v", got, tc.want)
				}
				if err != nil && err.Error() != setting.env+" must be a boolean" {
					t.Fatalf("error must identify setting: %v", err)
				}
			})
		}
	}
}

// The Hoglake setting never falls back to the DuckLake one, or the reverse:
// enabling the cache for one backend must not enable it for the other.
func TestTrinoFilesystemCacheSettingsAreIndependent(t *testing.T) {
	t.Setenv(envTrinoFilesystemCacheEnabled, "true")
	t.Setenv(envTrinoHoglakeFilesystemCacheEnabled, "")
	if ducklake, err := trinoFilesystemCacheEnabled(); err != nil || !ducklake {
		t.Fatalf("DuckLake cache = %v, %v; want true", ducklake, err)
	}
	if hoglake, err := trinoHoglakeFilesystemCacheEnabled(); err != nil || hoglake {
		t.Fatalf("Hoglake cache = %v, %v; want false", hoglake, err)
	}

	t.Setenv(envTrinoFilesystemCacheEnabled, "")
	t.Setenv(envTrinoHoglakeFilesystemCacheEnabled, "true")
	if ducklake, err := trinoFilesystemCacheEnabled(); err != nil || ducklake {
		t.Fatalf("DuckLake cache = %v, %v; want false", ducklake, err)
	}
	if hoglake, err := trinoHoglakeFilesystemCacheEnabled(); err != nil || !hoglake {
		t.Fatalf("Hoglake cache = %v, %v; want true", hoglake, err)
	}
}

func TestBuildTrinoWiringRejectsInvalidFilesystemCacheSetting(t *testing.T) {
	for _, tc := range []struct{ invalid, valid string }{
		{envTrinoFilesystemCacheEnabled, envTrinoHoglakeFilesystemCacheEnabled},
		{envTrinoHoglakeFilesystemCacheEnabled, envTrinoFilesystemCacheEnabled},
	} {
		t.Run(tc.invalid, func(t *testing.T) {
			t.Setenv(tc.valid, "false")
			t.Setenv(tc.invalid, "invalid")
			_, err := buildTrinoCellWiring(nil, nil, nil, trinoCell{Mode: trinoPoolModeShared})
			if err == nil || !strings.Contains(err.Error(), tc.invalid) {
				t.Fatalf("expected invalid cache setting error before constructing dependencies, got %v", err)
			}
		})
	}
}
