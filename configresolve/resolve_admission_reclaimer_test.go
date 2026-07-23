package configresolve

import (
	"strings"
	"testing"

	"github.com/posthog/duckgres/configloader"
)

func TestResolveEffectiveAdmissionReclaimerMaxReservationsDefault(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, nil, nil)

	if resolved.AdmissionReclaimerMaxReservations != 4096 {
		t.Fatalf("AdmissionReclaimerMaxReservations = %d, want 4096", resolved.AdmissionReclaimerMaxReservations)
	}
}

func TestResolveEffectiveAdmissionReclaimerMaxReservationsPrecedence(t *testing.T) {
	fileCfg := &configloader.FileConfig{AdmissionReclaimerMaxReservations: 1024}
	getenv := func(key string) string {
		if key == "DUCKGRES_ADMISSION_RECLAIMER_MAX_RESERVATIONS" {
			return "2048"
		}
		return ""
	}

	tests := []struct {
		name string
		cli  CLIInputs
		want int
	}{
		{name: "environment beats yaml", cli: CLIInputs{}, want: 2048},
		{
			name: "cli beats environment",
			cli: CLIInputs{
				Set:                               map[string]bool{"admission-reclaimer-max-reservations": true},
				AdmissionReclaimerMaxReservations: 3072,
			},
			want: 3072,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolved := ResolveEffective(fileCfg, tt.cli, getenv, nil)
			if resolved.AdmissionReclaimerMaxReservations != tt.want {
				t.Fatalf("AdmissionReclaimerMaxReservations = %d, want %d", resolved.AdmissionReclaimerMaxReservations, tt.want)
			}
		})
	}
}

func TestResolveEffectiveAdmissionReclaimerMaxReservationsRejectsInvalidOverrides(t *testing.T) {
	fileCfg := &configloader.FileConfig{AdmissionReclaimerMaxReservations: -1}
	warnings := make([]string, 0, 3)
	resolved := ResolveEffective(fileCfg, CLIInputs{
		Set:                               map[string]bool{"admission-reclaimer-max-reservations": true},
		AdmissionReclaimerMaxReservations: 0,
	}, func(key string) string {
		if key == "DUCKGRES_ADMISSION_RECLAIMER_MAX_RESERVATIONS" {
			return "not-an-integer"
		}
		return ""
	}, func(message string) {
		if strings.Contains(message, "admission_reclaimer_max_reservations") ||
			strings.Contains(message, "ADMISSION_RECLAIMER_MAX_RESERVATIONS") ||
			strings.Contains(message, "admission-reclaimer-max-reservations") {
			warnings = append(warnings, message)
		}
	})

	if resolved.AdmissionReclaimerMaxReservations != 4096 {
		t.Fatalf("AdmissionReclaimerMaxReservations = %d, want default 4096", resolved.AdmissionReclaimerMaxReservations)
	}
	if len(warnings) != 3 {
		t.Fatalf("warnings = %v, want one warning for each invalid YAML, environment, and CLI value", warnings)
	}
}
