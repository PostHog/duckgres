package server

import "testing"

func TestParseSessionAffinityOption(t *testing.T) {
	truthy := []string{"true", "TRUE", "on", "On", "yes", "1", " true "}
	for _, raw := range truthy {
		got, err := ParseSessionAffinityOption(raw)
		if err != nil {
			t.Errorf("ParseSessionAffinityOption(%q) returned error %v", raw, err)
		}
		if !got {
			t.Errorf("ParseSessionAffinityOption(%q) = false, want true", raw)
		}
	}

	falsy := []string{"", "false", "off", "no", "0", " OFF "}
	for _, raw := range falsy {
		got, err := ParseSessionAffinityOption(raw)
		if err != nil {
			t.Errorf("ParseSessionAffinityOption(%q) returned error %v", raw, err)
		}
		if got {
			t.Errorf("ParseSessionAffinityOption(%q) = true, want false", raw)
		}
	}

	for _, raw := range []string{"maybe", "2", "enable"} {
		if _, err := ParseSessionAffinityOption(raw); err == nil {
			t.Errorf("ParseSessionAffinityOption(%q) = nil error, want a rejection", raw)
		}
	}
}
