//go:build kubernetes

package controlplane

import "testing"

func TestProvisionerControllerEnabled(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"", true},
		{"true", true},
		{"1", true},
		{"false", false},
		{"0", false},
		{"not-a-bool", true},
	} {
		t.Setenv(envProvisionerEnabled, tc.value)
		if got := provisionerControllerEnabled(); got != tc.want {
			t.Errorf("%s=%q: got %v, want %v", envProvisionerEnabled, tc.value, got, tc.want)
		}
	}
}
