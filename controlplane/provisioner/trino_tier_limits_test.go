//go:build kubernetes

package provisioner

import "testing"

func TestTrinoTierLimitsMatchTheResourceGroupLanes(t *testing.T) {
	cases := []struct {
		tier                  string
		maxRunning, maxQueued int
	}{
		{"free", 3, 20},
		{"growth", 10, 50},
		{"scale", 25, 100},
		{"", 3, 20},
		{"unknown-tier", 3, 20},
	}
	for _, tc := range cases {
		maxRunning, maxQueued := TrinoTierLimits(tc.tier)
		if maxRunning != tc.maxRunning || maxQueued != tc.maxQueued {
			t.Errorf("TrinoTierLimits(%q) = %d, %d; want %d, %d", tc.tier, maxRunning, maxQueued, tc.maxRunning, tc.maxQueued)
		}
	}
}
