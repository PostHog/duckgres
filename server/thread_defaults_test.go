package server

import "testing"

func TestDefaultDuckDBThreads(t *testing.T) {
	tests := []struct {
		name            string
		cpuMillicores   int64
		expectedThreads int
	}{
		{name: "no CPU", cpuMillicores: 0, expectedThreads: 0},
		{name: "negative CPU", cpuMillicores: -1, expectedThreads: 0},
		{name: "fractional CPU", cpuMillicores: 500, expectedThreads: 2},
		{name: "three quarters CPU", cpuMillicores: 750, expectedThreads: 2},
		{name: "one CPU", cpuMillicores: 1000, expectedThreads: 3},
		{name: "one and a half CPUs", cpuMillicores: 1500, expectedThreads: 4},
		{name: "default production worker", cpuMillicores: 15000, expectedThreads: 38},
		{name: "maximum production worker", cpuMillicores: 46000, expectedThreads: 115},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DefaultDuckDBThreads(tt.cpuMillicores); got != tt.expectedThreads {
				t.Fatalf("DefaultDuckDBThreads(%d) = %d, want %d", tt.cpuMillicores, got, tt.expectedThreads)
			}
		})
	}
}
