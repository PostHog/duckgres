//go:build kubernetes

package controlplane

import "testing"

func TestHeadroomPlaceholdersNeeded(t *testing.T) {
	const (
		gi = 1 << 30
	)
	tests := []struct {
		name                          string
		allocCPUMillis, allocMemBytes int64
		phCPUMillis, phMemBytes       int64
		percent                       int
		want                          int
	}{
		{name: "disabled (0%)", allocCPUMillis: 100000, allocMemBytes: 1000 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 0, want: 0},
		// 100 cores, 200Gi alloc; 15% => 15 cores / 30Gi headroom; placeholder 8c/16Gi.
		// cpu: ceil(15/8)=2 ; mem: ceil(30/16)=2 => 2.
		{name: "cpu and mem both 2", allocCPUMillis: 100000, allocMemBytes: 200 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 15, want: 2},
		// memory-bound: 100c/2000Gi, 10% => 10c/200Gi; cpu ceil(10/8)=2, mem ceil(200/16)=13 => 13.
		{name: "memory bound", allocCPUMillis: 100000, allocMemBytes: 2000 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 10, want: 13},
		// cpu-bound: 1000c/100Gi, 20% => 200c/20Gi; cpu ceil(200/8)=25, mem ceil(20/16)=2 => 25.
		{name: "cpu bound", allocCPUMillis: 1000000, allocMemBytes: 100 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 20, want: 25},
		// tiny cluster, headroom rounds up to 1 placeholder.
		{name: "rounds up to one", allocCPUMillis: 8000, allocMemBytes: 16 * gi, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 10, want: 1},
		// zero allocatable => none.
		{name: "no nodes", allocCPUMillis: 0, allocMemBytes: 0, phCPUMillis: 8000, phMemBytes: 16 * gi, percent: 25, want: 0},
		// placeholder with no size => none (guards a misconfig).
		{name: "no placeholder size", allocCPUMillis: 100000, allocMemBytes: 200 * gi, phCPUMillis: 0, phMemBytes: 0, percent: 25, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := headroomPlaceholdersNeeded(tt.allocCPUMillis, tt.allocMemBytes, tt.phCPUMillis, tt.phMemBytes, tt.percent)
			if got != tt.want {
				t.Fatalf("headroomPlaceholdersNeeded = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestLabelSelectorStringDeterministic(t *testing.T) {
	got := labelSelectorString(map[string]string{"posthog.com/nodepool": "duckgres-workers", "kubernetes.io/arch": "arm64"})
	want := "kubernetes.io/arch=arm64,posthog.com/nodepool=duckgres-workers"
	if got != want {
		t.Fatalf("labelSelectorString = %q, want %q", got, want)
	}
	if labelSelectorString(nil) != "" {
		t.Fatal("nil selector must render empty")
	}
}
