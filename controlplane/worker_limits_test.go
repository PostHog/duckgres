//go:build !kubernetes

package controlplane

import (
	"testing"
)

func TestWorkerDuckDBLimits_RemoteBackend(t *testing.T) {
	tests := []struct {
		name       string
		cpuReq     string
		memReq     string
		wantMem    string
		wantThread int
	}{
		{
			name:       "typical large worker",
			cpuReq:     "46000m",
			memReq:     "360Gi",
			wantMem:    "270GB", // 75% of 360Gi (386547056640 bytes) = 270GB
			wantThread: 46,
		},
		{
			name:       "whole core CPU notation",
			cpuReq:     "46",
			memReq:     "360Gi",
			wantMem:    "270GB",
			wantThread: 46,
		},
		{
			name:       "small worker",
			cpuReq:     "4000m",
			memReq:     "8Gi",
			wantMem:    "6GB",
			wantThread: 4,
		},
		{
			name:       "fractional CPU rounds down to zero",
			cpuReq:     "500m",
			memReq:     "1Gi",
			wantMem:    "768MB",
			wantThread: 0, // 500m < 1 core, rounds to 0
		},
		{
			name:       "1 core minimum",
			cpuReq:     "1000m",
			memReq:     "2Gi",
			wantMem:    "1GB",
			wantThread: 1,
		},
		{
			name:       "empty resources",
			cpuReq:     "",
			memReq:     "",
			wantMem:    "",
			wantThread: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cp := &ControlPlane{
				cfg: ControlPlaneConfig{
					K8s: K8sConfig{
						WorkerCPURequest:    tt.cpuReq,
						WorkerMemoryRequest: tt.memReq,
					},
				},
				isRemoteBackend: true,
			}

			gotMem, gotThreads := cp.workerDuckDBLimits()
			if gotMem != tt.wantMem {
				t.Errorf("memLimit = %q, want %q", gotMem, tt.wantMem)
			}
			if gotThreads != tt.wantThread {
				t.Errorf("threads = %d, want %d", gotThreads, tt.wantThread)
			}
		})
	}
}

func TestWorkerDuckDBLimits_ProcessBackend_UsesRebalancer(t *testing.T) {
	// In process mode (isRemoteBackend=false), the rebalancer derives limits
	// from the CP's own system resources. Verify the rebalancer values are used.
	rebalancer := NewMemoryRebalancer(
		16*1024*1024*1024, // 16GB budget
		8,                 // 8 threads
		&mockSessionLister{},
		false, // rebalancing disabled
	)

	gotMem := rebalancer.MemoryLimit()
	gotThreads := rebalancer.PerSessionThreads()

	if gotMem != "16384MB" {
		t.Errorf("process mode memLimit = %q, want %q", gotMem, "16384MB")
	}
	if gotThreads != 8 {
		t.Errorf("process mode threads = %d, want %d", gotThreads, 8)
	}
}

// TestSessionLimitsRouting verifies that the CP picks the right source for
// DuckDB memory/thread limits depending on the worker backend mode.
// This mirrors the branching logic in handleConnection().
func TestSessionLimitsRouting(t *testing.T) {
	t.Run("remote backend uses worker pod resources", func(t *testing.T) {
		cp := &ControlPlane{
			cfg: ControlPlaneConfig{
				K8s: K8sConfig{
					WorkerCPURequest:    "46000m",
					WorkerMemoryRequest: "360Gi",
				},
			},
			isRemoteBackend: true,
			rebalancer: NewMemoryRebalancer(
				512*1024*1024, // 512MB — the CP's own tiny budget
				1,             // 1 thread — the CP's own tiny CPU
				&mockSessionLister{},
				false,
			),
		}

		// In remote mode, limits should come from worker pod spec, NOT the rebalancer
		memLimit, threads := cp.workerDuckDBLimits()
		if memLimit != "270GB" {
			t.Errorf("remote mode should use worker memory, got %q", memLimit)
		}
		if threads != 46 {
			t.Errorf("remote mode should use worker CPU, got %d", threads)
		}

		// Verify the rebalancer has the wrong (CP-derived) values
		rebalMem := cp.rebalancer.MemoryLimit()
		rebalThreads := cp.rebalancer.PerSessionThreads()
		if rebalMem == memLimit {
			t.Error("rebalancer should have CP-derived memory, not worker memory")
		}
		if rebalThreads == threads {
			t.Error("rebalancer should have CP-derived threads, not worker threads")
		}
	})

	t.Run("process backend uses rebalancer", func(t *testing.T) {
		rebalancer := NewMemoryRebalancer(
			16*1024*1024*1024, // 16GB
			8,
			&mockSessionLister{},
			false,
		)

		cp := &ControlPlane{
			cfg:             ControlPlaneConfig{},
			isRemoteBackend: false,
			rebalancer:      rebalancer,
		}

		// In process mode, workerDuckDBLimits returns empty (no K8s config)
		memLimit, threads := cp.workerDuckDBLimits()
		if memLimit != "" || threads != 0 {
			t.Errorf("process mode workerDuckDBLimits should be empty, got mem=%q threads=%d", memLimit, threads)
		}

		// The rebalancer should be the source of truth
		rebalMem := cp.rebalancer.MemoryLimit()
		rebalThreads := cp.rebalancer.PerSessionThreads()
		if rebalMem != "16384MB" {
			t.Errorf("process mode rebalancer memLimit = %q, want 16384MB", rebalMem)
		}
		if rebalThreads != 8 {
			t.Errorf("process mode rebalancer threads = %d, want 8", rebalThreads)
		}
	})
}

func TestParseK8sMemory(t *testing.T) {
	tests := []struct {
		input string
		want  uint64
	}{
		{"360Gi", 386547056640},
		{"8Gi", 8589934592},
		{"512Mi", 536870912},
		{"1Ti", 1099511627776},
		{"4GB", 4 * 1024 * 1024 * 1024},
		{"256MB", 256 * 1024 * 1024},
		{"", 0},
		{"garbage", 0},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseK8sMemory(tt.input)
			if got != tt.want {
				t.Errorf("parseK8sMemory(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseK8sCPU(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"46000m", 46},
		{"46", 46},
		{"500m", 0},
		{"1000m", 1},
		{"4", 4},
		{"", 0},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseK8sCPU(tt.input)
			if got != tt.want {
				t.Errorf("parseK8sCPU(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}
