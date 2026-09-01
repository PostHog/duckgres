//go:build !kubernetes

package controlplane

import (
	"fmt"
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
			wantMem:    "270GB", // 360Gi - 25% headroom (the floor does not bind) = 270GB
			wantThread: 115,
		},
		{
			name:       "whole core CPU notation",
			cpuReq:     "46",
			memReq:     "360Gi",
			wantMem:    "270GB",
			wantThread: 115,
		},
		{
			// Below the 24Gi crossover the absolute headroom floor binds:
			// 8Gi keeps min(6GiB, 40%) = 3.2GiB back, not 25%.
			name:       "small worker reserves a full 40 percent, un-truncated",
			cpuReq:     "4000m",
			memReq:     "8Gi",
			wantMem:    "4915MB",
			wantThread: 10,
		},
		{
			// Regression: 16Gi workers were sized to 12GB and OOMKilled in
			// mw-prod-us once a reused pod's RSS ratcheted to ~14.8GiB.
			// 6GiB of headroom keeps the plateau under the cgroup limit.
			name:       "16Gi worker keeps the absolute headroom floor",
			cpuReq:     "4000m",
			memReq:     "16Gi",
			wantMem:    "10GB",
			wantThread: 10,
		},
		{
			// At 24Gi the proportional reserve equals the floor, so this and
			// every larger pod keep byte-identical sizing to the flat rule.
			name:       "24Gi worker is the crossover and is unchanged",
			cpuReq:     "8000m",
			memReq:     "24Gi",
			wantMem:    "18GB",
			wantThread: 20,
		},
		{
			name:       "pool default 120Gi worker is unchanged",
			cpuReq:     "15",
			memReq:     "120Gi",
			wantMem:    "90GB",
			wantThread: 38,
		},
		{
			name:       "fractional CPU rounds multiplied threads up",
			cpuReq:     "500m",
			memReq:     "1Gi",
			wantMem:    "614MB",
			wantThread: 2,
		},
		{
			name:       "1 core minimum",
			cpuReq:     "1000m",
			memReq:     "2Gi",
			wantMem:    "1228MB",
			wantThread: 3,
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

			gotMem, gotThreads := cp.workerDuckDBLimits(nil)
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
		memLimit, threads := cp.workerDuckDBLimits(nil)
		if memLimit != "270GB" {
			t.Errorf("remote mode should use worker memory, got %q", memLimit)
		}
		if threads != 115 {
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
		memLimit, threads := cp.workerDuckDBLimits(nil)
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

func TestDuckDBThreadsForK8sCPU(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"46000m", 115},
		{"46", 115},
		{"500m", 2},
		{"750m", 2},
		{"1000m", 3},
		{"1500m", 4},
		{"1.5", 4},
		{"4", 10},
		{"", 0},
		{"garbage", 0},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := duckDBThreadsForK8sCPU(tt.input)
			if got != tt.want {
				t.Errorf("duckDBThreadsForK8sCPU(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

// TestWorkerMemoryHeadroom_FloorAndCrossover pins the headroom rule itself:
// max(25% of pod, min(6GiB, 40% of pod)).
//
// The load-bearing property is the crossover. Worker sizing is
// percentage-based above 24Gi and floor-based below it, so every pod size the
// fleet actually runs at scale — the 120Gi pool default and the 360Gi
// client-profile ceiling — must keep byte-identical sizing to the flat-75%
// rule this replaced. Only small pods may move.
func TestWorkerMemoryHeadroom_FloorAndCrossover(t *testing.T) {
	const gib = uint64(1024 * 1024 * 1024)

	t.Run("floor binds below the crossover", func(t *testing.T) {
		for _, tc := range []struct {
			podGiB       uint64
			wantHeadroom uint64
		}{
			{4, 4 * gib * 2 / 5}, // 40% cap: 1.6GiB
			{8, 8 * gib * 2 / 5}, // 40% cap: 3.2GiB
			{16, 6 * gib},        // absolute floor
			{23, 6 * gib},        // still the floor
		} {
			got := workerMemoryHeadroomBytes(tc.podGiB * gib)
			if got != tc.wantHeadroom {
				t.Errorf("headroom(%dGi) = %d, want %d", tc.podGiB, got, tc.wantHeadroom)
			}
		}
	})

	t.Run("proportional reserve binds at and above the crossover", func(t *testing.T) {
		for _, podGiB := range []uint64{24, 32, 64, 120, 360} {
			pod := podGiB * gib
			if got, want := workerMemoryHeadroomBytes(pod), pod/4; got != want {
				t.Errorf("headroom(%dGi) = %d, want 25%% = %d", podGiB, got, want)
			}
		}
	})

	// Compare BYTES, not the formatted string: the claim is that the sizing
	// rule is unchanged above the crossover, which is independent of how the
	// value is rendered. Swept, not spot-checked, so no pod size can drift.
	t.Run("sizing at and above the crossover is unchanged from the flat rule", func(t *testing.T) {
		for podMiB := uint64(24 * 1024); podMiB <= 1024*1024; podMiB += 64 {
			pod := podMiB * 1024 * 1024
			if got, want := pod-workerMemoryHeadroomBytes(pod), pod*3/4; got != want {
				t.Fatalf("pod %dMiB: budget %d, want flat-75%% %d", podMiB, got, want)
			}
		}
	})

	// The formatter must not give back the reserve the cap just took. A
	// fractional-GiB budget truncated to whole GB turns 40% into 50%.
	t.Run("formatted budget never truncates away the reserve", func(t *testing.T) {
		const mib = uint64(1024 * 1024)
		for podMiB := uint64(256); podMiB < 24*1024; podMiB += 16 {
			pod := podMiB * mib
			want := pod - workerMemoryHeadroomBytes(pod)
			got := duckdbMemoryLimitForPodMemory(pod)
			var n uint64
			var unit string
			if _, err := fmt.Sscanf(got, "%d%s", &n, &unit); err != nil {
				t.Fatalf("pod %dMiB: unparseable limit %q", podMiB, got)
			}
			mult := mib
			if unit == "GB" {
				mult = gib
			}
			// Formatting floors, so at most one unit may be lost — and a GB
			// unit is only allowed when the budget is a whole number of them.
			lost := want - n*mult
			if unit == "GB" && want%gib != 0 {
				t.Fatalf("pod %dMiB: budget %d is not a whole GiB but formatted as %q", podMiB, want, got)
			}
			if lost >= mib {
				t.Fatalf("pod %dMiB: limit %q loses %d bytes of a %d-byte budget", podMiB, got, lost, want)
			}
		}
	})

	t.Run("a 16Gi worker keeps enough margin for un-governed overhead", func(t *testing.T) {
		// Regression for the mw-prod-us OOM: a reused 16Gi worker's RSS
		// ratcheted to 14.8GiB against a 12GiB limit, i.e. 2.8GiB of
		// un-governed overhead above the limit. Headroom must exceed that,
		// otherwise the plateau lands on the cgroup limit again.
		const observedOvershoot = 2800 * 1024 * 1024
		if got := workerMemoryHeadroomBytes(16 * gib); got <= observedOvershoot {
			t.Errorf("headroom(16Gi) = %d, want > observed overshoot %d", got, observedOvershoot)
		}
	})

	t.Run("zero is unsized", func(t *testing.T) {
		if got := duckdbMemoryLimitForPodMemory(0); got != "" {
			t.Errorf("duckdbMemoryLimitForPodMemory(0) = %q, want empty", got)
		}
	})
}
