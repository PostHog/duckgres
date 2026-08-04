package controlplane

import "testing"

func TestParseK8sCPUMillicores(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"8", 8000},
		{"8000m", 8000},
		{"500m", 500},
		{"2", 2000},
		{"0.5", 500},
		{"1.5", 1500},
		{"garbage", 0},
		{"-1", 0},
	}
	for _, tt := range tests {
		if got := parseK8sCPUMillicores(tt.in); got != tt.want {
			t.Errorf("parseK8sCPUMillicores(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestParseK8sMemoryMiB(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"16Gi", 16 * 1024},
		{"512Mi", 512},
		{"1Gi", 1024},
		{"64Gi", 64 * 1024},
		{"garbage", 0},
	}
	for _, tt := range tests {
		if got := parseK8sMemoryMiB(tt.in); got != tt.want {
			t.Errorf("parseK8sMemoryMiB(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestWorkerBillingSize(t *testing.T) {
	cp := &ControlPlane{}
	cp.cfg.K8s.WorkerCPURequest = "8"
	cp.cfg.K8s.WorkerMemoryRequest = "16Gi"

	// nil profile → pool-global request.
	mc, mib := cp.workerBillingSize(nil)
	if mc != 8000 || mib != 16*1024 {
		t.Fatalf("nil profile = (%d,%d), want (8000,16384)", mc, mib)
	}

	// Profile overrides take precedence.
	mc, mib = cp.workerBillingSize(&WorkerProfile{CPU: "2", Memory: "4Gi"})
	if mc != 2000 || mib != 4*1024 {
		t.Fatalf("profile = (%d,%d), want (2000,4096)", mc, mib)
	}

	// Partially-set profile falls back per-field to the pool-global request.
	mc, mib = cp.workerBillingSize(&WorkerProfile{CPU: "4"})
	if mc != 4000 || mib != 16*1024 {
		t.Fatalf("partial profile = (%d,%d), want (4000,16384)", mc, mib)
	}
}
