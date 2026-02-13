package controlplane

import (
	"testing"
)

// mockSessionLister returns a fixed list of sessions.
type mockSessionLister struct {
	sessions []*ManagedSession
}

func (m *mockSessionLister) AllSessions() []*ManagedSession {
	return m.sessions
}

func TestMemoryLimit(t *testing.T) {
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}, true) // 24GB, 8 threads
	t.Cleanup(r.Stop)

	// Every session gets the full budget
	got := r.MemoryLimit()
	if got != "24576MB" {
		t.Errorf("MemoryLimit() = %q, want %q", got, "24576MB")
	}
}

func TestPerSessionThreads(t *testing.T) {
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}, true)
	t.Cleanup(r.Stop)

	// Threads are not subdivided — every session gets the full budget
	got := r.PerSessionThreads()
	if got != 8 {
		t.Errorf("PerSessionThreads() = %d, want 8", got)
	}
}

func TestMemoryLimitFloor(t *testing.T) {
	// Budget below 256MB floor should be clamped up
	r := NewMemoryRebalancer(128*1024*1024, 4, &mockSessionLister{}, true)
	t.Cleanup(r.Stop)
	got := r.MemoryLimit()
	if got != "256MB" {
		t.Errorf("expected floor of 256MB, got %q", got)
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes uint64
		want  string
	}{
		{256 * 1024 * 1024, "256MB"},
		{1024 * 1024 * 1024, "1024MB"},
		{24 * 1024 * 1024 * 1024, "24576MB"},
		{0, "0MB"},
	}

	for _, tt := range tests {
		got := formatBytes(tt.bytes)
		if got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.bytes, got, tt.want)
		}
	}
}

func TestNewMemoryRebalancerDefaults(t *testing.T) {
	// With 0 budget, should auto-detect (or fallback)
	r := NewMemoryRebalancer(0, 0, &mockSessionLister{}, true)
	t.Cleanup(r.Stop)
	if r.memoryBudget == 0 {
		t.Error("expected non-zero memory budget")
	}
	if r.threadBudget == 0 {
		t.Error("expected non-zero thread budget")
	}
}

func TestDefaultMaxWorkers(t *testing.T) {
	// 4GB budget / 256MB = 16
	r := NewMemoryRebalancer(4*1024*1024*1024, 8, &mockSessionLister{}, true)
	t.Cleanup(r.Stop)
	got := r.DefaultMaxWorkers()
	if got != 16 {
		t.Errorf("DefaultMaxWorkers() = %d, want 16", got)
	}

	// 24GB budget / 256MB = 96
	r2 := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}, true)
	t.Cleanup(r2.Stop)
	got2 := r2.DefaultMaxWorkers()
	if got2 != 96 {
		t.Errorf("DefaultMaxWorkers() = %d, want 96", got2)
	}
}

func TestDisabledRebalancerRequestIsNoop(t *testing.T) {
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}, false)
	t.Cleanup(r.Stop)

	// RequestRebalance should be a no-op (no debounce goroutine running)
	r.RequestRebalance()

	// Channel should remain empty since enabled=false
	select {
	case <-r.pendingRebalance:
		t.Error("expected pendingRebalance to be empty when disabled")
	default:
		// expected
	}
}

func TestDisabledRebalancerFullBudget(t *testing.T) {
	// Even when disabled, every session gets the full budget
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}, false)
	t.Cleanup(r.Stop)

	got := r.MemoryLimit()
	if got != "24576MB" {
		t.Errorf("expected full budget of 24576MB, got %q", got)
	}
}

func TestFormatBytesPrecision(t *testing.T) {
	// 4.8GB = 4915MB, not truncated to 4GB
	b := uint64(4915) * 1024 * 1024 // 4915MB
	got := formatBytes(b)
	if got != "4915MB" {
		t.Errorf("formatBytes(4915MB) = %q, want %q", got, "4915MB")
	}
}
