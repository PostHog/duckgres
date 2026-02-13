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

func TestPerSessionMemoryLimit(t *testing.T) {
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}, true, 0) // 24GB, 8 threads
	t.Cleanup(r.Stop)

	tests := []struct {
		sessions int
		want     string
	}{
		{1, "24576MB"},
		{2, "12288MB"},
		{3, "8192MB"},
		{4, "6144MB"},
		{8, "3072MB"},
		{24, "1024MB"},
		{100, "256MB"}, // floor
	}

	for _, tt := range tests {
		got := r.PerSessionMemoryLimit(tt.sessions)
		if got != tt.want {
			t.Errorf("PerSessionMemoryLimit(%d) = %q, want %q", tt.sessions, got, tt.want)
		}
	}
}

func TestPerSessionThreads(t *testing.T) {
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}, true, 0)
	t.Cleanup(r.Stop)

	// Threads are not subdivided — every session gets the full budget
	got := r.PerSessionThreads()
	if got != 8 {
		t.Errorf("PerSessionThreads() = %d, want 8", got)
	}
}

func TestPerSessionMemoryFloor(t *testing.T) {
	// 512MB budget with 100 sessions = 5MB per session, but floor is 256MB
	r := NewMemoryRebalancer(512*1024*1024, 4, &mockSessionLister{}, true, 0)
	t.Cleanup(r.Stop)
	got := r.PerSessionMemoryLimit(100)
	if got != "256MB" {
		t.Errorf("expected floor of 256MB, got %q", got)
	}
}

func TestPerSessionMemoryLimitZeroSessions(t *testing.T) {
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}, true, 0)
	t.Cleanup(r.Stop)
	// Zero sessions should not panic, returns full budget
	got := r.PerSessionMemoryLimit(0)
	if got != "24576MB" {
		t.Errorf("expected 24576MB for 0 sessions, got %q", got)
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
	r := NewMemoryRebalancer(0, 0, &mockSessionLister{}, true, 0)
	t.Cleanup(r.Stop)
	if r.memoryBudget == 0 {
		t.Error("expected non-zero memory budget")
	}
	if r.threadBudget == 0 {
		t.Error("expected non-zero thread budget")
	}
}

func TestMaxSessionsForBudget(t *testing.T) {
	// 4GB budget / 256MB floor = 16 max sessions
	r := NewMemoryRebalancer(4*1024*1024*1024, 8, &mockSessionLister{}, true, 0)
	t.Cleanup(r.Stop)
	got := r.MaxSessionsForBudget()
	if got != 16 {
		t.Errorf("MaxSessionsForBudget() = %d, want 16", got)
	}

	// 24GB budget / 256MB floor = 96 max sessions
	r2 := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}, true, 0)
	t.Cleanup(r2.Stop)
	got2 := r2.MaxSessionsForBudget()
	if got2 != 96 {
		t.Errorf("MaxSessionsForBudget() = %d, want 96", got2)
	}
}

func TestDisabledRebalancerRequestIsNoop(t *testing.T) {
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}, false, 10)
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

func TestDisabledRebalancerStaticAllocation(t *testing.T) {
	// 24GB budget, 10 max_workers → static allocation = 24GB/10 = 2457MB per session
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}, false, 10)
	t.Cleanup(r.Stop)

	if !r.enabled {
		// Verify the static allocation math: budget/maxWorkers
		expected := formatBytes(perSessionMemory(24*1024*1024*1024, 10))
		if expected != "2457MB" {
			t.Errorf("expected static allocation of 2457MB, got %q", expected)
		}
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
