package controlplane

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// mockExecutor records SET commands sent to it.
type mockExecutor struct {
	mu       sync.Mutex
	commands []string
	failNext bool
}

func (m *mockExecutor) ExecContext(_ context.Context, query string, _ ...any) (interface{ RowsAffected() (int64, error) }, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return nil, fmt.Errorf("mock error")
	}
	m.commands = append(m.commands, query)
	return &mockResult{}, nil
}

type mockResult struct{}

func (r *mockResult) RowsAffected() (int64, error) { return 0, nil }

// mockSessionLister returns a fixed list of sessions.
type mockSessionLister struct {
	sessions []*ManagedSession
}

func (m *mockSessionLister) AllSessions() []*ManagedSession {
	return m.sessions
}

func TestPerSessionMemoryLimit(t *testing.T) {
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{}) // 24GB, 8 threads

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
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{})

	tests := []struct {
		sessions int
		want     int
	}{
		{1, 8},
		{2, 4},
		{4, 2},
		{8, 1},
		{16, 1}, // floor
	}

	for _, tt := range tests {
		got := r.PerSessionThreads(tt.sessions)
		if got != tt.want {
			t.Errorf("PerSessionThreads(%d) = %d, want %d", tt.sessions, got, tt.want)
		}
	}
}

func TestPerSessionMemoryFloor(t *testing.T) {
	// 512MB budget with 100 sessions = 5MB per session, but floor is 256MB
	r := NewMemoryRebalancer(512*1024*1024, 4, &mockSessionLister{})
	got := r.PerSessionMemoryLimit(100)
	if got != "256MB" {
		t.Errorf("expected floor of 256MB, got %q", got)
	}
}

func TestPerSessionMemoryLimitZeroSessions(t *testing.T) {
	r := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{})
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
	r := NewMemoryRebalancer(0, 0, &mockSessionLister{})
	if r.memoryBudget == 0 {
		t.Error("expected non-zero memory budget")
	}
	if r.threadBudget == 0 {
		t.Error("expected non-zero thread budget")
	}
}

func TestMaxSessionsForBudget(t *testing.T) {
	// 4GB budget / 256MB floor = 16 max sessions
	r := NewMemoryRebalancer(4*1024*1024*1024, 8, &mockSessionLister{})
	got := r.MaxSessionsForBudget()
	if got != 16 {
		t.Errorf("MaxSessionsForBudget() = %d, want 16", got)
	}

	// 24GB budget / 256MB floor = 96 max sessions
	r2 := NewMemoryRebalancer(24*1024*1024*1024, 8, &mockSessionLister{})
	got2 := r2.MaxSessionsForBudget()
	if got2 != 96 {
		t.Errorf("MaxSessionsForBudget() = %d, want 96", got2)
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
