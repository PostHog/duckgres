//go:build !kubernetes

package controlplane

import (
	"context"
	"sync"
	"testing"
)

func TestOrgRoutedSessionProviderCreateSessionTeamNotFound(t *testing.T) {
	provider := &orgRoutedSessionProvider{
		orgRouter:  &mockOrgRouter{ok: false},
		pidSession: make(map[int32]*SessionManager),
	}

	_, _, err := provider.CreateSession(context.Background(), "unknown", 0, "", 0)
	if err == nil {
		t.Fatal("expected error for unknown org")
	}
	if err.Error() != `no org configured for user "unknown"` {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestOrgRoutedSessionProviderDestroySessionRemovesPid(t *testing.T) {
	sm := NewSessionManager(nil, nil)

	provider := &orgRoutedSessionProvider{
		orgRouter: &mockOrgRouter{sessions: sm, ok: true},
		pidSession: map[int32]*SessionManager{
			42: sm,
		},
	}

	// Destroy known pid — should remove from map.
	// sm.DestroySession(42) is a no-op for unknown internal session, which is fine;
	// we're testing the adapter's pid map bookkeeping.
	provider.DestroySession(42)

	provider.mu.RLock()
	_, exists := provider.pidSession[42]
	provider.mu.RUnlock()
	if exists {
		t.Fatal("expected pid 42 to be removed from pidSession map after destroy")
	}
}

func TestOrgRoutedSessionProviderDestroyUnknownPidNoOp(t *testing.T) {
	provider := &orgRoutedSessionProvider{
		orgRouter:  &mockOrgRouter{ok: true},
		pidSession: make(map[int32]*SessionManager),
	}

	// Should not panic.
	provider.DestroySession(999)
}

func TestOrgRoutedSessionProviderConcurrentDestroys(t *testing.T) {
	sm := NewSessionManager(nil, nil)

	provider := &orgRoutedSessionProvider{
		orgRouter:  &mockOrgRouter{sessions: sm, ok: true},
		pidSession: make(map[int32]*SessionManager),
	}

	// Pre-populate
	for i := int32(0); i < 100; i++ {
		provider.pidSession[i] = sm
	}

	// Concurrent destroys should not race.
	var wg sync.WaitGroup
	for i := int32(0); i < 100; i++ {
		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()
			provider.DestroySession(pid)
		}(i)
	}
	wg.Wait()

	provider.mu.RLock()
	remaining := len(provider.pidSession)
	provider.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("expected all pids removed, got %d remaining", remaining)
	}
}
