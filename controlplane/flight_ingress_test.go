package controlplane

import (
	"context"
	"sync"
	"testing"
)

// mockOrgRouter implements OrgRouterInterface for testing.
type mockOrgRouter struct {
	sessions   *SessionManager
	rebalancer *MemoryRebalancer
	ok         bool
}

func (m *mockOrgRouter) StackForOrg(_ string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	return nil, m.sessions, m.rebalancer, m.ok
}

func (m *mockOrgRouter) ShutdownAll() {}

// mockConfigStore implements ConfigStoreInterface for testing.
type mockConfigStore struct {
	orgID string
	ok    bool
}

func (m *mockConfigStore) ValidateOrgUser(_, _, _ string) bool {
	return m.ok
}

func (m *mockConfigStore) FindAndValidateUser(_, _ string) (string, bool) {
	return m.orgID, m.ok
}

func TestOrgRoutedSessionProviderCreateSessionTeamNotFound(t *testing.T) {
	provider := &orgRoutedSessionProvider{
		orgRouter:  &mockOrgRouter{ok: false},
		pidSession: make(map[int32]*SessionManager),
		userOrg:    make(map[string]string),
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
		orgRouter:  &mockOrgRouter{sessions: sm, ok: true},
		pidSession: map[int32]*SessionManager{42: sm},
		userOrg:    make(map[string]string),
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
		userOrg:    make(map[string]string),
	}

	// Should not panic.
	provider.DestroySession(999)
}

func TestOrgRoutedSessionProviderConcurrentDestroys(t *testing.T) {
	sm := NewSessionManager(nil, nil)

	provider := &orgRoutedSessionProvider{
		orgRouter:  &mockOrgRouter{sessions: sm, ok: true},
		pidSession: make(map[int32]*SessionManager),
		userOrg:    make(map[string]string),
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
