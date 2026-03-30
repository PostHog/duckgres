package controlplane

import (
	"context"
	"sync"
	"testing"

	"github.com/posthog/duckgres/server/flightsqlingress"
)

type reconnectTestOrgRouter struct {
	stackByOrgCalls int
	orgID           string
}

func (r *reconnectTestOrgRouter) StackForOrg(orgID string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	r.stackByOrgCalls++
	return nil, nil, nil, false
}

func (r *reconnectTestOrgRouter) IsMigratingForOrg(_ string) bool { return false }
func (r *reconnectTestOrgRouter) ShutdownAll()                    {}

func TestOrgRoutedSessionProviderReconnectSessionUsesDurableOrgID(t *testing.T) {
	router := &reconnectTestOrgRouter{
		orgID: "analytics",
	}
	provider := &orgRoutedSessionProvider{
		orgRouter:   router,
		pidSession:  make(map[int32]flightOwnedSession),
		userOrg:     make(map[string]string),
		configStore: nil,
	}

	pid, _, err := provider.ReconnectSession(context.Background(), flightsqlingress.DurableSessionRecord{
		SessionToken: "flight-token",
		Username:     "alice",
		OrgID:        "analytics",
		WorkerID:     17,
		OwnerEpoch:   4,
		CPInstanceID: "cp-old:boot-z",
	})
	if err == nil {
		t.Fatal("expected reconnect to fail without a live org stack")
	}
	if pid != 0 {
		t.Fatalf("expected pid 0 on failed reconnect, got %d", pid)
	}
	if router.stackByOrgCalls != 1 {
		t.Fatalf("expected reconnect to use StackForOrg once, got %d", router.stackByOrgCalls)
	}
}

func TestOrgRoutedSessionProviderDestroySessionRemovesPid(t *testing.T) {
	sm := NewSessionManager(nil, nil)

	provider := &orgRoutedSessionProvider{
		orgRouter:  &mockOrgRouter{sessions: sm, ok: true},
		pidSession: map[int32]flightOwnedSession{42: {orgID: "test", sessions: sm}},
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
		pidSession: make(map[int32]flightOwnedSession),
		userOrg:    make(map[string]string),
	}

	// Should not panic.
	provider.DestroySession(999)
}

func TestOrgRoutedSessionProviderConcurrentDestroys(t *testing.T) {
	sm := NewSessionManager(nil, nil)

	provider := &orgRoutedSessionProvider{
		orgRouter:  &mockOrgRouter{sessions: sm, ok: true},
		pidSession: make(map[int32]flightOwnedSession),
		userOrg:    make(map[string]string),
	}

	// Pre-populate
	for i := int32(0); i < 100; i++ {
		provider.pidSession[i] = flightOwnedSession{orgID: "test", sessions: sm}
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
