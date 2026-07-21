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
func (r *reconnectTestOrgRouter) BeginDrain()                     {}
func (r *reconnectTestOrgRouter) ShutdownAll()                    {}
func (r *reconnectTestOrgRouter) ReleaseIdleHotWorkers() int      { return 0 }

// recordingOrgRouter records the orgIDs StackForOrg is asked for. It returns no
// live stack, so CreateSession returns right after recording the routing org.
type recordingOrgRouter struct {
	mu    sync.Mutex
	calls []string
}

func (r *recordingOrgRouter) StackForOrg(orgID string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	r.mu.Lock()
	r.calls = append(r.calls, orgID)
	r.mu.Unlock()
	return nil, nil, nil, false
}
func (r *recordingOrgRouter) IsMigratingForOrg(_ string) bool { return false }
func (r *recordingOrgRouter) BeginDrain()                     {}
func (r *recordingOrgRouter) ShutdownAll()                    {}
func (r *recordingOrgRouter) ReleaseIdleHotWorkers() int      { return 0 }

type testFlightOrgKey struct{}

// TestOrgRoutedSessionProviderRoutesByContextSNINotUsername proves the fix for
// the username-collision: two connections sharing the username "alice" but from
// different org hostnames each route to THEIR OWN org, because the org is
// re-derived per-connection from the context (SNI) rather than a shared
// username→org map.
func TestOrgRoutedSessionProviderRoutesByContextSNINotUsername(t *testing.T) {
	router := &recordingOrgRouter{}
	provider := &orgRoutedSessionProvider{
		orgRouter:  router,
		pidSession: make(map[int32]flightOwnedSession),
		resolveOrg: func(ctx context.Context) (string, bool) {
			org, _ := ctx.Value(testFlightOrgKey{}).(string)
			return org, org != ""
		},
	}

	ctxA := context.WithValue(context.Background(), testFlightOrgKey{}, "org-a")
	ctxB := context.WithValue(context.Background(), testFlightOrgKey{}, "org-b")
	if _, _, err := provider.CreateSession(ctxA, "alice", 0, "", 0); err == nil {
		t.Fatal("expected failure (no live stack)")
	}
	if _, _, err := provider.CreateSession(ctxB, "alice", 0, "", 0); err == nil {
		t.Fatal("expected failure (no live stack)")
	}

	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.calls) != 2 || router.calls[0] != "org-a" || router.calls[1] != "org-b" {
		t.Fatalf("expected StackForOrg(org-a) then StackForOrg(org-b); got %v", router.calls)
	}
}

// TestOrgRoutedSessionProviderFailsClosedWhenSNIUnresolved: if the connection's
// SNI no longer resolves to an org, no session is created (fail closed).
func TestOrgRoutedSessionProviderFailsClosedWhenSNIUnresolved(t *testing.T) {
	router := &recordingOrgRouter{}
	provider := &orgRoutedSessionProvider{
		orgRouter:  router,
		pidSession: make(map[int32]flightOwnedSession),
		resolveOrg: func(_ context.Context) (string, bool) { return "", false },
	}
	if _, _, err := provider.CreateSession(context.Background(), "alice", 0, "", 0); err == nil {
		t.Fatal("expected CreateSession to fail closed when org can't be resolved")
	}
	router.mu.Lock()
	defer router.mu.Unlock()
	if len(router.calls) != 0 {
		t.Fatalf("expected no StackForOrg call when SNI unresolved; got %v", router.calls)
	}
}

func TestOrgRoutedSessionProviderReconnectSessionUsesDurableOrgID(t *testing.T) {
	router := &reconnectTestOrgRouter{
		orgID: "analytics",
	}
	provider := &orgRoutedSessionProvider{
		orgRouter:   router,
		pidSession:  make(map[int32]flightOwnedSession),
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
		return
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
	}

	// Should not panic.
	provider.DestroySession(999)
}

func TestOrgRoutedSessionProviderConcurrentDestroys(t *testing.T) {
	sm := NewSessionManager(nil, nil)

	provider := &orgRoutedSessionProvider{
		orgRouter:  &mockOrgRouter{sessions: sm, ok: true},
		pidSession: make(map[int32]flightOwnedSession),
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
