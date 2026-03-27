package controlplane

import (
	"context"
	"testing"

	"github.com/posthog/duckgres/server/flightsqlingress"
)

type reconnectTestOrgRouter struct {
	stackByUserCalls int
	stackByOrgCalls  int
	orgID            string
}

func (r *reconnectTestOrgRouter) StackForUser(username string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	r.stackByUserCalls++
	return nil, nil, nil, false
}

func (r *reconnectTestOrgRouter) StackForOrg(orgID string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	r.stackByOrgCalls++
	return nil, nil, nil, false
}

func (r *reconnectTestOrgRouter) IsMigratingForUser(string) (bool, string) { return false, "" }
func (r *reconnectTestOrgRouter) ShutdownAll()                              {}

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
	}
	if pid != 0 {
		t.Fatalf("expected pid 0 on failed reconnect, got %d", pid)
	}
	if router.stackByUserCalls != 0 {
		t.Fatalf("expected reconnect not to use StackForUser, got %d calls", router.stackByUserCalls)
	}
	if router.stackByOrgCalls != 1 {
		t.Fatalf("expected reconnect to use StackForOrg once, got %d", router.stackByOrgCalls)
	}
}
