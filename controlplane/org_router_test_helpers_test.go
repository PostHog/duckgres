package controlplane

// mockOrgRouter implements OrgRouterInterface for testing.
type mockOrgRouter struct {
	sessions   *SessionManager
	rebalancer *MemoryRebalancer
	ok         bool
}

func (m *mockOrgRouter) StackForUser(_ string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	return nil, m.sessions, m.rebalancer, m.ok
}

func (m *mockOrgRouter) IsMigratingForUser(_ string) (bool, string) { return false, "" }
func (m *mockOrgRouter) ShutdownAll()                              {}
