package controlplane

// mockOrgRouter implements OrgRouterInterface for testing.
type mockOrgRouter struct {
	sessions   *SessionManager
	rebalancer *MemoryRebalancer
	ok         bool
}

func (m *mockOrgRouter) StackForOrg(_ string) (WorkerPool, *SessionManager, *MemoryRebalancer, bool) {
	return nil, m.sessions, m.rebalancer, m.ok
}

func (m *mockOrgRouter) IsMigratingForOrg(_ string) bool { return false }
func (m *mockOrgRouter) SetWarmCapacityTarget(_ int)     {}
func (m *mockOrgRouter) ShutdownAll()                    {}
