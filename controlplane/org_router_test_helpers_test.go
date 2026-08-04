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
func (m *mockOrgRouter) BeginDrain() {
	if m.sessions != nil {
		m.sessions.BeginDrain()
	}
}
func (m *mockOrgRouter) ShutdownAll()               {}
func (m *mockOrgRouter) ReleaseIdleHotWorkers() int { return 0 }
