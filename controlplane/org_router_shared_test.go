//go:build !kubernetes

package controlplane

import "testing"

func TestMockOrgRouterSatisfiesInterface(t *testing.T) {
	mock := &mockOrgRouter{ok: true}
	var _ OrgRouterInterface = mock

	migrating, _ := mock.IsMigratingForUser("anyuser")
	if migrating {
		t.Fatal("expected default mock to return not migrating")
	}
}
