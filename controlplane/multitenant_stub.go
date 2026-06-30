//go:build !kubernetes

package controlplane

import (
	"fmt"
	"net/http"

	"github.com/posthog/duckgres/server"
)

// SetupMultiTenant is not available without the kubernetes build tag.
func SetupMultiTenant(
	cfg ControlPlaneConfig,
	srv *server.Server,
	memBudget uint64,
	isHealthy func() bool,
) (ConfigStoreInterface, OrgRouterInterface, *http.Server, *ControlPlaneRuntimeTracker, *JanitorLeaderManager, *computeMeter, error) {
	return nil, nil, nil, nil, nil, nil, errMultiTenantRequiresKubernetes()
}

// errMultiTenantRequiresKubernetes is returned via a helper (not an inline
// literal) so the always-error stub does not make callers' `if err != nil`
// statically provable as always-true (staticcheck SA4023). The real impl lives
// in multitenant.go under the kubernetes build tag.
func errMultiTenantRequiresKubernetes() error {
	return fmt.Errorf("multi-tenant mode requires -tags kubernetes build")
}
