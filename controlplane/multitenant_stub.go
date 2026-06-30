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
	return nil, nil, nil, nil, nil, nil, fmt.Errorf("multi-tenant mode requires -tags kubernetes build")
}
