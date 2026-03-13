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
	maxWorkers int,
) (ConfigStoreInterface, TeamRouterInterface, *http.Server, error) {
	return nil, nil, nil, fmt.Errorf("multi-tenant mode requires -tags kubernetes build")
}
