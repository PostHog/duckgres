//go:build kubernetes

package admin

import (
	"github.com/gin-gonic/gin"
)

// Extras bundles the dependencies for the admin endpoints added on top of the
// original orgs/users/warehouse CRUD: live cluster state, user-secret
// management, impersonation, audit log, and the Prometheus metrics proxy.
type Extras struct {
	Store        secretStore // *configstore.ConfigStore
	Live         LiveInfo
	Users        UserAdmin // per-user kill switch (disable/enable); *configstore.ConfigStore
	Impersonator Impersonator
	Audit        *AuditStore
	Metrics      *MetricsProxy
	Fetcher      PeerFetcher // cross-CP live-state aggregation (nil = single-CP)
}

// RegisterExtras wires the additional endpoints onto the authenticated /api/v1
// group. RoleGate (applied at the group level) enforces viewer/admin; the
// individual handlers re-check where needed (impersonation).
func RegisterExtras(r *gin.RouterGroup, x Extras) {
	r.GET("/me", meHandler)
	registerLiveAPI(r, x.Live, x.Fetcher, x.Users)
	if x.Store != nil {
		registerUserSecretsAPI(r, x.Store)
	}
	registerImpersonateAPI(r, x.Impersonator, x.Audit)
	if x.Audit != nil {
		registerAuditAPI(r, x.Audit)
	}
	if x.Metrics != nil {
		x.Metrics.RegisterMetricsProxy(r)
	}
}
