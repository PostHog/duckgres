package provisioning

import (
	"sort"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestProvisioningAPIRouteTopology pins the exact route set RegisterAPI mounts.
//
// This is the "one provisioning path" tripwire. The PostHog backend (Django)
// and the admin console both create warehouses by POSTing to
// /api/v1/orgs/:id/provision — the SAME gin route, the same handler, the same
// validation, the same transaction. That is what makes an operator-provisioned
// warehouse byte-for-byte equivalent to a user-provisioned one; there is
// deliberately no second, console-only provisioning implementation to drift
// from this one (see TestAdminAPIRegistersNoProvisioningRoutes on the admin
// side, which pins the absence).
//
// A change here means the public provisioning contract moved: update the
// PostHog backend client, the admin console's api.ts, and the e2e harness in
// the same PR.
func TestProvisioningAPIRouteTopology(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterAPI(r.Group("/api/v1"), newFakeStore(), "")

	want := []string{
		"DELETE /api/v1/orgs/:id/teams/:team_id",
		"GET /api/v1/database-name/check",
		"GET /api/v1/orgs/:id/teams",
		"GET /api/v1/orgs/:id/warehouse/status",
		"POST /api/v1/orgs/:id/deprovision",
		"POST /api/v1/orgs/:id/provision",
		"POST /api/v1/orgs/:id/reset-password",
		"POST /api/v1/orgs/:id/teams",
	}

	var got []string
	for _, ri := range r.Routes() {
		got = append(got, ri.Method+" "+ri.Path)
	}
	sort.Strings(got)

	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("provisioning route topology changed:\ngot:\n%s\n\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
