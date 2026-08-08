//go:build kubernetes

package admin

import (
	"strings"
	"testing"
)

// TestAdminAPIRegistersNoProvisioningRoutes is the other half of the "one
// provisioning path" tripwire (see
// provisioning.TestProvisioningAPIRouteTopology).
//
// The admin console provisions warehouses by calling the SAME endpoints the
// PostHog backend calls — POST /orgs/:id/provision, POST /orgs/:id/deprovision,
// POST /orgs/:id/reset-password, GET /orgs/:id/warehouse/status, GET
// /database-name/check — which the provisioning package registers on this very
// router group (see controlplane/multitenant.go). That shared registration is
// what guarantees a console-provisioned warehouse is identical to a
// user-provisioned one: same validation, same defaults, same transaction, same
// analytics event.
//
// If the admin package ever grows its own provisioning handler, gin would
// panic on the duplicate route — but only for an exactly-matching path. A
// near-miss (e.g. /orgs/:id/provision-warehouse) would silently fork the
// contract, so assert the absence explicitly.
func TestAdminAPIRegistersNoProvisioningRoutes(t *testing.T) {
	r := newTestAPIRouter(&fakeAPIStore{})

	// Substrings that would indicate an admin-local warehouse lifecycle
	// implementation. "warehouse" alone is fine — PUT /orgs/:id/warehouse and
	// PATCH /orgs/:id/warehouse/pinning edit the CONFIG ROW of an
	// already-provisioned warehouse; they do not create or destroy infra.
	forbidden := []string{"provision", "reset-password", "database-name", "warehouse/status"}

	for _, ri := range r.Routes() {
		for _, f := range forbidden {
			if strings.Contains(ri.Path, f) {
				t.Fatalf("admin API registered %s %s: warehouse provisioning must stay in controlplane/provisioning so the console and the PostHog backend share one implementation", ri.Method, ri.Path)
			}
		}
	}
}
