//go:build kubernetes

package controlplane

import (
	"slices"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// What duckgres has CHECKPOINTED for a tenant must always name the principal
// set the Gateway actually binds.
//
// The divergence this pins is subtle and permanent. A refused call is rolled
// back at the Gateway before anything is journalled, so the step identity stays
// unclaimed, and publishing principals carries no revision ordering of its own.
// So if a refusal of a REISSUE closed the occurrence, the controller could
// advance to a newer intent, publish it and checkpoint it - and then the copy
// that was still executing under the OLD occurrence lands and overwrites the
// Gateway's binding with the superseded set. duckgres believes it published the
// newer one and never sends it again: a removed login stays dispatchable and a
// current one cannot dispatch, until something else happens to move that
// tenant's binding.
//
// The property asserted here is the absence of that divergence, NOT that the
// newest desired set wins. Under the guard the tenant is deliberately HELD on
// its unresolved occurrence while an older copy may still be in flight, so the
// bound set may legitimately lag the desired one - see the limitation recorded
// in the shared-pool contract. What may never happen is the two records
// disagreeing about what is bound.
func TestACheckpointedBindingNeverDivergesFromTheGateway(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.fullMembership(t)
	harness.admitAll(t, 1)

	// 1. The binding changes; the publication REACHES the Gateway and is left
	//    executing there while the caller's response is lost.
	tenants.orgs[0].Users = []configstore.TrinoOrgUser{{Username: "engineer", PasswordHash: "hash"}}
	harness.gateway.deferPublish = map[string]bool{"org-a": true}
	for tick := 0; tick < 12 && harness.publications.rows["org-a"].PendingIntent == ""; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}
	if harness.publications.rows["org-a"].PendingIntent == "" {
		t.Fatalf("no publication is in flight: %+v", harness.publications.rows["org-a"])
	}

	// 2. The REISSUE of that same occurrence is refused definitively - another
	//    tenant currently owns one of the identifiers.
	harness.gateway.principalOf["acme.engineer"] = "org-old"
	for tick := 0; tick < 12; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	// 3. The cause clears and the desired intent moves on again.
	delete(harness.gateway.principalOf, "acme.engineer")
	tenants.orgs[0].Users = []configstore.TrinoOrgUser{{Username: "analyst", PasswordHash: "hash"}}
	for tick := 0; tick < 40; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	// 4. The copy left executing in step 1 finally commits, after everything
	//    else, and the controller runs on.
	harness.gateway.deliverDeferred()
	for tick := 0; tick < 40; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	bound := harness.gateway.principals["org-a"]
	checkpoint := harness.publications.rows["org-a"].PrincipalRevision
	if checkpoint != trinoPoolBindingRevision(bound) {
		t.Fatalf("duckgres has checkpointed revision %q while the Gateway binds %v (revision %q): "+
			"a delayed copy overwrote the binding and nothing will correct it",
			checkpoint, bound, trinoPoolBindingRevision(bound))
	}

	// And once the request finally settles - here, the in-flight copy having
	// landed IS its outcome - the tenant converges on the desired set rather
	// than being stuck describing a stale one.
	wanted := trinoPoolTenantBindingFor(tenants.orgs[0])
	for tick := 0; tick < 60 && !slices.Equal(harness.gateway.principals["org-a"], wanted.Principals); tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}
	if got := harness.gateway.principals["org-a"]; !slices.Equal(got, wanted.Principals) {
		t.Logf("the tenant is still held on its occurrence: bound %v, desired %v", got, wanted.Principals)
	}
	// Whatever it converged to, the two records must still agree.
	bound = harness.gateway.principals["org-a"]
	if got := harness.publications.rows["org-a"].PrincipalRevision; got != trinoPoolBindingRevision(bound) {
		t.Fatalf("checkpoint %q disagrees with the bound set %v after settling", got, bound)
	}
}
