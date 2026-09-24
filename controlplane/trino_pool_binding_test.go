//go:build kubernetes

package controlplane

import (
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
)

func bindingOrg(users ...string) configstore.TrinoEnabledOrg {
	org := configstore.TrinoEnabledOrg{
		OrgID: "org-a", DatabaseName: "acme", CellID: "registered:cell-001",
		RootPasswordHash: "hash",
	}
	for _, username := range users {
		org.Users = append(org.Users, configstore.TrinoOrgUser{Username: username, PasswordHash: "hash"})
	}
	return org
}

func TestBindingServiceAuthenticationPreservesExistingProjection(t *testing.T) {
	org := bindingOrg("analyst")
	t.Setenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE", "")
	disabled := trinoPoolBindingsFor([]configstore.TrinoEnabledOrg{org}, org.CellID)
	t.Setenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE", "/synthetic/service-auth-token")
	enabled := trinoPoolBindingsFor([]configstore.TrinoEnabledOrg{org}, org.CellID)
	if disabled[0].Revision != enabled[0].Revision || strings.Join(disabled[0].Principals, ",") != strings.Join(enabled[0].Principals, ",") {
		t.Fatalf("service authentication changed existing bindings: disabled=%+v enabled=%+v", disabled, enabled)
	}
}

// A warehouse has many logins and they all belong to one tenant. A binding that
// carried only the root principal would block every named user at the gate.
func TestBindingCoversEveryLoginOfTheWarehouse(t *testing.T) {
	binding := trinoPoolTenantBindingFor(bindingOrg("analyst", "dagster"))

	if binding.Tenant != "org-a" {
		t.Fatalf("tenant = %q", binding.Tenant)
	}
	want := map[string]bool{"acme": true, "acme.analyst": true, "acme.dagster": true}
	if len(binding.Principals) != len(want) {
		t.Fatalf("principals = %v", binding.Principals)
	}
	for _, principal := range binding.Principals {
		if !want[principal] {
			t.Errorf("unexpected principal %q", principal)
		}
	}
}

// The binding must contain EXACTLY what the coordinator's password file
// contains. Deriving the two from different rules is how a gate ends up
// blocking a user Trino would authenticate, or admitting one it would not.
func TestBindingMatchesTheProjectedAuthFile(t *testing.T) {
	rootless := bindingOrg("analyst", "dagster")
	// No enabled root: the bare principal leaves password.db, so it must
	// leave the binding too.
	rootless.RootPasswordHash = ""
	for name, org := range map[string]configstore.TrinoEnabledOrg{
		"with root": bindingOrg("analyst", "dagster"),
		"rootless":  rootless,
	} {
		t.Run(name, func(t *testing.T) { assertBindingMatchesAuthFile(t, org) })
	}
}

func assertBindingMatchesAuthFile(t *testing.T, org configstore.TrinoEnabledOrg) {
	t.Helper()
	binding := trinoPoolTenantBindingFor(org)

	passwordDB, _ := provisioner.BuildTrinoAuthFiles([]configstore.TrinoEnabledOrg{org},
		provisioner.TrinoClusterPrincipals{AdminPasswordHash: "admin-hash", ObserverPasswordHash: "observer-hash"})

	projected := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(passwordDB), "\n") {
		if line == "" {
			continue
		}
		principal, _, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		// The cluster's own operational principals are not tenant principals.
		if strings.HasPrefix(principal, "__") {
			continue
		}
		projected[principal] = true
	}

	if len(projected) == 0 {
		t.Fatal("the auth-file projection produced no tenant principals")
	}
	for _, principal := range binding.Principals {
		if !projected[principal] {
			t.Errorf("binding publishes %q, which password.db does not contain", principal)
		}
		delete(projected, principal)
	}
	for principal := range projected {
		t.Errorf("password.db contains %q, which the binding does not publish", principal)
	}
}

// A username the auth-file projection refuses never reaches password.db, so it
// can never authenticate. Publishing it would advertise a principal Trino
// rejects.
func TestBindingDropsUnprojectableUsernames(t *testing.T) {
	binding := trinoPoolTenantBindingFor(bindingOrg("analyst", "bad:user", "with space", "line\nbreak"))
	for _, principal := range binding.Principals {
		if strings.ContainsAny(principal, ": \n") {
			t.Errorf("binding published an unprojectable principal %q", principal)
		}
	}
	if len(binding.Principals) != 2 {
		t.Fatalf("principals = %v, want the root login and the one valid user", binding.Principals)
	}
}

// The revision is what tells the operator a tenant's binding must be
// republished before a new login can be dispatched.
func TestBindingRevisionTracksThePrincipalSet(t *testing.T) {
	base := trinoPoolTenantBindingFor(bindingOrg("analyst"))
	same := trinoPoolTenantBindingFor(bindingOrg("analyst"))
	if base.Revision != same.Revision {
		t.Fatal("the revision is not stable for an unchanged principal set")
	}
	// Ordering of the org's users must not change the revision: it is a set.
	reordered := trinoPoolTenantBindingFor(bindingOrg("dagster", "analyst"))
	ordered := trinoPoolTenantBindingFor(bindingOrg("analyst", "dagster"))
	if reordered.Revision != ordered.Revision {
		t.Fatal("the revision depends on user ordering")
	}
	if ordered.Revision == base.Revision {
		t.Fatal("adding a login did not change the revision")
	}
	removed := trinoPoolTenantBindingFor(bindingOrg())
	if removed.Revision == base.Revision {
		t.Fatal("removing a login did not change the revision")
	}
}

// A pool publishes bindings only for the orgs it owns. Publishing another
// cell's tenant would admit it on a pool that does not serve it.
func TestBindingsAreScopedToThePool(t *testing.T) {
	mine := bindingOrg("analyst")
	theirs := bindingOrg("analyst")
	theirs.OrgID, theirs.CellID = "org-b", "registered:cell-002"

	bindings := trinoPoolBindingsFor([]configstore.TrinoEnabledOrg{mine, theirs}, "registered:cell-001")
	if len(bindings) != 1 || bindings[0].Tenant != "org-a" {
		t.Fatalf("bindings = %+v", bindings)
	}
}
