package configstore

import "testing"

// The owner index must name exactly the principals the password file carries:
// the bare org principal authenticates with root's hash, so without an enabled
// root it is not a principal and must not resolve to anyone.
func TestTrinoPrincipalOwnersFollowTheRootHash(t *testing.T) {
	withRoot := TrinoEnabledOrg{OrgID: "org-a", DatabaseName: "acme", RootPasswordHash: "hash",
		Users: []TrinoOrgUser{{Username: "analyst", PasswordHash: "hash"}}}
	rootless := TrinoEnabledOrg{OrgID: "org-b", DatabaseName: "beta",
		Users: []TrinoOrgUser{{Username: "analyst", PasswordHash: "hash"}}}

	owners := NewTrinoPrincipalOwners([]TrinoEnabledOrg{withRoot, rootless})

	if got := owners["acme"]; got != (TrinoPrincipalOwner{OrgID: "org-a", Username: "root"}) {
		t.Errorf("acme = %+v, want org-a/root", got)
	}
	if got := owners.OrgID("acme.analyst"); got != "org-a" {
		t.Errorf("acme.analyst org = %q, want org-a", got)
	}
	if _, ok := owners["beta"]; ok {
		t.Error("a root-less org's bare principal must not resolve")
	}
	if got := owners.OrgID("beta.analyst"); got != "org-b" {
		t.Errorf("beta.analyst org = %q, want org-b", got)
	}
	// A minted service credential is attributed by database, independent of
	// root: it is not a projected login.
	if got := owners.OrgID("beta.svc_0123456789abcdef01234567"); got != "org-b" {
		t.Errorf("rootless service credential org = %q, want org-b", got)
	}
	// The database index is never itself a principal.
	for principal := range owners {
		if principal == "beta" {
			t.Error("index entry leaked as the bare principal")
		}
	}
}
