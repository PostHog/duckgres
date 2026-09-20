//go:build kubernetes

package controlplane

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
)

// The canonical principal/tenant binding.
//
// The Gateway's pooled admission gate restricts dispatch to principals whose
// tenant is admitted. It is NOT a second authenticator: Trino still verifies
// the credential, and OPA still authorizes the query. For that restriction to
// be sound the Gateway's lookup key has to be exactly the principal Trino
// authenticates, and the Gateway cannot derive that itself — it would have to
// guess at username projection and host qualification.
//
// So duckgres publishes the binding authoritatively, derived from the SAME
// projection that writes the coordinator's password.db. If the two ever
// disagree, the gate would either block a legitimate user or admit a principal
// Trino rejects; deriving both from one function is what prevents that.
//
// A warehouse has MANY principals: the bare database name (the org's root
// login) plus one per org user. All of them belong to the same tenant.

// trinoPoolTenantBinding is one tenant's authoritative principal set.
type trinoPoolTenantBinding struct {
	// Tenant is the Gateway's admission key: the org id.
	Tenant string `json:"tenant"`
	// Catalog is the org's Trino catalog, for operator readability. The gate
	// does not key on it.
	Catalog string `json:"catalog"`
	// Principals are the exact strings the coordinator's password file
	// contains, sorted for a stable revision.
	Principals []string `json:"principals"`
	// Revision changes whenever the principal set changes, so a stale binding
	// is detectable rather than silently served.
	Revision string `json:"revision"`
}

// trinoPoolTenantBindingFor derives one org's binding.
//
// Principals that the auth-file projection would REFUSE are dropped here too.
// The projection allowlist exists because duckgres barely validates usernames
// and a `:` or a newline would let whoever can create org users append lines to
// password.db; a username that cannot be projected never reaches password.db,
// so publishing it would bind a principal that can never authenticate.
func trinoPoolTenantBindingFor(org configstore.TrinoEnabledOrg) trinoPoolTenantBinding {
	principals := map[string]bool{}
	// The bare database name is the org's root login. It is always present:
	// ListTrinoEnabledOrgs only returns orgs that have one.
	if root := strings.TrimSpace(org.TrinoPrincipal()); root != "" {
		principals[root] = true
	}
	for _, user := range org.Users {
		if !provisioner.ProjectableTrinoUsername(user.Username) {
			continue
		}
		principals[org.TrinoUserPrincipal(user.Username)] = true
	}

	ordered := make([]string, 0, len(principals))
	for principal := range principals {
		ordered = append(ordered, principal)
	}
	sort.Strings(ordered)

	return trinoPoolTenantBinding{
		Tenant:     org.OrgID,
		Catalog:    configstore.TrinoCatalogName(org.DatabaseName),
		Principals: ordered,
		Revision:   trinoPoolBindingRevision(ordered),
	}
}

// trinoPoolBindingRevision is a stable digest of the principal set. Adding,
// removing or renaming a login changes it, which is what tells the operator a
// tenant's binding has to be republished before the new login can be dispatched.
func trinoPoolBindingRevision(principals []string) string {
	digest := sha256.New()
	for _, principal := range principals {
		_, _ = digest.Write([]byte(principal))
		_, _ = digest.Write([]byte{0})
	}
	return hex.EncodeToString(digest.Sum(nil))[:32]
}

// trinoPoolBindingsFor derives the bindings of every org a pool serves, sorted
// by tenant so a republication decision is reproducible.
func trinoPoolBindingsFor(orgs []configstore.TrinoEnabledOrg, poolID string) []trinoPoolTenantBinding {
	bindings := make([]trinoPoolTenantBinding, 0, len(orgs))
	for _, org := range orgs {
		if org.CellID != poolID {
			continue
		}
		bindings = append(bindings, trinoPoolTenantBindingFor(org))
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Tenant < bindings[j].Tenant })
	return bindings
}
