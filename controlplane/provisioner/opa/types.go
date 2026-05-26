// Package opa contains the OPA Rego policy and bundle generator that
// enforces multi-tenant isolation on the customer-facing Trino cluster.
//
// The Rego policy here is the load-bearing tenant-isolation boundary: the
// customer Trino pod can sts:AssumeRole into every per-org duckling-* role
// and its NetworkPolicy permits egress to the whole lakekeeper namespace,
// so nothing structurally stops Org A's query from reading Org B's catalog
// at the AWS/network layer. The OPA policy + the catalog config (embedded
// per-org IAM role-arn) are the only thing enforcing isolation. A bug here
// is a cross-tenant data exposure incident. Treat changes to this package
// as load-bearing security review.
//
// This file defines the shared types (UserCatalogs, BundleBuilder) that
// the Trino provisioner (controlplane/provisioner/trino_provisioner.go)
// imports to assemble and push the bundle. The shape is intentionally
// minimal so the policy/bundle code and the provisioner can land in
// either order and merge cleanly.
package opa

// UserCatalogs maps a Trino principal (username, typically "<team_id>" for
// customer principals or the special "__admin_provisioner" for the
// provisioner) to the set of catalog names that principal owns.
//
// The "set" is represented as map[string]bool with the value always true,
// so the Rego policy can do an O(1) presence check
// (`data.user_catalogs[user][catalog]`). A linear-scan policy at thousands
// of orgs is 10-50ms per decision and compounds across 30-40 decisions per
// query; the latency benchmark rejects bundles that take that path.
type UserCatalogs map[string]map[string]bool

// BundleBuilder builds an OPA bundle (gzip'd tarball per OPA's bundle spec)
// from a UserCatalogs input. The returned bytes are suitable for serving
// from a bundle endpoint or POSTing through OPA's bundle service API.
type BundleBuilder interface {
	BuildBundle(uc UserCatalogs) ([]byte, error)
}

// AdminPrincipal is the username the provisioner uses when invoking
// catalog-management operations against Trino. The OPA policy allows
// CreateCatalog/DropCatalog only for this principal; every other user
// (customer principals or anything unrecognised) is denied.
//
// Keep in sync with the Trino password-file principal name written by
// the provisioner's catalog-management credentials and with the Rego
// policy's `admin_principal` constant.
const AdminPrincipal = "__admin_provisioner"
