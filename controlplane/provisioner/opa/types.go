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
// This file defines the shared types (GroupCatalogs, BundleBuilder) that
// the Trino provisioner (controlplane/provisioner/trino_provisioner.go)
// imports to assemble and push the bundle. The shape is intentionally
// minimal so the policy/bundle code and the provisioner can land in
// either order and merge cleanly.
//
// Keying choice: the policy authorizes by *group* membership, not by
// username. Trino's file group provider (and, post-v1, OIDC group claims)
// stamps `org_<org>` (sanitized Org.Name; and `__admin_provisioner` for
// the admin) into
// every request's `identity.groups`. Keying on groups means the bundle
// schema does not change when v2 moves from password-file auth to OIDC
// with per-user identity within an org -- only the source of
// `identity.groups` changes (file group provider -> JWT claim), and the
// Rego policy stays put. Keying on user would force the bundle to grow
// per-user and require a bundle-shape migration during the OIDC rollout.
package opa

// GroupCatalogs maps a Trino group name (e.g. `org_<org>` for customer
// orgs, where `org` is the sanitized Org.Name; or the admin group for
// the provisioner's smoke-test access) to the set of catalog names that
// group owns.
//
// The "set" is represented as map[string]bool with the value always true,
// so the Rego policy can do an O(1) presence check
// (`data.group_catalogs[group][catalog]`). A linear-scan policy at thousands
// of orgs is 10-50ms per decision and compounds across 30-40 decisions per
// query; the latency benchmark rejects bundles that take that path.
//
// The policy iterates over `input.context.identity.groups` (typically 1-2
// entries per principal) and does an object-indexed lookup per group --
// bounded iteration, still O(1) in catalog count.
type GroupCatalogs map[string]map[string]bool

// BundleBuilder builds an OPA bundle (gzip'd tarball per OPA's bundle spec)
// from a GroupCatalogs input. The returned bytes are suitable for serving
// from a bundle endpoint or POSTing through OPA's bundle service API.
type BundleBuilder interface {
	BuildBundle(gc GroupCatalogs) ([]byte, error)
}

// AdminPrincipal is the Trino username the provisioner authenticates as
// when invoking catalog-management operations. The OPA policy allows
// CreateCatalog/DropCatalog/AlterCatalog only for this principal; every
// other user (customer principals or anything unrecognised) is denied.
//
// Keep in sync with the Trino password-file principal name written by
// the provisioner's catalog-management credentials and with the Rego
// policy's `admin_principal` constant.
const AdminPrincipal = "__admin_provisioner"

// AdminGroup is the Trino group name the provisioner writes into the file
// group provider's group.db for the admin principal. The policy uses
// AdminGroup in two distinct ways:
//
//  1. As an *identity claim*: catalog-management ops (CreateCatalog /
//     DropCatalog / AlterCatalog) require BOTH the admin username AND
//     `admin_group` membership in identity.groups (`is_admin` conjunction).
//     The provisioner MUST therefore include the admin principal in
//     AdminGroup in group.db -- otherwise even the legitimate provisioner
//     cannot perform catalog management.
//
//  2. As a *catalog ownership label*: the bundle generator places the
//     global catalog list under `data.group_catalogs[AdminGroup]` to give
//     the admin smoke-test read access to every managed catalog. This
//     entry is optional -- omitting it yields a catalog-management-only
//     admin with no read access (idempotency checks via SHOW CATALOGS
//     will then fail, so practically the entry should be present).
//
// A bare AdminGroup claim WITHOUT the admin username grants nothing in
// either dimension: read access via this group is gated on `is_admin`
// (full conjunction), and management is gated on `is_admin` directly.
//
// Keep in sync with the Rego policy's `admin_group` constant.
const AdminGroup = "__admin_provisioner"

// ManagedCatalogPattern is the regex (RE2 / OPA `regex.match` syntax)
// that defines the v1 catalog naming convention. It MUST stay in sync
// with the Rego policy's managed_catalog_name rule AND with the
// provisioner package's TrinoCatalogName / trinoSanitize functions.
//
// The pattern bounds:
//   - prefix: "org_"
//   - middle: one or more [a-z0-9_] characters (the sanitize grammar)
//   - suffix: "_iceberg"
//
// Drift between this constant and either the policy's regex literal or
// TrinoCatalogName's output silently breaks admin authority (admin
// loses enumeration/management for catalogs the Go code creates, or
// vice versa). Two tests guard the contract:
//
//   - opa/policy_test.go::TestPolicyRegoContainsManagedNamePattern
//     asserts the embedded policy.rego contains this literal substring.
//   - provisioner/trino_provisioner_test.go::TestTrinoCatalogNameMatchesManagedNamePattern
//     compiles this pattern as a Go regex and asserts TrinoCatalogName
//     outputs match for a representative set of inputs.
//
// If you change this string, update policy.rego's regex.match literal
// to match, then re-run both tests.
const ManagedCatalogPattern = `^org_[a-z0-9_]+_iceberg$`
