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

import "strings"

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

// GroupScope narrows one group to part of the catalog it owns. A group with
// no GroupScope is unscoped and reads the whole catalog; a group WITH one
// reads only what these sets name. The policy consults a scope only after the
// group has already been found to own the catalog in GroupCatalogs, so a
// scope can subtract access but never add any -- in particular it can never
// reach another tenant's catalog.
//
// Each field is a set represented as map[string]bool with the value always
// true, for the same O(1)-lookup reason GroupCatalogs is (see above): the
// policy indexes into these objects rather than scanning them.
type GroupScope struct {
	// Schemas are readable in full: every table in them is allowed.
	Schemas map[string]bool `json:"schemas"`
	// Relations are individually readable tables, keyed "<schema>.<table>",
	// for schemas the group does NOT hold in full. duckgres grants these for
	// a project's tables that live in the shared legacy `posthog` schema.
	Relations map[string]bool `json:"relations"`
	// RelationSchemas is the set of schema names appearing in Relations,
	// precomputed so a schema-level decision (FilterSchemas, ShowTables) is
	// an object lookup rather than a scan over Relations. Derived data --
	// build it with NewGroupScope rather than by hand, so it cannot drift
	// from Relations and silently hide a schema the group can read a table
	// in.
	RelationSchemas map[string]bool `json:"relation_schemas"`
}

// GroupScopes maps a Trino group name to the scope narrowing it. Only
// project-scoped groups appear; the absence of a key means "unscoped", which
// is what every org's own `org_<name>` group is.
type GroupScopes map[string]GroupScope

// NewGroupScope builds a GroupScope from the allowed-schema and
// allowed-relation lists duckgres derives for a project login, deriving
// RelationSchemas from relations so the two cannot disagree.
//
// A relation that is not "<schema>.<table>" is dropped rather than guessed
// at: it would otherwise land in the policy as a key no decision can ever
// match, which reads as a working grant and is not one.
func NewGroupScope(schemas, relations []string) GroupScope {
	scope := GroupScope{
		Schemas:         map[string]bool{},
		Relations:       map[string]bool{},
		RelationSchemas: map[string]bool{},
	}
	for _, s := range schemas {
		if s != "" {
			scope.Schemas[s] = true
		}
	}
	for _, r := range relations {
		schema, table, ok := strings.Cut(r, ".")
		if !ok || schema == "" || table == "" || strings.Contains(table, ".") {
			continue
		}
		scope.Relations[r] = true
		scope.RelationSchemas[schema] = true
	}
	return scope
}

// BundleBuilder builds an OPA bundle (gzip'd tarball per OPA's bundle spec)
// from a GroupCatalogs input. The returned bytes are suitable for serving
// from a bundle endpoint or POSTing through OPA's bundle service API.
type BundleBuilder interface {
	BuildBundle(gc GroupCatalogs, gs GroupScopes) ([]byte, error)
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
//
// There is no suffix: catalogs were `org_<id>_iceberg` while the backing
// table format was Iceberg/Lakekeeper. Ducklings are DuckLake now, so the
// suffix went with it (see migration 000014, which dropped every iceberg_*
// column). A catalog name is exactly "org_" + trinoSanitize(orgID).
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
const ManagedCatalogPattern = `^org_[a-z0-9_]+$`

// ObserverPrincipal is the Trino username the control plane's admin
// console authenticates as when it reads the cell's query and node state
// over the coordinator REST API. It is deliberately NOT AdminPrincipal.
//
// Trino routes operator-console reads through the same access-control SPI
// as everything else: `GET /v1/query` filters its result through
// FilterViewQueryOwnedBy, `GET /v1/query/{id}` is gated on
// ViewQueryOwnedBy, kill on KillQueryOwnedBy, and `/v1/node` +
// `/v1/resourceGroupState` are MANAGEMENT_READ (checkCanReadSystemInformation).
// A console credential with no policy grant sees an empty cluster, so the
// capability has to exist in policy.rego.
//
// Splitting it from the admin principal is the security bargain: the admin
// credential can CREATE/DROP catalogs but sees only its own queries; the
// observer sees every tenant's query metadata but holds no catalog in
// data.group_catalogs and therefore cannot read a single row of tenant
// data. Neither half can be levered into the other. Keeping them fused
// would mean one leaked credential yields both catalog authority and every
// tenant's SQL text.
//
// A query's SQL text is tenant data, so anything the console surfaces from
// this principal is redacted before it leaves the control plane, the same
// way the pgwire error ring is.
//
// Keep in sync with the Rego policy's `observer_principal` constant and
// with the provisioner's password.db/group.db projection.
const ObserverPrincipal = "__duckgres_observer"

// ObserverGroup is the Trino group the provisioner stamps onto
// ObserverPrincipal in group.db. Like AdminGroup it is required as an
// identity claim -- `is_observer` is the conjunction of the observer
// USERNAME and this group membership, so a projection regression that
// puts a tenant in this group grants nothing on its own.
//
// Unlike AdminGroup it is NEVER used as a catalog-ownership label: the
// bundle generator must not place catalogs under it, and the policy
// excludes it from tenant_owns_catalog and from the same-org query match
// so a stray bundle entry still grants no data access.
//
// Keep in sync with the Rego policy's `observer_group` constant.
const ObserverGroup = "__duckgres_observer"
