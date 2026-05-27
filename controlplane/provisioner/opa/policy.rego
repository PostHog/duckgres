# Customer Trino tenant-isolation policy.
#
# Per the customer-Trino plan ("Threat model honesty: OPA is the only real
# boundary in v1"), this policy is the single point of failure for cross-
# tenant data exposure on the shared Trino cluster. Review accordingly.
#
# Inputs (Trino OPA plugin schema, verified against trinodb/trino tag 476
# at plugin/trino-opa/src/main/java/io/trino/plugin/opa/):
#
#   input.context.identity.user        -- the Trino current_user. Used only
#                                         for the admin-principal carve-out
#                                         (catalog management).
#   input.context.identity.groups      -- group memberships resolved by
#                                         Trino's file group provider (v1)
#                                         or OIDC group claim (post-v1).
#                                         Customer principals get
#                                         `org_<team_id>`; the admin gets
#                                         the admin group. Customer access
#                                         decisions key on this, not on the
#                                         username -- so the bundle schema
#                                         is stable across v1's
#                                         password-file auth and v2's OIDC
#                                         per-user identity.
#   input.action.operation             -- one of the operation strings from
#                                         OpaAccessControl.java; we enumerate
#                                         only the ones we explicitly allow.
#   input.action.resource.catalog.name -- target catalog name where applicable.
#   input.action.resource.schema.catalogName / .schemaName
#   input.action.resource.table.catalogName / .schemaName / .tableName
#   input.action.resource.systemSessionProperty.name
#
# Data (mounted via the bundle's data.json under group_catalogs):
#
#   data.group_catalogs[<group>][<catalog>] == true
#       iff members of <group> own <catalog>. Object-indexed, NOT a linear
#       scan over orgs. At thousands of orgs a linear-scan policy is
#       10-50ms per decision; the latency benchmark in the Go side rejects
#       bundles that take that path. The per-decision `some g in
#       input.context.identity.groups` iterates over a typically 1-2
#       element list and is bounded.
#
# Defaults: deny everything not explicitly allowed below.
#
# !!! Cross-component invariant: this policy is the NON-BATCHED OPA
# contract. Trino 476's batched access control (OpaBatchAccessControl,
# enabled via `opa.policy.batched-uri` on the trino-opa plugin) sends
# candidates under `input.action.filterResources` (plural) instead of
# `input.action.resource` (singular). Every filter rule below reads
# `input.action.resource.*`, so under the batched shape they all fail
# their body and fall through to `default allow := false`. That is
# fail-closed — safe by the threat model — but operationally it means
# enabling batched mode in the chart silently denies every filter
# query, which looks like a tenant-isolation bug from the outside.
# DO NOT set `opa.policy.batched-uri` in the customer-Trino chart
# without first extending this policy to handle the batched shape (and
# the bundle generator's tests). TestBatchedInputDeniesByDefault below
# locks the fail-closed behaviour as a regression guard.

package trino

import rego.v1

# ---------------------------------------------------------------------------
# Default-deny scalar.
# ---------------------------------------------------------------------------

default allow := false

# ---------------------------------------------------------------------------
# Admin principal and group constants. The admin identity is the
# provisioner's catalog-management role; it requires BOTH the admin
# username AND admin_group membership (`is_admin` below). This conjunction
# is defense in depth: in v1 both signals come from the same K8s Secret
# (password.db + group.db projected by the provisioner), but the
# conjunction guards against a regression in projection logic that lets
# a customer's team_id collide with the admin name OR be added to the
# admin group. Under v2 OIDC, the username + group claim ride together
# in a signed JWT; the conjunction still hardens the boundary should
# any future identity flow split them.
#
# Keep in sync with opa.AdminPrincipal / opa.AdminGroup in types.go.
# ---------------------------------------------------------------------------

admin_principal := "__admin_provisioner"

admin_group := "__admin_provisioner"

# Convenience binding for rules below.
user := input.context.identity.user

# Admin identity check: both the principal name AND the admin group must
# be present. Either alone is insufficient.
is_admin if {
	user == admin_principal
	admin_group in input.context.identity.groups
}

# ---------------------------------------------------------------------------
# Catalog-name shape: matches what trinoSanitize + TrinoCatalogName produce
# (`org_<sanitized>_iceberg`, sanitized to [a-z0-9_]). Used as a defense-in-
# depth name constraint on admin-scoped operations so admin authority is
# always bounded to provisioner-managed names — admin cannot DROP `system`,
# `jmx`, or hand-rolled catalogs, and the orphan-cleanup carve-out (below)
# stays within the same naming convention.
#
# Keep this regex in sync with opa.ManagedCatalogPattern (the exported
# Go-side constant) and with the trinoSanitize grammar used by
# TrinoCatalogName. Two tests guard the three-way contract:
#   - opa/policy_test.go::TestPolicyRegoContainsManagedNamePattern
#   - provisioner/trino_provisioner_test.go::TestTrinoCatalogNameMatchesManagedNamePattern
# A change here without updating the constant (or vice versa) fails CI.
# ---------------------------------------------------------------------------

managed_catalog_name(catalog) if {
	is_string(catalog)
	regex.match(`^org_[a-z0-9_]+_iceberg$`, catalog)
}

# ---------------------------------------------------------------------------
# Ownership / readability / listability — three predicates kept distinct
# so admin's orphan-cleanup carve-out doesn't leak into data-plane reads.
#
# tenant_owns_catalog(c): a customer group has c in data.group_catalogs.
#   Excludes admin_group from iteration so a bare admin-group claim grants
#   nothing (security regression in the file/JWT group provider would
#   otherwise be a cross-tenant exposure).
#
# admin_bundle_catalog(c): admin has c listed in data.group_catalogs
#   [admin_group]. Gated on is_admin (username + group conjunction) so a
#   group-only claim grants nothing. This is the smoke-test read path.
#
# readable_catalog(c) = tenant_owns OR admin_bundle. Gates every read
#   surface (AccessCatalog, ShowSchemas, ShowTables, SelectFromColumns,
#   FilterSchemas, FilterTables, ShowColumns, FilterColumns). Catalogs
#   outside the bundle CANNOT be read by anyone.
#
# listable_catalog(c) = readable OR (is_admin AND managed_catalog_name).
#   The orphan-cleanup carve-out: admin sees `org_*_iceberg` catalogs
#   regardless of bundle ownership SOLELY through FilterCatalogs, so
#   reconcile can re-issue DROP CATALOG on a stale orphan. The orphan
#   visibility never grants read access.
# ---------------------------------------------------------------------------

tenant_owns_catalog(catalog) if {
	some g in input.context.identity.groups
	g != admin_group
	data.group_catalogs[g][catalog] == true
}

admin_bundle_catalog(catalog) if {
	is_admin
	data.group_catalogs[admin_group][catalog] == true
}

readable_catalog(catalog) if tenant_owns_catalog(catalog)
readable_catalog(catalog) if admin_bundle_catalog(catalog)

listable_catalog(catalog) if readable_catalog(catalog)

# Orphan-cleanup carve-out: admin enumeration of managed-name catalogs
# even when they're not in the bundle. SCOPED to FilterCatalogs only --
# see the allow rule for FilterCatalogs below. Reads (AccessCatalog,
# SelectFromColumns, etc.) stay gated on readable_catalog so an orphan
# catalog's data is never reachable via this carve-out.
listable_catalog(catalog) if {
	is_admin
	managed_catalog_name(catalog)
}

# ---------------------------------------------------------------------------
# Catalog-scope decisions.
# ---------------------------------------------------------------------------

# AccessCatalog: caller must be able to READ the catalog (bundle
# ownership). The admin orphan-cleanup carve-out does NOT apply here --
# only listable_catalog grants admin access to orphans, and only via
# FilterCatalogs below.
allow if {
	input.action.operation == "AccessCatalog"
	readable_catalog(input.action.resource.catalog.name)
}

# FilterCatalogs: the Trino OPA plugin calls this once per candidate catalog
# (parallelFilterFromOpa in OpaHighLevelClient.java). This is the ONE
# decision that uses listable_catalog -- so admin can SHOW CATALOGS and
# see an orphan org_*_iceberg catalog even if the bundle has rotated
# away from it, enabling reconcile to retry DROP CATALOG.
allow if {
	input.action.operation == "FilterCatalogs"
	listable_catalog(input.action.resource.catalog.name)
}

# ShowSchemas: scoped to catalogs the caller can READ.
allow if {
	input.action.operation == "ShowSchemas"
	readable_catalog(input.action.resource.catalog.name)
}

# ---------------------------------------------------------------------------
# Schema-scope decisions. Resource is TrinoSchema {catalogName, schemaName}.
# ---------------------------------------------------------------------------

allow if {
	input.action.operation == "FilterSchemas"
	readable_catalog(input.action.resource.schema.catalogName)
}

allow if {
	input.action.operation == "ShowTables"
	readable_catalog(input.action.resource.schema.catalogName)
}

# ---------------------------------------------------------------------------
# Table-scope decisions. Resource is TrinoTable
# {catalogName, schemaName, tableName, columns?}.
# ---------------------------------------------------------------------------

allow if {
	input.action.operation == "SelectFromColumns"
	readable_catalog(input.action.resource.table.catalogName)
}

allow if {
	input.action.operation == "FilterTables"
	readable_catalog(input.action.resource.table.catalogName)
}

allow if {
	input.action.operation == "ShowColumns"
	readable_catalog(input.action.resource.table.catalogName)
}

allow if {
	input.action.operation == "FilterColumns"
	readable_catalog(input.action.resource.table.catalogName)
}

# ---------------------------------------------------------------------------
# Session-property allowlist.
#
# Narrow allowlist (execution_policy, join_distribution_type). Both are
# query-tuning hints that affect the SUBMITTING query's own shape (execution
# strategy, join distribution), bounded by per-query memory/CPU caps in
# config.properties and the per-org resource-group memory limit. They do
# not influence cross-tenant scheduling.
#
# Memory, concurrency, and cross-tenant scheduling knobs are explicitly
# NOT here -- they're cross-tenant attack vectors. Notably absent:
#
#   - `query_priority`: under Trino's `query_priority` and `weighted_fair`
#     scheduling policies this would let one tenant degrade another's
#     queue position. The plan currently locks every resource group to
#     `fair`, under which `query_priority` is ignored, so allowing it
#     would be inert today -- but coupling the safety of this allowlist
#     to the chart's scheduling-policy choice is a load-bearing
#     invariant we don't need. Denied across the board; if we ever
#     adopt a non-fair scheduling policy intentionally, revisit this
#     allowlist as part of the same change.
#   - `query_max_memory*`, `query_max_total_memory`, `resource_overcommit`:
#     direct memory-budget escapes.
#   - `query_max_cpu_time`, `query_max_execution_time`: per-query caps the
#     cluster admin sets; letting customers raise them defeats per-query
#     bounds.
#
# Adding any property to this set is a threat-model decision: it must
# either be confined to the submitting query's own shape, OR there must
# be a clear argument why a tenant's setting cannot affect any other
# tenant's queries, queue position, or resource share.
# ---------------------------------------------------------------------------

safe_session_properties := {
	"execution_policy",
	"join_distribution_type",
}

allow if {
	input.action.operation == "SetSystemSessionProperty"
	input.action.resource.systemSessionProperty.name in safe_session_properties
}

# ---------------------------------------------------------------------------
# ExecuteQuery: every authenticated user can submit queries. Trino resource
# groups (configured per-org by the provisioner) handle concurrency and queue
# limits; per-query caps in config.properties cap individual queries.
# ---------------------------------------------------------------------------

allow if {
	input.action.operation == "ExecuteQuery"
}

# ---------------------------------------------------------------------------
# Hard denies for customer principals.
#
# These are listed explicitly even though `default allow := false` already
# covers them, because they're the load-bearing security boundary and the
# unit tests assert that named operations stay denied. If a future version
# of the Trino OPA plugin renames any of these, the unit tests fail loudly.
#
# - ImpersonateUser: NEVER allowed. There is no carve-out, not even for
#   the admin principal -- the provisioner has no need to impersonate.
# - WriteSystemInformation, KillQueryOwnedBy, ViewQueryOwnedBy: in Trino
#   476 these are gated by the `opa.allow-permission-management-operations`
#   config knob and not actually sent to OPA, so the unit tests covering
#   them are forward-compat only. They remain default-deny here regardless.
# - GRANT-related ops, view/function creation, table mutation: not part of
#   v1's per-org Iceberg-catalog model.
#
# All of the above fall through to default-deny; no explicit rule needed.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Admin carve-out: catalog management.
#
# The Trino provisioner connects as `__admin_provisioner` to run
# CREATE/DROP CATALOG. The carve-out is keyed on the username -- not on
# group membership -- because catalog management is a provisioner workflow,
# not a tenant role. Customer principals never reach these paths because
# we authenticate the provisioner against a separate password-file entry
# mounted only into the provisioner workflow.
#
# Catalog management is ALSO bounded to the provisioner naming convention
# (managed_catalog_name): an admin can CREATE/DROP/ALTER `org_<id>_iceberg`
# catalogs, but not `system`, `jmx`, or any hand-rolled catalog. If admin
# credentials are compromised, the blast radius is bounded to provisioner-
# managed catalogs.
#
# `AlterCatalog` is not a distinct operation in the Trino OPA plugin (476)
# -- catalog mutations happen via DROP+CREATE through the provisioner --
# but we keep the rule for forward compatibility with future Trino versions
# that may add the op. If the op never arrives the rule is dead code and
# costs nothing.
# ---------------------------------------------------------------------------

catalog_management_ops := {"CreateCatalog", "DropCatalog", "AlterCatalog"}

allow if {
	is_admin
	input.action.operation in catalog_management_ops
	managed_catalog_name(input.action.resource.catalog.name)
}

# Admin smoke-test READ access to catalogs flows through readable_catalog
# above (is_admin + data.group_catalogs[admin_group]). Admin can only
# READ catalogs that appear under data.group_catalogs[admin_group];
# omitting an entry yields a catalog-management-only admin for that
# catalog. The orphan-cleanup carve-out (listable_catalog) lets admin
# ENUMERATE managed-name catalogs not in the bundle but does NOT grant
# them read access. And a bare claim of admin_group membership (without
# the admin username) grants nothing.
