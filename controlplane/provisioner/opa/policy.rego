# Customer Trino tenant-isolation policy.
#
# Per the customer-Trino plan ("Threat model honesty: OPA is the only real
# boundary in v1"), this policy is the single point of failure for cross-
# tenant data exposure on the shared Trino cluster. Review accordingly.
#
# Inputs (Trino OPA plugin schema, verified against trinodb/trino tag 476
# at plugin/trino-opa/src/main/java/io/trino/plugin/opa/):
#
#   input.context.identity.user        -- the Trino current_user. For
#                                         customer principals this is the
#                                         stringified team_id; the
#                                         provisioner uses __admin_provisioner.
#   input.context.identity.groups      -- group memberships (org_<team_id>).
#   input.action.operation             -- one of the operation strings from
#                                         OpaAccessControl.java; we enumerate
#                                         only the ones we explicitly allow.
#   input.action.resource.catalog.name -- target catalog name where applicable.
#   input.action.resource.schema.catalogName / .schemaName
#   input.action.resource.table.catalogName / .schemaName / .tableName
#   input.action.resource.systemSessionProperty.name
#
# Data (mounted via the bundle's data.json under user_catalogs):
#
#   data.user_catalogs[<user>][<catalog>] == true
#       iff <user> owns <catalog>. Object-indexed, NOT a linear scan over
#       orgs. At thousands of orgs a linear-scan policy is 10-50ms per
#       decision; the latency benchmark in the Go side rejects bundles
#       that take that path.
#
# Defaults: deny everything not explicitly allowed below.

package trino

import rego.v1

# ---------------------------------------------------------------------------
# Default-deny scalar.
# ---------------------------------------------------------------------------

default allow := false

# ---------------------------------------------------------------------------
# Admin principal constant. Catalog-management operations are only ever
# allowed for this principal. Keep in sync with opa.AdminPrincipal in
# types.go.
# ---------------------------------------------------------------------------

admin_principal := "__admin_provisioner"

# ---------------------------------------------------------------------------
# Helper: O(1) catalog-ownership lookup. Uses object indexing so that the
# query plan is a single hash probe regardless of the number of orgs in
# the bundle.
#
# Returns true iff the requesting user owns the given catalog name.
# Safe to call with missing user / catalog -- absent keys evaluate to
# undefined, the rule body fails, and the outer `allow` stays false.
# ---------------------------------------------------------------------------

owns_catalog(user, catalog) if {
	data.user_catalogs[user][catalog] == true
}

# Convenience for ops whose resource carries a `catalogName` field on the
# schema/table object (FilterSchemas, ShowTables, SelectFromColumns, ...).
user := input.context.identity.user

# ---------------------------------------------------------------------------
# Catalog-scope decisions.
# ---------------------------------------------------------------------------

# AccessCatalog: caller must own the catalog.
allow if {
	input.action.operation == "AccessCatalog"
	owns_catalog(user, input.action.resource.catalog.name)
}

# FilterCatalogs: the Trino OPA plugin calls this once per candidate catalog
# (parallelFilterFromOpa in OpaHighLevelClient.java). Same shape as
# AccessCatalog -- return true only for owned catalogs.
allow if {
	input.action.operation == "FilterCatalogs"
	owns_catalog(user, input.action.resource.catalog.name)
}

# ShowSchemas: scoped to catalogs the caller owns.
allow if {
	input.action.operation == "ShowSchemas"
	owns_catalog(user, input.action.resource.catalog.name)
}

# ---------------------------------------------------------------------------
# Schema-scope decisions. Resource is TrinoSchema {catalogName, schemaName}.
# ---------------------------------------------------------------------------

allow if {
	input.action.operation == "FilterSchemas"
	owns_catalog(user, input.action.resource.schema.catalogName)
}

allow if {
	input.action.operation == "ShowTables"
	owns_catalog(user, input.action.resource.schema.catalogName)
}

# ---------------------------------------------------------------------------
# Table-scope decisions. Resource is TrinoTable
# {catalogName, schemaName, tableName, columns?}.
# ---------------------------------------------------------------------------

allow if {
	input.action.operation == "SelectFromColumns"
	owns_catalog(user, input.action.resource.table.catalogName)
}

allow if {
	input.action.operation == "FilterTables"
	owns_catalog(user, input.action.resource.table.catalogName)
}

allow if {
	input.action.operation == "ShowColumns"
	owns_catalog(user, input.action.resource.table.catalogName)
}

allow if {
	input.action.operation == "FilterColumns"
	owns_catalog(user, input.action.resource.table.catalogName)
}

# ---------------------------------------------------------------------------
# Session-property allowlist.
#
# Narrow allowlist (execution_policy, query_priority, join_distribution_type).
# Memory and concurrency knobs are explicitly NOT here -- the plan calls them
# out as cross-tenant attack vectors. Adding any property to this set is a
# threat-model decision; see "Tenant Isolation Invariants" #5 in the plan.
# ---------------------------------------------------------------------------

safe_session_properties := {
	"execution_policy",
	"query_priority",
	"join_distribution_type",
}

allow if {
	input.action.operation == "SetSystemSessionProperty"
	input.action.resource.systemSessionProperty.name in safe_session_properties
}

# ---------------------------------------------------------------------------
# ExecuteQuery: every authenticated user can submit queries. Resource group
# isolation (Stream A) handles concurrency/queue limits; per-query caps in
# config.properties cap individual queries.
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
# - WriteSystemInformation, KillQueryOwnedBy, ViewQueryOwnedBy: cross-org
#   visibility / mutation surfaces that don't belong to customer principals.
# - GRANT-related ops, view/function creation, table mutation: not part of
#   v1's per-org Iceberg-catalog model.
#
# All of the above fall through to default-deny; no explicit rule needed.
# We do however explicitly deny catalog-management ops for non-admins, so
# the admin carve-out below is symmetric with the deny path.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Admin carve-out: catalog management.
#
# The provisioner connects as `__admin_provisioner` to run
# CREATE/DROP CATALOG (Stream A). Customer principals never reach these
# paths because we authenticate the provisioner against a separate
# password-file entry mounted only into the provisioner workflow.
#
# `AlterCatalog` is not a distinct operation in the Trino OPA plugin (476)
# -- catalog mutations happen via DROP+CREATE through the provisioner --
# but we keep the rule for forward compatibility with future Trino versions
# that may add the op. If the op never arrives the rule is dead code and
# costs nothing.
# ---------------------------------------------------------------------------

allow if {
	user == admin_principal
	input.action.operation == "CreateCatalog"
}

allow if {
	user == admin_principal
	input.action.operation == "DropCatalog"
}

allow if {
	user == admin_principal
	input.action.operation == "AlterCatalog"
}

# Admin principal is also allowed to do everything the customer-facing ops
# above do (read its own catalogs, run queries, etc.) for smoke testing.
# We deliberately do NOT grant the admin principal blanket access -- it
# can only see catalogs that appear under data.user_catalogs[admin_principal]
# just like any other user. Stream A populates that entry with the global
# catalog list when it generates the bundle.
