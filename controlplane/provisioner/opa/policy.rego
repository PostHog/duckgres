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
#                                         `org_<org>` (org = sanitized
#                                         Org.Name); the admin gets
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
#   input.action.resource.user.user    -- on the query-ownership operations
#                                         (ViewQueryOwnedBy /
#                                         FilterViewQueryOwnedBy /
#                                         KillQueryOwnedBy) this is the QUERY
#                                         OWNER's Trino username, NOT the
#                                         requester's. The requester is
#                                         always input.context.identity.
#   input.action.resource.user.groups  -- the query OWNER's group
#                                         memberships, stamped by the same
#                                         file group provider that fills
#                                         input.context.identity.groups.
#
#   Shape note, verified against the plugin AND its tests: for the three
#   query-ownership ops OpaAccessControl.java builds the resource as
#   `.user(new TrinoUser(queryOwner))` with an *Identity* argument, so
#   TrinoUser.user is null and TrinoUser.identity (a TrinoIdentity of
#   {user, groups}) is `@JsonUnwrapped` -- the owner's `user` and `groups`
#   land DIRECTLY under resource.user. TestOpaAccessControl
#   .testIdentityResourceActions pins
#   `{"resource": {"user": {"user": "dummy-user", "groups": ["some-group"]}}}`
#   for ViewQueryOwnedBy / KillQueryOwnedBy, and
#   TestOpaAccessControlFiltering.testFilterViewQueryOwnedBy pins the same
#   shape (with `"groups": []`) for FilterViewQueryOwnedBy. ImpersonateUser
#   uses the OTHER TrinoUser constructor (a bare String), which emits
#   `{"user": {"user": "<name>"}}` with NO groups key -- the two shapes are
#   NOT interchangeable, and a rule keyed on the wrong one fails closed
#   while looking like a working policy.
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
# QUERY VISIBILITY is modelled below (see "Query visibility"). A query's
# SQL text routinely embeds table names, filter literals, customer
# identifiers and business logic, and Trino exposes it to any principal
# allowed to view the query -- through `SELECT * FROM
# system.runtime.queries` and through the coordinator's web UI. So the
# three query-ownership operations the plugin sends (ViewQueryOwnedBy,
# FilterViewQueryOwnedBy, KillQueryOwnedBy) are same-org-only, derived
# from the SAME group/catalog ownership map every other decision uses.
# ExecuteQuery stays unconditionally allowed -- resource groups, not OPA,
# bound concurrency.
#
# Post-v1 note: today every principal of an org is a single Trino user
# (its Trino username IS the sanitized org name), so "same org" and
# "same user" coincide. The rules are written on the GROUP axis anyway,
# so when per-user identity within an org lands (OIDC), org-mates keep
# seeing each other's queries with no policy change.
#
# !!! Cross-component invariant: this policy serves BOTH OPA shapes.
# `allow` answers the non-batched contract, where the candidate sits at
# `input.action.resource` (singular). `batch` answers Trino's
# OpaBatchAccessControl (`opa.policy.batched-uri`), where candidates
# arrive as a list at `input.action.filterResources` (plural) and the
# response is the list of allowed INDICES into it.
#
# The two are not independent: `batch` is defined by re-evaluating
# `allow` with each candidate substituted in at `input.action.resource`,
# so any rule added to `allow` is honoured in batched mode automatically
# and the shapes cannot drift. Do NOT hand-write a parallel set of
# batched rules.
#
# Both are wired deliberately. A coordinator with only `opa.policy.uri`
# set issues ONE request per candidate object, and filtering a catalog
# with more than ~1024 tables overruns the HTTP client's per-destination
# queue ("Max requests queued per destination 1024 exceeded"), which
# reaches the user as "Failed to query OPA backend" on any
# information_schema listing. TestBatchedFilteringMatchesNonBatched
# locks the two shapes to identical answers.

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
# a customer's org name collide with the admin name OR be added to the
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
# (`org_<sanitized>`, sanitized to [a-z0-9_]). Used as a defense-in-
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
	regex.match(`^org_[a-z0-9_]+$`, catalog)
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
#   The orphan-cleanup carve-out: admin sees `org_*` catalogs
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
# see an orphan org_* catalog even if the bundle has rotated
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
# Batched filtering.
#
# Trino's OpaBatchAccessControl POSTs one request carrying every candidate
# under `input.action.filterResources` and expects `{"result": [<indices>]}`
# naming the entries the caller may see. Each candidate is evaluated by
# substituting it at `input.action.resource` and re-running `allow`, so
# batched and non-batched decisions are the same decision by construction.
# ---------------------------------------------------------------------------

# Each rule dispatches on the operation and applies the SAME predicate its
# `allow` counterpart uses, reading the candidate straight out of
# filterResources.
#
# The obvious formulation — `allow with input.action.resource as candidate` —
# is quadratic and unusable here. `with` copies the whole input document per
# candidate, and the input holds every candidate, so evaluating one batch of n
# costs O(n^2): measured at 17ms for n=100, 1.0s for n=1,000, 17s for n=5,000
# and 4m for n=20,000. A catalog-wide table listing on this cell exceeded
# OPA's 60s request budget and returned 500. Reading the candidate directly is
# linear.
#
# The cost is that the operation dispatch is written twice, here and in
# `allow`. The authorization predicate itself is NOT duplicated — both call
# the same readable_catalog / listable_catalog helpers, so a change to who may
# see what lands in one place. TestBatchedFilteringMatchesNonBatched pins the
# two shapes to identical answers candidate by candidate, which is what
# catches dispatch drift.

batch contains i if {
	some i
	input.action.operation == "FilterCatalogs"
	listable_catalog(input.action.filterResources[i].catalog.name)
}

batch contains i if {
	some i
	input.action.operation == "FilterSchemas"
	readable_catalog(input.action.filterResources[i].schema.catalogName)
}

batch contains i if {
	some i
	input.action.operation == "FilterTables"
	readable_catalog(input.action.filterResources[i].table.catalogName)
}

# FilterColumns is the one operation whose indices point into the candidate's
# `columns` array rather than into filterResources: Trino sends a SINGLE table
# candidate carrying the columns.
batch contains i if {
	input.action.operation == "FilterColumns"
	count(input.action.filterResources) == 1
	readable_catalog(input.action.filterResources[0].table.catalogName)
	some i, _ in input.action.filterResources[0].table.columns
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
# Query visibility: same-org only.
#
# Threat: a query's SQL text is tenant data. Without these rules org A can
# read org B's SQL -- table names, filter literals, customer identifiers,
# business logic -- via `SELECT * FROM system.runtime.queries` or the
# coordinator web UI, and can KILL org B's running queries. ExecuteQuery
# being allowed unconditionally is exactly what makes those surfaces
# reachable, so this section is not optional hardening.
#
# The requester is input.context.identity, as everywhere else. The query
# OWNER arrives as input.action.resource.user -- see the shape note in the
# header: `.user` is the owner's username, `.groups` the owner's group
# memberships.
#
# Ownership is derived from the SAME source of truth as every other
# decision in this file: the bundle's group -> catalog map. A group only
# counts if the bundle knows it (`data.group_catalogs[g]`), so a group
# provider that starts emitting labels the provisioner never projected
# grants nothing. Object-indexed, O(1) in org count; the only iteration is
# over the requester's 1-2 group memberships and the owner's, exactly like
# tenant_owns_catalog.
#
# The admin principal (__admin_provisioner) gets NOTHING here beyond its
# own queries. The reconcile loop's Trino client issues only SHOW CATALOGS
# / CREATE CATALOG / DROP CATALOG (trino_provisioner.go); it never reads
# system.runtime.queries and never kills a query, so granting it
# cross-tenant query visibility would hand every tenant's SQL text to a
# credential with no use for it. admin_group is excluded from the same-org
# match on the requester side, so the bundle's
# data.group_catalogs[admin_group] entry -- which exists so admin can
# smoke-test catalog reads -- cannot be levered into query visibility.
# ---------------------------------------------------------------------------

query_visibility_ops := {
	"ViewQueryOwnedBy",
	"FilterViewQueryOwnedBy",
	"KillQueryOwnedBy",
}

# The query owner's group memberships. Undefined when the plugin sent no
# resource.user (or the String-form TrinoUser, as ImpersonateUser does),
# which fails same_org_query_owner closed.
query_owner_groups := input.action.resource.user.groups

# same_org_query_owner: requester and owner share at least one org group
# that the bundle actually knows about. admin_group is excluded so an
# admin-group claim can never be the shared group.
same_org_query_owner if {
	some g in input.context.identity.groups
	g != admin_group
	data.group_catalogs[g]
	g in query_owner_groups
}

# self_owned_query: the owner IS the requesting principal. Trino's own
# AccessControlUtil short-circuits this case before it reaches OPA
# (identity.getUser().equals(queryOwner.getUser()) -> return), so the rule
# is belt-and-braces: it keeps "my own query" visible even if the group
# provider has not stamped groups onto the owner identity, and it cannot
# widen anything, because an identical username IS an identical principal.
# The non-empty guard stops two absent/blank usernames from matching.
self_owned_query if {
	owner := input.action.resource.user.user
	owner != ""
	owner == user
}

visible_query_owner if same_org_query_owner

visible_query_owner if self_owned_query

allow if {
	input.action.operation in query_visibility_ops
	visible_query_owner
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
# - WriteSystemInformation / ReadSystemInformation: never allowed. Note
#   that `opa.allow-permission-management-operations` does NOT gate these
#   -- it gates only the grant/deny/revoke/set-authorization family, via
#   enforcePermissionManagementOperation in OpaAccessControl.java -- so
#   they really are sent to OPA, and really are denied here.
# - KillQueryOwnedBy / ViewQueryOwnedBy / FilterViewQueryOwnedBy: NOT
#   default-deny any more; see "Query visibility" above. They are allowed
#   for a same-org query owner and denied for everyone else, including
#   the admin principal. An input that omits resource.user, or its
#   `user` / `groups` fields, still falls through to default-deny.
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
# (managed_catalog_name): an admin can CREATE/DROP/ALTER `org_<id>`
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
