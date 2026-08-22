package opa

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/open-policy-agent/opa/v1/rego"
)

// preparedPolicy compiles the embedded Rego policy and returns a
// PreparedEvalQuery against `data.trino.allow`. Reused across the
// table-driven tests below so we pay the compile cost once per test
// binary, not once per case.
func preparedPolicy(t *testing.T, gc GroupCatalogs) rego.PreparedEvalQuery {
	t.Helper()
	ctx := context.Background()
	data, err := buildDataDocument(gc)
	if err != nil {
		t.Fatalf("buildDataDocument: %v", err)
	}
	q, err := rego.New(
		rego.Query("data.trino.allow"),
		rego.Module("policy.rego", string(policyRego)),
		rego.Data(data),
	).PrepareForEval(ctx)
	if err != nil {
		t.Fatalf("PrepareForEval: %v", err)
	}
	return q
}

// evalAllow runs `data.trino.allow` against the provided input and
// returns the boolean result. If the policy evaluates to undefined
// (which OPA returns as no result), evalAllow returns false -- that's
// the deny semantic for any operation we don't explicitly handle.
func evalAllow(t *testing.T, q rego.PreparedEvalQuery, input map[string]interface{}) bool {
	t.Helper()
	ctx := context.Background()
	rs, err := q.Eval(ctx, rego.EvalInput(input))
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	if len(rs) == 0 || len(rs[0].Expressions) == 0 {
		return false
	}
	v, ok := rs[0].Expressions[0].Value.(bool)
	if !ok {
		t.Fatalf("expected bool result, got %T (%v)", rs[0].Expressions[0].Value, rs[0].Expressions[0].Value)
	}
	return v
}

// twoOrgFixture is the canonical GroupCatalogs used by most tests:
// group org_42 owns org_42; group org_43 owns org_43; the
// admin group owns both (so the admin can smoke-test them).
func twoOrgFixture() GroupCatalogs {
	return GroupCatalogs{
		"org_42": {"org_42": true},
		"org_43": {"org_43": true},
		AdminGroup: {
			"org_42": true,
			"org_43": true,
		},
	}
}

// groupsFor returns the Trino group memberships we model for a given user.
// Mirrors the provisioner's projection logic: customers belong to
// `org_<org>` (org = sanitized Org.Name); the admin user belongs to AdminGroup.
func groupsFor(user string) []string {
	if user == AdminPrincipal {
		return []string{AdminGroup}
	}
	if user == "" {
		return nil
	}
	return []string{"org_" + user}
}

// buildInput is a small helper for assembling the input.action.resource
// shape the Trino OPA plugin sends. Keeps the test cases readable.
func buildInput(user, operation string, resource map[string]interface{}) map[string]interface{} {
	return buildInputWithGroups(user, groupsFor(user), operation, resource)
}

// buildInputWithGroups is the lower-level builder used by adversarial
// tests that need to set groups independently of user.
func buildInputWithGroups(user string, groups []string, operation string, resource map[string]interface{}) map[string]interface{} {
	in := map[string]interface{}{
		"context": map[string]interface{}{
			"identity": map[string]interface{}{
				"user":   user,
				"groups": groups,
			},
			"softwareStack": map[string]interface{}{
				"trinoVersion": "476",
			},
		},
		"action": map[string]interface{}{
			"operation": operation,
		},
	}
	if resource != nil {
		in["action"].(map[string]interface{})["resource"] = resource
	}
	return in
}

func catalogResource(name string) map[string]interface{} {
	return map[string]interface{}{
		"catalog": map[string]interface{}{"name": name},
	}
}

func schemaResource(catalog, schema string) map[string]interface{} {
	return map[string]interface{}{
		"schema": map[string]interface{}{
			"catalogName": catalog,
			"schemaName":  schema,
		},
	}
}

func tableResource(catalog, schema, table string) map[string]interface{} {
	return map[string]interface{}{
		"table": map[string]interface{}{
			"catalogName": catalog,
			"schemaName":  schema,
			"tableName":   table,
		},
	}
}

func sessionPropertyResource(name string) map[string]interface{} {
	return map[string]interface{}{
		"systemSessionProperty": map[string]interface{}{"name": name},
	}
}

// queryOwnerResource builds the `resource.user` shape the Trino OPA plugin
// sends for the three query-ownership operations. Verified against the
// plugin's own tests in the vendored Trino fork:
//
//	plugin/trino-opa/src/test/java/io/trino/plugin/opa/
//	  TestOpaAccessControl.java::testIdentityResourceActions
//	    -> {"resource": {"user": {"user": "dummy-user",
//	                              "groups": ["some-group"]}}}
//	  TestOpaAccessControlFiltering.java::testFilterViewQueryOwnedBy
//	    -> the same shape, with "groups": [], and it asserts the plugin
//	       reads back "/input/action/resource/user/user".
//
// OpaAccessControl.checkCanViewQueryOwnedBy / filterViewQueryOwnedBy /
// checkCanKillQueryOwnedBy all build the resource as
// `.user(new TrinoUser(queryOwner))` with an *Identity* argument, so
// TrinoUser.identity is @JsonUnwrapped and the owner's user+groups land
// directly under resource.user. (ImpersonateUser uses the String-form
// TrinoUser and emits no "groups" key -- deliberately a different shape.)
func queryOwnerResource(owner string, groups []string) map[string]interface{} {
	u := map[string]interface{}{"user": owner}
	if groups != nil {
		u["groups"] = groups
	}
	return map[string]interface{}{"user": u}
}

// ownerOf builds the query-owner resource for a customer principal,
// mirroring the provisioner's projection (username == org name, group ==
// org_<org>) the same way groupsFor does for the requester side.
func ownerOf(user string) map[string]interface{} {
	return queryOwnerResource(user, groupsFor(user))
}

// queryVisibilityOps are the operation strings the Trino OPA plugin sends
// for query ownership. These are the plugin's names, NOT the SPI method
// names: the filter op is `FilterViewQueryOwnedBy` (the SPI method is
// filterViewQueryOwnedBy), not `FilterViewQuery`.
var queryVisibilityOps = []string{
	"ViewQueryOwnedBy",
	"FilterViewQueryOwnedBy",
	"KillQueryOwnedBy",
}

// --------------------------------------------------------------------------
// AccessCatalog and FilterCatalogs: the load-bearing catalog-scope decisions.
// --------------------------------------------------------------------------

func TestAccessCatalog(t *testing.T) {
	cases := []struct {
		name string
		user string
		cat  string
		want bool
	}{
		{"owner accesses own catalog", "42", "org_42", true},
		{"owner accesses other org catalog", "42", "org_43", false},
		{"other user accesses 42's catalog", "43", "org_42", false},
		{"unknown user accesses any catalog", "99", "org_42", false},
		{"empty user", "", "org_42", false},
		{"empty catalog", "42", "", false},
		{"catalog name that almost matches", "42", "org_42_extra", false},
		// Adversarial: a user whose group maps to no catalogs at all.
		{"user with no catalogs", "no_catalogs", "org_42", false},
	}
	// Add a no-catalogs group for the last case so the group exists but
	// owns nothing.
	gc := twoOrgFixture()
	gc["org_no_catalogs"] = map[string]bool{}
	q := preparedPolicy(t, gc)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := evalAllow(t, q, buildInput(tc.user, "AccessCatalog", catalogResource(tc.cat)))
			if got != tc.want {
				t.Fatalf("AccessCatalog user=%q catalog=%q: want %v, got %v", tc.user, tc.cat, tc.want, got)
			}
		})
	}
}

func TestFilterCatalogs(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	for _, op := range []string{"FilterCatalogs"} {
		if !evalAllow(t, q, buildInput("42", op, catalogResource("org_42"))) {
			t.Errorf("%s: user 42 should see own catalog", op)
		}
		if evalAllow(t, q, buildInput("42", op, catalogResource("org_43"))) {
			t.Errorf("%s: user 42 must NOT see org 43's catalog", op)
		}
	}
}

// --------------------------------------------------------------------------
// Schema-scope ops.
// --------------------------------------------------------------------------

func TestSchemaScopeOps(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	for _, op := range []string{"FilterSchemas", "ShowTables"} {
		t.Run(op+"/own", func(t *testing.T) {
			if !evalAllow(t, q, buildInput("42", op, schemaResource("org_42", "public"))) {
				t.Errorf("%s on own catalog must allow", op)
			}
		})
		t.Run(op+"/other", func(t *testing.T) {
			if evalAllow(t, q, buildInput("42", op, schemaResource("org_43", "public"))) {
				t.Errorf("%s on another org's catalog must deny", op)
			}
		})
	}

	t.Run("ShowSchemas allowed on owned catalog", func(t *testing.T) {
		if !evalAllow(t, q, buildInput("42", "ShowSchemas", catalogResource("org_42"))) {
			t.Error("ShowSchemas on own catalog must allow")
		}
	})
	t.Run("ShowSchemas denied on other catalog", func(t *testing.T) {
		if evalAllow(t, q, buildInput("42", "ShowSchemas", catalogResource("org_43"))) {
			t.Error("ShowSchemas on other catalog must deny")
		}
	})
}

// --------------------------------------------------------------------------
// Table-scope ops, including the most important one (SelectFromColumns).
// --------------------------------------------------------------------------

func TestTableScopeOps(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	for _, op := range []string{"SelectFromColumns", "FilterTables", "ShowColumns", "FilterColumns"} {
		if !evalAllow(t, q, buildInput("42", op, tableResource("org_42", "public", "events"))) {
			t.Errorf("%s on own catalog must allow", op)
		}
		if evalAllow(t, q, buildInput("42", op, tableResource("org_43", "public", "events"))) {
			t.Errorf("%s on another org's catalog must deny -- cross-tenant leak!", op)
		}
		if evalAllow(t, q, buildInput("99", op, tableResource("org_42", "public", "events"))) {
			t.Errorf("%s by unknown user must deny", op)
		}
	}
}

// --------------------------------------------------------------------------
// Session-property allowlist.
// --------------------------------------------------------------------------

func TestSetSystemSessionProperty(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	// Allowed: query-tuning hints that affect only the submitting query's
	// own shape and stay bounded by the per-query / per-resource-group
	// caps. Adding to this set is a threat-model decision.
	allowed := []string{"execution_policy", "join_distribution_type"}
	for _, prop := range allowed {
		if !evalAllow(t, q, buildInput("42", "SetSystemSessionProperty", sessionPropertyResource(prop))) {
			t.Errorf("session property %q should be allowed", prop)
		}
	}

	// Cross-tenant attack vectors -- memory budget, CPU/time bounds,
	// cluster-shared scheduling priority. All denied. `query_priority` is
	// here even though the plan's `fair` scheduling policy makes it inert
	// in v1: we don't want the safety of this allowlist coupled to the
	// chart's scheduling-policy choice.
	denied := []string{
		"query_priority",
		"query_max_memory",
		"query_max_memory_per_node",
		"query_max_total_memory",
		"query_max_cpu_time",
		"query_max_execution_time",
		"max_concurrent_queries",
		"resource_overcommit",
		"query_priority2",       // close-but-not-equal: must not pass
		"execution_policy_evil", // ditto, against the allowed prefix
		"",                      // empty
	}
	for _, prop := range denied {
		if evalAllow(t, q, buildInput("42", "SetSystemSessionProperty", sessionPropertyResource(prop))) {
			t.Errorf("session property %q must be denied (cross-tenant attack surface)", prop)
		}
	}
}

// --------------------------------------------------------------------------
// Impersonation: always denied.
// --------------------------------------------------------------------------

func TestImpersonateUserAlwaysDenied(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	// Customer principal trying to impersonate another customer.
	cases := []struct {
		user   string
		target string
	}{
		{"42", "43"},
		{"42", "42"}, // even self-impersonation must deny
		{"43", AdminPrincipal},
		{AdminPrincipal, "42"}, // even the admin must not impersonate
	}
	for _, tc := range cases {
		input := buildInput(tc.user, "ImpersonateUser", map[string]interface{}{
			"user": map[string]interface{}{"user": tc.target},
		})
		if evalAllow(t, q, input) {
			t.Errorf("ImpersonateUser user=%q target=%q must deny", tc.user, tc.target)
		}
	}
}

// --------------------------------------------------------------------------
// Catalog-management ops: only the admin principal (by username).
// --------------------------------------------------------------------------

func TestCatalogManagementAdminOnly(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	// AlterCatalog isn't emitted by Trino 476's OPA plugin (catalog
	// mutations happen via DROP+CREATE); kept as forward-compat coverage
	// so a future Trino version flipping it on doesn't silently allow
	// customers.
	for _, op := range []string{"CreateCatalog", "DropCatalog", "AlterCatalog"} {
		// Admin: allowed.
		if !evalAllow(t, q, buildInput(AdminPrincipal, op, catalogResource("org_new"))) {
			t.Errorf("%s by %s must allow", op, AdminPrincipal)
		}
		// Customer: denied even for their own catalog.
		if evalAllow(t, q, buildInput("42", op, catalogResource("org_42"))) {
			t.Errorf("%s by customer 42 on their own catalog must deny", op)
		}
		// Customer trying admin's name: denied. The carve-out is on the
		// username, so an exact match is required -- a lookalike like
		// `__admin_provisioner_not_me` must NOT pass.
		if evalAllow(t, q, buildInput("__admin_provisioner_not_me", op, catalogResource("org_42"))) {
			t.Errorf("%s by lookalike admin name must deny", op)
		}
	}
}

// --------------------------------------------------------------------------
// ExecuteQuery: allowed for anyone (resource groups handle isolation).
// --------------------------------------------------------------------------

func TestExecuteQueryAllowed(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	in := buildInput("42", "ExecuteQuery", nil)
	if !evalAllow(t, q, in) {
		t.Error("ExecuteQuery must be allowed (resource groups gate concurrency, not OPA)")
	}
}

// --------------------------------------------------------------------------
// Default deny: any operation we don't recognize must be denied.
//
// Note on coverage: the GRANT-related ops here are gated at the plugin
// level by `opa.allow-permission-management-operations` and are not
// actually queried through OPA in Trino 476; keeping them is forward-compat
// so a future Trino version routing them to OPA stays default-deny.
//
// KillQueryOwnedBy / ViewQueryOwnedBy are DIFFERENT: that config knob does
// not gate them (it gates only the grant/deny/revoke/set-authorization
// family, via enforcePermissionManagementOperation in
// OpaAccessControl.java), so the coordinator really does send them, and
// they are modelled by the query-visibility rules further down. They stay
// in this list because the input built here carries a *table* resource and
// no resource.user -- so this case is the fail-closed guard for a
// query-ownership decision whose owner field is missing. See
// TestQueryVisibilityFailsClosed for the full absent/blank/wrong-shape
// matrix, and TestQueryVisibilitySameOrg for the allow path.
// --------------------------------------------------------------------------

func TestDefaultDeny(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	denied := []string{
		"WriteSystemInformation",
		"ReadSystemInformation",
		"KillQueryOwnedBy",
		"ViewQueryOwnedBy",
		"InsertIntoTable",
		"DeleteFromTable",
		"UpdateTableColumns",
		"TruncateTable",
		"CreateTable",
		"DropTable",
		"RenameTable",
		"CreateSchema",
		"DropSchema",
		"CreateView",
		"DropView",
		"SetCatalogSessionProperty",
		"GrantSchemaPrivilege",
		"DenySchemaPrivilege",
		"RevokeSchemaPrivilege",
		"ExecuteProcedure",
		"ExecuteFunction",
		"ExecuteTableProcedure",
		"ShowFunctions",
		"FilterFunctions",
		"ShowCreateFunction",
		"NonExistentOp",
		"",
	}
	for _, op := range denied {
		t.Run(op, func(t *testing.T) {
			res := tableResource("org_42", "public", "events")
			if evalAllow(t, q, buildInput("42", op, res)) {
				t.Errorf("operation %q must default-deny", op)
			}
		})
	}
}

// --------------------------------------------------------------------------
// Adversarial: confirm that the group-vs-catalog axis is correctly enforced
// even when the input is intentionally malformed or has surprising shape.
// --------------------------------------------------------------------------

func TestAdversarialInputs(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	// Missing identity entirely -> policy can't iterate groups -> deny.
	t.Run("missing identity", func(t *testing.T) {
		in := map[string]interface{}{
			"action": map[string]interface{}{
				"operation": "AccessCatalog",
				"resource":  catalogResource("org_42"),
			},
		}
		if evalAllow(t, q, in) {
			t.Error("missing identity must deny")
		}
	})

	// Missing resource for operations that need one.
	t.Run("missing resource", func(t *testing.T) {
		in := buildInput("42", "AccessCatalog", nil)
		if evalAllow(t, q, in) {
			t.Error("AccessCatalog without resource must deny")
		}
	})

	// Resource with wrong shape (table where catalog expected).
	t.Run("wrong resource shape", func(t *testing.T) {
		in := buildInput("42", "AccessCatalog", tableResource("org_42", "public", "x"))
		if evalAllow(t, q, in) {
			t.Error("AccessCatalog with table resource must deny (resource.catalog.name missing)")
		}
	})

	// Empty groups list -> no group to iterate -> deny even for a known
	// catalog. This is the bedrock isolation invariant: identity without
	// group membership grants nothing.
	t.Run("empty groups deny", func(t *testing.T) {
		in := buildInputWithGroups("42", []string{}, "AccessCatalog", catalogResource("org_42"))
		if evalAllow(t, q, in) {
			t.Error("AccessCatalog with empty groups must deny")
		}
	})

	// User claims membership in a group that exists but owns a different
	// catalog -- must deny on the target catalog.
	t.Run("forged group membership for someone else's catalog", func(t *testing.T) {
		in := buildInputWithGroups("42", []string{"org_43"}, "AccessCatalog", catalogResource("org_42"))
		if evalAllow(t, q, in) {
			t.Error("group org_43 must NOT grant access to org_42")
		}
		// But the forged group SHOULD grant access to ITS OWN catalog.
		// This isn't a policy bug -- it just confirms that group
		// membership is what authorizes, so the file group provider /
		// JWT claim issuer is the actual trust anchor. The policy gives
		// what the identity says it has.
		in2 := buildInputWithGroups("42", []string{"org_43"}, "AccessCatalog", catalogResource("org_43"))
		if !evalAllow(t, q, in2) {
			t.Error("group org_43 should grant access to org_43 (this confirms the policy trusts identity.groups -- the group provider is the trust boundary)")
		}
	})

	// Admin username without admin-group membership: catalog management
	// is denied (is_admin requires BOTH the username AND the group). This
	// is the defense-in-depth conjunction described in policy.rego --
	// neither signal alone unlocks admin operations.
	t.Run("admin username without admin group cannot manage catalogs", func(t *testing.T) {
		adminNoGroups := buildInputWithGroups(AdminPrincipal, nil, "CreateCatalog", catalogResource("any"))
		if evalAllow(t, q, adminNoGroups) {
			t.Error("admin username alone (without admin-group membership) must NOT grant CreateCatalog -- is_admin requires both")
		}
		// Same identity also cannot AccessCatalog -- no group means no
		// owns_catalog rule body succeeds.
		adminAccess := buildInputWithGroups(AdminPrincipal, nil, "AccessCatalog", catalogResource("org_42"))
		if evalAllow(t, q, adminAccess) {
			t.Error("admin username without admin-group membership must not AccessCatalog")
		}
	})

	// Admin-group membership without the admin username: catalog
	// management is denied (is_admin requires username) AND read access
	// to AdminGroup-listed catalogs is also denied (owns_catalog's
	// admin-path requires is_admin). This guards against a customer who
	// somehow ends up with admin_group in their groups list -- they get
	// nothing.
	t.Run("non-admin user with admin group claim gets nothing", func(t *testing.T) {
		// User 42's identity, but claiming admin_group membership. The
		// admin-listed catalogs (org_42, org_43 in the
		// twoOrgFixture) must NOT be reachable through this path -- the
		// admin-group ownership rule is gated on is_admin (full
		// username + group conjunction).
		for _, cat := range []string{"org_42", "org_43"} {
			in := buildInputWithGroups("42", []string{AdminGroup}, "AccessCatalog", catalogResource(cat))
			if evalAllow(t, q, in) {
				t.Errorf("non-admin user claiming admin_group must NOT access %s through the admin path", cat)
			}
			in = buildInputWithGroups("42", []string{AdminGroup}, "SelectFromColumns", tableResource(cat, "s", "t"))
			if evalAllow(t, q, in) {
				t.Errorf("non-admin user claiming admin_group must NOT SelectFromColumns on %s", cat)
			}
		}
		// And catalog management is denied for the same reason.
		mgmt := buildInputWithGroups("42", []string{AdminGroup}, "CreateCatalog", catalogResource("anything"))
		if evalAllow(t, q, mgmt) {
			t.Error("non-admin user with admin_group claim must NOT get catalog management")
		}
	})

	// Verify the positive admin path still works with BOTH signals.
	t.Run("admin with both username and admin group can manage and read", func(t *testing.T) {
		// twoOrgFixture puts both catalogs under AdminGroup.
		// CreateCatalog now requires managed_catalog_name -- admin
		// cannot CREATE arbitrary names like "any", only org_*.
		in := buildInputWithGroups(AdminPrincipal, []string{AdminGroup}, "CreateCatalog", catalogResource("org_new"))
		if !evalAllow(t, q, in) {
			t.Error("admin with full identity must be able to CreateCatalog a managed-name catalog")
		}
		// Non-managed names must be denied even for admin -- defense
		// in depth against an admin credential compromise.
		for _, badName := range []string{"system", "jmx", "any", "iceberg_org_42"} {
			in := buildInputWithGroups(AdminPrincipal, []string{AdminGroup}, "CreateCatalog", catalogResource(badName))
			if evalAllow(t, q, in) {
				t.Errorf("admin CreateCatalog on non-managed name %q must be denied", badName)
			}
		}
		in = buildInputWithGroups(AdminPrincipal, []string{AdminGroup}, "AccessCatalog", catalogResource("org_42"))
		if !evalAllow(t, q, in) {
			t.Error("admin with full identity must be able to AccessCatalog admin-listed catalogs")
		}
	})

	// Admin's orphan-cleanup carve-out is SCOPED to FilterCatalogs --
	// admin can enumerate `org_*` catalogs that aren't in the
	// bundle (so reconcile can find orphans to drop) but CANNOT read
	// their data (no AccessCatalog/Select/etc. on orphans). The
	// previous broad-prefix rule has been replaced by this narrower
	// listable_catalog/readable_catalog split.
	t.Run("admin orphan visibility is FilterCatalogs-only", func(t *testing.T) {
		// Bundle has nothing under AdminGroup for org_99.
		emptyAdmin := GroupCatalogs{
			"org_42": {"org_42": true},
			// no AdminGroup entry, no org_99 entry
		}
		q2 := preparedPolicy(t, emptyAdmin)

		// FilterCatalogs on an orphan managed-name catalog: allowed.
		// This is the enumeration carve-out so reconcile can SHOW
		// CATALOGS and find org_99 to retry DROP CATALOG.
		in := buildInputWithGroups(AdminPrincipal, []string{AdminGroup}, "FilterCatalogs", catalogResource("org_99"))
		if !evalAllow(t, q2, in) {
			t.Error("admin FilterCatalogs on orphan org_99 must be allowed via listable_catalog carve-out")
		}

		// AccessCatalog, ShowSchemas, SelectFromColumns, etc. on the
		// same orphan: DENIED. The bundle is the audit trail for
		// what admin can READ; orphans aren't in the bundle, so they
		// can be enumerated but not read.
		for _, op := range []string{"AccessCatalog", "ShowSchemas"} {
			in := buildInputWithGroups(AdminPrincipal, []string{AdminGroup}, op, catalogResource("org_99"))
			if evalAllow(t, q2, in) {
				t.Errorf("admin %s on orphan org_99 must be DENIED (carve-out is FilterCatalogs-only)", op)
			}
		}
		// Same for schema/table scope.
		denyOp := func(op string, res map[string]interface{}) {
			in := buildInputWithGroups(AdminPrincipal, []string{AdminGroup}, op, res)
			if evalAllow(t, q2, in) {
				t.Errorf("admin %s on orphan must be denied; was allowed", op)
			}
		}
		denyOp("SelectFromColumns", tableResource("org_99", "s", "t"))
		denyOp("ShowTables", schemaResource("org_99", "s"))
		denyOp("FilterTables", tableResource("org_99", "s", "t"))

		// FilterCatalogs on a non-managed name must still be denied
		// — even for admin. The carve-out is bounded by the
		// managed_catalog_name regex.
		for _, name := range []string{"system", "jmx", "iceberg_org_42", "ORG_42"} {
			in := buildInputWithGroups(AdminPrincipal, []string{AdminGroup}, "FilterCatalogs", catalogResource(name))
			if evalAllow(t, q2, in) {
				t.Errorf("admin FilterCatalogs on non-managed-name %q must be denied", name)
			}
		}
		// Non-admin with the same group claim still gets nothing
		// from the carve-out (it's is_admin-gated).
		in = buildInputWithGroups("42", []string{AdminGroup}, "FilterCatalogs", catalogResource("org_99"))
		if evalAllow(t, q2, in) {
			t.Error("non-admin claiming admin_group must not benefit from the orphan carve-out")
		}
	})
}

// --------------------------------------------------------------------------
// Cross-user enumeration: every user in a small fixture should only see
// their own catalogs across all relevant ops. This is the load-bearing
// isolation matrix — every cell must hold across catalog access, table
// access, and metadata enumeration.
// --------------------------------------------------------------------------

func TestIsolationMatrix(t *testing.T) {
	// Three orgs, three catalogs.
	gc := GroupCatalogs{
		"org_42": {"org_42": true},
		"org_43": {"org_43": true},
		"org_44": {"org_44": true},
	}
	q := preparedPolicy(t, gc)

	users := []string{"42", "43", "44"}
	catalogs := []string{"org_42", "org_43", "org_44"}
	ops := []struct {
		op  string
		res func(catalog string) map[string]interface{}
	}{
		{"AccessCatalog", catalogResource},
		{"FilterCatalogs", catalogResource},
		{"ShowSchemas", catalogResource},
		{"FilterSchemas", func(c string) map[string]interface{} { return schemaResource(c, "s") }},
		{"ShowTables", func(c string) map[string]interface{} { return schemaResource(c, "s") }},
		{"SelectFromColumns", func(c string) map[string]interface{} { return tableResource(c, "s", "t") }},
		{"FilterTables", func(c string) map[string]interface{} { return tableResource(c, "s", "t") }},
		{"ShowColumns", func(c string) map[string]interface{} { return tableResource(c, "s", "t") }},
		{"FilterColumns", func(c string) map[string]interface{} { return tableResource(c, "s", "t") }},
	}

	for _, user := range users {
		expectedOwned := "org_" + user + ""
		for _, cat := range catalogs {
			for _, op := range ops {
				want := cat == expectedOwned
				got := evalAllow(t, q, buildInput(user, op.op, op.res(cat)))
				if got != want {
					t.Errorf("isolation matrix: op=%s user=%s catalog=%s want=%v got=%v",
						op.op, user, cat, want, got)
				}
			}
		}
	}
}

// --------------------------------------------------------------------------
// Batched OPA shape: the policy is the NON-BATCHED contract. Trino's
// batched access control (OpaBatchAccessControl, behind
// `opa.policy.batched-uri`) sends candidates under `action.filterResources`
// (plural list), not `action.resource` (singular object). Our filter rules
// read `action.resource.*`, so under the batched shape they all fail their
// body and fall through to default-deny. That's fail-closed -- safe by the
// threat model -- but it means enabling batched mode in the chart silently
// denies every filter query.
//
// This test locks in the fail-closed behaviour: if someone later extends
// the policy to support batched decisions, this test must be updated
// deliberately (and the new batched contract reviewed for cross-tenant
// safety). Default-deny is the floor.
// --------------------------------------------------------------------------

// preparedBatch compiles the policy for the batched entrypoint.
func preparedBatch(t *testing.T, gc GroupCatalogs) rego.PreparedEvalQuery {
	t.Helper()
	data, err := buildDataDocument(gc)
	if err != nil {
		t.Fatalf("buildDataDocument: %v", err)
	}
	q, err := rego.New(
		rego.Query("data.trino.batch"),
		rego.Module("policy.rego", string(policyRego)),
		rego.Data(data),
	).PrepareForEval(context.Background())
	if err != nil {
		t.Fatalf("PrepareForEval: %v", err)
	}
	return q
}

// evalBatch returns the set of allowed indices the batched entrypoint names.
func evalBatch(t *testing.T, q rego.PreparedEvalQuery, input map[string]interface{}) map[int]bool {
	t.Helper()
	rs, err := q.Eval(context.Background(), rego.EvalInput(input))
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	out := map[int]bool{}
	if len(rs) == 0 || len(rs[0].Expressions) == 0 {
		return out
	}
	items, ok := rs[0].Expressions[0].Value.([]interface{})
	if !ok {
		t.Fatalf("expected a list of indices, got %T (%v)", rs[0].Expressions[0].Value, rs[0].Expressions[0].Value)
	}
	for _, it := range items {
		n, ok := it.(json.Number)
		if !ok {
			t.Fatalf("expected numeric index, got %T (%v)", it, it)
		}
		i, err := n.Int64()
		if err != nil {
			t.Fatalf("index %v: %v", n, err)
		}
		out[int(i)] = true
	}
	return out
}

// The batched entrypoint must return exactly the candidates the non-batched
// entrypoint allows. Enabling opa.policy.batched-uri is what stops a
// coordinator issuing one OPA request per table — filtering a catalog with
// more than ~1024 tables otherwise overruns the client's queue — so this is
// the rule that carries tenant isolation for every information_schema
// listing, and it must not diverge from `allow`.
func TestBatchedFilteringMatchesNonBatched(t *testing.T) {
	allowQ := preparedPolicy(t, twoOrgFixture())
	batchQ := preparedBatch(t, twoOrgFixture())

	identity := map[string]interface{}{
		"user":   "42",
		"groups": []string{"org_42"},
	}
	ctx := map[string]interface{}{
		"identity":      identity,
		"softwareStack": map[string]interface{}{"trinoVersion": "476"},
	}

	cases := []struct {
		op        string
		resources []map[string]interface{}
		want      map[int]bool
	}{
		{
			op: "FilterCatalogs",
			resources: []map[string]interface{}{
				{"catalog": map[string]interface{}{"name": "org_42"}},
				{"catalog": map[string]interface{}{"name": "org_43"}},
			},
			want: map[int]bool{0: true},
		},
		{
			op: "FilterSchemas",
			resources: []map[string]interface{}{
				{"schema": map[string]interface{}{"catalogName": "org_43", "schemaName": "main"}},
				{"schema": map[string]interface{}{"catalogName": "org_42", "schemaName": "main"}},
			},
			want: map[int]bool{1: true},
		},
		{
			op: "FilterTables",
			resources: []map[string]interface{}{
				{"table": map[string]interface{}{"catalogName": "org_42", "schemaName": "main", "tableName": "events"}},
				{"table": map[string]interface{}{"catalogName": "org_43", "schemaName": "main", "tableName": "events"}},
				{"table": map[string]interface{}{"catalogName": "org_42", "schemaName": "main", "tableName": "persons"}},
			},
			want: map[int]bool{0: true, 2: true},
		},
	}

	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			got := evalBatch(t, batchQ, map[string]interface{}{
				"context": ctx,
				"action": map[string]interface{}{
					"operation":       tc.op,
					"filterResources": tc.resources,
				},
			})
			if len(got) != len(tc.want) {
				t.Fatalf("batch = %v, want %v", got, tc.want)
			}
			for i := range tc.want {
				if !got[i] {
					t.Errorf("index %d missing from batch result %v", i, got)
				}
			}

			// Same verdict, candidate by candidate, through `allow`.
			for i, res := range tc.resources {
				action := map[string]interface{}{"operation": tc.op, "resource": res}
				allowed := evalAllow(t, allowQ, map[string]interface{}{"context": ctx, "action": action})
				if allowed != tc.want[i] {
					t.Errorf("candidate %d: allow=%v but batch expects %v — the two shapes disagree", i, allowed, tc.want[i])
				}
			}
		})
	}
}

// FilterColumns is the shape-shifter: ONE table candidate carrying a columns
// array, with indices pointing into that array rather than filterResources.
func TestBatchedFilterColumns(t *testing.T) {
	batchQ := preparedBatch(t, twoOrgFixture())

	mk := func(catalog string) map[string]interface{} {
		return map[string]interface{}{
			"context": map[string]interface{}{
				"identity":      map[string]interface{}{"user": "42", "groups": []string{"org_42"}},
				"softwareStack": map[string]interface{}{"trinoVersion": "476"},
			},
			"action": map[string]interface{}{
				"operation": "FilterColumns",
				"filterResources": []map[string]interface{}{{
					"table": map[string]interface{}{
						"catalogName": catalog,
						"schemaName":  "main",
						"tableName":   "events",
						"columns":     []string{"uuid", "timestamp", "properties"},
					},
				}},
			},
		}
	}

	own := evalBatch(t, batchQ, mk("org_42"))
	if len(own) != 3 || !own[0] || !own[1] || !own[2] {
		t.Errorf("every column of the caller's own table must be visible, got %v", own)
	}

	other := evalBatch(t, batchQ, mk("org_43"))
	if len(other) != 0 {
		t.Errorf("no column of another org's table may be visible, got %v", other)
	}
}

// --------------------------------------------------------------------------
// Sanity check: the Rego file itself uses object-indexed lookups.
// This is a static guard against accidentally re-introducing a linear-scan
// rule. If someone replaces the indexed lookup with `some group in data.groups`
// the test fires; the bench would also fire but this gives a faster signal.
//
// The bounded `some g in input.context.identity.groups` iteration in
// owns_catalog is intentional: identity.groups is typically 1-2 elements
// and is O(1) in catalog count. We allow it; we ban data-side iteration.
// --------------------------------------------------------------------------

func TestPolicyDoesNotUseLinearScan(t *testing.T) {
	src := stripRegoComments(string(policyRego))

	// Heuristics for linear-scan idioms on the data document. Any of these
	// in the policy is a strong signal we lost the O(1) property.
	banned := []string{
		"some org in data",
		"some user in data",
		"some group in data",
		"some i, j in",
		"every ",                 // every ... iterates
		"walk(",                  // walk() iterates entire trees
		"data.group_catalogs[_]", // wildcard index = iteration
	}
	for _, needle := range banned {
		if strings.Contains(src, needle) {
			t.Errorf("policy.rego contains linear-scan idiom %q -- re-check the perf invariant", needle)
		}
	}

	// Positive check: the indexed lookup is present.
	if !strings.Contains(src, "data.group_catalogs[g][catalog]") {
		t.Error("policy.rego should contain the object-indexed lookup `data.group_catalogs[g][catalog]`")
	}
}

// TestPolicyRegoContainsManagedNamePattern guards the contract between
// the exported ManagedCatalogPattern constant and the regex literal
// embedded in policy.rego. If someone edits one without the other, the
// admin's catalog enumeration / management authority silently drifts
// out of sync with the Go-side TrinoCatalogName grammar.
//
// Paired with TestTrinoCatalogNameMatchesManagedNamePattern in the
// provisioner package, which closes the other side of the contract:
// every name Go produces must match this pattern.
func TestPolicyRegoContainsManagedNamePattern(t *testing.T) {
	if !strings.Contains(string(policyRego), ManagedCatalogPattern) {
		t.Fatalf("policy.rego does not contain the literal substring %q.\n"+
			"This means the policy's `managed_catalog_name` regex has drifted from\n"+
			"the exported ManagedCatalogPattern constant. Update either the constant\n"+
			"in opa/types.go or the regex literal in opa/policy.rego so they agree.",
			ManagedCatalogPattern)
	}
}

// --------------------------------------------------------------------------
// Query visibility: same-org only.
//
// A query's SQL text is tenant data -- it embeds table names, filter
// literals, customer identifiers and business logic. Trino exposes it to
// anyone allowed to view the query, via `SELECT * FROM
// system.runtime.queries` and the coordinator web UI, and lets them KILL
// it. These tests are the cross-tenant-leak guard for that surface.
// --------------------------------------------------------------------------

// TestQueryVisibilitySameOrg is the core matrix: every principal sees and
// kills exactly their own org's queries, and nothing else.
func TestQueryVisibilitySameOrg(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	cases := []struct {
		name      string
		requester string
		owner     string
		want      bool
	}{
		{"42 views own query", "42", "42", true},
		{"43 views own query", "43", "43", true},
		{"42 views 43's query -- cross-tenant SQL-text leak", "42", "43", false},
		{"43 views 42's query -- cross-tenant SQL-text leak", "43", "42", false},
		{"unknown org views 42's query", "99", "42", false},
		{"42 views an unknown org's query", "42", "99", false},
	}

	for _, op := range queryVisibilityOps {
		for _, tc := range cases {
			t.Run(op+"/"+tc.name, func(t *testing.T) {
				got := evalAllow(t, q, buildInput(tc.requester, op, ownerOf(tc.owner)))
				if got != tc.want {
					t.Fatalf("%s requester=%q owner=%q: want %v, got %v",
						op, tc.requester, tc.owner, tc.want, got)
				}
			})
		}
	}
}

// TestFilterViewQueryReturnsOnlySameOrg models what the coordinator
// actually does: filterViewQueryOwnedBy issues ONE decision per candidate
// owner (parallelFilterFromOpa), and the surviving set is what the tenant
// sees in system.runtime.queries / the web UI. Asserting the filtered set
// -- not just individual decisions -- is what pins the user-visible
// behaviour.
func TestFilterViewQueryReturnsOnlySameOrg(t *testing.T) {
	gc := GroupCatalogs{
		"org_42": {"org_42": true},
		"org_43": {"org_43": true},
		"org_44": {"org_44": true},
		AdminGroup: {
			"org_42": true,
			"org_43": true,
			"org_44": true,
		},
	}
	q := preparedPolicy(t, gc)

	// Candidate query owners on a shared cell: three tenants plus the
	// provisioner's own reconcile DDL.
	candidates := []string{"42", "43", "44", AdminPrincipal}

	for _, requester := range []string{"42", "43", "44", AdminPrincipal} {
		t.Run("requester="+requester, func(t *testing.T) {
			var visible []string
			for _, owner := range candidates {
				in := buildInput(requester, "FilterViewQueryOwnedBy", ownerOf(owner))
				if evalAllow(t, q, in) {
					visible = append(visible, owner)
				}
			}
			// Every principal (tenant or admin) sees exactly itself.
			want := []string{requester}
			if len(visible) != len(want) || visible[0] != want[0] {
				t.Fatalf("FilterViewQueryOwnedBy requester=%q: visible owners = %v, want %v",
					requester, visible, want)
			}
		})
	}
}

// TestQueryVisibilityAdminPrincipal pins the admin decision.
//
// DECISION: the admin principal gets NO cross-tenant query visibility.
// The reconcile loop's Trino client (trino_provisioner.go) runs only
// SHOW CATALOGS / CREATE CATALOG / DROP CATALOG -- it never selects from
// system.runtime.queries and never kills a query -- so there is nothing
// for the grant to enable, and granting it would hand every tenant's SQL
// text to a credential that has no use for it. Admin retains visibility
// of its OWN queries (which Trino short-circuits before OPA anyway).
//
// The load-bearing part: the bundle DOES place every managed catalog
// under data.group_catalogs[AdminGroup] so admin can smoke-test catalog
// reads. This test proves that entry cannot be levered into query
// visibility.
func TestQueryVisibilityAdminPrincipal(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	for _, op := range queryVisibilityOps {
		t.Run(op+"/admin sees own query", func(t *testing.T) {
			in := buildInput(AdminPrincipal, op, ownerOf(AdminPrincipal))
			if !evalAllow(t, q, in) {
				t.Errorf("%s: admin must retain visibility of its own queries", op)
			}
		})
		for _, owner := range []string{"42", "43"} {
			t.Run(op+"/admin vs tenant "+owner, func(t *testing.T) {
				in := buildInput(AdminPrincipal, op, ownerOf(owner))
				if evalAllow(t, q, in) {
					t.Errorf("%s: admin must NOT see or kill tenant %s's queries -- "+
						"the reconcile loop never reads system.runtime.queries, so this "+
						"would be tenant SQL text handed to a credential with no use for it",
						op, owner)
				}
			})
		}
		t.Run(op+"/tenant vs admin", func(t *testing.T) {
			in := buildInput("42", op, ownerOf(AdminPrincipal))
			if evalAllow(t, q, in) {
				t.Errorf("%s: tenant must NOT see the provisioner's queries", op)
			}
		})
	}
}

// TestQueryVisibilityFailsClosed covers every way the owner field can be
// missing, blank, or the wrong shape. All of them must deny: a rule keyed
// on a field the plugin never sends would look like a working policy while
// silently denying (or, combined with another allow, silently permitting).
func TestQueryVisibilityFailsClosed(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	cases := []struct {
		name     string
		resource map[string]interface{}
	}{
		{"no resource at all", nil},
		{"resource without user", tableResource("org_42", "public", "events")},
		{"user object is empty", map[string]interface{}{"user": map[string]interface{}{}}},
		{"owner name absent, groups present", map[string]interface{}{
			"user": map[string]interface{}{"groups": []string{"org_99"}},
		}},
		// The ImpersonateUser shape: String-form TrinoUser, no groups key.
		// A cross-org owner here must still deny.
		{"string-form TrinoUser for another org", queryOwnerResource("43", nil)},
		{"owner name empty string", queryOwnerResource("", nil)},
		{"owner name empty string with empty groups", queryOwnerResource("", []string{})},
		{"owner has no groups", queryOwnerResource("43", []string{})},
		{"owner groups is not a list", map[string]interface{}{
			"user": map[string]interface{}{"user": "43", "groups": "org_43"},
		}},
		{"owner in a group the bundle never projected", queryOwnerResource("99", []string{"org_99"})},
	}

	for _, op := range queryVisibilityOps {
		for _, tc := range cases {
			t.Run(op+"/"+tc.name, func(t *testing.T) {
				if evalAllow(t, q, buildInput("42", op, tc.resource)) {
					t.Fatalf("%s with %s must fail closed", op, tc.name)
				}
			})
		}
	}
}

// TestQueryVisibilityIgnoresForgedGroups: the same-org match is bound to
// the bundle's group roster, so a group label the provisioner never
// projected grants nothing even when BOTH sides carry it. Also pins that
// a shared admin-group claim can never be the matching group.
func TestQueryVisibilityIgnoresForgedGroups(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	for _, op := range queryVisibilityOps {
		t.Run(op+"/shared unknown group", func(t *testing.T) {
			in := buildInputWithGroups("42", []string{"org_not_in_bundle"}, op,
				queryOwnerResource("43", []string{"org_not_in_bundle"}))
			if evalAllow(t, q, in) {
				t.Errorf("%s: a group absent from data.group_catalogs must grant nothing", op)
			}
		})
		t.Run(op+"/shared admin group", func(t *testing.T) {
			in := buildInputWithGroups("42", []string{AdminGroup}, op,
				queryOwnerResource("43", []string{AdminGroup}))
			if evalAllow(t, q, in) {
				t.Errorf("%s: a bare admin-group claim must never be the matching group", op)
			}
		})
		t.Run(op+"/tenant claiming an org group it does not own", func(t *testing.T) {
			// Requester 42 forges membership of org_43 (a real bundle
			// group). This is the group-provider-compromise case: the
			// policy CANNOT stop it, and that is by design -- group
			// membership is the identity assertion. Documented here so
			// the boundary is explicit rather than assumed.
			in := buildInputWithGroups("42", []string{"org_43"}, op, ownerOf("43"))
			if !evalAllow(t, q, in) {
				t.Errorf("%s: sanity -- a forged org_43 claim DOES match; "+
					"group membership is the identity assertion and the file group "+
					"provider is the thing that must be trustworthy", op)
			}
		})
	}
}

// TestQueryVisibilityAllOrgsFixture runs the same-org matrix against a
// group with no catalogs, confirming the bundle-membership check treats
// "known group, owns nothing" as a real org rather than an unknown label.
// A tenant mid-provision (group projected, catalog not yet created) must
// still be able to see its own queries.
func TestQueryVisibilityGroupWithNoCatalogs(t *testing.T) {
	gc := twoOrgFixture()
	gc["org_pending"] = map[string]bool{}
	q := preparedPolicy(t, gc)

	for _, op := range queryVisibilityOps {
		if !evalAllow(t, q, buildInput("pending", op, ownerOf("pending"))) {
			t.Errorf("%s: an org whose group is projected but owns no catalog yet "+
				"must still see its own queries", op)
		}
		if evalAllow(t, q, buildInput("pending", op, ownerOf("42"))) {
			t.Errorf("%s: org_pending must not see org 42's queries", op)
		}
	}
}

// TestQueryVisibilityBatchedInputDeniesByDefault extends the batched-shape
// regression guard to the query-ownership ops. Under
// `opa.policy.batched-uri`, FilterViewQueryOwnedBy candidates arrive under
// action.filterResources (plural) and action.resource is absent, so the
// rules fail their body and fall through to default-deny. Fail-closed is
// the floor; enabling batched mode still requires extending the policy.
func TestQueryVisibilityBatchedInputDeniesByDefault(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	batched := map[string]interface{}{
		"context": map[string]interface{}{
			"identity": map[string]interface{}{
				"user":   "42",
				"groups": []string{"org_42"},
			},
			"softwareStack": map[string]interface{}{"trinoVersion": "476"},
		},
		"action": map[string]interface{}{
			"operation": "FilterViewQueryOwnedBy",
			"filterResources": []map[string]interface{}{
				{"user": map[string]interface{}{"user": "42", "groups": []string{"org_42"}}},
				{"user": map[string]interface{}{"user": "43", "groups": []string{"org_43"}}},
			},
		},
	}
	if evalAllow(t, q, batched) {
		t.Error("FilterViewQueryOwnedBy under the batched input shape must NOT allow -- " +
			"the policy is the non-batched contract")
	}
}

// TestQueryVisibilityOperationNames pins the plugin's operation strings
// into the policy source. The filter op is `FilterViewQueryOwnedBy`, NOT
// `FilterViewQuery`: a rule keyed on a name the plugin never sends is
// dead code that denies everything while reading like a working policy.
// The names come from OpaAccessControl.java and are asserted verbatim by
// the plugin's own tests (see queryOwnerResource's doc comment).
func TestQueryVisibilityOperationNames(t *testing.T) {
	src := string(policyRego)
	for _, op := range queryVisibilityOps {
		if !strings.Contains(src, `"`+op+`"`) {
			t.Errorf("policy.rego does not mention operation %q -- "+
				"the query-visibility rules must key on the plugin's exact "+
				"operation strings from OpaAccessControl.java", op)
		}
	}
	if strings.Contains(stripRegoComments(src), `"FilterViewQuery"`) {
		t.Error(`policy.rego keys on "FilterViewQuery"; the Trino OPA plugin sends ` +
			`"FilterViewQueryOwnedBy". A rule on the wrong name never matches.`)
	}
}

// stripRegoComments removes `#` line comments from a Rego source so the
// linear-scan static check doesn't false-positive on words like "every"
// that appear in commentary.
func stripRegoComments(src string) string {
	var out strings.Builder
	for _, line := range strings.Split(src, "\n") {
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		out.WriteString(line)
		out.WriteByte('\n')
	}
	return out.String()
}
