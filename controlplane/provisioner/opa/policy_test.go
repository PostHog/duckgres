package opa

import (
	"context"
	"strings"
	"testing"

	"github.com/open-policy-agent/opa/rego"
)

// preparedPolicy compiles the embedded Rego policy and returns a
// PreparedEvalQuery against `data.trino.allow`. Reused across the
// table-driven tests below so we pay the compile cost once per test
// binary, not once per case.
func preparedPolicy(t *testing.T, uc UserCatalogs) rego.PreparedEvalQuery {
	t.Helper()
	ctx := context.Background()
	data, err := buildDataDocument(uc)
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

// twoOrgFixture is the canonical UserCatalogs used by most tests:
// user "42" owns org_42_iceberg; user "43" owns org_43_iceberg; the
// provisioner admin owns both (so it can manage them).
func twoOrgFixture() UserCatalogs {
	return UserCatalogs{
		"42": {"org_42_iceberg": true},
		"43": {"org_43_iceberg": true},
		AdminPrincipal: {
			"org_42_iceberg": true,
			"org_43_iceberg": true,
		},
	}
}

// buildInput is a small helper for assembling the input.action.resource
// shape the Trino OPA plugin sends. Keeps the test cases readable.
func buildInput(user, operation string, resource map[string]interface{}) map[string]interface{} {
	in := map[string]interface{}{
		"context": map[string]interface{}{
			"identity": map[string]interface{}{
				"user":   user,
				"groups": []string{"org_" + user},
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

// --------------------------------------------------------------------------
// AccessCatalog and FilterCatalogs: the load-bearing catalog-scope decisions.
// --------------------------------------------------------------------------

func TestAccessCatalog(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	cases := []struct {
		name string
		user string
		cat  string
		want bool
	}{
		{"owner accesses own catalog", "42", "org_42_iceberg", true},
		{"owner accesses other org catalog", "42", "org_43_iceberg", false},
		{"other user accesses 42's catalog", "43", "org_42_iceberg", false},
		{"unknown user accesses any catalog", "99", "org_42_iceberg", false},
		{"empty user", "", "org_42_iceberg", false},
		{"empty catalog", "42", "", false},
		{"catalog name that almost matches", "42", "org_42_iceberg2", false},
		// Adversarial: a user "42" that maps to no catalogs at all.
		{"user with no catalogs", "no_catalogs", "org_42_iceberg", false},
	}
	// Add the no-catalogs user to the fixture for the last case.
	uc := twoOrgFixture()
	uc["no_catalogs"] = map[string]bool{}
	q = preparedPolicy(t, uc)

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
		if !evalAllow(t, q, buildInput("42", op, catalogResource("org_42_iceberg"))) {
			t.Errorf("%s: user 42 should see own catalog", op)
		}
		if evalAllow(t, q, buildInput("42", op, catalogResource("org_43_iceberg"))) {
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
			if !evalAllow(t, q, buildInput("42", op, schemaResource("org_42_iceberg", "public"))) {
				t.Errorf("%s on own catalog must allow", op)
			}
		})
		t.Run(op+"/other", func(t *testing.T) {
			if evalAllow(t, q, buildInput("42", op, schemaResource("org_43_iceberg", "public"))) {
				t.Errorf("%s on another org's catalog must deny", op)
			}
		})
	}

	t.Run("ShowSchemas allowed on owned catalog", func(t *testing.T) {
		if !evalAllow(t, q, buildInput("42", "ShowSchemas", catalogResource("org_42_iceberg"))) {
			t.Error("ShowSchemas on own catalog must allow")
		}
	})
	t.Run("ShowSchemas denied on other catalog", func(t *testing.T) {
		if evalAllow(t, q, buildInput("42", "ShowSchemas", catalogResource("org_43_iceberg"))) {
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
		if !evalAllow(t, q, buildInput("42", op, tableResource("org_42_iceberg", "public", "events"))) {
			t.Errorf("%s on own catalog must allow", op)
		}
		if evalAllow(t, q, buildInput("42", op, tableResource("org_43_iceberg", "public", "events"))) {
			t.Errorf("%s on another org's catalog must deny -- cross-tenant leak!", op)
		}
		if evalAllow(t, q, buildInput("99", op, tableResource("org_42_iceberg", "public", "events"))) {
			t.Errorf("%s by unknown user must deny", op)
		}
	}
}

// --------------------------------------------------------------------------
// Session-property allowlist.
// --------------------------------------------------------------------------

func TestSetSystemSessionProperty(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	allowed := []string{"execution_policy", "query_priority", "join_distribution_type"}
	for _, prop := range allowed {
		if !evalAllow(t, q, buildInput("42", "SetSystemSessionProperty", sessionPropertyResource(prop))) {
			t.Errorf("session property %q should be allowed", prop)
		}
	}

	// Memory and concurrency knobs MUST be denied -- these are the
	// cross-tenant attack vectors per the plan.
	denied := []string{
		"query_max_memory",
		"query_max_memory_per_node",
		"query_max_total_memory",
		"query_max_cpu_time",
		"query_max_execution_time",
		"max_concurrent_queries",
		"resource_overcommit",
		"query_priority2", // close-but-not-equal: must not pass
		"",                // empty
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
// Catalog-management ops: only the admin principal.
// --------------------------------------------------------------------------

func TestCatalogManagementAdminOnly(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	for _, op := range []string{"CreateCatalog", "DropCatalog", "AlterCatalog"} {
		// Admin: allowed.
		if !evalAllow(t, q, buildInput(AdminPrincipal, op, catalogResource("org_new_iceberg"))) {
			t.Errorf("%s by %s must allow", op, AdminPrincipal)
		}
		// Customer: denied even for their own catalog.
		if evalAllow(t, q, buildInput("42", op, catalogResource("org_42_iceberg"))) {
			t.Errorf("%s by customer 42 on their own catalog must deny", op)
		}
		// Customer trying admin's name: denied.
		if evalAllow(t, q, buildInput("__admin_provisioner_not_me", op, catalogResource("org_42_iceberg"))) {
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
// --------------------------------------------------------------------------

func TestDefaultDeny(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	// Sample of operations the policy doesn't explicitly allow. If
	// Trino's OPA plugin ever sends one of these and a future policy
	// edit decides to allow it, the test must be updated deliberately
	// (with a security review note).
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
		"NonExistentOp",
		"",
	}
	for _, op := range denied {
		t.Run(op, func(t *testing.T) {
			res := tableResource("org_42_iceberg", "public", "events")
			if evalAllow(t, q, buildInput("42", op, res)) {
				t.Errorf("operation %q must default-deny", op)
			}
		})
	}
}

// --------------------------------------------------------------------------
// Adversarial: confirm that the user-vs-catalog axis is correctly enforced
// even when the input is intentionally malformed or has surprising shape.
// --------------------------------------------------------------------------

func TestAdversarialInputs(t *testing.T) {
	q := preparedPolicy(t, twoOrgFixture())

	// Missing identity entirely -> policy can't bind `user` -> deny.
	t.Run("missing identity", func(t *testing.T) {
		in := map[string]interface{}{
			"action": map[string]interface{}{
				"operation": "AccessCatalog",
				"resource":  catalogResource("org_42_iceberg"),
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
		in := buildInput("42", "AccessCatalog", tableResource("org_42_iceberg", "public", "x"))
		if evalAllow(t, q, in) {
			t.Error("AccessCatalog with table resource must deny (resource.catalog.name missing)")
		}
	})

	// Identity user set to admin name but admin not in user_catalogs.
	t.Run("admin not in user_catalogs", func(t *testing.T) {
		uc := UserCatalogs{
			"42": {"org_42_iceberg": true},
			// no AdminPrincipal entry
		}
		q2 := preparedPolicy(t, uc)
		// Admin can still create/drop catalogs (the carve-out is by
		// principal name, not catalog ownership).
		if !evalAllow(t, q2, buildInput(AdminPrincipal, "CreateCatalog", catalogResource("any"))) {
			t.Error("admin should be able to CreateCatalog regardless of user_catalogs")
		}
		// But admin still can't AccessCatalog without owning it.
		if evalAllow(t, q2, buildInput(AdminPrincipal, "AccessCatalog", catalogResource("org_42_iceberg"))) {
			t.Error("admin without ownership entry must not AccessCatalog")
		}
	})
}

// --------------------------------------------------------------------------
// Cross-user enumeration: every user in a small fixture should only see
// their own catalogs across all relevant ops. This is the matrix the
// "Stream B self-smoke" calls out in the plan.
// --------------------------------------------------------------------------

func TestIsolationMatrix(t *testing.T) {
	// Three orgs, three catalogs.
	uc := UserCatalogs{
		"42": {"org_42_iceberg": true},
		"43": {"org_43_iceberg": true},
		"44": {"org_44_iceberg": true},
	}
	q := preparedPolicy(t, uc)

	users := []string{"42", "43", "44"}
	catalogs := []string{"org_42_iceberg", "org_43_iceberg", "org_44_iceberg"}
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
		expectedOwned := "org_" + user + "_iceberg"
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
// Sanity check: the Rego file itself uses object-indexed lookups.
// This is a static guard against accidentally re-introducing a linear-scan
// rule. If someone replaces the indexed lookup with `some org in data.orgs`
// the test fires; the bench would also fire but this gives a faster signal.
// --------------------------------------------------------------------------

func TestPolicyDoesNotUseLinearScan(t *testing.T) {
	src := stripRegoComments(string(policyRego))

	// Heuristics for linear-scan idioms; tighten over time. Any of these
	// in the policy is a strong signal we lost the O(1) property.
	banned := []string{
		"some org in",
		"some user in",
		"some i, j in",
		"every ",                // every ... iterates
		"walk(",                 // walk() iterates entire trees
		"data.user_catalogs[_]", // wildcard index = iteration
	}
	for _, needle := range banned {
		if strings.Contains(src, needle) {
			t.Errorf("policy.rego contains linear-scan idiom %q -- re-check the perf invariant", needle)
		}
	}

	// Positive check: the indexed lookup is present.
	if !strings.Contains(src, "data.user_catalogs[user][catalog]") {
		t.Error("policy.rego should contain the object-indexed lookup `data.user_catalogs[user][catalog]`")
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
