package querymeta

import (
	"fmt"
	"slices"
	"strings"
	"testing"
)

func rawRelations(rels []Relation) []string {
	out := make([]string, 0, len(rels))
	for _, rel := range rels {
		out = append(out, rel.Raw)
	}
	slices.Sort(out)
	return out
}

func assertRelations(t *testing.T, label string, got []Relation, want ...string) {
	t.Helper()
	gotRaw := rawRelations(got)
	slices.Sort(want)
	if len(want) == 0 && len(gotRaw) == 0 {
		return
	}
	if !slices.Equal(gotRaw, want) {
		t.Fatalf("%s relations = %v, want %v", label, gotRaw, want)
	}
}

func assertKinds(t *testing.T, got []AccessKind, want ...AccessKind) {
	t.Helper()
	gotStr := make([]string, 0, len(got))
	for _, k := range got {
		gotStr = append(gotStr, string(k))
	}
	wantStr := make([]string, 0, len(want))
	for _, k := range want {
		wantStr = append(wantStr, string(k))
	}
	slices.Sort(gotStr)
	slices.Sort(wantStr)
	if !slices.Equal(gotStr, wantStr) {
		t.Fatalf("access kinds = %v, want %v", gotStr, wantStr)
	}
}

func TestExtractReadAndWriteSplit(t *testing.T) {
	cases := []struct {
		name   string
		sql    string
		kind   string
		kinds  []AccessKind
		reads  []string
		writes []string
	}{
		{
			name:  "select",
			sql:   "SELECT a FROM analytics.events WHERE b > 1",
			kind:  KindSelect,
			kinds: []AccessKind{AccessRead},
			reads: []string{"analytics.events"},
		},
		{
			name:   "insert from select reads its source and writes its target",
			sql:    "INSERT INTO main.daily SELECT * FROM main.events",
			kind:   KindInsert,
			kinds:  []AccessKind{AccessRead, AccessWrite},
			reads:  []string{"main.events"},
			writes: []string{"main.daily"},
		},
		{
			name:   "update",
			sql:    "UPDATE main.t SET a = 1 FROM main.src WHERE t.id = src.id",
			kind:   KindUpdate,
			kinds:  []AccessKind{AccessRead, AccessWrite},
			reads:  []string{"main.src"},
			writes: []string{"main.t"},
		},
		{
			name:   "delete",
			sql:    "DELETE FROM main.t WHERE id IN (SELECT id FROM main.stale)",
			kind:   KindDelete,
			kinds:  []AccessKind{AccessRead, AccessWrite},
			reads:  []string{"main.stale"},
			writes: []string{"main.t"},
		},
		{
			name:   "create table as",
			sql:    "CREATE TABLE main.snapshot AS SELECT * FROM main.events",
			kind:   KindCreate,
			kinds:  []AccessKind{AccessDDL, AccessRead},
			reads:  []string{"main.events"},
			writes: []string{"main.snapshot"},
		},
		{
			name:   "truncate writes and is ddl",
			sql:    "TRUNCATE main.t",
			kind:   KindOther,
			kinds:  []AccessKind{AccessDDL, AccessWrite},
			writes: []string{"main.t"},
		},
		{
			name:  "three part name keeps catalog",
			sql:   "SELECT 1 FROM ducklake.main.events",
			kind:  KindSelect,
			kinds: []AccessKind{AccessRead},
			reads: []string{"ducklake.main.events"},
		},
		{
			name:  "join collects both sides",
			sql:   "SELECT * FROM main.a JOIN main.b ON a.id = b.id",
			kind:  KindSelect,
			kinds: []AccessKind{AccessRead},
			reads: []string{"main.a", "main.b"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := Extract(tc.sql)
			if !meta.Complete {
				t.Fatalf("extraction incomplete (%s) for %q", meta.IncompleteReason, tc.sql)
			}
			if meta.QueryKind != tc.kind {
				t.Fatalf("query kind = %q, want %q", meta.QueryKind, tc.kind)
			}
			assertKinds(t, meta.AccessKinds, tc.kinds...)
			assertRelations(t, "read", meta.ReadRelations, tc.reads...)
			assertRelations(t, "write", meta.WriteRelations, tc.writes...)
		})
	}
}

// TestExtractWritableCTE is the case a command-tag classifier gets wrong: the
// statement looks like a SELECT but mutates a table. For authorization that is
// the difference between allowed and denied.
func TestExtractWritableCTE(t *testing.T) {
	meta := Extract(`WITH moved AS (
		DELETE FROM main.staging RETURNING *
	) INSERT INTO main.final SELECT * FROM moved`)

	if !meta.Complete {
		t.Fatalf("extraction incomplete: %s", meta.IncompleteReason)
	}
	if !meta.HasKind(AccessWrite) {
		t.Fatalf("a writable CTE must report write access, got %v", meta.AccessKinds)
	}
	assertRelations(t, "write", meta.WriteRelations, "main.staging", "main.final")
	// "moved" is a CTE name, not a relation — reporting it would put a phantom
	// table in the audit trail.
	for _, rel := range meta.ReadRelations {
		if strings.EqualFold(rel.Name, "moved") {
			t.Fatalf("CTE name leaked into read relations: %v", rawRelations(meta.ReadRelations))
		}
	}
}

func TestExtractCTEShadowsRelation(t *testing.T) {
	meta := Extract(`WITH events AS (SELECT 1 AS a) SELECT * FROM events`)
	if !meta.Complete {
		t.Fatalf("extraction incomplete: %s", meta.IncompleteReason)
	}
	if len(meta.ReadRelations) != 0 {
		t.Fatalf("a CTE that shadows a table name is not a relation, got %v", rawRelations(meta.ReadRelations))
	}
}

func TestExtractRecursiveCTEIsNotARelation(t *testing.T) {
	meta := Extract(`WITH RECURSIVE walk AS (
		SELECT 1 AS n UNION ALL SELECT n + 1 FROM walk WHERE n < 5
	) SELECT * FROM walk`)
	if len(meta.ReadRelations) != 0 {
		t.Fatalf("recursive CTE self-reference is not a relation, got %v", rawRelations(meta.ReadRelations))
	}
}

// TestExtractCatalogIntrospectionIsMetadata: driver introspection is not a read
// of tenant data, and policies treat the two differently.
func TestExtractCatalogIntrospectionIsMetadata(t *testing.T) {
	for _, sql := range []string{
		"SELECT * FROM pg_catalog.pg_class",
		"SELECT * FROM information_schema.tables",
	} {
		meta := Extract(sql)
		if !meta.HasKind(AccessMetadata) {
			t.Fatalf("%q should be metadata access, got %v", sql, meta.AccessKinds)
		}
	}
}

// TestExtractTableFunctionsAreAccessTargets is the bypass that matters:
// read_parquet reaches data without naming a relation, so a policy built on
// relations alone would let it through.
func TestExtractTableFunctionsAreAccessTargets(t *testing.T) {
	meta := Extract(`SELECT * FROM read_parquet('s3://other-tenant/secrets.parquet')`)

	if len(meta.TableFunctions) != 1 {
		t.Fatalf("expected the table function to be recorded, got %v", meta.TableFunctions)
	}
	fn := meta.TableFunctions[0]
	if fn.Name != "read_parquet" {
		t.Fatalf("table function name = %q", fn.Name)
	}
	if len(fn.Args) != 1 || !strings.Contains(fn.Args[0], "other-tenant") {
		t.Fatalf("table function args = %v, want the target URI", fn.Args)
	}
	if !meta.HasKind(AccessAdmin) {
		t.Fatalf("reaching outside the warehouse is admin-class access, got %v", meta.AccessKinds)
	}
}

// TestSanitizeTableFunctionArgs: a presigned URL carries its credential in the
// query string, so the recorded argument must not.
func TestSanitizeTableFunctionArgs(t *testing.T) {
	meta := Extract(`SELECT * FROM read_csv('https://host/bucket/key.csv?X-Amz-Signature=deadbeef&X-Amz-Credential=AKIA')`)
	if len(meta.TableFunctions) != 1 {
		t.Fatalf("expected one table function, got %v", meta.TableFunctions)
	}
	arg := meta.TableFunctions[0].Args[0]
	for _, leak := range []string{"deadbeef", "AKIA", "X-Amz-Signature"} {
		if strings.Contains(arg, leak) {
			t.Fatalf("sanitized arg leaked %q: %s", leak, arg)
		}
	}
	if !strings.Contains(arg, "host/bucket/key.csv") {
		t.Fatalf("sanitized arg lost the target: %s", arg)
	}

	meta = Extract(`SELECT * FROM read_csv('https://user:pass@host/f.csv')`)
	if arg := meta.TableFunctions[0].Args[0]; strings.Contains(arg, "pass") {
		t.Fatalf("sanitized arg leaked userinfo: %s", arg)
	}
}

// TestCopyToExternalIsAdmin: COPY TO a URI moves tenant data out of the
// warehouse.
func TestCopyToExternalIsAdmin(t *testing.T) {
	meta := Extract(`COPY main.events TO 's3://bucket/dump.parquet'`)
	if !meta.HasKind(AccessAdmin) {
		t.Fatalf("COPY TO an external URI is admin-class, got %v", meta.AccessKinds)
	}
	if !meta.HasKind(AccessRead) {
		t.Fatalf("COPY TO also reads, got %v", meta.AccessKinds)
	}
	assertRelations(t, "read", meta.ReadRelations, "main.events")
}

func TestCopyFromWrites(t *testing.T) {
	meta := Extract(`COPY main.events FROM STDIN`)
	if !meta.HasKind(AccessWrite) {
		t.Fatalf("COPY FROM writes, got %v", meta.AccessKinds)
	}
	assertRelations(t, "write", meta.WriteRelations, "main.events")
}

func TestExtractColumns(t *testing.T) {
	meta := Extract("SELECT e.id, e.name, other FROM main.events e")
	names := map[string]string{}
	for _, col := range meta.Columns {
		names[col.Name] = col.Relation
	}
	for _, want := range []string{"id", "name", "other"} {
		if _, ok := names[want]; !ok {
			t.Fatalf("column %q not extracted, got %v", want, meta.Columns)
		}
	}
	if names["id"] != "e" {
		t.Fatalf("qualified column should keep its qualifier, got %q", names["id"])
	}
}

// TestColumnsResolvedFlag pins the honest-uncertainty rule: with two relations
// in scope an unqualified column cannot be attributed without a catalog, and a
// column-level policy must see that rather than a confident wrong answer.
func TestColumnsResolvedFlag(t *testing.T) {
	single := Extract("SELECT id FROM main.events")
	if !single.ColumnsResolved {
		t.Fatal("one relation in scope makes an unqualified column unambiguous")
	}

	ambiguous := Extract("SELECT id FROM main.a JOIN main.b ON a.x = b.x")
	if ambiguous.ColumnsResolved {
		t.Fatal("an unqualified column with two relations in scope is not resolvable")
	}

	qualified := Extract("SELECT a.id FROM main.a JOIN main.b ON a.x = b.x")
	if !qualified.ColumnsResolved {
		t.Fatal("fully qualified columns are resolvable regardless of scope size")
	}
}

func TestSelectStar(t *testing.T) {
	meta := Extract("SELECT * FROM main.events")
	if !meta.SelectStar {
		t.Fatal("SELECT * must be flagged: it means every column of the relation")
	}
	qualified := Extract("SELECT e.* FROM main.events e")
	if !qualified.SelectStar {
		t.Fatal("t.* must be flagged too")
	}
}

// TestUnparseableStatementIsIncompleteNotEmpty is the single most important
// property in this package. An authorization decision that reads "no relations
// referenced" from a statement we failed to parse would be a hole; incomplete
// must be distinguishable from empty.
func TestUnparseableStatementIsIncompleteNotEmpty(t *testing.T) {
	meta := Extract("PIVOT main.events ON kind USING sum(v)")
	if meta.Complete {
		t.Fatal("DuckDB-native syntax cannot be fully extracted by a PostgreSQL parser")
	}
	if meta.IncompleteReason == "" {
		t.Fatal("an incomplete extraction must say why")
	}
	if len(meta.AccessKinds) == 0 {
		t.Fatal("an incomplete extraction still needs an access kind, even if unknown")
	}
}

// TestUnparseableAdminStatementsStillClassify: the statements a PostgreSQL
// parser rejects include the ones that matter most for authorization, so the
// lexical fallback must not shrug at them.
func TestUnparseableAdminStatementsStillClassify(t *testing.T) {
	for _, sql := range []string{
		"CREATE PERSISTENT SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')",
		"DROP SECRET s",
		"ATTACH 'other.db' AS other",
		"INSTALL httpfs",
		"LOAD httpfs",
	} {
		meta := Extract(sql)
		if !meta.HasKind(AccessAdmin) {
			t.Fatalf("%q must classify as admin access, got %v", sql, meta.AccessKinds)
		}
		if meta.Complete {
			t.Fatalf("%q is not fully parseable and must be marked incomplete", sql)
		}
	}
}

func TestUnparseableCopyClaimsUnionOfAccess(t *testing.T) {
	// Without a parse we cannot tell read from write from egress, so the
	// fallback claims all three rather than guessing low.
	meta := Extract("COPY (SELECT * FROM x) TO 's3://b/k' (FORMAT parquet, PARTITION_BY (d))")
	if meta.Complete {
		return // parsed fine; the union rule does not apply
	}
	for _, kind := range []AccessKind{AccessRead, AccessWrite, AccessAdmin} {
		if !meta.HasKind(kind) {
			t.Fatalf("unparseable COPY must claim %s, got %v", kind, meta.AccessKinds)
		}
	}
}

func TestSetAndTransactionKinds(t *testing.T) {
	if meta := Extract("SET search_path = main"); !meta.HasKind(AccessConfig) {
		t.Fatalf("SET is config access, got %v", meta.AccessKinds)
	}
	if meta := Extract("BEGIN"); !meta.HasKind(AccessTransaction) {
		t.Fatalf("BEGIN is transaction access, got %v", meta.AccessKinds)
	}
	if meta := Extract("SHOW search_path"); !meta.HasKind(AccessMetadata) {
		t.Fatalf("SHOW is metadata access, got %v", meta.AccessKinds)
	}
}

// TestExplainClassifiesByInnerStatement: EXPLAIN of a write must not read as a
// plain read.
func TestExplainClassifiesByInnerStatement(t *testing.T) {
	meta := Extract("EXPLAIN INSERT INTO main.t SELECT * FROM main.s")
	if meta.QueryKind != KindExplain {
		t.Fatalf("query kind = %q, want %q", meta.QueryKind, KindExplain)
	}
	if !meta.HasKind(AccessWrite) {
		t.Fatalf("EXPLAIN of a write should still surface write access, got %v", meta.AccessKinds)
	}
	assertRelations(t, "write", meta.WriteRelations, "main.t")
}

func TestMultiStatementCounts(t *testing.T) {
	meta := Extract("SELECT 1; SELECT 2")
	if meta.StatementCount != 2 {
		t.Fatalf("statement count = %d, want 2", meta.StatementCount)
	}
}

// TestTruncationMarksIncomplete: a capped list must never read as exhaustive.
func TestTruncationMarksIncomplete(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("SELECT 1 FROM main.t0")
	for i := 1; i < maxCollected+10; i++ {
		fmt.Fprintf(&sb, ", main.t%d", i)
	}
	meta := Extract(sb.String())
	if len(meta.ReadRelations) != maxCollected {
		t.Fatalf("read relations = %d, want the cap %d", len(meta.ReadRelations), maxCollected)
	}
	if meta.Complete {
		t.Fatal("a truncated relation list must be marked incomplete")
	}
	if meta.IncompleteReason != "truncated" {
		t.Fatalf("incomplete reason = %q, want %q", meta.IncompleteReason, "truncated")
	}
}

func TestExtractHandlesEmptyAndGarbage(t *testing.T) {
	for _, sql := range []string{"", "   ", "!!!not sql!!!"} {
		meta := Extract(sql)
		if meta.Complete && len(meta.AccessKinds) == 0 {
			t.Fatalf("%q produced a complete-but-classless result", sql)
		}
		if len(meta.AccessKinds) == 0 {
			t.Fatalf("%q must still carry an access kind", sql)
		}
	}
}
