package server

import (
	"strings"
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// TestNewTranspilerRewritesLogicalCatalogAlias pins the connection-level wiring
// that makes the logical catalog alias usable in SQL: the session's PG-visible
// database name becomes the transpiler's logical catalog, so a three-part
// reference written against the org's Trino catalog name reaches the physical
// DuckLake catalog. Without this, a client that sees only the alias could not
// write a fully-qualified reference at all.
func TestNewTranspilerRewritesLogicalCatalogAlias(t *testing.T) {
	c := &clientConn{
		server:          &Server{},
		database:        "org_acme_analytics",
		physicalCatalog: physicalDuckLakeCatalog,
	}

	got, err := c.newTranspiler(false).Transpile("SELECT id FROM org_acme_analytics.public.events")
	if err != nil {
		t.Fatalf("transpile: %v", err)
	}
	if !strings.Contains(got.SQL, "ducklake.main.events") {
		t.Fatalf("transpiled = %q, want a rewrite to ducklake.main.events", got.SQL)
	}

	// A sibling org's catalog name is not this session's alias, so it is left
	// alone and fails on the worker rather than resolving to this tenant's data.
	got, err = c.newTranspiler(false).Transpile("SELECT id FROM org_billing_db.public.events")
	if err != nil {
		t.Fatalf("transpile foreign catalog: %v", err)
	}
	if strings.Contains(got.SQL, "ducklake.") {
		t.Fatalf("transpiled = %q, a foreign catalog name must not be rewritten onto ducklake", got.SQL)
	}
}

// aliasSessionConn builds a connection the way controlplane.handleConnection
// actually builds one for a session that selected its org's catalog name:
// NewClientConn takes the VISIBLE catalog as the PG-visible database, and the
// physical catalog + USE rewriting are stamped through the exported setters
// afterwards. Going through the real constructor and the real setters is the
// point — hand-building a clientConn is what let a broken session field pass
// unit tests and fail in the e2e lane.
func aliasSessionConn(t *testing.T, visibleCatalog string) *clientConn {
	t.Helper()
	cc := NewClientConn(&Server{}, nil, nil, nil,
		"root", "acme", visibleCatalog, "psql", nil, 1, 2, 0, "")
	// control.go, in this order: physical catalog, then USE rewriting.
	SetConnectionPhysicalCatalog(cc, physicalDuckLakeCatalog)
	SetCatalogUseRewrite(cc, true)
	return cc
}

// TestLogicalCatalogAliasThroughConnectionSetup exercises the alias through the
// real connection-setup sequence, for both query protocols. The simple-query
// path rewrites the raw statement; the extended-query path rewrites the
// TRANSPILED statement (conn_extended_query.go stores
// rewriteDirectQuery(result.SQL) as the prepared statement's converted query),
// so both compositions are asserted here.
func TestLogicalCatalogAliasThroughConnectionSetup(t *testing.T) {
	const alias = "org_acme_analytics"
	cc := aliasSessionConn(t, alias)

	if got, want := cc.rewriteDirectQuery("USE "+alias), "USE ducklake.main"; got != want {
		t.Fatalf("simple-query USE = %q, want %q", got, want)
	}

	// Extended query: Parse transpiles first, then rewrites the result. `USE`
	// is not PostgreSQL, so it falls back to the raw statement and the rewrite
	// still has to fire on it.
	res, err := cc.newTranspiler(true).Transpile("USE " + alias)
	if err != nil {
		t.Fatalf("transpile USE: %v", err)
	}
	if got, want := cc.rewriteDirectQuery(res.SQL), "USE ducklake.main"; got != want {
		t.Fatalf("extended-query USE = %q, want %q", got, want)
	}

	// A three-part reference on the same connection.
	res, err = cc.newTranspiler(false).Transpile("SELECT id FROM " + alias + ".public.events")
	if err != nil {
		t.Fatalf("transpile three-part: %v", err)
	}
	if !strings.Contains(res.SQL, "ducklake.main.events") {
		t.Fatalf("three-part reference = %q, want a rewrite to ducklake.main.events", res.SQL)
	}

	// After a worker switch the control plane re-stamps the same three fields
	// (control.go's activation path). The alias must survive that replay.
	SetConnectionPhysicalCatalog(cc, physicalDuckLakeCatalog)
	SetCatalogUseRewrite(cc, true)
	SetConnectionDatabase(cc, alias)
	if got, want := cc.rewriteDirectQuery("USE "+alias), "USE ducklake.main"; got != want {
		t.Fatalf("USE after a worker switch = %q, want %q", got, want)
	}
}

// TestUseStatementIsNeverSplitOutOfASimpleQueryBatch pins the pre-existing
// limitation that broke the first version of the e2e assertion for this
// feature, so nobody re-learns it from a CI failure.
//
// handleQuery splits a multi-statement simple query only when pg_query can
// parse it (conn.go: `parseErr == nil && len(tree.Stmts) > 1`). `USE` is not
// PostgreSQL syntax, so a USE-led batch never splits: the whole string reaches
// rewriteDirectQuery as one statement, whose USE target is then everything
// after `USE` — semicolon, following statements and all — which matches no
// catalog name and is correctly left alone. DuckDB then splits the batch
// itself and fails the bare `USE`.
//
// This is NOT specific to the alias: `USE ducklake; SELECT ...` behaves
// identically. Send `USE` as its own statement.
func TestUseStatementIsNeverSplitOutOfASimpleQueryBatch(t *testing.T) {
	const alias = "org_acme_analytics"
	cc := aliasSessionConn(t, alias)

	for _, batch := range []string{
		"USE " + alias + "; SELECT v FROM events;",
		"USE ducklake; SELECT v FROM events;",
	} {
		if got := cc.rewriteDirectQuery(batch); got != batch {
			t.Fatalf("rewriteDirectQuery(%q) = %q; a USE-led batch must be left alone, not partially rewritten", batch, got)
		}
	}

	// The reason it is left alone: the batch never splits upstream.
	if _, err := pg_query.Parse("USE " + alias + "; SELECT v FROM events;"); err == nil {
		t.Fatal("pg_query now parses a USE-led batch; handleQuery would split it and this limitation is gone — " +
			"re-check the e2e assertion and delete this test")
	}
}
