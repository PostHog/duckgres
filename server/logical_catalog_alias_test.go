package server

import (
	"strings"
	"testing"
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
