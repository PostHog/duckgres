package server

import "testing"

// TestQuerySourceGetter covers the session accessor a future compute meter
// reads: unset defaults to "standard", a SET value round-trips, and reset to
// empty falls back to the default.
func TestQuerySourceGetter(t *testing.T) {
	c := &clientConn{}

	// Unset → default "standard" (never an error for a missing value).
	if got := c.QuerySource(); got != "standard" {
		t.Fatalf("unset QuerySource() = %q, want %q", got, "standard")
	}
	if got := c.QuerySource(); got != defaultQuerySource {
		t.Fatalf("unset QuerySource() = %q, want defaultQuerySource %q", got, defaultQuerySource)
	}

	// SET → value round-trips (pass-through: any string is accepted).
	c.setQuerySource("endpoints")
	if got := c.QuerySource(); got != "endpoints" {
		t.Fatalf("after set, QuerySource() = %q, want %q", got, "endpoints")
	}

	c.setQuerySource("whatever")
	if got := c.QuerySource(); got != "whatever" {
		t.Fatalf("after set, QuerySource() = %q, want %q", got, "whatever")
	}

	// RESET / empty → back to default.
	c.setQuerySource("")
	if got := c.QuerySource(); got != "standard" {
		t.Fatalf("after reset, QuerySource() = %q, want %q", got, "standard")
	}
}

// TestQuerySourceStartupOption asserts a `-c duckgres.query_source=...` startup
// option is parsed into the map ParseStartupOptions returns (the value the
// startup handler applies to the session).
func TestQuerySourceStartupOption(t *testing.T) {
	opts := ParseStartupOptions("-c duckgres.query_source=endpoints")
	if got := opts[querySourceGUCName]; got != "endpoints" {
		t.Fatalf("ParseStartupOptions[%q] = %q, want %q", querySourceGUCName, got, "endpoints")
	}
}
