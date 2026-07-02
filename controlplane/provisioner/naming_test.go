//go:build kubernetes

package provisioner

import "testing"

// TestDucklingNamePreservesHyphens locks in the post-fix behavior: k8s/AWS
// resource names keep hyphens (only lowercasing is applied), and the transform
// is injective — the regression being the old de-hyphenation where "a-b" and
// "ab" both collapsed to "ab".
func TestDucklingNamePreservesHyphens(t *testing.T) {
	for in, want := range map[string]string{
		"ben-warehouse-cnpg":                   "ben-warehouse-cnpg",
		"Ben-Warehouse":                        "ben-warehouse",
		"team123":                              "team123",
		"f47ac10b-58cc-4372-a567-0e02b2c3d479": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
	} {
		if got := ducklingName(in); got != want {
			t.Errorf("ducklingName(%q) = %q, want %q", in, got, want)
		}
	}
	if ducklingName("a-b") == ducklingName("ab") {
		t.Error("ducklingName must not collide \"a-b\" with \"ab\" (the old de-hyphenation bug)")
	}
}

// TestPgIdentSuffixSanitizes verifies the Postgres-identifier transform maps
// hyphens to underscores (PG identifiers can't be unquoted-hyphenated) and
// stays injective for [a-z0-9-] inputs.
func TestPgIdentSuffixSanitizes(t *testing.T) {
	for in, want := range map[string]string{
		"ben-warehouse-cnpg": "ben_warehouse_cnpg",
		"team123":            "team123",
		"ABC-1":              "abc_1",
	} {
		if got := pgIdentSuffix(in); got != want {
			t.Errorf("pgIdentSuffix(%q) = %q, want %q", in, got, want)
		}
	}
}
