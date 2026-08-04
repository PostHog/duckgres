//go:build kubernetes

package provisioner

import "testing"

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
