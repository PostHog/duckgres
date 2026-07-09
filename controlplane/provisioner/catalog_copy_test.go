//go:build kubernetes

package provisioner

import "testing"

// TestShouldReplayConstraint pins the PG-18 NOT NULL skip: catalogued
// not-null constraints (contype 'n') must never be replayed — the CREATE
// TABLE already carries column-level NOT NULL, and the ADD CONSTRAINT form
// is PG-18-only syntax that an older external-RDS target rejects
// ("syntax error at or near \"NOT\"", observed in a real cnpg→ext reshard).
func TestShouldReplayConstraint(t *testing.T) {
	replayed := map[string]bool{
		"p": true, "u": true, "f": true, "c": true, "x": true,
		"n": false,
	}
	for contype, want := range replayed {
		if got := shouldReplayConstraint(contype); got != want {
			t.Errorf("shouldReplayConstraint(%q) = %t, want %t", contype, got, want)
		}
	}
}
