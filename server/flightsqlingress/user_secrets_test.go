package flightsqlingress

import "testing"

// Persistent-secret DDL must be rejected on the Flight ingress when the
// deployment manages user secrets via the PG protocol: over Flight the
// statement would execute, never persist, and be wiped at the next session.
func TestCheckUserSecretDDL(t *testing.T) {
	h := &ControlPlaneFlightSQLHandler{rejectPersistentSecretDDL: true}

	rejected := []string{
		"CREATE PERSISTENT SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')",
		"CREATE OR REPLACE PERSISTENT SECRET s (TYPE s3)",
		"DROP PERSISTENT SECRET s",
		"CREATE PERSISTENT SECRET s (TYPE s3); SELECT 1",
	}
	for _, q := range rejected {
		if err := h.checkUserSecretDDL(q); err == nil {
			t.Errorf("checkUserSecretDDL(%q) = nil, want rejection", q)
		}
	}

	allowed := []string{
		"CREATE SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')", // session-scoped, fine
		"CREATE TEMPORARY SECRET s (TYPE s3)",
		"DROP SECRET s",
		"SELECT 1",
	}
	for _, q := range allowed {
		if err := h.checkUserSecretDDL(q); err != nil {
			t.Errorf("checkUserSecretDDL(%q) = %v, want nil", q, err)
		}
	}

	// Flag off (no user-secret manager on the deployment): everything passes.
	off := &ControlPlaneFlightSQLHandler{}
	for _, q := range rejected {
		if err := off.checkUserSecretDDL(q); err != nil {
			t.Errorf("flag off: checkUserSecretDDL(%q) = %v, want nil", q, err)
		}
	}
}
