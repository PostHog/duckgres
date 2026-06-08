package server

import (
	"errors"
	"strings"
	"testing"
)

// The Iceberg "newer schema id" scan failure (which a DROP COLUMN can leave a
// table in) must be classified as feature_not_supported (0A000) and rewritten
// into an actionable message, both when raw and when Flight-wrapped.
func TestSchemaEvolutionErrorMapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{
			name: "raw",
			err:  errors.New("INTERNAL Error: Tried to scan a snapshot created with a newer schema id (1) than the schema id selected for the scan (0)"),
		},
		{
			name: "flight-wrapped",
			err:  errors.New("flight execute: rpc error: code = Internal desc = Invalid Input Error: Tried to scan a snapshot created with a newer schema id (1) than the schema id selected for the scan (0)"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyErrorCode(tc.err); got != "0A000" {
				t.Errorf("classifyErrorCode = %q, want 0A000", got)
			}
			msg := friendlyExecError(tc.err)
			if !strings.Contains(msg, "DROP COLUMN is not safely supported") {
				t.Errorf("friendlyExecError = %q, want the DROP COLUMN guidance", msg)
			}
		})
	}
}

func TestFriendlyExecErrorPassesThroughUnrelated(t *testing.T) {
	err := errors.New("Catalog Error: Table with name foo does not exist!")
	if got := friendlyExecError(err); got != err.Error() {
		t.Errorf("friendlyExecError rewrote an unrelated error: %q", got)
	}
}
