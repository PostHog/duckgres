package core

import "testing"

func TestValidateSuite(t *testing.T) {
	for _, suite := range []string{"tables", "properties", "coverage"} {
		if err := ValidateSuite(suite); err != nil {
			t.Errorf("suite %q: %v", suite, err)
		}
	}
	for _, suite := range []string{"", "covergae", "Tables"} {
		if err := ValidateSuite(suite); err == nil {
			t.Errorf("accepted unknown suite %q", suite)
		}
	}
}
