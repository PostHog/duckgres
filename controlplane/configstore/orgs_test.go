package configstore

import (
	"strings"
	"testing"
)

func TestValidateDatabaseName(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"simple slug", "acme", false},
		{"hyphenated", "entirely-chief-wildcat", false},
		{"digits", "acme123", false},
		{"canonical uuid", "0123abcd-4567-4890-abcd-ef0123456789", false},
		{"single char", "a", false},
		{"exactly 63 chars", strings.Repeat("a", 63), false},

		{"empty", "", true},
		{"over 63 chars", strings.Repeat("a", 64), true},
		{"space", "ACME INC", true},
		{"dot (multi-label hostname would be unroutable)", "acme.inc", true},
		{"underscore", "acme_inc", true},
		{"uppercase", "Acme", true},
		{"leading hyphen", "-acme", true},
		{"trailing hyphen", "acme-", true},
		{"slash", "acme/inc", true},
		{"leading digit is fine (DNS-1123 allows)", "1acme", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDatabaseName(tc.value)
			if tc.wantErr && err == nil {
				t.Errorf("ValidateDatabaseName(%q) = nil, want error", tc.value)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("ValidateDatabaseName(%q) = %v, want nil", tc.value, err)
			}
		})
	}
}
