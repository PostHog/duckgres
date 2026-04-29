//go:build kubernetes

package controlplane

import "testing"

func TestExtractOrgFromSNI(t *testing.T) {
	cp := &ControlPlane{
		cfg: ControlPlaneConfig{
			ManagedHostnameSuffixes: []string{".dw.us.postwh.com", ".dw.dev.postwh.com"},
		},
	}

	cases := []struct {
		name      string
		sni       string
		wantOrg   string
		wantMatch bool
	}{
		{"empty SNI", "", "", false},
		{"single-label match prod", "acme.dw.us.postwh.com", "acme", true},
		{"single-label match dev", "betalabs.dw.dev.postwh.com", "betalabs", true},
		{"unmanaged hostname", "duckgres-db.internal.ec2.us-east-1.dev.posthog.dev", "", false},
		{"bare suffix only (no prefix)", ".dw.us.postwh.com", "", false},
		{"multi-label prefix", "evil.acme.dw.us.postwh.com", "", false},
		{"different domain entirely", "example.com", "", false},
		{"prefix with hyphens is fine", "my-org.dw.us.postwh.com", "my-org", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotOrg, gotMatch := cp.extractOrgFromSNI(tc.sni)
			if gotOrg != tc.wantOrg || gotMatch != tc.wantMatch {
				t.Fatalf("extractOrgFromSNI(%q) = (%q, %v); want (%q, %v)",
					tc.sni, gotOrg, gotMatch, tc.wantOrg, tc.wantMatch)
			}
		})
	}
}

func TestExtractOrgFromSNIEmptySuffixes(t *testing.T) {
	cp := &ControlPlane{cfg: ControlPlaneConfig{}}
	if org, ok := cp.extractOrgFromSNI("acme.dw.us.postwh.com"); ok || org != "" {
		t.Fatalf("with no suffixes configured, want (\"\", false); got (%q, %v)", org, ok)
	}
}
