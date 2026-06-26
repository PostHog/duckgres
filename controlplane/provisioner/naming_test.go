//go:build kubernetes

package provisioner

import "testing"

// TestDucklingNamePreservesHyphens locks in the post-fix behavior: k8s/AWS
// resource names keep hyphens (only lowercasing is applied), and the transform
// is injective — the regression being the old de-hyphenation where "a-b" and
// "ab" both collapsed to "ab".
func TestDucklingNamePreservesHyphens(t *testing.T) {
	for in, want := range map[string]string{
		"ben-ducklake-cnpg":                    "ben-ducklake-cnpg",
		"Ben-DuckLake":                         "ben-ducklake",
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
