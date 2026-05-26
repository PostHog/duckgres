package opa

import (
	"testing"
)

// TestStubBuilderReturnsEmptyBundle pins the contract the Trino provisioner
// relies on while the real Rego builder is in flight. When the real builder
// lands, this test is replaced (or moved) — but until then it guards the
// expected behavior so an accidental change here doesn't break the
// provisioner's reconcile loop.
func TestStubBuilderReturnsEmptyBundle(t *testing.T) {
	b := NewStubBuilder()
	out, err := b.BuildBundle(UserCatalogs{
		"42": {"org_42_iceberg": true},
	})
	if err != nil {
		t.Fatalf("BuildBundle returned error: %v", err)
	}
	if string(out) != "{}" {
		t.Errorf("BuildBundle = %q, want %q", string(out), "{}")
	}
}

func TestUserCatalogsShape(t *testing.T) {
	// Compile-time-ish smoke that the map shape stays consistent —
	// bumping the alias to a struct in the future requires touching
	// this test too.
	uc := UserCatalogs{
		"42": {"a": true, "b": true},
		"43": {"c": true},
	}
	if !uc["42"]["a"] || !uc["43"]["c"] {
		t.Fatal("expected object-indexed lookups to work")
	}
}
