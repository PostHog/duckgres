package backend

import "testing"

func TestIcebergProfilePolicies(t *testing.T) {
	profile := ForName(Iceberg)
	if profile.Catalog().PhysicalName != "iceberg" {
		t.Fatalf("PhysicalName = %q, want iceberg", profile.Catalog().PhysicalName)
	}
	if profile.Catalog().MapPublicToMain {
		t.Fatal("Iceberg must not map public to main")
	}
	if profile.DDL().ConstraintHandling != StripConstraints {
		t.Fatalf("ConstraintHandling = %v, want StripConstraints", profile.DDL().ConstraintHandling)
	}
	if profile.DML().ConflictHandling != RewriteToMerge {
		t.Fatalf("ConflictHandling = %v, want RewriteToMerge", profile.DML().ConflictHandling)
	}
}

func TestMemoryProfileOnlyMapsPublicToMain(t *testing.T) {
	profile := ForName(Memory)
	if !profile.Catalog().MapPublicToMain {
		t.Fatal("Memory should map public to main")
	}
	if profile.DDL().ConstraintHandling != PreserveConstraints {
		t.Fatalf("ConstraintHandling = %v, want PreserveConstraints", profile.DDL().ConstraintHandling)
	}
	if profile.DDL().NeedsTransform() {
		t.Fatal("Memory should not need DDL transform")
	}
}
