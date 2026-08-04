package backend

import "testing"

func TestDuckLakeProfilePolicies(t *testing.T) {
	profile := ForName(DuckLake)
	if profile.Catalog().PhysicalName != "ducklake" {
		t.Fatalf("PhysicalName = %q, want ducklake", profile.Catalog().PhysicalName)
	}
	if !profile.Catalog().MapPublicToMain {
		t.Fatal("DuckLake must map public to main")
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
