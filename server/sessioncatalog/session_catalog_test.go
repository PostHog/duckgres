package sessioncatalog

import "testing"

func TestSelectionPreservesClientDatabase(t *testing.T) {
	selection, ok := ResolveSelection(Request{
		ClientDatabase: "posthog",
		DefaultCatalog: CatalogIceberg,
		Attached: AttachedCatalogs{
			DuckLake: true,
			Iceberg:  true,
		},
	})
	if !ok {
		t.Fatal("ResolveSelection returned !ok")
	}
	if selection.ClientDatabase != "posthog" {
		t.Fatalf("ClientDatabase = %q, want posthog", selection.ClientDatabase)
	}
	if selection.PhysicalCatalog != CatalogIceberg {
		t.Fatalf("PhysicalCatalog = %q, want iceberg", selection.PhysicalCatalog)
	}
}

func TestSelectionRejectsUnavailableRequestedCatalog(t *testing.T) {
	_, ok := ResolveSelection(Request{
		ClientDatabase:   "posthog",
		RequestedCatalog: CatalogIceberg,
		DefaultCatalog:   CatalogDuckLake,
		Attached:         AttachedCatalogs{DuckLake: true},
	})
	if ok {
		t.Fatal("ResolveSelection unexpectedly accepted unavailable iceberg catalog")
	}
}

func TestSelectionUsesDuckLakeBeforeIcebergWhenNoDefault(t *testing.T) {
	selection, ok := ResolveSelection(Request{
		ClientDatabase: "posthog",
		Attached: AttachedCatalogs{
			DuckLake: true,
			Iceberg:  true,
		},
	})
	if !ok {
		t.Fatal("ResolveSelection returned !ok")
	}
	if selection.PhysicalCatalog != CatalogDuckLake {
		t.Fatalf("PhysicalCatalog = %q, want ducklake", selection.PhysicalCatalog)
	}
}
