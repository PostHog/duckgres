package server

import (
	"strings"
	"testing"
)

// TestActivateDBConnectionRequiresCatalog covers the iceberg-only activation
// guard: with neither a DuckLake metadata store nor an enabled Iceberg catalog
// there is nothing to attach, so activation is rejected up front (before any DB
// work — hence the nil *sql.DB here never gets touched).
//
// The success branches (DuckLake-only, iceberg-only, both) attach real
// catalogs against a live metadata store / Lakekeeper REST endpoint and are
// exercised by the k8s integration suite + live validation, not here.
func TestActivateDBConnectionRequiresCatalog(t *testing.T) {
	// Config{} → DuckLake.MetadataStore == "" and Iceberg.Enabled == false.
	err := ActivateDBConnection(nil, Config{}, nil, "tenant")
	if err == nil {
		t.Fatal("expected error when neither DuckLake nor Iceberg is configured")
	}
	if !strings.Contains(err.Error(), "ducklake metadata_store or an enabled iceberg catalog") {
		t.Fatalf("unexpected error: %v", err)
	}
}
