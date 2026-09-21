package perf

import (
	"context"
	"maps"
	"strings"
	"testing"

	trinodriver "github.com/posthog/duckgres/tests/perf/drivers/trino"
)

func TestFixtureHoglakeCatalogUsesPodIdentity(t *testing.T) {
	managed := map[string]string{
		"connector.name": "hoglake", "hoglake.uri": "http://hoglake.example:8080",
		"hoglake.catalog": "org-fixture", "fs.cache.enabled": "false",
		"fs.s3.enabled": "true", "s3.auth-type": "IAM_ROLE",
		"s3.iam-role": "arn:aws:iam::123456789012:role/tenant-fixture",
		"s3.region":   "us-east-1", "s3.max-connections": "500",
	}
	before := maps.Clone(managed)
	for _, catalog := range []string{"org-fixture-frozen", "org-fixture-properties", "org-fixture-frozen"} {
		selected, err := fixtureHoglakeProperties(managed, catalog)
		if err != nil {
			t.Fatal(err)
		}
		want := maps.Clone(before)
		delete(want, "s3.iam-role")
		want["hoglake.catalog"] = catalog
		if !maps.Equal(selected, want) {
			t.Fatalf("fixture properties = %v, want %v", selected, want)
		}
		if !maps.Equal(managed, before) {
			t.Fatal("mutated source catalog properties")
		}
		managed, before = selected, maps.Clone(selected)
	}
}

func TestFixtureHoglakeCatalogRejectsInvalidBaseline(t *testing.T) {
	for _, change := range []map[string]string{
		{"connector.name": "ducklake"}, {"hoglake.uri": ""}, {"hoglake.catalog": ""},
		{"fs.cache.enabled": "true"}, {"s3.auth-type": "STATIC"},
	} {
		original := map[string]string{"connector.name": "hoglake", "hoglake.uri": "http://hoglake.example:8080", "hoglake.catalog": "org-fixture", "fs.cache.enabled": "false", "s3.auth-type": "IAM_ROLE"}
		maps.Copy(original, change)
		if _, err := fixtureHoglakeProperties(original, "org-fixture-frozen"); err == nil {
			t.Fatalf("accepted invalid baseline %v", change)
		}
	}
}

func TestFixtureHoglakeCatalogRejectsManagedDataset(t *testing.T) {
	original := map[string]string{"connector.name": "hoglake", "hoglake.uri": "http://hoglake.example:8080", "hoglake.catalog": "org-fixture", "fs.cache.enabled": "false", "s3.auth-type": "IAM_ROLE", "s3.iam-role": "tenant-role"}
	if _, err := fixtureHoglakeProperties(original, "org-fixture"); err == nil {
		t.Fatal("accepted managed dataset as a read-only fixture")
	}
}

func TestSelectHoglakeCatalogRequiresIsolatedStore(t *testing.T) {
	for _, factory := range []defaultDriverFactory{{}, {trinoCatalogStoreDSN: "postgres://fixture.example/db"}} {
		err := factory.selectHoglakeCatalog(context.Background(), trinodriver.ConnectionConfig{Catalog: "org_fixture", HoglakeCatalog: "org-fixture-frozen"})
		if err == nil || !strings.Contains(err.Error(), "explicit isolated benchmark catalog store and cell") {
			t.Fatalf("got %v, want isolated store guard", err)
		}
	}
}
