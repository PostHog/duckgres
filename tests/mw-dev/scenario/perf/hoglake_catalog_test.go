package perf

import "testing"

func managedHoglakeCatalogProperties() map[string]string {
	return map[string]string{
		"connector.name":   "hoglake",
		"hoglake.uri":      "http://duckgres-hoglake.test.svc:8080",
		"hoglake.catalog":  "ci-pr-1-cnpg",
		"fs.s3.enabled":    "true",
		"fs.cache.enabled": "false",
		"s3.region":        "us-east-1",
		"s3.auth-type":     "IAM_ROLE",
		"s3.iam-role":      "arn:aws:iam::123456789012:role/tenant-writer",
	}
}

func TestFixtureHoglakePropertiesReadsThroughThePodIdentity(t *testing.T) {
	original := managedHoglakeCatalogProperties()
	got, err := fixtureHoglakeProperties(original, "ci-pr-1-cnpg-frozen")
	if err != nil {
		t.Fatalf("fixtureHoglakeProperties: %v", err)
	}
	if got["hoglake.catalog"] != "ci-pr-1-cnpg-frozen" {
		t.Fatalf("hoglake.catalog = %q, want the fixture catalog", got["hoglake.catalog"])
	}
	// Trino's S3 filesystem rejects a catalog whose s3.iam-role is set unless
	// s3.auth-type=IAM_ROLE, and vice versa (S3FileSystemConfig.isIamRoleValid).
	// Dropping only the role left IAM_ROLE behind and every benchmark catalog
	// creation failed with ApplicationConfigurationException.
	_, hasRole := got["s3.iam-role"]
	if hasRole || got["s3.auth-type"] == "IAM_ROLE" {
		t.Fatalf("fixture catalog must use the default credential chain, got s3.auth-type=%q s3.iam-role present=%v", got["s3.auth-type"], hasRole)
	}
	for _, key := range []string{"connector.name", "hoglake.uri", "fs.s3.enabled", "fs.cache.enabled", "s3.region"} {
		if got[key] != original[key] {
			t.Fatalf("%s = %q, want %q preserved from the managed catalog", key, got[key], original[key])
		}
	}
	if original["s3.auth-type"] != "IAM_ROLE" || original["s3.iam-role"] == "" || original["hoglake.catalog"] != "ci-pr-1-cnpg" {
		t.Fatal("fixtureHoglakeProperties mutated the managed catalog properties")
	}
}

func TestFixtureHoglakePropertiesRejectsUnexpectedManagedCatalogs(t *testing.T) {
	for name, mutate := range map[string]func(map[string]string){
		"not hoglake":        func(p map[string]string) { p["connector.name"] = "ducklake" },
		"cache enabled":      func(p map[string]string) { p["fs.cache.enabled"] = "true" },
		"static credentials": func(p map[string]string) { p["s3.auth-type"] = "DEFAULT" },
		"missing uri":        func(p map[string]string) { delete(p, "hoglake.uri") },
	} {
		t.Run(name, func(t *testing.T) {
			props := managedHoglakeCatalogProperties()
			mutate(props)
			if _, err := fixtureHoglakeProperties(props, "ci-pr-1-cnpg-frozen"); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
	if _, err := fixtureHoglakeProperties(managedHoglakeCatalogProperties(), "ci-pr-1-cnpg"); err == nil {
		t.Fatal("expected fixtures sharing the managed tenant catalog to be rejected")
	}
}
