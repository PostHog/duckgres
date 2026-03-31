package configstore

import "testing"

func TestValidateTenantSecretRefRequiresTenantNamespace(t *testing.T) {
	err := ValidateTenantSecretRef("analytics", "", "", "metadata_store_credentials", SecretRef{
		Name: "analytics-metadata",
		Key:  "dsn",
	})
	if err == nil {
		t.Fatal("expected missing tenant namespace to be rejected")
	}
}

func TestValidateTenantSecretRefRejectsNamespaceMismatch(t *testing.T) {
	err := ValidateTenantSecretRef("analytics", "tenant-a", "", "metadata_store_credentials", SecretRef{
		Namespace: "tenant-b",
		Name:      "analytics-metadata",
		Key:       "dsn",
	})
	if err == nil {
		t.Fatal("expected namespace mismatch to be rejected")
	}
}

func TestValidateTenantSecretRefRejectsSubstringOnlyOrgMatch(t *testing.T) {
	err := ValidateTenantSecretRef("analytics", "tenant-a", "", "metadata_store_credentials", SecretRef{
		Namespace: "tenant-a",
		Name:      "shared-analytics-metadata",
		Key:       "dsn",
	})
	if err == nil {
		t.Fatal("expected substring-only org match to be rejected")
	}
}

func TestValidateTenantSecretRefRejectsDefaultNamespaceFallbackWithoutTenantNamespace(t *testing.T) {
	err := ValidateTenantSecretRef("analytics", "", "duckgres", "metadata_store_credentials", SecretRef{
		Name: "analytics-metadata",
		Key:  "dsn",
	})
	if err == nil {
		t.Fatal("expected secret ref without tenant namespace to be rejected even if a default namespace exists")
	}
}
