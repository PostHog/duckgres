package configstore

import (
	"fmt"
	"strings"
)

// SecretRefEmpty reports whether the ref is entirely unset.
func SecretRefEmpty(ref SecretRef) bool {
	return strings.TrimSpace(ref.Namespace) == "" &&
		strings.TrimSpace(ref.Name) == "" &&
		strings.TrimSpace(ref.Key) == ""
}

func tenantSecretNamePrefix(orgID string) string {
	orgID = strings.TrimSpace(orgID)
	if orgID == "" {
		return ""
	}
	return orgID + "-"
}

// ValidateTenantSecretRef constrains shared-worker secret refs to tenant-owned
// locations so one org cannot point activation at another org's secret.
func ValidateTenantSecretRef(orgID, tenantNamespace, defaultNamespace, field string, ref SecretRef) error {
	if SecretRefEmpty(ref) {
		return nil
	}
	name := strings.TrimSpace(ref.Name)
	key := strings.TrimSpace(ref.Key)
	refNamespace := strings.TrimSpace(ref.Namespace)
	effectiveTenantNamespace := strings.TrimSpace(tenantNamespace)

	if name == "" || key == "" {
		return fmt.Errorf("%s secret ref requires name and key", field)
	}
	if effectiveTenantNamespace == "" {
		if strings.TrimSpace(defaultNamespace) != "" {
			return fmt.Errorf("%s secret ref requires worker_identity.namespace; refusing shared fallback namespace %q", field, strings.TrimSpace(defaultNamespace))
		}
		return fmt.Errorf("%s secret ref requires worker_identity.namespace to keep secret lookups tenant-owned", field)
	}
	if refNamespace == "" {
		refNamespace = effectiveTenantNamespace
	}
	if refNamespace != effectiveTenantNamespace {
		return fmt.Errorf("%s secret ref must stay tenant-owned: namespace %q must match tenant namespace %q", field, refNamespace, effectiveTenantNamespace)
	}

	prefix := tenantSecretNamePrefix(orgID)
	if prefix != "" && !strings.HasPrefix(name, prefix) {
		return fmt.Errorf("%s secret ref must stay tenant-owned: name %q must start with %q", field, name, prefix)
	}
	return nil
}

// ValidateManagedWarehouseSecretRefs validates every secret reference carried by
// a managed-warehouse config against the tenant namespace/prefix rules.
func ValidateManagedWarehouseSecretRefs(orgID, defaultNamespace string, warehouse *ManagedWarehouseConfig) error {
	if warehouse == nil {
		return nil
	}
	tenantNamespace := strings.TrimSpace(warehouse.WorkerIdentity.Namespace)

	for _, candidate := range []struct {
		field string
		ref   SecretRef
	}{
		{field: "warehouse_database_credentials", ref: warehouse.WarehouseDatabaseCredentials},
		{field: "metadata_store_credentials", ref: warehouse.MetadataStoreCredentials},
		{field: "s3_credentials", ref: warehouse.S3Credentials},
		{field: "runtime_config", ref: warehouse.RuntimeConfig},
	} {
		if err := ValidateTenantSecretRef(orgID, tenantNamespace, defaultNamespace, candidate.field, candidate.ref); err != nil {
			return err
		}
	}
	return nil
}
