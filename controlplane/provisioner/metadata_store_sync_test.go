//go:build kubernetes

package provisioner

import (
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func syncTestCnpgWarehouse() *configstore.ManagedWarehouse {
	return &configstore.ManagedWarehouse{
		OrgID: "acme",
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind: configstore.MetadataStoreKindCnpgShard,
		},
	}
}

func syncTestCnpgStatus() *DucklingStatus {
	st := &DucklingStatus{}
	st.MetadataStore.Type = configstore.MetadataStoreKindCnpgShard
	st.MetadataStore.Endpoint = "shard-001-pooler.cnpg-shards.svc.cluster.local"
	st.MetadataStore.Database = "mdstore_acme"
	st.MetadataStore.User = "mdstore_acme"
	st.MetadataCredentialSecretRef = SecretReference{
		Namespace: "ducklings",
		Name:      "cnpg-tenant-acme-password",
		Key:       "password",
	}
	return st
}

func syncTestSteadyCnpgWarehouse() *configstore.ManagedWarehouse {
	w := syncTestCnpgWarehouse()
	st := syncTestCnpgStatus()
	w.MetadataStore.Endpoint = st.MetadataStore.Endpoint
	w.MetadataStore.DatabaseName = st.MetadataStore.Database
	w.MetadataStore.Username = st.MetadataStore.User
	w.MetadataStore.Port = 5432
	w.MetadataStoreSecretRef = configstore.SecretRef{
		Namespace: "ducklings", Name: "cnpg-tenant-acme-password", Key: "password",
	}
	return w
}

func TestMetadataStoreRowUpdatesCnpgFullSync(t *testing.T) {
	updates := metadataStoreRowUpdates(syncTestCnpgWarehouse(), syncTestCnpgStatus())
	want := map[string]interface{}{
		"metadata_store_endpoint":             "shard-001-pooler.cnpg-shards.svc.cluster.local",
		"metadata_store_database_name":        "mdstore_acme",
		"metadata_store_username":             "mdstore_acme",
		"metadata_store_port":                 5432,
		"metadata_store_secret_ref_namespace": "ducklings",
		"metadata_store_secret_ref_name":      "cnpg-tenant-acme-password",
		"metadata_store_secret_ref_key":       "password",
	}
	if len(updates) != len(want) {
		t.Fatalf("updates = %v, want %v", updates, want)
	}
	for k, v := range want {
		if updates[k] != v {
			t.Fatalf("updates[%s] = %v, want %v", k, updates[k], v)
		}
	}
}

func TestMetadataStoreRowUpdatesNeverTouchActivationCredentials(t *testing.T) {
	// The mirror writes the DISCOVERY-ONLY metadata_store_secret_ref_*
	// columns. metadata_store_credentials_* is a worker-activation input
	// validated tenant-owned by ValidateManagedWarehouseSecretRefs — the
	// composition-owned refs mirrored here fail that validation by design,
	// and writing them there breaks every cold worker activation.
	updates := metadataStoreRowUpdates(syncTestCnpgWarehouse(), syncTestCnpgStatus())
	for k := range updates {
		if k == "metadata_store_credentials_namespace" ||
			k == "metadata_store_credentials_name" ||
			k == "metadata_store_credentials_key" {
			t.Fatalf("mirror must never write activation credential column %s: %v", k, updates)
		}
	}
}

func TestMetadataStoreRowUpdatesNoDriftIsEmpty(t *testing.T) {
	// A steady-state reconcile tick must produce ZERO updates — a non-empty
	// map bumps updated_at, which would churn the discovery
	// config_generation every tick.
	if updates := metadataStoreRowUpdates(syncTestSteadyCnpgWarehouse(), syncTestCnpgStatus()); len(updates) != 0 {
		t.Fatalf("steady state must be a no-op, got %v", updates)
	}
}

func TestMetadataStoreRowUpdatesTypeDriftProducesNothing(t *testing.T) {
	// A status whose metadata-store type contradicts the row kind is
	// identity drift (the condition recordSource refuses a reshard over) —
	// mirroring across it would stamp one store's connection details onto a
	// row claiming the other. The gate must silence the WHOLE update set,
	// secret ref included.
	w := syncTestCnpgWarehouse()
	st := syncTestCnpgStatus()
	st.MetadataStore.Type = configstore.MetadataStoreKindExternal
	if updates := metadataStoreRowUpdates(w, st); len(updates) != 0 {
		t.Fatalf("type drift must produce no updates, got %v", updates)
	}
}

func TestMetadataStoreRowUpdatesEmptyStatusTypeStillSyncs(t *testing.T) {
	// Older compositions publish no status.metadataStore.type — the gate
	// must not strand them (empty type is "unknown", not "mismatched").
	st := syncTestCnpgStatus()
	st.MetadataStore.Type = ""
	if updates := metadataStoreRowUpdates(syncTestCnpgWarehouse(), st); len(updates) == 0 {
		t.Fatal("empty status type must not block the mirror")
	}
}

func TestMetadataStoreRowUpdatesEmptyRowKindStillSyncsSecretRef(t *testing.T) {
	// A kind-less row (should not happen — the provision handler writes it,
	// but the column is nullable) is "unknown" to the gate, not
	// "mismatched": the secret-ref mirror must still flow. The cnpg
	// connection block stays gated on the row EXPLICITLY claiming cnpg.
	w := syncTestCnpgWarehouse()
	w.MetadataStore.Kind = ""
	updates := metadataStoreRowUpdates(w, syncTestCnpgStatus())
	if updates["metadata_store_secret_ref_name"] != "cnpg-tenant-acme-password" {
		t.Fatalf("empty row kind must not strand the secret-ref mirror: %v", updates)
	}
	if _, ok := updates["metadata_store_endpoint"]; ok {
		t.Fatalf("connection block requires the row to claim cnpg explicitly: %v", updates)
	}
}

func TestMetadataStoreRowUpdatesEmptyStatusKeyDefaultsPassword(t *testing.T) {
	// A ref with a name but no key is a real mid-migration shape (#972's
	// harness defaults it too): the mirror must default to the conventional
	// "password" key, never write an empty key. Against a steady row whose
	// key IS "password", the default therefore means no-op — the drift
	// invariant holds through the defaulting.
	w := syncTestSteadyCnpgWarehouse()
	st := syncTestCnpgStatus()
	st.MetadataCredentialSecretRef.Key = ""
	if updates := metadataStoreRowUpdates(w, st); len(updates) != 0 {
		t.Fatalf("key-less status vs steady password-keyed row must be a no-op, got %v", updates)
	}
	if updates := metadataStoreRowUpdates(syncTestCnpgWarehouse(), st); updates["metadata_store_secret_ref_key"] != "password" {
		t.Fatalf("key-less status vs empty row must default the key to password, got %v", updates)
	}
}

func TestMetadataStoreRowUpdatesEmptyRefNamespaceDefaultsDucklings(t *testing.T) {
	// The composition publishes same-namespace refs without an explicit
	// namespace field; Duckling CRs live in "ducklings". Serving an
	// empty namespace would send consumers' Secret GETs to their OWN
	// namespace, where the secret doesn't exist.
	st := syncTestCnpgStatus()
	st.MetadataCredentialSecretRef.Namespace = ""
	updates := metadataStoreRowUpdates(syncTestCnpgWarehouse(), st)
	if updates["metadata_store_secret_ref_namespace"] != "ducklings" {
		t.Fatalf("empty ref namespace must default to ducklings, got %v", updates)
	}
}

func TestMetadataStoreRowUpdatesExternalSyncsOnlySecretRef(t *testing.T) {
	// External stores: endpoint/db/user are provision-time INPUTS on the
	// row — never overwritten from status. Only the credential Secret ref
	// (no provisioning-time writer for either kind) is mirrored.
	w := &configstore.ManagedWarehouse{
		OrgID: "ext",
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Kind:         configstore.MetadataStoreKindExternal,
			Endpoint:     "duckling-ext.rds.example",
			DatabaseName: "ducklingext",
			Username:     "ducklingext",
			Port:         5432,
		},
	}
	st := &DucklingStatus{}
	st.MetadataStore.Type = configstore.MetadataStoreKindExternal
	st.MetadataStore.Endpoint = "something-else-derived"
	st.MetadataCredentialSecretRef = SecretReference{
		Namespace: "ducklings", Name: "duckling-ext-password", Key: "password",
	}

	updates := metadataStoreRowUpdates(w, st)
	if len(updates) != 3 {
		t.Fatalf("external must sync only the 3 secret-ref fields, got %v", updates)
	}
	if updates["metadata_store_secret_ref_name"] != "duckling-ext-password" {
		t.Fatalf("secret ref name wrong: %v", updates)
	}
}

func TestMetadataStoreRowUpdatesEmptyStatusFieldsNeverClobber(t *testing.T) {
	// Partial/blank status (composition mid-render) must not overwrite
	// non-empty row values or write empty strings.
	w := syncTestSteadyCnpgWarehouse()
	st := &DucklingStatus{} // everything empty
	st.MetadataStore.Type = configstore.MetadataStoreKindCnpgShard
	if updates := metadataStoreRowUpdates(w, st); len(updates) != 0 {
		t.Fatalf("empty status must never clobber, got %v", updates)
	}
}
