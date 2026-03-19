package controlplane

import (
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestResolveTeamRuntimeReturnsNoRuntimeWithoutWarehouse(t *testing.T) {
	runtime, ok := ResolveTeamRuntime(&configstore.TeamConfig{
		Name: "ingestion",
	})
	if ok {
		t.Fatal("expected no runtime when team has no managed warehouse")
	}
	if runtime != nil {
		t.Fatalf("expected nil runtime, got %#v", runtime)
	}
}

func TestResolveTeamRuntimeCopiesManagedWarehouseInputs(t *testing.T) {
	team := &configstore.TeamConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			TeamName: "analytics",
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace:          "duckgres-analytics",
				ServiceAccountName: "analytics-worker",
				IAMRoleARN:         "arn:aws:iam::123456789012:role/analytics-worker",
			},
			WarehouseDatabase: configstore.ManagedWarehouseDatabase{
				Region:       "us-east-1",
				Endpoint:     "analytics-db.internal",
				Port:         5432,
				DatabaseName: "analytics_wh",
				Username:     "warehouse_user",
			},
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Kind:         "dedicated_rds",
				Engine:       "postgres",
				Region:       "us-east-1",
				Endpoint:     "analytics-meta.internal",
				Port:         5432,
				DatabaseName: "ducklake_metadata",
				Username:     "ducklake_user",
			},
			S3: configstore.ManagedWarehouseS3{
				Provider:   "aws",
				Region:     "us-east-1",
				Bucket:     "analytics-warehouse",
				PathPrefix: "ducklake/",
				Endpoint:   "s3.us-east-1.amazonaws.com",
				UseSSL:     true,
				URLStyle:   "vhost",
			},
			WarehouseDatabaseCredentials: configstore.SecretRef{
				Namespace: "duckgres-analytics",
				Name:      "analytics-warehouse-db",
				Key:       "dsn",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Namespace: "duckgres-analytics",
				Name:      "analytics-metadata",
				Key:       "dsn",
			},
			S3Credentials: configstore.SecretRef{
				Namespace: "duckgres-analytics",
				Name:      "analytics-s3",
				Key:       "credentials",
			},
			RuntimeConfig: configstore.SecretRef{
				Namespace: "duckgres-analytics",
				Name:      "analytics-runtime",
				Key:       "duckgres.yaml",
			},
		},
	}

	runtime, ok := ResolveTeamRuntime(team)
	if !ok {
		t.Fatal("expected runtime to be resolved")
	}
	if runtime == nil {
		t.Fatal("expected runtime to be non-nil")
	}
	if runtime.TeamName != "analytics" {
		t.Fatalf("expected team analytics, got %q", runtime.TeamName)
	}
	if runtime.WorkerIdentity.ServiceAccountName != "analytics-worker" {
		t.Fatalf("expected service account analytics-worker, got %q", runtime.WorkerIdentity.ServiceAccountName)
	}
	if runtime.WorkerIdentity.Namespace != "duckgres-analytics" {
		t.Fatalf("expected namespace duckgres-analytics, got %q", runtime.WorkerIdentity.Namespace)
	}
	if runtime.RuntimeConfig.Name != "analytics-runtime" {
		t.Fatalf("expected runtime secret analytics-runtime, got %q", runtime.RuntimeConfig.Name)
	}
	if runtime.MetadataStore.Endpoint != "analytics-meta.internal" {
		t.Fatalf("expected metadata endpoint analytics-meta.internal, got %q", runtime.MetadataStore.Endpoint)
	}
	if runtime.S3.Bucket != "analytics-warehouse" {
		t.Fatalf("expected bucket analytics-warehouse, got %q", runtime.S3.Bucket)
	}

	team.Warehouse.WorkerIdentity.ServiceAccountName = "mutated"
	team.Warehouse.RuntimeConfig.Name = "mutated-runtime"
	team.Warehouse.S3.Bucket = "mutated-bucket"

	if runtime.WorkerIdentity.ServiceAccountName != "analytics-worker" {
		t.Fatalf("expected runtime to be independent of source mutations, got %q", runtime.WorkerIdentity.ServiceAccountName)
	}
	if runtime.RuntimeConfig.Name != "analytics-runtime" {
		t.Fatalf("expected runtime secret to be independent of source mutations, got %q", runtime.RuntimeConfig.Name)
	}
	if runtime.S3.Bucket != "analytics-warehouse" {
		t.Fatalf("expected bucket to be independent of source mutations, got %q", runtime.S3.Bucket)
	}
}

func TestApplyTeamRuntimeToPoolConfigOverridesWorkerRuntime(t *testing.T) {
	base := K8sWorkerPoolConfig{
		Namespace:      "shared-workers",
		ConfigMap:      "shared-duckgres-config",
		ConfigPath:     "/etc/duckgres/duckgres.yaml",
		ServiceAccount: "default",
	}
	runtime := &TeamRuntime{
		TeamName: "analytics",
		WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
			Namespace:          "team-analytics",
			ServiceAccountName: "analytics-worker",
		},
		RuntimeConfig: configstore.SecretRef{
			Namespace: "team-analytics",
			Name:      "analytics-runtime",
			Key:       "duckgres.yaml",
		},
	}

	got, err := ApplyTeamRuntimeToPoolConfig(base, runtime)
	if err != nil {
		t.Fatalf("ApplyTeamRuntimeToPoolConfig: %v", err)
	}
	if got.Namespace != "team-analytics" {
		t.Fatalf("expected namespace team-analytics, got %q", got.Namespace)
	}
	if got.ServiceAccount != "analytics-worker" {
		t.Fatalf("expected service account analytics-worker, got %q", got.ServiceAccount)
	}
	if got.ConfigMap != "" {
		t.Fatalf("expected config map to be cleared, got %q", got.ConfigMap)
	}
	if got.ConfigSecretName != "analytics-runtime" {
		t.Fatalf("expected config secret analytics-runtime, got %q", got.ConfigSecretName)
	}
	if got.ConfigSecretKey != "duckgres.yaml" {
		t.Fatalf("expected config secret key duckgres.yaml, got %q", got.ConfigSecretKey)
	}
	if got.ConfigPath != teamRuntimeConfigPath {
		t.Fatalf("expected config path %q, got %q", teamRuntimeConfigPath, got.ConfigPath)
	}
}

func TestApplyTeamRuntimeToPoolConfigRejectsInvalidRuntimeConfig(t *testing.T) {
	base := K8sWorkerPoolConfig{Namespace: "team-analytics"}

	_, err := ApplyTeamRuntimeToPoolConfig(base, &TeamRuntime{
		TeamName: "analytics",
		RuntimeConfig: configstore.SecretRef{
			Name: "analytics-runtime",
		},
	})
	if err == nil {
		t.Fatal("expected missing runtime config key to fail")
	}

	_, err = ApplyTeamRuntimeToPoolConfig(base, &TeamRuntime{
		TeamName: "analytics",
		RuntimeConfig: configstore.SecretRef{
			Namespace: "different-namespace",
			Name:      "analytics-runtime",
			Key:       "duckgres.yaml",
		},
	})
	if err == nil {
		t.Fatal("expected namespace mismatch to fail")
	}
}
