package controlplane

import (
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"gopkg.in/yaml.v3"
)

type manifestDoc struct {
	Kind       string `yaml:"kind"`
	Type       string `yaml:"type"`
	Metadata   struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	StringData map[string]string `yaml:"stringData"`
}

type renderedRuntimeConfig struct {
	DataDir    string   `yaml:"data_dir"`
	Extensions []string `yaml:"extensions"`
	DuckLake   struct {
		MetadataStore string `yaml:"metadata_store"`
		ObjectStore   string `yaml:"object_store"`
		S3Provider    string `yaml:"s3_provider"`
		S3Endpoint    string `yaml:"s3_endpoint"`
		S3AccessKey   string `yaml:"s3_access_key"`
		S3SecretKey   string `yaml:"s3_secret_key"`
		S3SessionToken string `yaml:"s3_session_token"`
		S3Region      string `yaml:"s3_region"`
		S3UseSSL      bool   `yaml:"s3_use_ssl"`
		S3URLStyle    string `yaml:"s3_url_style"`
	} `yaml:"ducklake"`
}

func TestBuildManagedWarehouseRuntimeArtifactsRendersSecretsFromContract(t *testing.T) {
	team := &configstore.TeamConfig{
		Name: "local",
		Warehouse: &configstore.ManagedWarehouseConfig{
			WarehouseDatabase: configstore.ManagedWarehouseDatabase{
				Endpoint:     "warehouse.internal",
				Port:         5432,
				DatabaseName: "warehouse_local",
				Username:     "warehouse_user",
			},
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint:     "metadata.internal",
				Port:         5433,
				DatabaseName: "ducklake_metadata_local",
				Username:     "ducklake_user",
			},
			S3: configstore.ManagedWarehouseS3{
				Provider:   "minio",
				Region:     "us-east-1",
				Bucket:     "duckgres-local",
				PathPrefix: "teams/local",
				Endpoint:   "minio.internal:9000",
				URLStyle:   "path",
			},
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace:          "duckgres",
				ServiceAccountName: "duckgres-local-worker",
			},
			WarehouseDatabaseCredentials: configstore.SecretRef{
				Name: "duckgres-local-warehouse-db",
				Key:  "dsn",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Name: "duckgres-local-metadata",
				Key:  "dsn",
			},
			S3Credentials: configstore.SecretRef{
				Name: "duckgres-local-s3",
				Key:  "credentials",
			},
			RuntimeConfig: configstore.SecretRef{
				Name: "duckgres-local-runtime",
				Key:  "duckgres.yaml",
			},
		},
	}

	manifest, err := BuildManagedWarehouseRuntimeArtifacts(team, ManagedWarehouseSecretMaterial{
		WarehouseDatabasePassword: "warehouse-pass",
		MetadataStorePassword:     "metadata-pass",
		S3AccessKey:               "minioadmin",
		S3SecretKey:               "miniosecret",
	}, ManagedWarehouseRuntimeArtifactOptions{
		DefaultNamespace: "duckgres",
		DataDir:          "/data/runtime-secret",
		Extensions:       []string{"ducklake", "httpfs"},
	})
	if err != nil {
		t.Fatalf("BuildManagedWarehouseRuntimeArtifacts: %v", err)
	}

	docs := decodeManifestDocs(t, manifest)
	if len(docs) != 5 {
		t.Fatalf("expected 5 manifest docs, got %d", len(docs))
	}

	var (
		serviceAccount *manifestDoc
		warehouseDB    *manifestDoc
		metadataStore  *manifestDoc
		s3Credentials  *manifestDoc
		runtimeConfig  *manifestDoc
	)
	for i := range docs {
		doc := &docs[i]
		switch doc.Metadata.Name {
		case "duckgres-local-worker":
			serviceAccount = doc
		case "duckgres-local-warehouse-db":
			warehouseDB = doc
		case "duckgres-local-metadata":
			metadataStore = doc
		case "duckgres-local-s3":
			s3Credentials = doc
		case "duckgres-local-runtime":
			runtimeConfig = doc
		}
	}

	if serviceAccount == nil || serviceAccount.Kind != "ServiceAccount" {
		t.Fatalf("expected service account manifest, got %#v", serviceAccount)
	}
	if serviceAccount.Metadata.Namespace != "duckgres" {
		t.Fatalf("expected service account namespace duckgres, got %q", serviceAccount.Metadata.Namespace)
	}

	if warehouseDB == nil || warehouseDB.Kind != "Secret" {
		t.Fatalf("expected warehouse db secret manifest, got %#v", warehouseDB)
	}
	if got := warehouseDB.StringData["dsn"]; got != "postgres:host=warehouse.internal port=5432 user=warehouse_user password=warehouse-pass dbname=warehouse_local" {
		t.Fatalf("unexpected warehouse dsn %q", got)
	}

	if metadataStore == nil || metadataStore.Kind != "Secret" {
		t.Fatalf("expected metadata secret manifest, got %#v", metadataStore)
	}
	if got := metadataStore.StringData["dsn"]; got != "postgres:host=metadata.internal port=5433 user=ducklake_user password=metadata-pass dbname=ducklake_metadata_local" {
		t.Fatalf("unexpected metadata dsn %q", got)
	}

	if s3Credentials == nil || s3Credentials.Kind != "Secret" {
		t.Fatalf("expected s3 secret manifest, got %#v", s3Credentials)
	}
	if got := s3Credentials.StringData["credentials"]; !strings.Contains(got, `"access_key_id":"minioadmin"`) || !strings.Contains(got, `"secret_access_key":"miniosecret"`) {
		t.Fatalf("unexpected s3 credentials payload %q", got)
	}

	if runtimeConfig == nil || runtimeConfig.Kind != "Secret" {
		t.Fatalf("expected runtime config secret manifest, got %#v", runtimeConfig)
	}

	var cfg renderedRuntimeConfig
	if err := yaml.Unmarshal([]byte(runtimeConfig.StringData["duckgres.yaml"]), &cfg); err != nil {
		t.Fatalf("unmarshal duckgres.yaml: %v", err)
	}
	if cfg.DataDir != "/data/runtime-secret" {
		t.Fatalf("expected data_dir /data/runtime-secret, got %q", cfg.DataDir)
	}
	if strings.Join(cfg.Extensions, ",") != "ducklake,httpfs" {
		t.Fatalf("unexpected extensions %#v", cfg.Extensions)
	}
	if cfg.DuckLake.MetadataStore != "postgres:host=metadata.internal port=5433 user=ducklake_user password=metadata-pass dbname=ducklake_metadata_local" {
		t.Fatalf("unexpected ducklake metadata store %q", cfg.DuckLake.MetadataStore)
	}
	if cfg.DuckLake.ObjectStore != "s3://duckgres-local/teams/local/" {
		t.Fatalf("unexpected ducklake object store %q", cfg.DuckLake.ObjectStore)
	}
	if cfg.DuckLake.S3Provider != "config" {
		t.Fatalf("expected s3_provider config, got %q", cfg.DuckLake.S3Provider)
	}
	if cfg.DuckLake.S3Endpoint != "minio.internal:9000" {
		t.Fatalf("unexpected s3 endpoint %q", cfg.DuckLake.S3Endpoint)
	}
	if cfg.DuckLake.S3AccessKey != "minioadmin" || cfg.DuckLake.S3SecretKey != "miniosecret" {
		t.Fatalf("unexpected s3 explicit credentials in runtime config: %#v", cfg.DuckLake)
	}
}

func TestRenderManagedWarehouseRuntimeConfigPrefersIdentityBasedS3WhenNoExplicitCredentials(t *testing.T) {
	runtime := &TeamRuntime{
		TeamName: "analytics",
		MetadataStore: configstore.ManagedWarehouseMetadataStore{
			Endpoint:     "aurora.internal",
			Port:         5432,
			DatabaseName: "ducklake_analytics",
			Username:     "ducklake_user",
		},
		S3: configstore.ManagedWarehouseS3{
			Provider:   "aws",
			Region:     "us-east-1",
			Bucket:     "analytics-bucket",
			PathPrefix: "warehouse/analytics",
		},
		WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
			ServiceAccountName: "analytics-worker",
			IAMRoleARN:         "arn:aws:iam::123456789012:role/analytics-worker",
		},
	}

	rendered, err := RenderManagedWarehouseRuntimeConfig(runtime, ManagedWarehouseSecretMaterial{
		MetadataStorePassword: "metadata-pass",
	}, ManagedWarehouseRuntimeArtifactOptions{
		DataDir:    "/data",
		Extensions: []string{"ducklake"},
	})
	if err != nil {
		t.Fatalf("RenderManagedWarehouseRuntimeConfig: %v", err)
	}

	var cfg renderedRuntimeConfig
	if err := yaml.Unmarshal(rendered, &cfg); err != nil {
		t.Fatalf("unmarshal rendered config: %v", err)
	}
	if cfg.DuckLake.S3Provider != "aws_sdk" {
		t.Fatalf("expected aws_sdk provider, got %q", cfg.DuckLake.S3Provider)
	}
	if cfg.DuckLake.S3AccessKey != "" || cfg.DuckLake.S3SecretKey != "" {
		t.Fatalf("expected no explicit s3 credentials, got %#v", cfg.DuckLake)
	}
	if cfg.DuckLake.ObjectStore != "s3://analytics-bucket/warehouse/analytics/" {
		t.Fatalf("unexpected object store %q", cfg.DuckLake.ObjectStore)
	}
}

func decodeManifestDocs(t *testing.T, manifest []byte) []manifestDoc {
	t.Helper()

	parts := strings.Split(string(manifest), "\n---\n")
	docs := make([]manifestDoc, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		var doc manifestDoc
		if err := yaml.Unmarshal([]byte(part), &doc); err != nil {
			t.Fatalf("unmarshal manifest doc: %v\n%s", err, part)
		}
		docs = append(docs, doc)
	}
	return docs
}
