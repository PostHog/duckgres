package controlplane

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"gopkg.in/yaml.v3"
	k8syaml "sigs.k8s.io/yaml"
)

// ManagedWarehouseSecretMaterial contains resolved secret values used to render
// the worker-ready runtime artifact and supporting secrets from a managed-warehouse contract.
type ManagedWarehouseSecretMaterial struct {
	WarehouseDatabasePassword string
	MetadataStorePassword     string
	S3AccessKey               string
	S3SecretKey               string
}

// ManagedWarehouseRuntimeArtifactOptions control how runtime artifacts are rendered.
type ManagedWarehouseRuntimeArtifactOptions struct {
	DefaultNamespace string
	DataDir          string
	Extensions       []string
}

type renderedManagedWarehouseRuntimeConfig struct {
	DataDir    string                              `yaml:"data_dir,omitempty"`
	Extensions []string                            `yaml:"extensions,omitempty"`
	DuckLake   renderedManagedWarehouseDuckLakeCfg `yaml:"ducklake"`
}

type renderedManagedWarehouseDuckLakeCfg struct {
	MetadataStore string `yaml:"metadata_store,omitempty"`
	ObjectStore   string `yaml:"object_store,omitempty"`
	S3Provider    string `yaml:"s3_provider,omitempty"`
	S3Endpoint    string `yaml:"s3_endpoint,omitempty"`
	S3AccessKey   string `yaml:"s3_access_key,omitempty"`
	S3SecretKey   string `yaml:"s3_secret_key,omitempty"`
	S3Region      string `yaml:"s3_region,omitempty"`
	S3UseSSL      bool   `yaml:"s3_use_ssl,omitempty"`
	S3URLStyle    string `yaml:"s3_url_style,omitempty"`
}

type renderedS3Credentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

// RenderManagedWarehouseRuntimeConfig renders the worker-ready duckgres.yaml
// implied by a resolved team runtime and resolved secret material. This helper
// is intended for provisioning flows that materialize the runtime_config Secret.
func RenderManagedWarehouseRuntimeConfig(runtime *TeamRuntime, secretMaterial ManagedWarehouseSecretMaterial, opts ManagedWarehouseRuntimeArtifactOptions) ([]byte, error) {
	if runtime == nil {
		return nil, fmt.Errorf("managed warehouse runtime is required")
	}

	metadataStoreDSN, err := buildManagedWarehousePostgresDSN(runtime.MetadataStore.Endpoint, runtime.MetadataStore.Port, runtime.MetadataStore.Username, secretMaterial.MetadataStorePassword, runtime.MetadataStore.DatabaseName)
	if err != nil {
		return nil, fmt.Errorf("metadata store dsn: %w", err)
	}

	cfg := renderedManagedWarehouseRuntimeConfig{
		DataDir:    opts.DataDir,
		Extensions: append([]string(nil), opts.Extensions...),
		DuckLake: renderedManagedWarehouseDuckLakeCfg{
			MetadataStore: metadataStoreDSN,
			ObjectStore:   buildManagedWarehouseObjectStore(runtime.S3),
			S3Endpoint:    runtime.S3.Endpoint,
			S3Region:      runtime.S3.Region,
			S3UseSSL:      runtime.S3.UseSSL,
			S3URLStyle:    runtime.S3.URLStyle,
		},
	}

	switch {
	case secretMaterial.S3AccessKey != "" || secretMaterial.S3SecretKey != "":
		if secretMaterial.S3AccessKey == "" || secretMaterial.S3SecretKey == "" {
			return nil, fmt.Errorf("s3 explicit credentials require both access key and secret key")
		}
		cfg.DuckLake.S3Provider = "config"
		cfg.DuckLake.S3AccessKey = secretMaterial.S3AccessKey
		cfg.DuckLake.S3SecretKey = secretMaterial.S3SecretKey
	case cfg.DuckLake.ObjectStore != "":
		cfg.DuckLake.S3Provider = "aws_sdk"
	}

	rendered, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("marshal managed warehouse runtime config: %w", err)
	}
	return rendered, nil
}

// BuildManagedWarehouseRuntimeArtifacts renders the provisioning-side artifact
// set implied by a team's managed-warehouse contract: the worker service account,
// DSN secrets, credential secret, and the worker-ready runtime_config Secret.
func BuildManagedWarehouseRuntimeArtifacts(team *configstore.TeamConfig, secretMaterial ManagedWarehouseSecretMaterial, opts ManagedWarehouseRuntimeArtifactOptions) ([]byte, error) {
	runtime, ok := ResolveTeamRuntime(team)
	if !ok {
		return nil, fmt.Errorf("team %q has no managed warehouse runtime", teamName(team))
	}

	namespace := opts.DefaultNamespace
	if runtime.WorkerIdentity.Namespace != "" {
		namespace = runtime.WorkerIdentity.Namespace
	}
	if namespace == "" {
		return nil, fmt.Errorf("team %s runtime namespace is required", runtime.TeamName)
	}

	runtimeConfig, err := RenderManagedWarehouseRuntimeConfig(runtime, secretMaterial, opts)
	if err != nil {
		return nil, err
	}

	var objects []any

	if runtime.WorkerIdentity.ServiceAccountName != "" {
		objects = append(objects, &corev1.ServiceAccount{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "ServiceAccount",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      runtime.WorkerIdentity.ServiceAccountName,
				Namespace: namespace,
			},
		})
	}

	if runtime.WarehouseDatabaseCredentials.Name != "" {
		dsn, err := buildManagedWarehousePostgresDSN(runtime.WarehouseDatabase.Endpoint, runtime.WarehouseDatabase.Port, runtime.WarehouseDatabase.Username, secretMaterial.WarehouseDatabasePassword, runtime.WarehouseDatabase.DatabaseName)
		if err != nil {
			return nil, fmt.Errorf("warehouse database dsn: %w", err)
		}
		objects = append(objects, buildStringSecret(secretNamespace(runtime.WarehouseDatabaseCredentials, namespace), runtime.WarehouseDatabaseCredentials.Name, runtime.WarehouseDatabaseCredentials.Key, dsn))
	}

	if runtime.MetadataStoreCredentials.Name != "" {
		dsn, err := buildManagedWarehousePostgresDSN(runtime.MetadataStore.Endpoint, runtime.MetadataStore.Port, runtime.MetadataStore.Username, secretMaterial.MetadataStorePassword, runtime.MetadataStore.DatabaseName)
		if err != nil {
			return nil, fmt.Errorf("metadata store dsn secret: %w", err)
		}
		objects = append(objects, buildStringSecret(secretNamespace(runtime.MetadataStoreCredentials, namespace), runtime.MetadataStoreCredentials.Name, runtime.MetadataStoreCredentials.Key, dsn))
	}

	if runtime.S3Credentials.Name != "" {
		if secretMaterial.S3AccessKey == "" || secretMaterial.S3SecretKey == "" {
			return nil, fmt.Errorf("team %s s3 secret ref requires explicit credential material", runtime.TeamName)
		}
		payload, err := json.Marshal(renderedS3Credentials{
			AccessKeyID:     secretMaterial.S3AccessKey,
			SecretAccessKey: secretMaterial.S3SecretKey,
		})
		if err != nil {
			return nil, fmt.Errorf("marshal s3 credential payload: %w", err)
		}
		objects = append(objects, buildStringSecret(secretNamespace(runtime.S3Credentials, namespace), runtime.S3Credentials.Name, runtime.S3Credentials.Key, string(payload)))
	}

	if runtime.RuntimeConfig.Name == "" || runtime.RuntimeConfig.Key == "" {
		return nil, fmt.Errorf("team %s runtime_config secret ref is required", runtime.TeamName)
	}
	objects = append(objects, buildStringSecret(secretNamespace(runtime.RuntimeConfig, namespace), runtime.RuntimeConfig.Name, runtime.RuntimeConfig.Key, string(runtimeConfig)))

	return marshalManifestDocs(objects)
}

func buildManagedWarehousePostgresDSN(host string, port int, username, password, database string) (string, error) {
	if host == "" {
		return "", fmt.Errorf("host is required")
	}
	if port == 0 {
		return "", fmt.Errorf("port is required")
	}
	if username == "" {
		return "", fmt.Errorf("username is required")
	}
	if password == "" {
		return "", fmt.Errorf("password is required")
	}
	if database == "" {
		return "", fmt.Errorf("database name is required")
	}

	return fmt.Sprintf("postgres:host=%s port=%d user=%s password=%s dbname=%s", host, port, username, password, database), nil
}

func buildManagedWarehouseObjectStore(s3 configstore.ManagedWarehouseS3) string {
	if s3.Bucket == "" {
		return ""
	}

	prefix := strings.Trim(s3.PathPrefix, "/")
	if prefix == "" {
		return fmt.Sprintf("s3://%s/", s3.Bucket)
	}
	return fmt.Sprintf("s3://%s/%s/", s3.Bucket, prefix)
}

func buildStringSecret(namespace, name, key, value string) *corev1.Secret {
	return &corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{key: value},
	}
}

func secretNamespace(ref configstore.SecretRef, fallback string) string {
	if ref.Namespace != "" {
		return ref.Namespace
	}
	return fallback
}

func marshalManifestDocs(objects []any) ([]byte, error) {
	rendered := make([]string, 0, len(objects))
	for _, obj := range objects {
		doc, err := k8syaml.Marshal(obj)
		if err != nil {
			return nil, fmt.Errorf("marshal manifest doc: %w", err)
		}
		rendered = append(rendered, strings.TrimSpace(string(doc)))
	}
	return []byte(strings.Join(rendered, "\n---\n") + "\n"), nil
}

func teamName(team *configstore.TeamConfig) string {
	if team == nil {
		return ""
	}
	return team.Name
}
