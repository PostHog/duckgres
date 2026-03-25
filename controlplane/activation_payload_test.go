//go:build kubernetes

package controlplane

import (
	"context"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildTenantActivationPayloadBuildsDuckLakeRuntimeFromWarehouseSecrets(t *testing.T) {
	pool, cs := newTestK8sPool(t, 5)

	_, err := cs.CoreV1().Secrets("default").Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "analytics-metadata", Namespace: "default"},
		StringData: map[string]string{"dsn": "metadata-password"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create metadata secret: %v", err)
	}

	_, err = cs.CoreV1().Secrets("default").Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "analytics-s3", Namespace: "default"},
		StringData: map[string]string{"credentials": `{"access_key_id":"key","secret_access_key":"secret"}`},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create s3 secret: %v", err)
	}

	org := &configstore.OrgConfig{
		Name: "analytics",
		Users: map[string]string{
			"alice": "ignored",
			"bob":   "ignored",
		},
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint:     "metadata.internal",
				Port:         5432,
				DatabaseName: "ducklake",
				Username:     "ducklake",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Name: "analytics-metadata",
				Key:  "dsn",
			},
			S3: configstore.ManagedWarehouseS3{
				Provider:   "minio",
				Region:     "us-east-1",
				Bucket:     "analytics-bucket",
				PathPrefix: "teams/analytics/",
				Endpoint:   "minio.internal:9000",
				URLStyle:   "path",
			},
			S3Credentials: configstore.SecretRef{
				Name: "analytics-s3",
				Key:  "credentials",
			},
		},
	}

	payload, err := BuildTenantActivationPayload(context.Background(), pool.clientset, pool.namespace, org, nil)
	if err != nil {
		t.Fatalf("BuildTenantActivationPayload: %v", err)
	}

	if payload.OrgID != "analytics" {
		t.Fatalf("expected org analytics, got %q", payload.OrgID)
	}
	if len(payload.Usernames) != 2 {
		t.Fatalf("expected two users, got %v", payload.Usernames)
	}
	if got := payload.DuckLake.MetadataStore; got != "postgres:host=metadata.internal port=5432 user=ducklake password=metadata-password dbname=ducklake" {
		t.Fatalf("unexpected metadata store: %q", got)
	}
	if got := payload.DuckLake.ObjectStore; got != "s3://analytics-bucket/teams/analytics/" {
		t.Fatalf("unexpected object store: %q", got)
	}
	if payload.DuckLake.S3AccessKey != "key" || payload.DuckLake.S3SecretKey != "secret" {
		t.Fatalf("expected explicit s3 credentials, got %#v", payload.DuckLake)
	}
}
