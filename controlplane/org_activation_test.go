//go:build kubernetes

package controlplane

import (
	"context"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestSharedWorkerActivatorBuildsActivationRequestFromManagedWarehouse(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "tenant-a",
				Name:      "analytics-metadata",
			},
			Data: map[string][]byte{
				"dsn": []byte("metadata-password"),
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "tenant-a",
				Name:      "analytics-s3",
			},
			Data: map[string][]byte{
				"creds": []byte(`{"access_key_id":"abc","secret_access_key":"xyz"}`),
			},
		},
	)

	activator := &SharedWorkerActivator{
		clientset:        clientset,
		defaultNamespace: "duckgres-workers",
	}

	org := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint:     "metadata.example.internal",
				Port:         5432,
				Username:     "ducklake_user",
				DatabaseName: "ducklake_metadata",
			},
			S3: configstore.ManagedWarehouseS3{
				Provider:   "aws",
				Region:     "us-east-2",
				Bucket:     "tenant-bucket",
				PathPrefix: "analytics",
				Endpoint:   "s3.us-east-2.amazonaws.com",
				UseSSL:     true,
				URLStyle:   "path",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Namespace: "tenant-a",
				Name:      "analytics-metadata",
				Key:       "dsn",
			},
			S3Credentials: configstore.SecretRef{
				Namespace: "tenant-a",
				Name:      "analytics-s3",
				Key:       "creds",
			},
		},
	}

	assignment := &WorkerAssignment{
		OrgID:          "analytics",
		LeaseExpiresAt: time.Date(2026, time.March, 22, 12, 0, 0, 0, time.UTC),
	}

	req, err := activator.BuildActivationRequest(context.Background(), org, assignment)
	if err != nil {
		t.Fatalf("BuildActivationRequest: %v", err)
	}

	if req.OrgID != "analytics" {
		t.Fatalf("expected org analytics, got %q", req.OrgID)
	}
	if !req.LeaseExpiresAt.Equal(assignment.LeaseExpiresAt) {
		t.Fatalf("expected lease expiry %v, got %v", assignment.LeaseExpiresAt, req.LeaseExpiresAt)
	}
	if got := req.DuckLake.MetadataStore; got != "postgres:host=metadata.example.internal port=5432 user=ducklake_user password=metadata-password dbname=ducklake_metadata" {
		t.Fatalf("unexpected metadata store dsn: %q", got)
	}
	if got := req.DuckLake.ObjectStore; got != "s3://tenant-bucket/analytics/" {
		t.Fatalf("unexpected object store: %q", got)
	}
	if req.DuckLake.S3Provider != "config" {
		t.Fatalf("expected config s3 provider, got %q", req.DuckLake.S3Provider)
	}
	if req.DuckLake.S3AccessKey != "abc" || req.DuckLake.S3SecretKey != "xyz" {
		t.Fatalf("unexpected s3 credentials: %#v", req.DuckLake)
	}
}

func TestSharedWorkerActivatorRequiresManagedWarehouse(t *testing.T) {
	activator := &SharedWorkerActivator{
		clientset:        fake.NewSimpleClientset(),
		defaultNamespace: "duckgres-workers",
	}

	_, err := activator.BuildActivationRequest(context.Background(), &configstore.OrgConfig{Name: "analytics"}, &WorkerAssignment{
		OrgID:          "analytics",
		LeaseExpiresAt: time.Now().Add(time.Hour),
	})
	if err == nil {
		t.Fatal("expected missing warehouse to fail")
	}
}

func TestSharedWorkerActivatorActivateReservedWorkerUsesLatestResolvedOrgConfig(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "tenant-a",
				Name:      "analytics-metadata-old",
			},
			Data: map[string][]byte{
				"dsn": []byte("old-password"),
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "tenant-a",
				Name:      "analytics-metadata-new",
			},
			Data: map[string][]byte{
				"dsn": []byte("new-password"),
			},
		},
	)

	worker := &ManagedWorker{ID: 7, done: make(chan struct{})}
	leaseExpiry := time.Date(2026, time.March, 22, 12, 0, 0, 0, time.UTC)
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			OrgID:          "analytics",
			LeaseExpiresAt: leaseExpiry,
		},
	}); err != nil {
		t.Fatalf("SetSharedState: %v", err)
	}

	currentOrg := &configstore.OrgConfig{
		Name: "analytics",
		Users: map[string]string{
			"alice": "ignored",
		},
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint:     "old-metadata.example.internal",
				Port:         5432,
				Username:     "ducklake_user",
				DatabaseName: "ducklake_metadata",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Namespace: "tenant-a",
				Name:      "analytics-metadata-old",
				Key:       "dsn",
			},
		},
	}

	var captured TenantActivationPayload
	activator := &SharedWorkerActivator{
		clientset:        clientset,
		defaultNamespace: "duckgres-workers",
		resolveOrgConfig: func(orgID string) (*configstore.OrgConfig, error) {
			if orgID != "analytics" {
				t.Fatalf("expected analytics org lookup, got %q", orgID)
			}
			return currentOrg, nil
		},
		activateReservedWorker: func(ctx context.Context, got *ManagedWorker, payload TenantActivationPayload) error {
			if got.ID != worker.ID {
				t.Fatalf("expected worker %d, got %d", worker.ID, got.ID)
			}
			captured = payload
			return nil
		},
	}

	currentOrg = &configstore.OrgConfig{
		Name: "analytics",
		Users: map[string]string{
			"bob": "ignored",
		},
		Warehouse: &configstore.ManagedWarehouseConfig{
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint:     "new-metadata.example.internal",
				Port:         5432,
				Username:     "ducklake_user",
				DatabaseName: "ducklake_metadata",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Namespace: "tenant-a",
				Name:      "analytics-metadata-new",
				Key:       "dsn",
			},
		},
	}

	if err := activator.ActivateReservedWorker(context.Background(), worker); err != nil {
		t.Fatalf("ActivateReservedWorker: %v", err)
	}

	if captured.OrgID != "analytics" {
		t.Fatalf("expected org analytics, got %q", captured.OrgID)
	}
	if !captured.LeaseExpiresAt.Equal(leaseExpiry) {
		t.Fatalf("expected lease expiry %v, got %v", leaseExpiry, captured.LeaseExpiresAt)
	}
	if len(captured.Usernames) != 1 || captured.Usernames[0] != "bob" {
		t.Fatalf("expected latest users to be captured, got %#v", captured.Usernames)
	}
	if got := captured.DuckLake.MetadataStore; got != "postgres:host=new-metadata.example.internal port=5432 user=ducklake_user password=new-password dbname=ducklake_metadata" {
		t.Fatalf("expected latest warehouse runtime in activation payload, got %q", got)
	}
}
