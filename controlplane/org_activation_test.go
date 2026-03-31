//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
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
			OrgID: "analytics",
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
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace: "tenant-a",
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

	assignment := &WorkerAssignment{OrgID: "analytics"}

	req, err := activator.BuildActivationRequest(context.Background(), org, assignment)
	if err != nil {
		t.Fatalf("BuildActivationRequest: %v", err)
	}

	if req.OrgID != "analytics" {
		t.Fatalf("expected org analytics, got %q", req.OrgID)
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
		OrgID: "analytics",
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
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle: WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{
			OrgID: "analytics",
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
			OrgID: "analytics",
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint:     "old-metadata.example.internal",
				Port:         5432,
				Username:     "ducklake_user",
				DatabaseName: "ducklake_metadata",
			},
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace: "tenant-a",
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
			OrgID: "analytics",
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint:     "new-metadata.example.internal",
				Port:         5432,
				Username:     "ducklake_user",
				DatabaseName: "ducklake_metadata",
			},
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace: "tenant-a",
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
	if len(captured.Usernames) != 1 || captured.Usernames[0] != "bob" {
		t.Fatalf("expected latest users to be captured, got %#v", captured.Usernames)
	}
	if got := captured.DuckLake.MetadataStore; got != "postgres:host=new-metadata.example.internal port=5432 user=ducklake_user password=new-password dbname=ducklake_metadata" {
		t.Fatalf("expected latest warehouse runtime in activation payload, got %q", got)
	}
}

func TestSharedWorkerActivatorDucklingCRRequiresSTSBroker(t *testing.T) {
	activator := &SharedWorkerActivator{
		clientset:        fake.NewSimpleClientset(),
		defaultNamespace: "duckgres-workers",
		resolveDucklingStatus: func(ctx context.Context, orgID string) (*provisioner.DucklingStatus, error) {
			if orgID != "test-org" {
				t.Fatalf("expected test-org, got %q", orgID)
			}
			return &provisioner.DucklingStatus{
				MetadataStore: struct {
					Type     string
					Endpoint string
					Password string
					User     string
					Database string
				}{
					Endpoint: "test-org.cluster.rds.amazonaws.com",
					Password: "duckling-password-123",
					User:     "postgres",
					Database: "postgres",
				},
				DataStore: struct {
					Type       string
					BucketName string
					S3Region   string
				}{
					BucketName: "posthog-duckling-test-org",
					S3Region:   "us-east-1",
				},
				IAMRoleARN: "arn:aws:iam::123:role/duckling-test-org",
			}, nil
		},
	}

	org := &configstore.OrgConfig{
		Name:      "test-org",
		Users:     map[string]string{"testuser": "hash"},
		Warehouse: &configstore.ManagedWarehouseConfig{
			// SecretRef intentionally empty — simulates Crossplane-provisioned duckling
		},
	}

	_, err := activator.BuildActivationRequest(context.Background(), org, &WorkerAssignment{OrgID: "test-org"})
	if err == nil {
		t.Fatal("expected shared warm activation to fail without an STS broker")
	}
}

func TestSharedWorkerActivatorPrefersSecretRefOverDucklingCR(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "org-with-secretref-metadata"},
			Data:       map[string][]byte{"pw": []byte("secret-ref-password")},
		},
	)

	ducklingCalled := false
	activator := &SharedWorkerActivator{
		clientset:        clientset,
		defaultNamespace: "ns",
		resolveDucklingStatus: func(ctx context.Context, orgID string) (*provisioner.DucklingStatus, error) {
			ducklingCalled = true
			return &provisioner.DucklingStatus{
				MetadataStore: struct {
					Type     string
					Endpoint string
					Password string
					User     string
					Database string
				}{Password: "cr-password"},
			}, nil
		},
	}

	org := &configstore.OrgConfig{
		Name: "org-with-secretref",
		Warehouse: &configstore.ManagedWarehouseConfig{
			OrgID: "org-with-secretref",
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "host", Port: 5432, Username: "u", DatabaseName: "db",
			},
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace: "ns",
			},
			MetadataStoreCredentials: configstore.SecretRef{Namespace: "ns", Name: "org-with-secretref-metadata", Key: "pw"},
		},
	}

	req, err := activator.BuildActivationRequest(context.Background(), org, &WorkerAssignment{OrgID: "org-with-secretref"})
	if err != nil {
		t.Fatalf("BuildActivationRequest: %v", err)
	}

	// Duckling CR path is tried first but the config store fallback should not be needed.
	// However, the CR resolver IS called since it's non-nil. The key check is that the
	// final DSN uses the correct password.
	_ = ducklingCalled
	if got := req.DuckLake.MetadataStore; got != "postgres:host=host port=5432 user=u password=secret-ref-password dbname=db" {
		t.Fatalf("expected secret-ref password in DSN, got %q", got)
	}
}

func TestSharedWorkerActivatorDucklingCRErrorFallsBackToConfigStore(t *testing.T) {
	clientset := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "fallback-org-metadata"},
			Data:       map[string][]byte{"pw": []byte("fallback-password")},
		},
	)

	activator := &SharedWorkerActivator{
		clientset:        clientset,
		defaultNamespace: "ns",
		resolveDucklingStatus: func(ctx context.Context, orgID string) (*provisioner.DucklingStatus, error) {
			return nil, fmt.Errorf("duckling CR not found")
		},
	}

	org := &configstore.OrgConfig{
		Name: "fallback-org",
		Warehouse: &configstore.ManagedWarehouseConfig{
			OrgID: "fallback-org",
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "host", Port: 5432, Username: "u", DatabaseName: "db",
			},
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace: "ns",
			},
			MetadataStoreCredentials: configstore.SecretRef{Namespace: "ns", Name: "fallback-org-metadata", Key: "pw"},
		},
	}

	req, err := activator.BuildActivationRequest(context.Background(), org, &WorkerAssignment{OrgID: "fallback-org"})
	if err != nil {
		t.Fatalf("expected fallback to config store, got error: %v", err)
	}

	if got := req.DuckLake.MetadataStore; got != "postgres:host=host port=5432 user=u password=fallback-password dbname=db" {
		t.Fatalf("expected fallback password in DSN, got %q", got)
	}
}

func TestSharedWorkerActivatorRejectsSecretRefOutsideTenantScope(t *testing.T) {
	activator := &SharedWorkerActivator{
		clientset:        fake.NewSimpleClientset(),
		defaultNamespace: "duckgres-workers",
	}

	org := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			OrgID: "analytics",
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "host", Port: 5432, Username: "u", DatabaseName: "db",
			},
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace: "tenant-a",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Namespace: "tenant-b",
				Name:      "billing-metadata",
				Key:       "dsn",
			},
		},
	}

	_, err := activator.BuildActivationRequest(context.Background(), org, &WorkerAssignment{OrgID: "analytics"})
	if err == nil {
		t.Fatal("expected cross-tenant secret ref to be rejected")
	}
}

func TestSharedWorkerActivatorRejectsSecretRefOutsideTenantPrefix(t *testing.T) {
	activator := &SharedWorkerActivator{
		clientset:        fake.NewSimpleClientset(),
		defaultNamespace: "duckgres-workers",
	}

	org := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			OrgID: "analytics",
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "host", Port: 5432, Username: "u", DatabaseName: "db",
			},
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace: "tenant-a",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Namespace: "tenant-a",
				Name:      "shared-metadata",
				Key:       "dsn",
			},
		},
	}

	_, err := activator.BuildActivationRequest(context.Background(), org, &WorkerAssignment{OrgID: "analytics"})
	if err == nil {
		t.Fatal("expected non-org-prefixed secret ref to be rejected")
	}
}

func TestSharedWorkerActivatorRejectsSecretRefWithoutTenantNamespace(t *testing.T) {
	activator := &SharedWorkerActivator{
		clientset:        fake.NewSimpleClientset(),
		defaultNamespace: "",
	}

	org := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			OrgID: "analytics",
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "host", Port: 5432, Username: "u", DatabaseName: "db",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Name: "analytics-metadata",
				Key:  "dsn",
			},
		},
	}

	_, err := activator.BuildActivationRequest(context.Background(), org, &WorkerAssignment{OrgID: "analytics"})
	if err == nil {
		t.Fatal("expected secret ref without tenant namespace to be rejected")
	}
}

func TestSharedWorkerActivatorRejectsSecretRefWithoutExplicitNamespace(t *testing.T) {
	activator := &SharedWorkerActivator{
		clientset:        fake.NewSimpleClientset(),
		defaultNamespace: "duckgres-workers",
	}

	org := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			OrgID: "analytics",
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "host", Port: 5432, Username: "u", DatabaseName: "db",
			},
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace: "tenant-a",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Name: "analytics-metadata",
				Key:  "dsn",
			},
		},
	}

	_, err := activator.BuildActivationRequest(context.Background(), org, &WorkerAssignment{OrgID: "analytics"})
	if err == nil {
		t.Fatal("expected secret ref without explicit namespace to be rejected")
	}
	if !strings.Contains(err.Error(), "SecretRef.Namespace") {
		t.Fatalf("expected explicit namespace guidance, got %v", err)
	}
}

func TestSharedWorkerActivatorReadSecretValueRejectsBlankNamespace(t *testing.T) {
	activator := &SharedWorkerActivator{
		clientset:        fake.NewSimpleClientset(),
		defaultNamespace: "duckgres-workers",
	}

	_, err := activator.readSecretValue(context.Background(), configstore.SecretRef{
		Name: "analytics-metadata",
		Key:  "dsn",
	})
	if err == nil {
		t.Fatal("expected blank secret namespace to be rejected")
	}
	if !strings.Contains(err.Error(), "SecretRef.Namespace") {
		t.Fatalf("expected explicit namespace guidance, got %v", err)
	}
}

func TestSharedWorkerActivatorRejectsSecretRefWithOnlySubstringOrgMatch(t *testing.T) {
	activator := &SharedWorkerActivator{
		clientset:        fake.NewSimpleClientset(),
		defaultNamespace: "duckgres-workers",
	}

	org := &configstore.OrgConfig{
		Name: "analytics",
		Warehouse: &configstore.ManagedWarehouseConfig{
			OrgID: "analytics",
			MetadataStore: configstore.ManagedWarehouseMetadataStore{
				Endpoint: "host", Port: 5432, Username: "u", DatabaseName: "db",
			},
			WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
				Namespace: "tenant-a",
			},
			MetadataStoreCredentials: configstore.SecretRef{
				Namespace: "tenant-a",
				Name:      "shared-analytics-metadata",
				Key:       "dsn",
			},
		},
	}

	_, err := activator.BuildActivationRequest(context.Background(), org, &WorkerAssignment{OrgID: "analytics"})
	if err == nil {
		t.Fatal("expected substring-only org match to be rejected")
	}
}
