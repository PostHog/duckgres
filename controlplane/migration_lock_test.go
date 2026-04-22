//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestOrgRouterMigrationLock(t *testing.T) {
	router := &OrgRouter{}

	if router.IsMigrating("org-1") {
		t.Fatal("expected org-1 to not be migrating initially")
	}

	router.SetMigrating("org-1")
	if !router.IsMigrating("org-1") {
		t.Fatal("expected org-1 to be migrating after SetMigrating")
	}

	if router.IsMigrating("org-2") {
		t.Fatal("expected org-2 to not be migrating")
	}

	router.ClearMigrating("org-1")
	if router.IsMigrating("org-1") {
		t.Fatal("expected org-1 to not be migrating after ClearMigrating")
	}
}

// newTestActivator creates a SharedWorkerActivator with fake k8s secrets
// and configurable activation behavior for migration lock testing.
func newTestActivator(t *testing.T, activateFn func(ctx context.Context, w *ManagedWorker, p TenantActivationPayload) error) (*SharedWorkerActivator, *string, *string) {
	t.Helper()

	clientset := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "test-org-metadata"},
			Data:       map[string][]byte{"dsn": []byte("meta-pass")},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "fail-org-metadata"},
			Data:       map[string][]byte{"dsn": []byte("meta-pass")},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "org-ok-metadata"},
			Data:       map[string][]byte{"dsn": []byte("meta-pass")},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "test-org-s3"},
			Data:       map[string][]byte{"creds": []byte(`{"access_key_id":"a","secret_access_key":"b"}`)},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "fail-org-s3"},
			Data:       map[string][]byte{"creds": []byte(`{"access_key_id":"a","secret_access_key":"b"}`)},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "org-ok-s3"},
			Data:       map[string][]byte{"creds": []byte(`{"access_key_id":"a","secret_access_key":"b"}`)},
		},
	)

	var setOrg, clearOrg string
	activator := &SharedWorkerActivator{
		clientset:        clientset,
		defaultNamespace: "ns",
		resolveOrgConfig: func(orgID string) (*configstore.OrgConfig, error) {
			return &configstore.OrgConfig{
				Name: orgID,
				Warehouse: &configstore.ManagedWarehouseConfig{
					OrgID: orgID,
					MetadataStore: configstore.ManagedWarehouseMetadataStore{
						Endpoint: "meta-host", Port: 5432,
						Username: "user", DatabaseName: "db",
					},
					S3: configstore.ManagedWarehouseS3{
						Provider: "aws", Region: "us-east-1",
						Bucket: "bucket", PathPrefix: "data",
						Endpoint: "s3.amazonaws.com", UseSSL: true, URLStyle: "path",
					},
					WorkerIdentity: configstore.ManagedWarehouseWorkerIdentity{
						Namespace: "ns",
					},
					MetadataStoreCredentials: configstore.SecretRef{
						Namespace: "ns", Name: orgID + "-metadata", Key: "dsn",
					},
					S3Credentials: configstore.SecretRef{
						Namespace: "ns", Name: orgID + "-s3", Key: "creds",
					},
				},
			}, nil
		},
		activateReservedWorker: activateFn,
		setMigrating:           func(orgID string) { setOrg = orgID },
		clearMigrating:         func(orgID string) { clearOrg = orgID },
	}

	return activator, &setOrg, &clearOrg
}

func newTestWorker(t *testing.T, id int, orgID string) *ManagedWorker {
	t.Helper()
	worker := &ManagedWorker{ID: id}
	if err := worker.SetSharedState(SharedWorkerState{
		Lifecycle:  WorkerLifecycleReserved,
		Assignment: &WorkerAssignment{OrgID: orgID},
	}); err != nil {
		t.Fatalf("SetSharedState: %v", err)
	}
	return worker
}

func TestActivatorSetsClearsMigrationLock(t *testing.T) {
	activator, setOrg, clearOrg := newTestActivator(t,
		func(ctx context.Context, w *ManagedWorker, p TenantActivationPayload) error {
			if !p.DuckLake.Migrate {
				t.Error("expected Migrate=true in payload")
			}
			return nil
		})

	// Pre-seed cache: migration needed
	activator.migrationChecked.Store("test-org", true)

	worker := newTestWorker(t, 1, "test-org")

	if err := activator.ActivateReservedWorker(context.Background(), worker); err != nil {
		t.Fatalf("ActivateReservedWorker failed: %v", err)
	}

	if *setOrg != "test-org" {
		t.Errorf("expected setMigrating(test-org), got %q", *setOrg)
	}
	if *clearOrg != "test-org" {
		t.Errorf("expected clearMigrating(test-org), got %q", *clearOrg)
	}

	// After success, cache should flip to false
	cached, ok := activator.migrationChecked.Load("test-org")
	if !ok || cached.(bool) {
		t.Error("expected migration cache=false after successful migration")
	}
}

func TestActivatorClearsMigrationLockOnFailure(t *testing.T) {
	activator, _, clearOrg := newTestActivator(t,
		func(ctx context.Context, w *ManagedWorker, p TenantActivationPayload) error {
			return fmt.Errorf("activation failed")
		})

	activator.migrationChecked.Store("fail-org", true)
	worker := newTestWorker(t, 2, "fail-org")

	err := activator.ActivateReservedWorker(context.Background(), worker)
	if err == nil {
		t.Fatal("expected error from ActivateReservedWorker")
		return
	}

	if *clearOrg != "fail-org" {
		t.Errorf("expected clearMigrating(fail-org), got %q", *clearOrg)
	}

	// Cache should stay true on failure (retry migration next time)
	cached, _ := activator.migrationChecked.Load("fail-org")
	if !cached.(bool) {
		t.Error("expected migration cache to remain true after failure")
	}
}

func TestActivatorSkipsCheckWhenCachedFalse(t *testing.T) {
	var migrateFlagSeen bool
	activator, setOrg, _ := newTestActivator(t,
		func(ctx context.Context, w *ManagedWorker, p TenantActivationPayload) error {
			migrateFlagSeen = p.DuckLake.Migrate
			return nil
		})

	// Cache says no migration needed
	activator.migrationChecked.Store("org-ok", false)
	worker := newTestWorker(t, 3, "org-ok")

	if err := activator.ActivateReservedWorker(context.Background(), worker); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if migrateFlagSeen {
		t.Error("expected Migrate=false when cache says no migration needed")
	}
	if *setOrg != "" {
		t.Errorf("expected setMigrating not called, got %q", *setOrg)
	}
}

func TestMockOrgRouterSatisfiesInterface(t *testing.T) {
	mock := &mockOrgRouter{ok: true}
	var _ OrgRouterInterface = mock

	if mock.IsMigratingForOrg("anyorg") {
		t.Fatal("expected default mock to return not migrating")
	}
}
