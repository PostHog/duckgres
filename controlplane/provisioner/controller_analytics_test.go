//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/internal/analytics"
	"github.com/posthog/duckgres/internal/notifications"
)

// capturedEvent records one analytics.Capture call.
type capturedEvent struct {
	event string
	orgID string
	props map[string]any
}

// fakeTracker records captured events for assertions.
type fakeTracker struct {
	events []capturedEvent
}

func (f *fakeTracker) Capture(event, orgID string, props map[string]any) {
	f.events = append(f.events, capturedEvent{event: event, orgID: orgID, props: props})
}
func (f *fakeTracker) Close() {}

// installFakeTracker swaps in a recording tracker for the duration of a test.
func installFakeTracker(t *testing.T) *fakeTracker {
	t.Helper()
	fake := &fakeTracker{}
	analytics.SetDefault(fake)
	t.Cleanup(func() { analytics.SetDefault(nil) })
	return fake
}

type capturedNotification struct {
	event string
	orgID string
	props map[string]any
}

type fakeNotifier struct {
	events []capturedNotification
}

func (f *fakeNotifier) Notify(event notifications.Event) {
	f.events = append(f.events, capturedNotification{event: event.Name, orgID: event.OrgID, props: event.Props})
}
func (f *fakeNotifier) Close() {}

func installFakeNotifier(t *testing.T) *fakeNotifier {
	t.Helper()
	fake := &fakeNotifier{}
	notifications.SetDefault(fake)
	t.Cleanup(func() { notifications.SetDefault(nil) })
	return fake
}

func (f *fakeNotifier) only(t *testing.T, event string) capturedNotification {
	t.Helper()
	var found []capturedNotification
	for _, e := range f.events {
		if e.event == event {
			found = append(found, e)
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly one %q notification, got %d (all: %+v)", event, len(found), f.events)
	}
	return found[0]
}

func (f *fakeNotifier) count(event string) int {
	n := 0
	for _, e := range f.events {
		if e.event == event {
			n++
		}
	}
	return n
}

// only returns the single event with the given name, failing if there isn't
// exactly one.
func (f *fakeTracker) only(t *testing.T, event string) capturedEvent {
	t.Helper()
	var found []capturedEvent
	for _, e := range f.events {
		if e.event == event {
			found = append(found, e)
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly one %q event, got %d (all: %+v)", event, len(found), f.events)
	}
	return found[0]
}

// count returns how many events with the given name were captured.
func (f *fakeTracker) count(event string) int {
	n := 0
	for _, e := range f.events {
		if e.event == event {
			n++
		}
	}
	return n
}

// TestReconcileProvisioningSuccessEmitsEvent asserts that the warehouse
// reaching Ready emits a single warehouse_provision_success carrying the
// warehouse metadata, and no failure event.
func TestReconcileProvisioningSuccessEmitsEvent(t *testing.T) {
	fake := installFakeTracker(t)
	notifier := installFakeNotifier(t)
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-ok"] = &configstore.ManagedWarehouse{
		OrgID:         "org-ok",
		DucklingName:  "org-ok",
		State:         configstore.ManagedWarehouseStateProvisioning,
		CreatedAt:     time.Now(),
		MetadataStore: configstore.ManagedWarehouseMetadataStore{Kind: configstore.MetadataStoreKindExternal},
		DuckLake:      configstore.ManagedWarehouseDuckLake{Enabled: true},
	}

	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata":   map[string]interface{}{"name": "org-ok", "namespace": ducklingNamespace},
		"status": map[string]interface{}{
			"metadataStore": map[string]interface{}{
				"type": "external", "endpoint": "rds.example", "password": "secret", "user": "postgres", "database": "postgres",
			},
			"dataStore":  map[string]interface{}{"type": "s3bucket", "bucketName": "org-ok-bucket"},
			"iamRoleArn": "arn:aws:iam::123456789012:role/duckling-org-ok",
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
				map[string]interface{}{"type": "Synced", "status": "True"},
			},
		},
	}}
	ctx := context.Background()
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.SetProbe(func(context.Context, string, string, string, string, string) error { return nil })
	ctrl.reconcile(ctx)

	if fs.warehouses["org-ok"].State != configstore.ManagedWarehouseStateReady {
		t.Fatalf("expected ready state, got %q", fs.warehouses["org-ok"].State)
	}
	e := fake.only(t, "warehouse_provision_success")
	if e.orgID != "org-ok" {
		t.Errorf("orgID = %q, want org-ok", e.orgID)
	}
	if e.props["metadata_store"] != string(configstore.MetadataStoreKindExternal) {
		t.Errorf("metadata_store = %v, want external", e.props["metadata_store"])
	}
	if e.props["ducklake_enabled"] != true {
		t.Errorf("ducklake_enabled = %v, want true", e.props["ducklake_enabled"])
	}
	// Iceberg support was removed: the event no longer carries iceberg_enabled.
	if _, ok := e.props["iceberg_enabled"]; ok {
		t.Errorf("iceberg_enabled prop must be gone, got %v", e.props["iceberg_enabled"])
	}
	if n := fake.count("warehouse_provision_failed"); n != 0 {
		t.Errorf("expected no failure event, got %d", n)
	}
	notification := notifier.only(t, "warehouse_provision_success")
	if notification.orgID != "org-ok" {
		t.Errorf("notification orgID = %q, want org-ok", notification.orgID)
	}
	if notification.props["metadata_store"] != string(configstore.MetadataStoreKindExternal) {
		t.Errorf("notification metadata_store = %v, want external", notification.props["metadata_store"])
	}

	// A second reconcile finds the warehouse already Ready (routed to
	// reconcileReady), so the success event must not fire again.
	ctrl.reconcile(ctx)
	if n := fake.count("warehouse_provision_success"); n != 1 {
		t.Errorf("expected exactly one success event across two reconciles, got %d", n)
	}
	if n := notifier.count("warehouse_provision_success"); n != 1 {
		t.Errorf("expected exactly one success notification across two reconciles, got %d", n)
	}
}

// TestReconcileProvisioningTimeoutEmitsFailure asserts the 30-minute timeout
// path emits warehouse_provision_failed with reason=provisioning_timeout.
func TestReconcileProvisioningTimeoutEmitsFailure(t *testing.T) {
	fake := installFakeTracker(t)
	notifier := installFakeNotifier(t)
	dc, _ := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-slow"] = &configstore.ManagedWarehouse{
		OrgID:         "org-slow",
		DucklingName:  "org-slow",
		State:         configstore.ManagedWarehouseStateProvisioning,
		CreatedAt:     time.Now().Add(-31 * time.Minute),
		MetadataStore: configstore.ManagedWarehouseMetadataStore{Kind: configstore.MetadataStoreKindCnpgShard},
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(context.Background())

	if fs.warehouses["org-slow"].State != configstore.ManagedWarehouseStateFailed {
		t.Fatalf("expected failed state, got %q", fs.warehouses["org-slow"].State)
	}
	e := fake.only(t, "warehouse_provision_failed")
	if e.orgID != "org-slow" {
		t.Errorf("orgID = %q, want org-slow", e.orgID)
	}
	if e.props["reason"] != "provisioning_timeout" {
		t.Errorf("reason = %v, want provisioning_timeout", e.props["reason"])
	}
	if e.props["metadata_store"] != string(configstore.MetadataStoreKindCnpgShard) {
		t.Errorf("metadata_store = %v, want cnpg-shard", e.props["metadata_store"])
	}
	notification := notifier.only(t, "warehouse_provision_failed")
	if notification.props["reason"] != "provisioning_timeout" {
		t.Errorf("notification reason = %v, want provisioning_timeout", notification.props["reason"])
	}
}

// TestReconcileProvisioningCrossplaneFailureEmitsFailure asserts that a
// persistent Crossplane sync failure (>10 min) emits warehouse_provision_failed
// with reason=crossplane_sync_failure.
func TestReconcileProvisioningCrossplaneFailureEmitsFailure(t *testing.T) {
	fake := installFakeTracker(t)
	notifier := installFakeNotifier(t)
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-xp"] = &configstore.ManagedWarehouse{
		OrgID:         "org-xp",
		DucklingName:  "org-xp",
		State:         configstore.ManagedWarehouseStateProvisioning,
		CreatedAt:     time.Now().Add(-15 * time.Minute), // past the 10-min Crossplane grace, under the 30-min timeout
		MetadataStore: configstore.ManagedWarehouseMetadataStore{Kind: configstore.MetadataStoreKindExternal},
	}

	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata":   map[string]interface{}{"name": "org-xp", "namespace": ducklingNamespace},
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{"type": "Synced", "status": "False", "message": "cannot create metadata store: InvalidParameterException"},
			},
		},
	}}
	ctx := context.Background()
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(ctx)

	if fs.warehouses["org-xp"].State != configstore.ManagedWarehouseStateFailed {
		t.Fatalf("expected failed state, got %q", fs.warehouses["org-xp"].State)
	}
	e := fake.only(t, "warehouse_provision_failed")
	if e.props["reason"] != "crossplane_sync_failure" {
		t.Errorf("reason = %v, want crossplane_sync_failure", e.props["reason"])
	}
	if n := fake.count("warehouse_provision_success"); n != 0 {
		t.Errorf("expected no success event, got %d", n)
	}
	notification := notifier.only(t, "warehouse_provision_failed")
	if notification.props["reason"] != "crossplane_sync_failure" {
		t.Errorf("notification reason = %v, want crossplane_sync_failure", notification.props["reason"])
	}
}

// TestReconcileDeletingSuccessEmitsEvent asserts that a completed teardown
// emits exactly one warehouse_deprovision_success and no failure event.
func TestReconcileDeletingSuccessEmitsEvent(t *testing.T) {
	fake := installFakeTracker(t)
	notifier := installFakeNotifier(t)
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-del"] = &configstore.ManagedWarehouse{
		OrgID:        "org-del",
		DucklingName: "org-del",
		State:        configstore.ManagedWarehouseStateDeleting,
	}
	ctx := context.Background()
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata":   map[string]interface{}{"name": "org-del", "namespace": ducklingNamespace},
	}}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create CR: %v", err)
	}

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(ctx)

	if fs.warehouses["org-del"].State != configstore.ManagedWarehouseStateDeleted {
		t.Fatalf("expected deleted state, got %q", fs.warehouses["org-del"].State)
	}
	if e := fake.only(t, "warehouse_deprovision_success"); e.orgID != "org-del" {
		t.Errorf("orgID = %q, want org-del", e.orgID)
	}
	if n := fake.count("warehouse_deprovision_failed"); n != 0 {
		t.Errorf("expected no failure event, got %d", n)
	}
	if e := notifier.only(t, "warehouse_deprovision_success"); e.orgID != "org-del" {
		t.Errorf("notification orgID = %q, want org-del", e.orgID)
	}
}

// TestReconcileDeletingFailureEmitsEvent asserts that a non-NotFound error
// deleting the Duckling CR emits warehouse_deprovision_failed and leaves the
// warehouse in Deleting (so the controller retries).
func TestReconcileDeletingFailureEmitsEvent(t *testing.T) {
	fake := installFakeTracker(t)
	notifier := installFakeNotifier(t)
	dc, fakeK8s := newFakeDucklingClient()
	fs := newFakeStore()
	fs.warehouses["org-stuck"] = &configstore.ManagedWarehouse{
		OrgID:        "org-stuck",
		DucklingName: "org-stuck",
		State:        configstore.ManagedWarehouseStateDeleting,
	}
	ctx := context.Background()
	cr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "k8s.posthog.com/v1alpha1",
		"kind":       "Duckling",
		"metadata":   map[string]interface{}{"name": "org-stuck", "namespace": ducklingNamespace},
	}}
	if _, err := fakeK8s.Resource(ducklingGVR).Namespace(ducklingNamespace).Create(ctx, cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create CR: %v", err)
	}
	// Make the CR delete fail with a non-NotFound error so reconcileDeleting
	// takes the retry branch. getCR (a "get") still succeeds, so Delete reaches
	// the actual delete call.
	fakeK8s.PrependReactor("delete", "ducklings", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("etcdserver: request timed out")
	})

	ctrl := NewControllerWithClient(fs, dc, time.Second)
	ctrl.reconcile(ctx)

	if fs.warehouses["org-stuck"].State != configstore.ManagedWarehouseStateDeleting {
		t.Fatalf("expected state to remain deleting on retry, got %q", fs.warehouses["org-stuck"].State)
	}
	e := fake.only(t, "warehouse_deprovision_failed")
	if e.orgID != "org-stuck" {
		t.Errorf("orgID = %q, want org-stuck", e.orgID)
	}
	if e.props["reason"] != "duckling_delete_failed" {
		t.Errorf("reason = %v, want duckling_delete_failed", e.props["reason"])
	}
	if n := fake.count("warehouse_deprovision_success"); n != 0 {
		t.Errorf("expected no success event, got %d", n)
	}
	notification := notifier.only(t, "warehouse_deprovision_failed")
	if notification.props["reason"] != "duckling_delete_failed" {
		t.Errorf("notification reason = %v, want duckling_delete_failed", notification.props["reason"])
	}
}
