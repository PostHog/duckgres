package provisioning

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/internal/analytics"
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

func TestProvisionEmitsAnalyticsEvent(t *testing.T) {
	fake := installFakeTracker(t)
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "acme-db", "metadata_store": {"type": "cnpg-shard"}, "iceberg": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/acme/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}

	e := fake.only(t, "warehouse_provision_begin")
	if e.orgID != "acme" {
		t.Errorf("orgID = %q, want acme", e.orgID)
	}
	if e.props["database_name"] != "acme-db" {
		t.Errorf("database_name = %v, want acme-db", e.props["database_name"])
	}
	if e.props["metadata_store"] != "cnpg-shard" {
		t.Errorf("metadata_store = %v, want cnpg-shard", e.props["metadata_store"])
	}
	if e.props["iceberg_enabled"] != true {
		t.Errorf("iceberg_enabled = %v, want true", e.props["iceberg_enabled"])
	}
	if e.props["ducklake_enabled"] != false {
		t.Errorf("ducklake_enabled = %v, want false", e.props["ducklake_enabled"])
	}
}

func TestProvisionFailureEmitsNoEvent(t *testing.T) {
	fake := installFakeTracker(t)
	store := newFakeStore()
	router := newTestRouter(store)

	// Missing database_name → 400, no provisioning happens.
	body := []byte(`{"metadata_store": {"type": "cnpg-shard"}, "iceberg": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/acme/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
	}
	if len(fake.events) != 0 {
		t.Fatalf("expected no events on failure, got %+v", fake.events)
	}
}

func TestDeprovisionEmitsAnalyticsEvent(t *testing.T) {
	fake := installFakeTracker(t)
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.warehouses["acme"] = &configstore.ManagedWarehouse{
		OrgID: "acme",
		State: configstore.ManagedWarehouseStateReady,
	}
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/acme/deprovision", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}
	if e := fake.only(t, "warehouse_deprovision_begin"); e.orgID != "acme" {
		t.Errorf("orgID = %q, want acme", e.orgID)
	}
}

func TestDeprovisionNotFoundEmitsNoEvent(t *testing.T) {
	fake := installFakeTracker(t)
	store := newFakeStore()
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/ghost/deprovision", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", rec.Code, rec.Body.String())
	}
	if len(fake.events) != 0 {
		t.Fatalf("expected no events when warehouse missing, got %+v", fake.events)
	}
}

func TestResetPasswordEmitsAnalyticsEvent(t *testing.T) {
	fake := installFakeTracker(t)
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	store.warehouses["acme"] = &configstore.ManagedWarehouse{
		OrgID: "acme",
		State: configstore.ManagedWarehouseStateReady,
	}
	store.users[configstore.OrgUserKey{OrgID: "acme", Username: "root"}] = "oldhash"
	router := newTestRouter(store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/acme/reset-password", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	e := fake.only(t, "warehouse_password_reset")
	if e.orgID != "acme" {
		t.Errorf("orgID = %q, want acme", e.orgID)
	}
	if e.props["username"] != "root" {
		t.Errorf("username = %v, want root", e.props["username"])
	}
}
