package provisioning

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/internal/notifications"
)

type capturedNotification struct {
	name  string
	orgID string
	props map[string]any
}

type fakeNotifier struct {
	events []capturedNotification
}

func (f *fakeNotifier) Notify(event notifications.Event) {
	f.events = append(f.events, capturedNotification{name: event.Name, orgID: event.OrgID, props: event.Props})
}
func (f *fakeNotifier) Close() {}

func installFakeNotifier(t *testing.T) *fakeNotifier {
	t.Helper()
	fake := &fakeNotifier{}
	notifications.SetDefault(fake)
	t.Cleanup(func() { notifications.SetDefault(nil) })
	return fake
}

func (f *fakeNotifier) count(name string) int {
	n := 0
	for _, event := range f.events {
		if event.name == name {
			n++
		}
	}
	return n
}

func TestProvisionNotifiesOnlyWhenOrgIsCreated(t *testing.T) {
	fake := installFakeNotifier(t)
	store := newFakeStore()
	router := newTestRouter(store)

	body := []byte(`{"database_name": "acme-db", "default_team_id": 1, "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/acme/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}

	if got := fake.count("org_created"); got != 1 {
		t.Fatalf("org_created notifications = %d, want 1 (all: %+v)", got, fake.events)
	}
	if got := fake.count("warehouse_provision_begin"); got != 1 {
		t.Fatalf("warehouse_provision_begin notifications = %d, want 1 (all: %+v)", got, fake.events)
	}
}

func TestProvisionDoesNotNotifyOrgCreatedForExistingOrg(t *testing.T) {
	fake := installFakeNotifier(t)
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme", DatabaseName: "old-db"}
	router := newTestRouter(store)

	body := []byte(`{"database_name": "acme-db", "metadata_store": {"type": "cnpg-shard"}, "ducklake": {"enabled": true}}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/acme/provision", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}

	if got := fake.count("org_created"); got != 0 {
		t.Fatalf("org_created notifications = %d, want 0 (all: %+v)", got, fake.events)
	}
	if got := fake.count("warehouse_provision_begin"); got != 1 {
		t.Fatalf("warehouse_provision_begin notifications = %d, want 1 (all: %+v)", got, fake.events)
	}
}
