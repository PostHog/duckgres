package provisioning

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoAdmissionGuardsBothEnableSurfaces(t *testing.T) {
	for _, endpoint := range []string{"trino", "provision"} {
		for _, allowed := range []bool{false, true} {
			store := newFakeStore()
			store.orgs["tenant"] = &configstore.Org{Name: "tenant"}
			store.users[configstore.OrgUserKey{OrgID: "tenant", Username: "root"}] = "hash"
			r := gin.New()
			calls := 0
			RegisterAPIWithTrinoAdmission(r.Group("/api/v1"), store, store, "", nil, "", func(org string) error {
				calls++
				if org != "tenant" {
					t.Fatalf("wrong org %s", org)
				}
				if !allowed {
					return ErrTrinoCellSelectionRequired
				}
				return nil
			}, WithTrinoBackendValidator(func(configstore.TrinoBackend) error { return nil }))
			body := `{"enabled":true,"tier":"free"}`
			if endpoint == "provision" {
				body = `{"database_name":"tenant","team_id":1,"metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":true},"trino":{"enabled":true}}`
			}
			req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/tenant/"+endpoint, bytes.NewBufferString(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			want := http.StatusConflict
			if allowed {
				want = http.StatusAccepted
			}
			if rec.Code != want || calls != 1 {
				t.Fatalf("%s allowed=%v status=%d calls=%d body=%s", endpoint, allowed, rec.Code, calls, rec.Body.String())
			}
			if !allowed && (store.trino["tenant"] != nil || store.lastProvision != nil) {
				t.Fatal("admission rejection mutated warehouse")
			}
		}
	}
}

func TestTrinoAdmissionHidesStoreErrors(t *testing.T) {
	h := &handler{trinoAdmission: func(string) error { return errors.New("private database endpoint") }}
	r := gin.New()
	r.GET("/", func(c *gin.Context) { h.admitTrino(c, "tenant") })
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "private database") {
		t.Fatalf("unsafe error: %d %s", rec.Code, rec.Body.String())
	}
}

// Capture the settings crossing the HTTP/store boundary; the real PostgreSQL
// tests assert durable ownership and concurrency separately.
type defaultCellStore struct {
	*fakeStore
	settings configstore.TrinoSettings
}

func (s *defaultCellStore) EnableTrino(orgID string, settings configstore.TrinoSettings) error {
	s.settings = settings
	return s.fakeStore.EnableTrino(orgID, settings)
}
func (s *defaultCellStore) Provision(req ProvisionRequest) error {
	if req.Trino != nil {
		s.settings = *req.Trino
	}
	return s.fakeStore.Provision(req)
}
func TestTrinoDefaultCellReachesBothEnableTransactions(t *testing.T) {
	for _, endpoint := range []string{"trino", "provision"} {
		t.Run(endpoint, func(t *testing.T) {
			store := &defaultCellStore{fakeStore: newFakeStore()}
			store.orgs["tenant"] = &configstore.Org{Name: "tenant"}
			store.users[configstore.OrgUserKey{OrgID: "tenant", Username: "root"}] = "hash"
			r := gin.New()
			RegisterAPIWithTrinoAdmission(r.Group("/api/v1"), store, store, "", nil, "", nil,
				WithTrinoBackendValidator(func(configstore.TrinoBackend) error { return nil }), WithTrinoDefaultCell("registered:pool-test"))
			body := `{"enabled":true}`
			if endpoint == "provision" {
				body = `{"database_name":"tenant","team_id":1,"metadata_store":{"type":"cnpg-shard"},"ducklake":{"enabled":true},"trino":{"enabled":true}}`
			}
			req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/tenant/"+endpoint, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			if store.settings.DefaultCellID != "registered:pool-test" {
				t.Fatal("deployment placement missing from transaction")
			}
		})
	}
}
