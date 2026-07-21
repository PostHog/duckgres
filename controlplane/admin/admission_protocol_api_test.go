//go:build kubernetes

package admin

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakeAdmissionOfferProtocolStore struct {
	err   error
	calls int
}

func (s *fakeAdmissionOfferProtocolStore) ActivateOrgConnectionAdmissionOffers() error {
	s.calls++
	return s.err
}

func newAdmissionOfferProtocolRouter(store admissionOfferProtocolStore, role Role) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if role != "" {
		r.Use(func(c *gin.Context) {
			c.Set(ctxIdentityKey, &Identity{Email: "admin@posthog.com", Role: role, Source: "sso"})
		})
	}
	registerAdmissionOfferProtocolAPI(r.Group("/api/v1"), store)
	return r
}

func activateAdmissionOfferProtocol(t *testing.T, r *gin.Engine) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admission/offers/activate", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func TestAdmissionOfferProtocolActivationRequiresAdmin(t *testing.T) {
	tests := []struct {
		name string
		role Role
		want int
	}{
		{name: "unauthenticated", want: http.StatusUnauthorized},
		{name: "viewer", role: RoleViewer, want: http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeAdmissionOfferProtocolStore{}
			rec := activateAdmissionOfferProtocol(t, newAdmissionOfferProtocolRouter(store, tt.role))
			if rec.Code != tt.want {
				t.Fatalf("activation status = %d, want %d (%s)", rec.Code, tt.want, rec.Body.String())
			}
			if store.calls != 0 {
				t.Fatalf("activation called %d times for unauthorized request, want 0", store.calls)
			}
		})
	}
}

func TestAdmissionOfferProtocolActivationSucceeds(t *testing.T) {
	store := &fakeAdmissionOfferProtocolStore{}
	rec := activateAdmissionOfferProtocol(t, newAdmissionOfferProtocolRouter(store, RoleAdmin))
	if rec.Code != http.StatusOK {
		t.Fatalf("activation status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if store.calls != 1 {
		t.Fatalf("activation called %d times, want 1", store.calls)
	}
}

func TestAdmissionOfferProtocolActivationSetsAuditDetail(t *testing.T) {
	store := &fakeAdmissionOfferProtocolStore{}
	var detail string
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(ctxIdentityKey, &Identity{Email: "admin@posthog.com", Role: RoleAdmin, Source: "sso"})
	})
	r.Use(func(c *gin.Context) {
		c.Next()
		detail = c.GetString(ctxAuditDetailKey)
	})
	registerAdmissionOfferProtocolAPI(r.Group("/api/v1"), store)

	rec := activateAdmissionOfferProtocol(t, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("activation status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if detail != "durable admission offer protocol enabled" {
		t.Fatalf("activation audit detail = %q, want protocol enablement detail", detail)
	}
}

func TestAdmissionOfferProtocolActivationAcceptsInternalSecretHeader(t *testing.T) {
	store := &fakeAdmissionOfferProtocolStore{}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(AuthMiddleware(NewTokenSet("test-secret", nil), nil))
	registerAdmissionOfferProtocolAPI(r.Group("/api/v1"), store)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/admission/offers/activate", nil)
	req.Header.Set("X-Duckgres-Internal-Secret", "test-secret")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("activation with internal-secret header status = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if store.calls != 1 {
		t.Fatalf("activation called %d times, want 1", store.calls)
	}
}

func TestAdmissionOfferProtocolActivationReportsUnsafeFleet(t *testing.T) {
	store := &fakeAdmissionOfferProtocolStore{err: configstore.ErrAdmissionOfferProtocolActivationBlocked}
	rec := activateAdmissionOfferProtocol(t, newAdmissionOfferProtocolRouter(store, RoleAdmin))
	if rec.Code != http.StatusConflict {
		t.Fatalf("activation status = %d, want 409 (%s)", rec.Code, rec.Body.String())
	}
}

func TestAdmissionOfferProtocolActivationReportsStoreFailure(t *testing.T) {
	store := &fakeAdmissionOfferProtocolStore{err: errors.New("database unavailable")}
	rec := activateAdmissionOfferProtocol(t, newAdmissionOfferProtocolRouter(store, RoleAdmin))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("activation status = %d, want 500 (%s)", rec.Code, rec.Body.String())
	}
}
