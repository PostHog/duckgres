package provisioning

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakeTrinoServiceCredentialValidator struct {
	identity *configstore.TrinoServiceCredentialIdentity
	err      error
	calls    int
	cellID   string
}

func (f *fakeTrinoServiceCredentialValidator) ValidateTrinoServiceCredential(ctx context.Context, cellID, username, password string) (*configstore.TrinoServiceCredentialIdentity, error) {
	f.calls++
	f.cellID = cellID
	if _, ok := ctx.Deadline(); !ok {
		panic("validation must have a deadline")
	}
	return f.identity, f.err
}

func TestTrinoServiceCredentialAuthentication(t *testing.T) {
	const token = "synthetic-validation-token-for-tests"
	const username = "acme.svc_0123456789abcdef01234567"
	for _, tc := range []struct {
		name, auth, body string
		err              error
		status, calls    int
	}{
		{"valid", "Bearer " + token, `{"username":"` + username + `","password":"synthetic-secret"}`, nil, 200, 1},
		{"no token", "", `{}`, nil, 401, 0},
		{"wrong token", "Bearer unrelated", `{}`, nil, 401, 0},
		{"body cannot override cell", "Bearer " + token, `{"username":"` + username + `","password":"secret","cell_id":"registered:cell-b"}`, nil, 400, 0},
		{"malformed", "Bearer " + token, `{`, nil, 400, 0},
		{"oversized", "Bearer " + token, strings.Repeat("x", 20000), nil, 400, 0},
		{"denied", "Bearer " + token, `{"username":"` + username + `","password":"bad"}`, configstore.ErrTrinoServiceCredentialDenied, 401, 1},
		{"database unavailable", "Bearer " + token, `{"username":"` + username + `","password":"synthetic-secret"}`, errors.New("sensitive backend error"), 503, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validator := &fakeTrinoServiceCredentialValidator{identity: &configstore.TrinoServiceCredentialIdentity{User: username, Groups: []string{"org_acme", "tier_scale"}, ExpiresAt: time.Now().Add(time.Minute)}, err: tc.err}
			r := gin.New()
			RegisterTrinoServiceCredentialAuth(r, validator, []TrinoServiceAuthCell{{CellID: "registered:cell-a", Tokens: []string{token}}})
			req := httptest.NewRequest(http.MethodPost, "/auth/trino/service-credentials", strings.NewReader(tc.body))
			req.Header.Set("Authorization", tc.auth)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)
			if rec.Code != tc.status || validator.calls != tc.calls {
				t.Fatalf("status=%d calls=%d, want %d/%d", rec.Code, validator.calls, tc.status, tc.calls)
			}
			if strings.Contains(rec.Body.String(), "sensitive backend error") || strings.Contains(rec.Body.String(), "synthetic-secret") {
				t.Fatal("response leaked secrets or database detail")
			}
			if tc.calls > 0 && validator.cellID != "registered:cell-a" {
				t.Fatalf("wrong authenticated cell: %s", validator.cellID)
			}
			if tc.status == 200 {
				var got configstore.TrinoServiceCredentialIdentity
				if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if got.User != username || len(got.Groups) != 2 || got.ExpiresAt.IsZero() {
					t.Fatalf("unexpected identity: %+v", got)
				}
			}
		})
	}
}

func TestTrinoServiceCredentialTokenSelectsCell(t *testing.T) {
	validator := &fakeTrinoServiceCredentialValidator{identity: &configstore.TrinoServiceCredentialIdentity{}}
	r := gin.New()
	RegisterTrinoServiceCredentialAuth(r, validator, []TrinoServiceAuthCell{
		{CellID: "registered:cell-a", Tokens: []string{"current-a", "previous-a"}},
		{CellID: "registered:cell-b", Tokens: []string{"current-b"}},
	})
	for token, cellID := range map[string]string{"current-a": "registered:cell-a", "previous-a": "registered:cell-a", "current-b": "registered:cell-b"} {
		req := httptest.NewRequest(http.MethodPost, "/auth/trino/service-credentials", strings.NewReader(`{"username":"test","password":"synthetic"}`))
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != 200 || validator.cellID != cellID {
			t.Fatalf("status=%d cell=%s", rec.Code, validator.cellID)
		}
	}
}

func TestTrinoServiceCredentialAuthenticationDisabled(t *testing.T) {
	r := gin.New()
	RegisterTrinoServiceCredentialAuth(r, &fakeTrinoServiceCredentialValidator{}, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/auth/trino/service-credentials", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want 404", rec.Code)
	}
}

func TestTrinoServiceCredentialMintAndRefreshTarget(t *testing.T) {
	store := newFakeStore()
	store.orgs["acme"] = &configstore.Org{Name: "acme"}
	r := gin.New()
	RegisterAPIWithTrinoAdmission(r.Group("/api/v1"), store, store, "", nil, ".warehouse.example.com", nil,
		WithTrinoServiceCredentialConnect(func(org, grant string) *TrinoServiceCredentialConnect {
			return &TrinoServiceCredentialConnect{Host: "acme.warehouse.example.com", Port: 443, Catalog: "org_acme", Username: grant, HTTPScheme: "https"}
		}))
	mint := doJSON(t, r, http.MethodPost, "/api/v1/orgs/acme/service-credentials", `{"principal":"worker:test"}`)
	var first serviceCredentialResponse
	if err := json.Unmarshal(mint.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if mint.Code != 200 || first.TrinoConnect == nil || first.TrinoConnect.Username != first.CredentialID {
		t.Fatalf("mint did not include grant target: status=%d", mint.Code)
	}
	refresh := doJSON(t, r, http.MethodPost, "/api/v1/orgs/acme/service-credentials/refresh", `{"credential_id":"`+first.CredentialID+`"}`)
	var next serviceCredentialResponse
	if err := json.Unmarshal(refresh.Body.Bytes(), &next); err != nil {
		t.Fatal(err)
	}
	if refresh.Code != 200 || next.TrinoConnect == nil || next.TrinoConnect.Username != first.CredentialID || next.CredentialID != first.CredentialID {
		t.Fatal("refresh changed identity or omitted target")
	}
	renew := doJSON(t, r, http.MethodPost, "/api/v1/orgs/acme/service-credentials/refresh", `{"credential_id":"`+first.CredentialID+`","rotate_secret":false}`)
	var renewed map[string]any
	if err := json.Unmarshal(renew.Body.Bytes(), &renewed); err != nil {
		t.Fatal(err)
	}
	if renew.Code != 200 || renewed["secret_rotated"] != false || renewed["credential_secret"] != nil || renewed["credential_id"] != first.CredentialID {
		t.Fatal("renewal must explicitly retain the same credential and omit secret")
	}
}
