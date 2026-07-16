//go:build kubernetes

package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// fakeOperatorStore is an in-memory operatorStore for handler tests: it exercises
// the handler's validation + last-admin guard without a real config store.
type fakeOperatorStore struct {
	ops map[string]configstore.Operator
}

func newFakeOperatorStore(seed ...configstore.Operator) *fakeOperatorStore {
	s := &fakeOperatorStore{ops: make(map[string]configstore.Operator)}
	for _, o := range seed {
		s.ops[strings.ToLower(o.Email)] = o
	}
	return s
}

func (s *fakeOperatorStore) ListOperators() ([]configstore.Operator, error) {
	out := make([]configstore.Operator, 0, len(s.ops))
	for _, o := range s.ops {
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Email < out[j].Email })
	return out, nil
}

func (s *fakeOperatorStore) OperatorRole(email string) (string, error) {
	if o, ok := s.ops[strings.ToLower(strings.TrimSpace(email))]; ok {
		return o.Role, nil
	}
	return "", nil
}

func (s *fakeOperatorStore) CountAdmins() (int64, error) {
	var n int64
	for _, o := range s.ops {
		if o.Role == string(RoleAdmin) {
			n++
		}
	}
	return n, nil
}

func (s *fakeOperatorStore) UpsertOperator(email, role, addedBy string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	now := time.Now()
	existing, ok := s.ops[email]
	created := now
	if ok {
		created = existing.CreatedAt
	}
	s.ops[email] = configstore.Operator{
		Email:     email,
		Role:      role,
		AddedBy:   addedBy,
		CreatedAt: created,
		UpdatedAt: now,
	}
	return nil
}

func (s *fakeOperatorStore) DeleteOperator(email string) (bool, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if _, ok := s.ops[email]; !ok {
		return false, nil
	}
	delete(s.ops, email)
	return true, nil
}

// newOperatorsRouter mounts the operators API with an injected identity of the
// given role, standing in for AuthMiddleware (which RequireAdmin reads from the
// context). role="" simulates an unauthenticated caller (no identity set).
func newOperatorsRouter(store operatorStore, role Role) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if role != "" {
		r.Use(func(c *gin.Context) {
			c.Set(ctxIdentityKey, &Identity{Email: "admin@posthog.com", Role: role, Source: "sso"})
		})
	}
	registerOperatorsAPI(r.Group("/api/v1"), store)
	return r
}

func doOperatorReq(t *testing.T, r *gin.Engine, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *bytes.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func TestOperatorsListReturnsAll(t *testing.T) {
	store := newFakeOperatorStore(
		configstore.Operator{Email: "alice@posthog.com", Role: "admin", AddedBy: "bootstrap"},
		configstore.Operator{Email: "bob@posthog.com", Role: "viewer", AddedBy: "alice@posthog.com"},
	)
	r := newOperatorsRouter(store, RoleAdmin)

	rec := doOperatorReq(t, r, http.MethodGet, "/api/v1/operators", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /operators = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	var resp struct {
		Operators []configstore.Operator `json:"operators"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Operators) != 2 {
		t.Fatalf("got %d operators, want 2", len(resp.Operators))
	}
	if resp.Operators[0].Email != "alice@posthog.com" || resp.Operators[0].Role != "admin" {
		t.Fatalf("unexpected first operator: %+v", resp.Operators[0])
	}
}

func TestOperatorsRequireAuth(t *testing.T) {
	store := newFakeOperatorStore()
	r := newOperatorsRouter(store, "") // no identity injected
	rec := doOperatorReq(t, r, http.MethodGet, "/api/v1/operators", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("GET /operators unauthenticated = %d, want 401", rec.Code)
	}
}

func TestOperatorUpsertGrantsRole(t *testing.T) {
	store := newFakeOperatorStore()
	r := newOperatorsRouter(store, RoleAdmin)

	rec := doOperatorReq(t, r, http.MethodPost, "/api/v1/operators",
		`{"email":"NewViewer@posthog.com","role":"viewer"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /operators = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	var op configstore.Operator
	if err := json.Unmarshal(rec.Body.Bytes(), &op); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Email is lowercased, role recorded, and the acting admin is the granter.
	if op.Email != "newviewer@posthog.com" || op.Role != "viewer" {
		t.Fatalf("unexpected upsert result: %+v", op)
	}
	if op.AddedBy != "admin@posthog.com" {
		t.Fatalf("added_by = %q, want the acting admin", op.AddedBy)
	}
	if got, _ := store.OperatorRole("newviewer@posthog.com"); got != "viewer" {
		t.Fatalf("store role = %q, want viewer", got)
	}
}

func TestOperatorUpsertValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"empty email", `{"email":"","role":"admin"}`},
		{"wrong domain", `{"email":"outsider@example.com","role":"admin"}`},
		{"invalid role", `{"email":"someone@posthog.com","role":"superuser"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeOperatorStore()
			r := newOperatorsRouter(store, RoleAdmin)
			rec := doOperatorReq(t, r, http.MethodPost, "/api/v1/operators", tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("POST %s = %d, want 400 (%s)", tc.body, rec.Code, rec.Body.String())
			}
			if len(store.ops) != 0 {
				t.Fatalf("invalid upsert mutated the store: %+v", store.ops)
			}
		})
	}
}

// The last-admin guard prevents locking everyone out of the console: the final
// admin can be neither demoted to viewer nor deleted.
func TestOperatorLastAdminGuard(t *testing.T) {
	t.Run("cannot demote the last admin", func(t *testing.T) {
		store := newFakeOperatorStore(
			configstore.Operator{Email: "alice@posthog.com", Role: "admin", AddedBy: "bootstrap"},
		)
		r := newOperatorsRouter(store, RoleAdmin)
		rec := doOperatorReq(t, r, http.MethodPost, "/api/v1/operators",
			`{"email":"alice@posthog.com","role":"viewer"}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("demote last admin = %d, want 409 (%s)", rec.Code, rec.Body.String())
		}
		if got, _ := store.OperatorRole("alice@posthog.com"); got != "admin" {
			t.Fatalf("alice role = %q after rejected demote, want admin (unchanged)", got)
		}
	})

	t.Run("cannot delete the last admin", func(t *testing.T) {
		store := newFakeOperatorStore(
			configstore.Operator{Email: "alice@posthog.com", Role: "admin", AddedBy: "bootstrap"},
		)
		r := newOperatorsRouter(store, RoleAdmin)
		rec := doOperatorReq(t, r, http.MethodDelete, "/api/v1/operators/alice@posthog.com", "")
		if rec.Code != http.StatusConflict {
			t.Fatalf("delete last admin = %d, want 409 (%s)", rec.Code, rec.Body.String())
		}
		if _, ok := store.ops["alice@posthog.com"]; !ok {
			t.Fatalf("alice was deleted despite the last-admin guard")
		}
	})

	t.Run("demote allowed when another admin remains", func(t *testing.T) {
		store := newFakeOperatorStore(
			configstore.Operator{Email: "alice@posthog.com", Role: "admin", AddedBy: "bootstrap"},
			configstore.Operator{Email: "carol@posthog.com", Role: "admin", AddedBy: "bootstrap"},
		)
		r := newOperatorsRouter(store, RoleAdmin)
		rec := doOperatorReq(t, r, http.MethodPost, "/api/v1/operators",
			`{"email":"alice@posthog.com","role":"viewer"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("demote with a second admin present = %d, want 200 (%s)", rec.Code, rec.Body.String())
		}
		if got, _ := store.OperatorRole("alice@posthog.com"); got != "viewer" {
			t.Fatalf("alice role = %q, want viewer", got)
		}
	})
}

func TestOperatorDelete(t *testing.T) {
	store := newFakeOperatorStore(
		configstore.Operator{Email: "alice@posthog.com", Role: "admin", AddedBy: "bootstrap"},
		configstore.Operator{Email: "bob@posthog.com", Role: "viewer", AddedBy: "bootstrap"},
	)
	r := newOperatorsRouter(store, RoleAdmin)

	rec := doOperatorReq(t, r, http.MethodDelete, "/api/v1/operators/bob@posthog.com", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("DELETE existing = %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if _, ok := store.ops["bob@posthog.com"]; ok {
		t.Fatalf("bob was not deleted")
	}

	rec = doOperatorReq(t, r, http.MethodDelete, "/api/v1/operators/ghost@posthog.com", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("DELETE missing = %d, want 404 (%s)", rec.Code, rec.Body.String())
	}
}

func TestOperatorMutationForbiddenForViewer(t *testing.T) {
	store := newFakeOperatorStore()
	r := newOperatorsRouter(store, RoleViewer)
	rec := doOperatorReq(t, r, http.MethodPost, "/api/v1/operators",
		`{"email":"someone@posthog.com","role":"viewer"}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("viewer POST /operators = %d, want 403 (%s)", rec.Code, rec.Body.String())
	}
}
