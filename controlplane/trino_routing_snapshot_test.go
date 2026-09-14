//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

type routingSnapshotStore struct {
	rows  []configstore.TrinoRoutingPrincipal
	err   error
	calls int
}

func TestTrinoRoutingSnapshotLimits(t *testing.T) {
	for _, tc := range []struct {
		name        string
		count, size int
	}{
		{"entry count", configstore.MaxTrinoRoutingPrincipals + 1, 1},
		{"response bytes", 40000, 255},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := make([]configstore.TrinoRoutingPrincipal, tc.count)
			for i := range rows {
				rows[i] = configstore.TrinoRoutingPrincipal{Principal: fmt.Sprintf("%d%s", i, strings.Repeat("x", tc.size)), CellID: "cell-a"}
			}
			snapshot := newTrinoRoutingSnapshot(&routingSnapshotStore{rows: rows}, trinoFleet{&trinoWiring{Cell: trinoCell{ID: "cell-a", RoutingGroup: "pool-a"}}})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if data, err := snapshot.encode(ctx); err == nil || data != nil {
				t.Fatal("oversized snapshot did not fail atomically")
			}
		})
	}
}

func (s *routingSnapshotStore) ListTrinoRoutingPrincipals(ctx context.Context) ([]configstore.TrinoRoutingPrincipal, error) {
	s.calls++
	if _, ok := ctx.Deadline(); !ok {
		panic("routing query needs a deadline")
	}
	return s.rows, s.err
}

func TestTrinoRoutingSnapshot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cells := trinoFleet{
		&trinoWiring{Cell: trinoCell{ID: "cell-001", RoutingGroup: "legacy"}},
		&trinoWiring{Cell: trinoCell{ID: "registered:cell-001", PublicID: "cell-001", RoutingGroup: "pool-a"}},
	}
	store := &routingSnapshotStore{rows: []configstore.TrinoRoutingPrincipal{
		{Principal: "warehouse_z", CellID: "cell-001"},
		{Principal: "warehouse_a", CellID: "registered:cell-001"},
		{Principal: "unassigned", CellID: ""},
		{Principal: "unknown", CellID: "unknown"},
	}}
	r := gin.New()
	registerReadOnlyGroup(r, admin.NewTokenSet("reader", []string{"reader-old"}), admin.NewTokenSet("writer", nil), stubProvisioningStore{}, newTrinoRoutingSnapshot(store, cells))
	r.POST("/api/v1/orgs/:id/trino", admin.APIAuthMiddleware(admin.NewTokenSet("writer", nil)), func(c *gin.Context) { c.Status(200) })
	request := func(token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/trino/routing-snapshot", nil)
		req.Header.Set("X-Duckgres-Internal-Secret", token)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		return rec
	}
	for _, token := range []string{"", "bad"} {
		if rec := request(token); rec.Code != 401 {
			t.Fatalf("bad token status %d", rec.Code)
		}
	}
	if store.calls != 0 {
		t.Fatal("unauthorized request queried store")
	}
	for _, token := range []string{"reader", "reader-old", "writer"} {
		rec := request(token)
		if rec.Code != 200 || rec.Body.String() != `{"routes":[{"principal":"warehouse_a","routingGroup":"pool-a"},{"principal":"warehouse_z","routingGroup":"legacy"}]}` {
			t.Fatalf("snapshot: %d %s", rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("snapshot must not be cached by HTTP intermediaries")
		}
	}
	for _, token := range []string{"reader", "reader-old"} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/orgs/example/trino", nil)
		req.Header.Set("X-Duckgres-Internal-Secret", token)
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != 401 {
			t.Fatalf("routing credential reached mutation: %d", rec.Code)
		}
	}
	for _, source := range []string{"cookie", "sso"} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/trino/routing-snapshot", nil)
		if source == "cookie" {
			req.AddCookie(&http.Cookie{Name: "duckgres_admin_token", Value: "writer"})
		} else {
			req.Header.Set("X-Amzn-Oidc-Data", "writer")
		}
		rec := httptest.NewRecorder()
		r.ServeHTTP(rec, req)
		if rec.Code != 401 {
			t.Fatalf("%s was accepted on machine-only endpoint", source)
		}
	}
	store.rows = []configstore.TrinoRoutingPrincipal{{Principal: "warehouse_z", CellID: "cell-001"}, {Principal: "warehouse_z", CellID: "registered:cell-001"}}
	if rec := request("reader"); rec.Code != 503 || strings.Contains(rec.Body.String(), "warehouse_z") {
		t.Fatalf("duplicate must fail atomically: %d %s", rec.Code, rec.Body.String())
	}
	store.err = errors.New("database password private-secret")
	if rec := request("reader"); rec.Code != 503 || strings.Contains(rec.Body.String(), "private-secret") {
		t.Fatalf("query error leaked or succeeded: %d %s", rec.Code, rec.Body.String())
	}
	store.err = nil
	for _, principal := range []string{"", " ", "a:b", "a\nb", "a\x7fb", string([]byte{0xff}), strings.Repeat("x", 1025)} {
		store.rows = []configstore.TrinoRoutingPrincipal{{Principal: principal, CellID: "cell-001"}}
		if rec := request("reader"); rec.Code != 503 {
			t.Fatalf("malformed principal was exported: %d", rec.Code)
		}
	}
	store.rows = nil
	if rec := request("reader"); rec.Code != 200 || rec.Body.String() != `{"routes":[]}` {
		t.Fatalf("empty snapshot: %d %s", rec.Code, rec.Body.String())
	}
}
