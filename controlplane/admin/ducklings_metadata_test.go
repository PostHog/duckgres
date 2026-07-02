//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestCnpgShardFromEndpoint pins the shard-name extraction: the first DNS
// label minus the CloudNativePG service-role suffix, degrading to the bare
// label for unrecognized shapes.
func TestCnpgShardFromEndpoint(t *testing.T) {
	cases := []struct {
		endpoint string
		want     string
	}{
		{"shard-001-pooler.cnpg-shards.svc.cluster.local", "shard-001"},
		{"shard-042-rw.cnpg-shards.svc.cluster.local", "shard-042"},
		{"shard-007-ro.cnpg-shards.svc", "shard-007"},
		{"shard-003-r.cnpg-shards.svc", "shard-003"},
		{"shard-001-pooler.cnpg-shards.svc.cluster.local:5432", "shard-001"},
		{"shard-001", "shard-001"},
		{"someotherhost.example.com", "someotherhost"},
		// A host that IS just the suffix must not collapse to "".
		{"-rw.cnpg-shards.svc", "-rw"},
	}
	for _, tc := range cases {
		if got := cnpgShardFromEndpoint(tc.endpoint); got != tc.want {
			t.Errorf("cnpgShardFromEndpoint(%q) = %q, want %q", tc.endpoint, got, tc.want)
		}
	}
}

type stubMetadataLister struct {
	stores map[string]DucklingMetadataStore
	err    error
}

func (s stubMetadataLister) CRMetadataStores(context.Context) (map[string]DucklingMetadataStore, error) {
	return s.stores, s.err
}

func metadataRouter(lister DucklingMetadataLister) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	RegisterDucklingsMetadata(r.Group("/api/v1"), lister)
	return r
}

// TestDucklingsMetadataHandler pins the wire shape: entries keyed by CR name,
// cnpg-shard entries carry the parsed shard, external entries do not.
func TestDucklingsMetadataHandler(t *testing.T) {
	r := metadataRouter(stubMetadataLister{stores: map[string]DucklingMetadataStore{
		"acme-cnpg": {Kind: "cnpg-shard", Endpoint: "shard-001-pooler.cnpg-shards.svc.cluster.local"},
		"acme-ext":  {Kind: "external", Endpoint: "db.example.rds.amazonaws.com"},
	}})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/ducklings/metadata", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp struct {
		Available bool                             `json:"available"`
		Entries   map[string]ducklingMetadataEntry `json:"entries"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.Available {
		t.Fatal("available = false, want true")
	}
	cnpg := resp.Entries["acme-cnpg"]
	if cnpg.Kind != "cnpg-shard" || cnpg.CnpgShard != "shard-001" {
		t.Fatalf("cnpg entry = %+v, want kind=cnpg-shard shard=shard-001", cnpg)
	}
	ext := resp.Entries["acme-ext"]
	if ext.Kind != "external" || ext.CnpgShard != "" {
		t.Fatalf("ext entry = %+v, want kind=external no shard", ext)
	}
}

// TestDucklingsMetadataDegrades pins the nil-lister and error paths: nil →
// available=false (not 500), lister error → 500.
func TestDucklingsMetadataDegrades(t *testing.T) {
	w := httptest.NewRecorder()
	metadataRouter(nil).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/ducklings/metadata", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("nil lister status = %d, want 200", w.Code)
	}
	var resp struct {
		Available bool `json:"available"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Available {
		t.Fatal("available = true with nil lister, want false")
	}

	w = httptest.NewRecorder()
	metadataRouter(stubMetadataLister{err: errors.New("boom")}).
		ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/ducklings/metadata", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("error lister status = %d, want 500", w.Code)
	}
}
