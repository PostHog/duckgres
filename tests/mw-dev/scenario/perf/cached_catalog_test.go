package perf

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"github.com/jackc/pgx/v5"
	trinodriver "github.com/posthog/duckgres/tests/perf/drivers/trino"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"strings"
	"testing"
)

func TestCachedCatalogStatementPreservesProperties(t *testing.T) {
	props := map[string]string{"connector.name": "ducklake", "fs.cache.enabled": "false", "ducklake.data-path": "s3://fixture/a'b", "ducklake.metadata.connection-password-file": "/etc/trino/tenant-secrets/org_fixture"}
	sql, err := cachedCatalogStatement("org_fixture", props)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`CREATE CATALOG "org_fixture" USING "ducklake"`, `"fs.cache.enabled" = 'true'`, `'s3://fixture/a''b'`, `'/etc/trino/tenant-secrets/org_fixture'`} {
		if !strings.Contains(sql, want) {
			t.Fatalf("statement missing %q: %s", want, sql)
		}
	}
	if props["fs.cache.enabled"] != "false" {
		t.Fatal("baseline mutated")
	}
	for _, name := range []string{"system", "org_bad-name", "org_foo;DROP"} {
		if _, err := cachedCatalogStatement(name, props); err == nil {
			t.Fatalf("accepted unmanaged name %q", name)
		}
	}
}

func TestCachedCatalogReuseRequiresExactProperties(t *testing.T) {
	baseline := map[string]string{"connector.name": "ducklake", "fs.cache.enabled": "false", "ducklake.data-path": "s3://fixture/data"}
	cached := map[string]string{"connector.name": "ducklake", "fs.cache.enabled": "true", "ducklake.data-path": "s3://fixture/data"}
	if err := checkCachedCatalogProperties(baseline, cached); err != nil {
		t.Fatal(err)
	}
	cached["ducklake.data-path"] = "s3://different/data"
	if err := checkCachedCatalogProperties(baseline, cached); err == nil {
		t.Fatal("accepted different dataset")
	}
}

type catalogPropertiesRow struct {
	missing bool
	props   map[string]string
}

func (r catalogPropertiesRow) Scan(dest ...any) error {
	if r.missing {
		return pgx.ErrNoRows
	}
	*dest[0].(*string) = "ducklake"
	raw, err := json.Marshal(r.props)
	*dest[1].(*[]byte) = raw
	return err
}

type catalogPropertiesStore struct {
	created atomic.Bool
	reads   []string
}

func (s *catalogPropertiesStore) QueryRow(_ context.Context, query string, args ...any) pgx.Row {
	cell := args[0].(string)
	s.reads = append(s.reads, cell)
	if !strings.Contains(query, "cell_id = $1 AND catalog_name = $2") || args[1] != "org_fixture" {
		panic("catalog read lost scope")
	}
	mode := "false"
	if cell == "cached-cell" {
		mode = "true"
	}
	return catalogPropertiesRow{missing: cell == "cached-cell" && !s.created.Load(), props: map[string]string{"fs.cache.enabled": mode, "ducklake.data-path": "s3://fixture/data"}}
}

func TestCachedCatalogBootstrapUsesSeparateClusterAndTenant(t *testing.T) {
	store := &catalogPropertiesStore{}
	var creates, reads atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		statement, _ := io.ReadAll(r.Body)
		user, password, ok := r.BasicAuth()
		if !ok {
			t.Error("missing verified password auth")
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(string(statement), "CREATE CATALOG") {
			if user != "__admin_provisioner" || password != "fixture-admin-password" {
				t.Error("catalog setup did not use admin")
			}
			if !strings.Contains(string(statement), `"fs.cache.enabled" = 'true'`) {
				t.Error("catalog setup did not enable caching")
			}
			// First attempt models delayed secret projection; a later attempt succeeds.
			if creates.Add(1) == 1 {
				_, _ = io.WriteString(w, `{"id":"fixture","error":{"message":"secret error must not leak","errorCode":1,"errorName":"GENERIC_INTERNAL_ERROR","errorType":"INTERNAL_ERROR"},"stats":{"state":"FAILED"}}`)
				return
			}
			store.created.Store(true)
			_, _ = io.WriteString(w, `{"id":"fixture","updateType":"CREATE CATALOG","stats":{"state":"FINISHED"}}`)
			return
		}
		if user != "org_fixture" || password != "fixture-tenant-password" || r.Header.Get("X-Trino-Catalog") != "org_fixture" {
			t.Error("catalog readiness did not use tenant identity and catalog")
		}
		if !strings.Contains(string(statement), "information_schema.schemata") {
			t.Errorf("readiness statement %s does not resolve catalog", statement)
		}
		reads.Add(1)
		_, _ = io.WriteString(w, `{"id":"fixture","stats":{"state":"FINISHED"},"columns":[{"name":"schema_name","type":"varchar","typeSignature":{"rawType":"varchar","arguments":[]}}],"data":[["posthog"]]}`)
	}))
	defer server.Close()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	password := filepath.Join(dir, "admin-password")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(password, []byte("fixture-admin-password"), 0600); err != nil {
		t.Fatal(err)
	}
	factory := defaultDriverFactory{trinoCachedURL: server.URL, trinoCachedCellID: "cached-cell", trinoAdminPasswordFile: password}
	connection := trinodriver.ConnectionConfig{ServerURL: "https://baseline.invalid", CatalogStoreCellID: "baseline-cell", Username: "org_fixture", Password: "fixture-tenant-password", Catalog: "org_fixture", CACertFile: ca, Startup: trinodriver.StartupOptions{PollInterval: time.Millisecond}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := factory.initializeCachedCatalog(ctx, store, connection); err != nil {
		t.Fatal(err)
	}
	if creates.Load() != 2 || reads.Load() != 1 {
		t.Fatalf("creates=%d tenant catalog checks=%d", creates.Load(), reads.Load())
	}
	if store.reads[0] != "baseline-cell" {
		t.Fatal("did not copy authoritative baseline cell")
	}
	if err := factory.initializeCachedCatalog(ctx, store, connection); err != nil {
		t.Fatal(err)
	}
	if creates.Load() != 2 || reads.Load() != 2 {
		t.Fatal("existing matching catalog should be validated and reused")
	}
}
