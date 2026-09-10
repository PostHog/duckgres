package hogqlcatalog

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestHTTPPublishesAndReadsLatestAndPinnedSnapshots(t *testing.T) {
	store := NewMemoryStore()
	router := testRouter(store)

	publishSnapshot(t, router, testSnapshot(1), http.StatusNoContent)
	publishSnapshot(t, router, testSnapshot(2), http.StatusNoContent)

	latest := getSnapshot(t, router, compatibilityPath("ducklake", false, 0, "1.0.0"), http.StatusOK)
	if latest.Generation != 2 {
		t.Fatalf("latest generation = %d, want 2", latest.Generation)
	}
	if latest.ProtocolVersion != SnapshotProtocolVersion {
		t.Fatalf("protocol version = %d, want %d", latest.ProtocolVersion, SnapshotProtocolVersion)
	}
	pinned := getSnapshot(t, router, compatibilityPath("ducklake", false, 1, "1.0.0"), http.StatusOK)
	if pinned.Generation != 1 {
		t.Fatalf("pinned generation = %d, want 1", pinned.Generation)
	}
}

func TestHTTPPublishesAndReadsDelimitedCatalog(t *testing.T) {
	store := NewMemoryStore()
	router := testRouter(store)
	snapshot := testSnapshot(1)
	snapshot.Catalog = PhysicalIdentifier{Value: "Case Sensitive", Delimited: true}
	for index := range snapshot.LogicalTables {
		snapshot.LogicalTables[index].PhysicalTable.Catalog = snapshot.Catalog
	}

	publishSnapshot(t, router, snapshot, http.StatusNoContent)
	read := getSnapshot(t, router, compatibilityPath("Case Sensitive", true, 0, "1.0.0"), http.StatusOK)
	if read.Catalog != snapshot.Catalog {
		t.Fatalf("catalog = %#v, want %#v", read.Catalog, snapshot.Catalog)
	}
}

func TestHTTPFailsClosedForUnknownAndMismatchedReads(t *testing.T) {
	store := NewMemoryStore()
	router := testRouter(store)
	publishSnapshot(t, router, testSnapshot(1), http.StatusNoContent)

	tests := []struct {
		name   string
		path   string
		status int
		code   string
	}{
		{
			name:   "unknown catalog",
			path:   compatibilityPath("missing", false, 0, "1.0.0"),
			status: http.StatusNotFound,
			code:   "HOGQL_CATALOG_NOT_FOUND",
		},
		{
			name:   "unknown generation",
			path:   compatibilityPath("ducklake", false, 9, "1.0.0"),
			status: http.StatusNotFound,
			code:   "HOGQL_CATALOG_GENERATION_NOT_FOUND",
		},
		{
			name:   "language mismatch",
			path:   compatibilityPath("ducklake", false, 0, "2.0.0"),
			status: http.StatusConflict,
			code:   "HOGQL_CATALOG_LANGUAGE_MISMATCH",
		},
		{
			name:   "missing language version",
			path:   "/v1/hogql/compatibility/semantic-catalog?protocolVersion=1&catalog=ducklake&catalogDelimited=false",
			status: http.StatusBadRequest,
			code:   "HOGQL_CATALOG_INVALID_REQUEST",
		},
		{
			name:   "unsupported protocol version",
			path:   "/v1/hogql/compatibility/semantic-catalog?protocolVersion=2&languageVersion=1.0.0&catalog=ducklake&catalogDelimited=false",
			status: http.StatusConflict,
			code:   "HOGQL_CATALOG_PROTOCOL_MISMATCH",
		},
		{
			name:   "unknown query field",
			path:   compatibilityPath("ducklake", false, 0, "1.0.0") + "&unknown=true",
			status: http.StatusBadRequest,
			code:   "HOGQL_CATALOG_INVALID_REQUEST",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := doRequest(router, http.MethodGet, test.path, nil)
			if rec.Code != test.status {
				t.Fatalf("status = %d, want %d: %s", rec.Code, test.status, rec.Body.String())
			}
			var response errorResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if response.Code != test.code {
				t.Fatalf("error code = %q, want %q", response.Code, test.code)
			}
		})
	}
}

func TestHTTPRejectsUnknownJSONFields(t *testing.T) {
	store := NewMemoryStore()
	router := testRouter(store)

	body := []byte(`{
		"schemaVersion": 1,
		"languageVersion": "1.0.0",
		"catalog": {"value": "ducklake", "delimited": false},
		"generation": 1,
		"logicalTables": [],
		"executableSql": "SELECT 1"
	}`)
	rec := doRequest(router, http.MethodPut, "/v1/hogql/compatibility/semantic-catalog", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown JSON field status = %d, want 400: %s", rec.Code, rec.Body.String())
	}

	if _, err := store.Latest(t.Context(), testCatalog()); err == nil {
		t.Fatal("rejected manifests must not publish a snapshot")
	}
}

func TestDecodeSnapshotRejectsTrailingAndUnknownJSON(t *testing.T) {
	valid, err := json.Marshal(testSnapshot(1))
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	tests := map[string]string{
		"trailing document":        string(valid) + `{}`,
		"unknown field":            strings.Replace(string(valid), `"generation":1`, `"generation":1,"unknown":true`, 1),
		"missing protocol version": strings.Replace(string(valid), `"protocolVersion":1,`, ``, 1),
		"missing required scalar":  strings.Replace(string(valid), `,"starVisible":true`, ``, 1),
		"missing required list":    strings.Replace(string(valid), `,"relationships":[]`, ``, 1),
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeSnapshot(strings.NewReader(document)); err == nil {
				t.Fatal("DecodeSnapshot accepted an incompatible document")
			}
		})
	}
}

func testRouter(store *MemoryStore) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	api := router.Group("/v1/hogql")
	RegisterAPI(api, api, store, store)
	return router
}

func publishSnapshot(t *testing.T, router http.Handler, snapshot *HogQLSemanticCatalogSnapshot, expectedStatus int) {
	t.Helper()
	body, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	rec := doRequest(router, http.MethodPut, "/v1/hogql/compatibility/semantic-catalog", body)
	if rec.Code != expectedStatus {
		t.Fatalf("publish status = %d, want %d: %s", rec.Code, expectedStatus, rec.Body.String())
	}
}

func compatibilityPath(catalog string, catalogDelimited bool, generation int64, languageVersion string) string {
	path := "/v1/hogql/compatibility/semantic-catalog?protocolVersion=1&languageVersion=" + url.QueryEscape(languageVersion) + "&catalog=" + url.QueryEscape(catalog) + "&catalogDelimited=" + strconv.FormatBool(catalogDelimited)
	if generation > 0 {
		path += "&generation=" + strconv.FormatInt(generation, 10)
	}
	return path
}

func getSnapshot(t *testing.T, router http.Handler, path string, expectedStatus int) *HogQLSemanticCatalogSnapshot {
	t.Helper()
	rec := doRequest(router, http.MethodGet, path, nil)
	if rec.Code != expectedStatus {
		t.Fatalf("GET %s status = %d, want %d: %s", path, rec.Code, expectedStatus, rec.Body.String())
	}
	var snapshot HogQLSemanticCatalogSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	return &snapshot
}

func doRequest(router http.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}
