//go:build kubernetes

package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/hogqlcatalog"
)

func TestHogQLCatalogGroupRequiresInternalToken(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	store := hogqlcatalog.NewMemoryStore()
	snapshot := &hogqlcatalog.HogQLSemanticCatalogSnapshot{
		ProtocolVersion: hogqlcatalog.SnapshotProtocolVersion,
		SchemaVersion:   hogqlcatalog.SnapshotSchemaVersion,
		LanguageVersion: "1.0.0",
		Catalog:         hogqlcatalog.PhysicalIdentifier{Value: "ducklake"},
		Generation:      1,
		LogicalTables:   []hogqlcatalog.LogicalTableDefinition{},
	}
	if err := store.Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("publish fixture: %v", err)
	}
	registerHogQLCatalogGroup(engine, admin.NewTokenSet("internal-secret", nil), store, store)

	path := "/v1/hogql/compatibility/semantic-catalog?protocolVersion=1&languageVersion=1.0.0&catalog=ducklake&catalogDelimited=false"
	unauthorized := httptest.NewRecorder()
	engine.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, path, nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, want 401", unauthorized.Code)
	}

	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.Header.Set("X-Duckgres-Internal-Secret", "internal-secret")
	authorized := httptest.NewRecorder()
	engine.ServeHTTP(authorized, request)
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized status = %d, want 200: %s", authorized.Code, authorized.Body.String())
	}
}
