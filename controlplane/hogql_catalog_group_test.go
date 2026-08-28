//go:build kubernetes

package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/hogqlcatalog"
)

func TestHogQLCatalogGroupMethodAuthorization(t *testing.T) {
	tests := []struct {
		name      string
		token     string
		getStatus int
		putStatus int
	}{
		{name: "current read-only token", token: "read-current", getStatus: http.StatusOK, putStatus: http.StatusUnauthorized},
		{name: "fallback read-only token", token: "read-fallback", getStatus: http.StatusOK, putStatus: http.StatusUnauthorized},
		{name: "current admin token", token: "admin-current", getStatus: http.StatusOK, putStatus: http.StatusNoContent},
		{name: "fallback admin token", token: "admin-fallback", getStatus: http.StatusOK, putStatus: http.StatusNoContent},
		{name: "invalid token", token: "invalid", getStatus: http.StatusUnauthorized, putStatus: http.StatusUnauthorized},
		{name: "missing token", getStatus: http.StatusUnauthorized, putStatus: http.StatusUnauthorized},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			engine := gin.New()
			store := hogqlcatalog.NewMemoryStore()
			if err := store.Publish(context.Background(), hogQLCatalogSnapshot(1)); err != nil {
				t.Fatalf("publish fixture: %v", err)
			}
			readOnlyTokens := admin.NewTokenSet("read-current", []string{"read-fallback"})
			adminTokens := admin.NewTokenSet("admin-current", []string{"admin-fallback"})
			registerHogQLCatalogGroup(engine, readOnlyTokens, adminTokens, store, store)

			path := "/v1/hogql/compatibility/semantic-catalog?protocolVersion=1&languageVersion=1.0.0&catalog=ducklake&catalogDelimited=false"
			getRequest := httptest.NewRequest(http.MethodGet, path, nil)
			if test.token != "" {
				getRequest.Header.Set("X-Duckgres-Internal-Secret", test.token)
			}
			getResponse := httptest.NewRecorder()
			engine.ServeHTTP(getResponse, getRequest)
			if getResponse.Code != test.getStatus {
				t.Fatalf("GET status = %d, want %d: %s", getResponse.Code, test.getStatus, getResponse.Body.String())
			}

			body, err := json.Marshal(hogQLCatalogSnapshot(2))
			if err != nil {
				t.Fatalf("marshal snapshot: %v", err)
			}
			putRequest := httptest.NewRequest(http.MethodPut, "/v1/hogql/compatibility/semantic-catalog", bytes.NewReader(body))
			putRequest.Header.Set("Content-Type", "application/json")
			if test.token != "" {
				putRequest.Header.Set("X-Duckgres-Internal-Secret", test.token)
			}
			putResponse := httptest.NewRecorder()
			engine.ServeHTTP(putResponse, putRequest)
			if putResponse.Code != test.putStatus {
				t.Fatalf("PUT status = %d, want %d: %s", putResponse.Code, test.putStatus, putResponse.Body.String())
			}
		})
	}
}

func hogQLCatalogSnapshot(generation int64) *hogqlcatalog.HogQLSemanticCatalogSnapshot {
	return &hogqlcatalog.HogQLSemanticCatalogSnapshot{
		ProtocolVersion:   hogqlcatalog.SnapshotProtocolVersion,
		SchemaVersion:     hogqlcatalog.SnapshotSchemaVersion,
		LanguageVersion:   "1.0.0",
		Catalog:           hogqlcatalog.PhysicalIdentifier{Value: "ducklake"},
		Generation:        generation,
		LogicalTables:     []hogqlcatalog.LogicalTableDefinition{},
		ExpressionFields:  []hogqlcatalog.ExpressionFieldDefinition{},
		VirtualTables:     []hogqlcatalog.VirtualTableDefinition{},
		SavedQueries:      []hogqlcatalog.SavedQueryReference{},
		MaterializedViews: []hogqlcatalog.MaterializedViewReference{},
		Functions:         []hogqlcatalog.FunctionCapabilityDefinition{},
		ModifierDefaults:  []hogqlcatalog.SemanticModifierDefault{},
	}
}
