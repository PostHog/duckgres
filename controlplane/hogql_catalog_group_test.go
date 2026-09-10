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
			if err := store.PublishExchangeRates(context.Background(), hogQLExchangeRateSnapshot(1)); err != nil {
				t.Fatalf("publish exchange-rate fixture: %v", err)
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

			exchangeGetRequest := httptest.NewRequest(http.MethodGet, "/v1/hogql/compatibility/exchange-rates?protocolVersion=1", nil)
			if test.token != "" {
				exchangeGetRequest.Header.Set("X-Duckgres-Internal-Secret", test.token)
			}
			exchangeGetResponse := httptest.NewRecorder()
			engine.ServeHTTP(exchangeGetResponse, exchangeGetRequest)
			if exchangeGetResponse.Code != test.getStatus {
				t.Fatalf("exchange-rate GET status = %d, want %d: %s", exchangeGetResponse.Code, test.getStatus, exchangeGetResponse.Body.String())
			}

			exchangeBody, err := json.Marshal(hogQLExchangeRateSnapshot(2))
			if err != nil {
				t.Fatalf("marshal exchange-rate snapshot: %v", err)
			}
			exchangePutRequest := httptest.NewRequest(http.MethodPut, "/v1/hogql/compatibility/exchange-rates", bytes.NewReader(exchangeBody))
			exchangePutRequest.Header.Set("Content-Type", "application/json")
			if test.token != "" {
				exchangePutRequest.Header.Set("X-Duckgres-Internal-Secret", test.token)
			}
			exchangePutResponse := httptest.NewRecorder()
			engine.ServeHTTP(exchangePutResponse, exchangePutRequest)
			if exchangePutResponse.Code != test.putStatus {
				t.Fatalf("exchange-rate PUT status = %d, want %d: %s", exchangePutResponse.Code, test.putStatus, exchangePutResponse.Body.String())
			}
		})
	}
}

func hogQLExchangeRateSnapshot(generation int64) *hogqlcatalog.ExchangeRateSnapshot {
	return &hogqlcatalog.ExchangeRateSnapshot{
		ProtocolVersion: hogqlcatalog.ExchangeRateProtocolVersion,
		SchemaVersion:   hogqlcatalog.ExchangeRateSchemaVersion,
		Generation:      generation,
		BaseCurrency:    hogqlcatalog.ExchangeRateBaseCurrency,
		DecimalScale:    hogqlcatalog.ExchangeRateDecimalScale,
		Rates: []hogqlcatalog.ExchangeRateEntry{
			{Currency: "EUR", EffectiveDate: "2024-01-01", UnscaledRate: "9049000000"},
			{Currency: "USD", EffectiveDate: "1970-01-01", UnscaledRate: "10000000000"},
		},
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
