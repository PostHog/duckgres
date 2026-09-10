package hogqlcatalog

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestExchangeRateHTTPPublishesAndReadsLatestAndExactGenerations(t *testing.T) {
	store := NewMemoryStore()
	router := exchangeRateTestRouter(store)
	publishExchangeRates(t, router, testExchangeRateSnapshot(1), http.StatusNoContent)
	second := testExchangeRateSnapshot(2)
	second.Rates[1].UnscaledRate = "9100000000"
	publishExchangeRates(t, router, second, http.StatusNoContent)
	publishExchangeRates(t, router, second, http.StatusNoContent)

	latest, response := getExchangeRates(t, router, exchangeRateCompatibilityPath(0), http.StatusOK)
	if latest.Generation != 2 {
		t.Fatalf("latest generation = %d, want 2", latest.Generation)
	}
	if got := response.Header().Get("ETag"); got != `"hogql-exchange-rates-2"` {
		t.Fatalf("ETag = %q, want %q", got, `"hogql-exchange-rates-2"`)
	}
	exact, _ := getExchangeRates(t, router, exchangeRateCompatibilityPath(1), http.StatusOK)
	if exact.Rates[1].UnscaledRate != "9049000000" {
		t.Fatalf("generation 1 EUR rate = %q, want 9049000000", exact.Rates[1].UnscaledRate)
	}
}

func TestExchangeRateHTTPFailsClosedForInvalidPublicationAndReads(t *testing.T) {
	store := NewMemoryStore()
	router := exchangeRateTestRouter(store)
	publishExchangeRates(t, router, testExchangeRateSnapshot(2), http.StatusNoContent)

	changed := testExchangeRateSnapshot(2)
	changed.Rates[1].UnscaledRate = "9100000000"
	publishExchangeRates(t, router, changed, http.StatusConflict)
	publishExchangeRates(t, router, testExchangeRateSnapshot(1), http.StatusConflict)
	invalid := testExchangeRateSnapshot(3)
	invalid.Rates[0].Currency = "aed"
	publishExchangeRates(t, router, invalid, http.StatusBadRequest)

	tests := []struct {
		name   string
		path   string
		status int
		code   string
	}{
		{name: "unknown generation", path: exchangeRateCompatibilityPath(9), status: http.StatusNotFound, code: "HOGQL_EXCHANGE_RATES_GENERATION_NOT_FOUND"},
		{name: "unsupported protocol", path: "/v1/hogql/compatibility/exchange-rates?protocolVersion=2", status: http.StatusConflict, code: "HOGQL_EXCHANGE_RATES_PROTOCOL_MISMATCH"},
		{name: "missing protocol", path: "/v1/hogql/compatibility/exchange-rates", status: http.StatusBadRequest, code: "HOGQL_EXCHANGE_RATES_INVALID_REQUEST"},
		{name: "invalid generation", path: "/v1/hogql/compatibility/exchange-rates?protocolVersion=1&generation=0", status: http.StatusBadRequest, code: "HOGQL_EXCHANGE_RATES_INVALID_REQUEST"},
		{name: "unknown query field", path: exchangeRateCompatibilityPath(0) + "&unknown=true", status: http.StatusBadRequest, code: "HOGQL_EXCHANGE_RATES_INVALID_REQUEST"},
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

func TestExchangeRateHTTPReturnsNotFoundBeforeFirstPublication(t *testing.T) {
	_, response := getExchangeRates(t, exchangeRateTestRouter(NewMemoryStore()), exchangeRateCompatibilityPath(0), http.StatusNotFound)
	var failure errorResponse
	if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if failure.Code != "HOGQL_EXCHANGE_RATES_NOT_FOUND" {
		t.Fatalf("error code = %q, want HOGQL_EXCHANGE_RATES_NOT_FOUND", failure.Code)
	}
}

func exchangeRateTestRouter(store *MemoryStore) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	api := router.Group("/v1/hogql")
	RegisterExchangeRateAPI(api, api, store, store)
	return router
}

func publishExchangeRates(t *testing.T, router http.Handler, snapshot *ExchangeRateSnapshot, expectedStatus int) {
	t.Helper()
	body, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal exchange rates: %v", err)
	}
	rec := doRequest(router, http.MethodPut, "/v1/hogql/compatibility/exchange-rates", body)
	if rec.Code != expectedStatus {
		t.Fatalf("publish status = %d, want %d: %s", rec.Code, expectedStatus, rec.Body.String())
	}
}

func exchangeRateCompatibilityPath(generation int64) string {
	path := "/v1/hogql/compatibility/exchange-rates?protocolVersion=1"
	if generation > 0 {
		path += "&generation=" + strconv.FormatInt(generation, 10)
	}
	return path
}

func getExchangeRates(t *testing.T, router http.Handler, path string, expectedStatus int) (*ExchangeRateSnapshot, *httptest.ResponseRecorder) {
	t.Helper()
	rec := doRequest(router, http.MethodGet, path, nil)
	if rec.Code != expectedStatus {
		t.Fatalf("GET %s status = %d, want %d: %s", path, rec.Code, expectedStatus, rec.Body.String())
	}
	var snapshot ExchangeRateSnapshot
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &snapshot); err != nil {
			t.Fatalf("decode exchange rates: %v", err)
		}
	}
	return &snapshot, rec
}
