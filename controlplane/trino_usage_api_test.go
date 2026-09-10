package controlplane

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakeTrinoUsageStore struct {
	events []configstore.QueryUsageEvent
	err    error
}

func (s *fakeTrinoUsageStore) RecordTrinoQueryUsage(_ context.Context, event configstore.QueryUsageEvent) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	s.events = append(s.events, event)
	return len(s.events) == 1, nil
}

const completedUsageJSON = `{"metadata":{"queryId":"20260907_123456_00001_abcde","queryState":"FINISHED","query":"SELECT private_column FROM private_table"},"context":{"user":"tenant_catalog","principal":"tenant_catalog","source":"client-selected-source","serverVersion":"481"},"statistics":{"physicalInputBytes":9007199254740993,"processedInputBytes":1234,"complete":true},"endTime":"2026-09-07T12:34:56.123Z"}`

func TestTrinoUsageDelivery(t *testing.T) {
	for _, state := range []string{"FINISHED", "FAILED"} {
		t.Run(state, func(t *testing.T) {
			store := &fakeTrinoUsageStore{}
			r := gin.New()
			registerTrinoUsageAPI(r, store, "test-cell", "test-token")
			body := strings.Replace(completedUsageJSON, "FINISHED", state, 1)
			body = strings.Replace(body, `"complete":true`, `"complete":false`, 1)
			for range 2 {
				response := usageRequest(r, body, "Bearer test-token")
				if response.Code != http.StatusOK {
					t.Fatalf("status %d: %s", response.Code, response.Body.String())
				}
			}
			e := store.events[0]
			if e.ClusterID != "test-cell" || e.Principal != "tenant_catalog" || e.PhysicalInputBytes != 9007199254740993 || e.State != state || e.StatisticsComplete || e.CompletedAt.IsZero() {
				t.Fatalf("incorrect normalized event: %+v", e)
			}
		})
	}
}

func TestTrinoUsageValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body, auth string
		status           int
	}{
		{"missing auth", completedUsageJSON, "", 401},
		{"wrong credential", completedUsageJSON, "Bearer admin-token", 401},
		{"missing bytes", strings.Replace(completedUsageJSON, `"physicalInputBytes":9007199254740993,`, "", 1), "Bearer test-token", 400},
		{"negative bytes", strings.Replace(completedUsageJSON, "9007199254740993", "-1", 1), "Bearer test-token", 400},
		{"overflow", strings.Replace(completedUsageJSON, "9007199254740993", "9223372036854775808", 1), "Bearer test-token", 400},
		{"nonterminal", strings.Replace(completedUsageJSON, "FINISHED", "RUNNING", 1), "Bearer test-token", 400},
		{"no completion", strings.Replace(completedUsageJSON, "2026-09-07T12:34:56.123Z", "", 1), "Bearer test-token", 400},
		{"trailing JSON", completedUsageJSON + `{}`, "Bearer test-token", 400},
		{"oversized", completedUsageJSON + strings.Repeat(" ", maxTrinoUsageBodyBytes), "Bearer test-token", 413},
		{"zero bytes", strings.Replace(completedUsageJSON, "9007199254740993", "0", 1), "Bearer test-token", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeTrinoUsageStore{}
			r := gin.New()
			registerTrinoUsageAPI(r, store, "test-cell", "test-token")
			res := usageRequest(r, tc.body, tc.auth)
			if res.Code != tc.status {
				t.Fatalf("status %d want %d: %s", res.Code, tc.status, res.Body.String())
			}
			if tc.status != 200 && len(store.events) != 0 {
				t.Fatal("invalid request was stored")
			}
		})
	}
}

func TestTrinoUsageServiceIdentityAndRetry(t *testing.T) {
	store := &fakeTrinoUsageStore{}
	r := gin.New()
	registerTrinoUsageAPI(r, store, "test-cell", "test-token")
	for _, service := range []string{"__admin_provisioner", "__duckgres_observer"} {
		res := usageRequest(r, strings.ReplaceAll(completedUsageJSON, "tenant_catalog", service), "Bearer test-token")
		if res.Code != 200 || len(store.events) != 0 {
			t.Fatal("service query was billed")
		}
	}
	// A client cannot exempt itself by choosing the observer's source name.
	res := usageRequest(r, strings.ReplaceAll(completedUsageJSON, "client-selected-source", "__duckgres_observer"), "Bearer test-token")
	if res.Code != 200 || len(store.events) != 1 {
		t.Fatal("client source changed billing attribution")
	}
	store.err = errors.New("database unavailable")
	res = usageRequest(r, completedUsageJSON, "Bearer test-token")
	if res.Code != 503 {
		t.Fatalf("database failure must trigger listener retry: %d", res.Code)
	}
}

func TestTrinoUsageCancellationRetainsWork(t *testing.T) {
	store := &fakeTrinoUsageStore{}
	r := gin.New()
	registerTrinoUsageAPI(r, store, "test-cell", "test-token")
	body := strings.Replace(completedUsageJSON, "FINISHED", "FAILED", 1)
	body = strings.TrimSuffix(body, "}") + `,"failureInfo":{"errorCode":{"name":"USER_CANCELED"}}}`
	res := usageRequest(r, body, "Bearer test-token")
	if res.Code != 200 || len(store.events) != 1 {
		t.Fatalf("cancelled query rejected: %d %s", res.Code, res.Body.String())
	}
	if event := store.events[0]; event.ErrorCode != "USER_CANCELED" || event.PhysicalInputBytes != 9007199254740993 {
		t.Fatalf("lost cancellation usage: %+v", event)
	}
}

func usageRequest(r http.Handler, body, auth string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/trino/usage", strings.NewReader(body))
	req.Header.Set("Authorization", auth)
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)
	return res
}
