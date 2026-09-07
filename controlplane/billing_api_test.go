package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

type fakeBatchStore struct {
	batch *configstore.BillingBatch
	err   error
	limit int
	ack   string
}

func (s *fakeBatchStore) NextBillingBatch(_ context.Context, limit int) (*configstore.BillingBatch, error) {
	s.limit = limit
	return s.batch, s.err
}
func (s *fakeBatchStore) GetBillingBatch(_ context.Context, _ string) (*configstore.BillingBatch, error) {
	return s.batch, s.err
}
func (s *fakeBatchStore) AckBillingBatch(_ context.Context, id string) error {
	s.ack = id
	return s.err
}

func TestBillingBatchHTTP(t *testing.T) {
	s := &fakeBatchStore{batch: &configstore.BillingBatch{ID: "f6812c40-4e36-43d2-bd2a-0c9dba371cde", Scans: []configstore.ScanUsageRow{{BytesScanned: json.Number("9223372036854775809")}}}}
	r := gin.New()
	registerBillingAPI(r.Group("/api/v1"), s, func(c *gin.Context) {
		if c.GetHeader("Authorization") != "Bearer test-admin" {
			c.AbortWithStatus(403)
		}
	})
	for _, tc := range []struct {
		method, path, auth string
		status             int
	}{
		{"POST", "/billing/batches/next", "", 403},
		{"POST", "/billing/batches/next?limit=0", "Bearer test-admin", 400},
		{"POST", "/billing/batches/next?limit=10001", "Bearer test-admin", 400},
		{"POST", "/billing/batches/next?limit=no", "Bearer test-admin", 400},
		{"POST", "/billing/batches/next?limit=2", "Bearer test-admin", 200},
		{"GET", "/billing/batches/" + s.batch.ID, "Bearer test-admin", 200},
		{"POST", "/billing/batches/" + s.batch.ID + "/ack", "Bearer test-admin", 200},
		{"POST", "/billing/batches/invalid/ack", "Bearer test-admin", 400},
		{"GET", "/billing/usage", "Bearer test-admin", 404},
		{"POST", "/billing/ack", "Bearer test-admin", 404},
	} {
		req := httptest.NewRequest(tc.method, "/api/v1"+tc.path, nil)
		req.Header.Set("Authorization", tc.auth)
		res := httptest.NewRecorder()
		r.ServeHTTP(res, req)
		if res.Code != tc.status {
			t.Errorf("%s %s got %d want %d: %s", tc.method, tc.path, res.Code, tc.status, res.Body.String())
		}
		if tc.method == "GET" && res.Code == 200 && !json.Valid(res.Body.Bytes()) {
			t.Fatal("invalid batch JSON")
		}
		if tc.method == "GET" && res.Code == 200 && !strings.Contains(res.Body.String(), `"bytes_scanned":9223372036854775809`) {
			t.Fatal("batch response lost integer precision")
		}
	}
	if s.limit != 2 || s.ack != s.batch.ID {
		t.Fatalf("wrong store arguments: %+v", s)
	}
}

func TestBillingBatchErrorsAndEmpty(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
	}{{nil, 200}, {configstore.ErrBillingBatchNotFound, 404}, {errors.New("db down"), 503}} {
		s := &fakeBatchStore{err: tc.err}
		r := gin.New()
		registerBillingAPI(r.Group("/api/v1"), s, func(_ *gin.Context) {})
		req := httptest.NewRequest(http.MethodGet, "/api/v1/billing/batches/f6812c40-4e36-43d2-bd2a-0c9dba371cde", nil)
		res := httptest.NewRecorder()
		r.ServeHTTP(res, req)
		if res.Code != tc.status {
			t.Errorf("got %d want %d", res.Code, tc.status)
		}
	}
	s := &fakeBatchStore{}
	r := gin.New()
	registerBillingAPI(r.Group("/api/v1"), s, func(_ *gin.Context) {})
	res := httptest.NewRecorder()
	r.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/api/v1/billing/batches/next", nil))
	if res.Code != 200 || res.Body.String() != `{"batch":null}` {
		t.Fatalf("empty queue response: %d %s", res.Code, res.Body.String())
	}
}
