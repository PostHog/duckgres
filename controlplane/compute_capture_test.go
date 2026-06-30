package controlplane

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestComputeCapturePayload asserts the capture request routes to a project via
// the top-level api_key (the team is set by the token, not the URL), carries the
// two raw metrics + the validity token property, and uses the deterministic uuid
// and bucket_start timestamp.
func TestComputeCapturePayload(t *testing.T) {
	var gotPath string
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	const token = "phc_test_token"
	c := newComputeCaptureClient(srv.URL, token)
	bucket := computeUsageBucket{
		OrgID:         "org-123",
		BucketStart:   time.Unix(1_700_000_040, 0).UTC(), // aligned 60s bucket
		CPUSeconds:    80,
		MemorySeconds: 160,
	}
	if err := c.Ship(context.Background(), bucket); err != nil {
		t.Fatalf("Ship: %v", err)
	}

	if gotPath != "/capture/" {
		t.Errorf("path = %q, want /capture/", gotPath)
	}
	// Team routing: api_key MUST be the token (not just a property).
	if body["api_key"] != token {
		t.Errorf("api_key = %v, want %q (routes event to the project/team)", body["api_key"], token)
	}
	if body["event"] != "managed warehouse compute usage" {
		t.Errorf("event = %v", body["event"])
	}
	if body["distinct_id"] != bucket.OrgID {
		t.Errorf("distinct_id = %v, want %q", body["distinct_id"], bucket.OrgID)
	}
	if body["uuid"] != computeEventUUID(bucket.OrgID, bucket.BucketStart) {
		t.Errorf("uuid = %v, want deterministic hash", body["uuid"])
	}
	props, ok := body["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties missing/not an object: %v", body["properties"])
	}
	if props["cpu_seconds"] != float64(80) || props["memory_seconds"] != float64(160) {
		t.Errorf("metrics = cpu %v / mem %v, want 80 / 160", props["cpu_seconds"], props["memory_seconds"])
	}
	// Validity marker for the billing gather query.
	if props["token"] != token {
		t.Errorf("properties.token = %v, want %q", props["token"], token)
	}
}

// TestComputeCaptureNon2xxIsError ensures a non-2xx keeps the bucket (drainer retries).
func TestComputeCaptureNon2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := newComputeCaptureClient(srv.URL, "tok")
	err := c.Ship(context.Background(), computeUsageBucket{OrgID: "o", BucketStart: time.Unix(1_700_000_040, 0).UTC()})
	if err == nil {
		t.Fatal("expected error on non-2xx so the drainer keeps the row and retries")
	}
}
