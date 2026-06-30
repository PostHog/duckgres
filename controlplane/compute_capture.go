package controlplane

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// computeUsageEventName is the PostHog capture event carrying the two raw
// compute-usage metrics. Billing derives its two usage_keys by summing each
// property. Must match the posthog-side gather query.
const computeUsageEventName = "managed warehouse compute usage"

// computeCaptureClient ships closed compute-usage buckets to PostHog's public
// ingestion endpoint via the capture API, exactly like any SDK. Authed by a
// project API token stamped on the event `token` property.
type computeCaptureClient struct {
	baseURL string
	token   string
	http    *http.Client
}

func newComputeCaptureClient(baseURL, token string) *computeCaptureClient {
	return &computeCaptureClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		http:    &http.Client{Timeout: 15 * time.Second},
	}
}

// computeEventUUID derives a deterministic, stable event UUID from
// (org_id, bucket_start) so a re-ship after an ingestion ack but before the
// delete commits collapses onto the same ClickHouse row (ReplacingMergeTree
// dedups on cityHash64(uuid)). Formatted as a UUID string for ingestion.
func computeEventUUID(orgID string, bucketStart time.Time) string {
	h := sha256.Sum256([]byte(orgID + "|" + fmt.Sprintf("%d", bucketStart.UTC().Unix())))
	// Stamp RFC-4122 version (4) and variant bits so it is a well-formed UUID.
	var b [16]byte
	copy(b[:], h[:16])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]),
	)
}

// capturePayload is the PostHog capture API request body.
type capturePayload struct {
	Event      string                 `json:"event"`
	DistinctID string                 `json:"distinct_id"`
	Properties map[string]interface{} `json:"properties"`
	Timestamp  string                 `json:"timestamp"`
	UUID       string                 `json:"uuid"`
}

// Ship sends one bucket as a capture event. Any 2xx is success. On a non-2xx or
// transport error it returns an error so the drainer keeps the row and retries.
func (c *computeCaptureClient) Ship(ctx context.Context, b computeUsageBucket) error {
	uuid := computeEventUUID(b.OrgID, b.BucketStart)
	payload := capturePayload{
		Event:      computeUsageEventName,
		DistinctID: b.OrgID,
		Properties: map[string]interface{}{
			"cpu_seconds":    b.CPUSeconds,
			"memory_seconds": b.MemorySeconds,
			"bucket_start":   b.BucketStart.UTC().Format(time.RFC3339),
			"token":          c.token,
		},
		Timestamp: b.BucketStart.UTC().Format(time.RFC3339),
		UUID:      uuid,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal capture payload: %w", err)
	}

	url := c.baseURL + "/capture/"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build capture request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("post capture event: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("capture event rejected: status %d", resp.StatusCode)
	}
	return nil
}

// computeUsageBucket is the drainer's local view of one buffer row. It mirrors
// configstore.ComputeUsageBucket so the capture client and drainer do not
// depend on the store package's concrete type in tests.
type computeUsageBucket struct {
	OrgID         string
	BucketStart   time.Time
	CPUSeconds    int64
	MemorySeconds int64
}
