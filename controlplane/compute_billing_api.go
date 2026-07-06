package controlplane

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// billingUsageStore is the config-store surface the billing pull API needs.
type billingUsageStore interface {
	AggregateComputeUsage(low, high time.Time) ([]configstore.ComputeUsageRow, error)
	ComputeBillingCursor() (time.Time, bool, error)
	AckComputeUsage(watermarkHigh time.Time) (int64, error)
}

// billingAPIHandler serves the pull-based compute-billing API
// (docs/design/billing-pull-api.md): billing GETs the usage accumulated since
// its last ack, processes it, and POSTs the watermark back; duckgres advances
// the cursor and deletes everything at or below it. Reads/deletes on the
// config store only — never on the query hot path.
type billingAPIHandler struct {
	store billingUsageStore
	now   func() time.Time
}

// registerBillingAPI mounts the billing pull API on an authenticated route
// group. The caller passes the admin-gating middleware (the billing service
// authenticates with the internal secret, which resolves to admin).
func registerBillingAPI(r gin.IRouter, store billingUsageStore, requireAdmin gin.HandlerFunc) {
	h := &billingAPIHandler{store: store, now: time.Now}
	r.GET("/billing/usage", requireAdmin, h.getUsage)
	r.POST("/billing/ack", requireAdmin, h.postAck)
}

// latestClosedBucket returns the newest bucket_start that is fully closed:
// every contribution for it has landed (grace exceeds the in-process flush
// interval), so it is safe to serve — and later delete on ack.
func (h *billingAPIHandler) latestClosedBucket() time.Time {
	return h.now().UTC().Add(-computeBucketWidth - computeBucketGrace).Truncate(computeBucketWidth)
}

// getUsage returns usage aggregated since the last ack — one row per key
// (org, team, query_source, worker size) per UTC day — plus the watermark
// window. watermark_low is the server cursor (what billing last acked;
// billing should cross-check it against its own record), watermark_high is
// what billing acks after processing.
func (h *billingAPIHandler) getUsage(c *gin.Context) {
	low, _, err := h.store.ComputeBillingCursor()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "read billing cursor: " + err.Error()})
		return
	}
	low = low.UTC() // zero time (never acked) serves everything buffered

	high := h.latestClosedBucket()
	if !high.After(low) {
		// Nothing closed beyond the cursor yet (fresh deploy or a pull racing
		// right behind an ack). An empty window with high == low is a valid
		// response: billing acks it as a no-op.
		c.JSON(http.StatusOK, gin.H{
			"watermark_low":  low.Format(time.RFC3339),
			"watermark_high": low.Format(time.RFC3339),
			"usage":          []configstore.ComputeUsageRow{},
		})
		return
	}

	rows, err := h.store.AggregateComputeUsage(low, high)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "aggregate usage: " + err.Error()})
		return
	}
	if rows == nil {
		rows = []configstore.ComputeUsageRow{}
	}
	c.JSON(http.StatusOK, gin.H{
		"watermark_low":  low.Format(time.RFC3339),
		"watermark_high": high.Format(time.RFC3339),
		"usage":          rows,
	})
}

type billingAckRequest struct {
	WatermarkHigh time.Time `json:"watermark_high" binding:"required"`
}

// postAck advances the cursor to watermark_high and deletes every buffered
// bucket at or below it. Idempotent: re-acking an already-acked (or older)
// watermark is a no-op. The watermark must be a closed bucket boundary the
// server could have served — acking into the still-accumulating present is
// rejected so an ack can never delete buckets that were never returned.
func (h *billingAPIHandler) postAck(c *gin.Context) {
	var req billingAckRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid ack body (want {\"watermark_high\": RFC3339}): " + err.Error()})
		return
	}
	high := req.WatermarkHigh.UTC()
	if latest := h.latestClosedBucket(); high.After(latest) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "watermark_high is beyond the latest closed bucket; ack exactly the watermark_high returned by GET /billing/usage",
		})
		return
	}
	deleted, err := h.store.AckComputeUsage(high)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "ack usage: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"acked":   high.Format(time.RFC3339),
		"deleted": deleted,
	})
}
