package controlplane

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	gingzip "github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/posthog/duckgres/controlplane/configstore"
)

type billingBatchStore interface {
	NextBillingBatch(context.Context, int) (*configstore.BillingBatch, error)
	GetBillingBatch(context.Context, string) (*configstore.BillingBatch, error)
	AckBillingBatch(context.Context, string) error
}

// Billing has one consumer. A batch is frozen before delivery, repeated until
// acknowledged, and retained for replay. See docs/design/billing-pull-api.md.
func registerBillingAPI(r gin.IRouter, store billingBatchStore, requireAdmin gin.HandlerFunc) {
	batches := r.Group("/billing/batches", requireAdmin, gingzip.Gzip(gingzip.DefaultCompression))
	batches.POST("/next", func(c *gin.Context) {
		limit := configstore.MaxBillingBatchEvents
		if raw, ok := c.GetQuery("limit"); ok {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > configstore.MaxBillingBatchEvents {
				c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be between 1 and 10000"})
				return
			}
			limit = parsed
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()
		batch, err := store.NextBillingBatch(ctx, limit)
		if billingError(c, err) {
			return
		}
		c.JSON(http.StatusOK, gin.H{"batch": batch})
	})
	batches.GET("/:batch_id", func(c *gin.Context) {
		if !validBillingBatchID(c) {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()
		batch, err := store.GetBillingBatch(ctx, c.Param("batch_id"))
		if billingError(c, err) {
			return
		}
		c.JSON(http.StatusOK, gin.H{"batch": batch})
	})
	batches.POST("/:batch_id/ack", func(c *gin.Context) {
		if !validBillingBatchID(c) {
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
		defer cancel()
		if billingError(c, store.AckBillingBatch(ctx, c.Param("batch_id"))) {
			return
		}
		c.JSON(http.StatusOK, gin.H{"acked": c.Param("batch_id")})
	})
}

func validBillingBatchID(c *gin.Context) bool {
	if _, err := uuid.Parse(c.Param("batch_id")); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "batch_id must be a UUID"})
		return false
	}
	return true
}

func billingError(c *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, configstore.ErrBillingBatchNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "billing batch not found"})
	} else {
		slog.Error("Billing batch operation failed.", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "billing store unavailable"})
	}
	return true
}
