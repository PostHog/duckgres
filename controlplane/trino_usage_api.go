package controlplane

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner/opa"
)

const maxTrinoUsageBodyBytes = 16 << 20

type trinoUsageStore interface {
	RecordTrinoQueryUsage(context.Context, configstore.QueryUsageEvent) (bool, error)
}

// Decode only the stable accounting fields; SQL, plans, and failure messages are
// deliberately not retained. Unknown fields allow additive Trino SPI changes.
type trinoCompletedUsage struct {
	Metadata struct {
		QueryID    string `json:"queryId"`
		QueryState string `json:"queryState"`
	} `json:"metadata"`
	Context struct {
		User          string `json:"user"`
		Source        string `json:"source"`
		ServerVersion string `json:"serverVersion"`
	} `json:"context"`
	Statistics struct {
		PhysicalInputBytes  *int64 `json:"physicalInputBytes"`
		ProcessedInputBytes int64  `json:"processedInputBytes"`
		Complete            bool   `json:"complete"`
	} `json:"statistics"`
	FailureInfo struct {
		ErrorCode struct {
			Name string `json:"name"`
		} `json:"errorCode"`
	} `json:"failureInfo"`
	EndTime time.Time `json:"endTime"`
}

// The route is outside admin authentication: this credential grants ingestion
// only. Cell attribution comes from local configuration, never request headers.
func registerTrinoUsageAPI(r gin.IRouter, store trinoUsageStore, cellID, token string) {
	if token == "" || cellID == "" {
		panic("Trino usage requires a cell and ingestion token")
	}
	r.POST("/api/v1/trino/usage", func(c *gin.Context) {
		if subtle.ConstantTimeCompare([]byte(c.GetHeader("Authorization")), []byte("Bearer "+token)) != 1 {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxTrinoUsageBodyBytes)
		decoder := json.NewDecoder(c.Request.Body)
		var event trinoCompletedUsage
		err := decoder.Decode(&event)
		if err == nil {
			// Read to EOF to reject trailing documents and enforce the size cap
			// even when a small JSON document precedes an oversized suffix.
			var extra any
			err = decoder.Decode(&extra)
			switch err {
			case io.EOF:
				err = nil
			case nil:
				err = errors.New("trailing JSON")
			}
		}
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				c.AbortWithStatus(http.StatusRequestEntityTooLarge)
			} else {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid Trino completion event"})
			}
			return
		}
		if event.Metadata.QueryID == "" || event.Context.User == "" || event.EndTime.IsZero() ||
			(event.Metadata.QueryState != "FINISHED" && event.Metadata.QueryState != "FAILED") ||
			event.Statistics.PhysicalInputBytes == nil || *event.Statistics.PhysicalInputBytes < 0 || event.Statistics.ProcessedInputBytes < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "completion requires queryId, user, endTime, terminal queryState and nonnegative physicalInputBytes"})
			return
		}
		// OPA forbids impersonation. The effective authenticated user owns the
		// query; client-controlled source/catalog fields cannot exempt usage.
		if event.Context.User == opa.AdminPrincipal || event.Context.User == opa.ObserverPrincipal {
			c.JSON(http.StatusOK, gin.H{"ignored": true})
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
		defer cancel()
		inserted, err := store.RecordTrinoQueryUsage(ctx, configstore.QueryUsageEvent{
			ClusterID: cellID, QueryID: event.Metadata.QueryID, Principal: event.Context.User,
			Source: event.Context.Source, State: event.Metadata.QueryState, ErrorCode: event.FailureInfo.ErrorCode.Name,
			CompletedAt: event.EndTime, PhysicalInputBytes: *event.Statistics.PhysicalInputBytes,
			ProcessedInputBytes: event.Statistics.ProcessedInputBytes, StatisticsComplete: event.Statistics.Complete,
			TrinoVersion: event.Context.ServerVersion,
		})
		if err != nil {
			slog.Error("Trino usage persistence failed; listener should retry.", "error", err)
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "usage store unavailable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"inserted": inserted})
	})
}
