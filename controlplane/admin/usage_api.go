//go:build kubernetes

package admin

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// monthlyUsageStore is the config-store surface the admin monthly-usage
// endpoint needs. *configstore.ConfigStore satisfies it; tests fake it.
type monthlyUsageStore interface {
	AggregateComputeUsageMonthly(from time.Time) ([]configstore.MonthlyComputeUsageRow, error)
	AggregateStorageUsageMonthly(from time.Time) ([]configstore.MonthlyStorageUsageRow, error)
	ComputeBillingCursor() (cursor time.Time, ok bool, err error)
}

// usageAPIHandler serves the admin console's monthly per-team usage page
// ("Usage"). It reads the SAME durable billing buffer the pull API serves —
// there is no second accounting pipeline — so its retention is the buffer's:
// buckets billing has acked are deleted (AckComputeUsage) and buckets older
// than 30 days are GC'd. The response surfaces the ack cursor as
// watermark_low so the UI can caveat the window instead of silently implying
// all-time totals.
type usageAPIHandler struct {
	store monthlyUsageStore
	now   func() time.Time
}

const (
	usageDefaultMonths = 6
	usageMaxMonths     = 36
)

// monthlyUsageResponseRow is one (month, org, team) line of the merged view.
// gib_seconds is the exact-decimal GiB-seconds text carried as json.Number
// (unquoted JSON number, full precision), matching the billing pull API.
type monthlyUsageResponseRow struct {
	Month         string      `json:"month"`
	OrgID         string      `json:"org_id"`
	TeamID        int64       `json:"team_id"`
	SchemaName    *string     `json:"schema_name"`
	CPUSeconds    int64       `json:"cpu_seconds"`
	MemorySeconds int64       `json:"memory_seconds"`
	GiBSeconds    json.Number `json:"gib_seconds"`
}

// registerUsageAPI mounts the monthly-usage read on the admin API group.
// Although it is a GET (RoleGate would admit viewers), it self-gates with
// RequireAdmin: this is per-team COST data for every org — the billing pull
// API gates the raw families behind RequireAdmin for exactly that reason, and
// the monthly aggregate is no less sensitive. The gate travels with the route
// (not RoleGate's path list), so renaming the route cannot downgrade it.
func registerUsageAPI(r gin.IRouter, store monthlyUsageStore) {
	h := &usageAPIHandler{store: store, now: time.Now}
	r.GET("/usage/monthly", RequireAdmin(), h.getMonthlyUsage)
}

func (h *usageAPIHandler) getMonthlyUsage(c *gin.Context) {
	months := usageDefaultMonths
	if raw := c.Query("months"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > usageMaxMonths {
			c.JSON(http.StatusBadRequest, gin.H{"error": "months must be an integer in [1, " + strconv.Itoa(usageMaxMonths) + "]"})
			return
		}
		months = n
	}
	// Window opens at the first of the UTC month (months-1) before the current
	// month, so months=1 is the current (partial) month.
	now := h.now().UTC()
	from := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -(months - 1), 0)

	compute, err := h.store.AggregateComputeUsageMonthly(from)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "aggregate monthly compute usage: " + err.Error()})
		return
	}
	storage, err := h.store.AggregateStorageUsageMonthly(from)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "aggregate monthly storage usage: " + err.Error()})
		return
	}
	cursor, hasCursor, err := h.store.ComputeBillingCursor()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "read billing cursor: " + err.Error()})
		return
	}

	type key struct {
		month string
		org   string
		team  int64
	}
	merged := make(map[key]*monthlyUsageResponseRow, len(compute)+len(storage))
	for _, r := range compute {
		k := key{r.Month, r.OrgID, r.TeamID}
		merged[k] = &monthlyUsageResponseRow{
			Month:         r.Month,
			OrgID:         r.OrgID,
			TeamID:        r.TeamID,
			SchemaName:    r.SchemaName,
			CPUSeconds:    r.CPUSeconds,
			MemorySeconds: r.MemorySeconds,
			GiBSeconds:    "0",
		}
	}
	for _, r := range storage {
		k := key{r.Month, r.OrgID, r.TeamID}
		row, ok := merged[k]
		if !ok {
			row = &monthlyUsageResponseRow{Month: r.Month, OrgID: r.OrgID, TeamID: r.TeamID}
			merged[k] = row
		}
		row.GiBSeconds = r.GiBSeconds
		if row.SchemaName == nil {
			row.SchemaName = r.SchemaName
		}
	}
	rows := make([]monthlyUsageResponseRow, 0, len(merged))
	for _, r := range merged {
		rows = append(rows, *r)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Month != rows[j].Month {
			return rows[i].Month > rows[j].Month // newest month first
		}
		if rows[i].OrgID != rows[j].OrgID {
			return rows[i].OrgID < rows[j].OrgID
		}
		return rows[i].TeamID < rows[j].TeamID
	})

	var watermarkLow interface{}
	if hasCursor {
		watermarkLow = cursor.UTC().Format(time.RFC3339)
	}
	c.JSON(http.StatusOK, gin.H{
		"months":        months,
		"from":          from.Format(time.RFC3339),
		"watermark_low": watermarkLow,
		"rows":          rows,
	})
}
