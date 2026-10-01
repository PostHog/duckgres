//go:build kubernetes

package admin

import (
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
)

const (
	trinoMonitoringSchemaVersion   = 1
	trinoMonitoringQueryLimit      = 200
	trinoMonitoringStateNotEnabled = "not_enabled"
)

// In-flight states reported to tenants and used as the `state` label of the
// per-org in-flight gauge. They partition every query that has not finished.
const (
	TrinoInFlightQueued  = "queued"
	TrinoInFlightRunning = "running"
	TrinoInFlightBlocked = "blocked"
	TrinoInFlightOther   = "other"
)

// TrinoInFlightStates lists every value TrinoInFlightState returns.
var TrinoInFlightStates = []string{TrinoInFlightQueued, TrinoInFlightRunning, TrinoInFlightBlocked, TrinoInFlightOther}

// TrinoInFlightState classifies a query that has not finished. ok is false
// for a finished or failed query.
//
// "other" is a query past the queue that is not executing yet or any more
// (planning, starting, finishing). It still holds a concurrency slot.
func TrinoInFlightState(q TrinoQuery) (state string, ok bool) {
	switch {
	case !isActiveTrinoState(q.State):
		return "", false
	case q.State == trinoStateQueued:
		return TrinoInFlightQueued, true
	case q.State == trinoStateRunning && q.FullyBlocked:
		return TrinoInFlightBlocked, true
	case q.State == trinoStateRunning:
		return TrinoInFlightRunning, true
	default:
		return TrinoInFlightOther, true
	}
}

// trinoMonitoringMetrics is the allow-list behind the Trino series route.
// Every template carries $ORG, so no series can be read without an exact org
// selector. The in-flight gauge takes the maximum across pods: the collector
// runs under a leader lease, and during a handover two pods can export it.
var trinoMonitoringMetrics = map[string]monitoringMetricSpec{
	"queries_in_flight": {
		PromQL: `max by (state) (duckgres_trino_org_queries$ORG)`, Unit: "queries",
		AllowedLabels: labelSet("state"),
	},
	"query_rate": {
		PromQL: `sum by (status, error_type) (rate(duckgres_trino_org_query_total$ORG[$WIN]))`, Unit: "queries_per_second",
		AllowedLabels: labelSet("status", "error_type"),
	},
	"error_ratio": {
		PromQL: `(sum(rate(duckgres_trino_org_query_total$ORGERR[$WIN])) or vector(0)) / clamp_min((sum(rate(duckgres_trino_org_query_total$ORG[$WIN])) or vector(0)), 1e-9)`,
		Unit:   "ratio", AllowedLabels: labelSet(),
	},
	"duration_p50": {
		PromQL: `histogram_quantile(0.50, sum by (le) (rate(duckgres_trino_org_query_duration_seconds_bucket$ORG[$WIN])))`, Unit: "seconds",
		AllowedLabels: labelSet(),
	},
	"duration_p95": {
		PromQL: `histogram_quantile(0.95, sum by (le) (rate(duckgres_trino_org_query_duration_seconds_bucket$ORG[$WIN])))`, Unit: "seconds",
		AllowedLabels: labelSet(),
	},
	"queue_time_p95": {
		PromQL: `histogram_quantile(0.95, sum by (le) (rate(duckgres_trino_org_query_queued_seconds_bucket$ORG[$WIN])))`, Unit: "seconds",
		AllowedLabels: labelSet(),
	},
	"scanned_bytes_rate": {
		PromQL: `(sum(rate(duckgres_trino_org_query_physical_input_bytes_total$ORG[$WIN])) or vector(0))`, Unit: "bytes_per_second",
		AllowedLabels: labelSet(),
	},
	"cpu_seconds_rate": {
		PromQL: `(sum(rate(duckgres_trino_org_query_cpu_seconds_total$ORG[$WIN])) or vector(0))`, Unit: "cpu_seconds_per_second",
		AllowedLabels: labelSet(),
	},
	"storage_bytes": {
		PromQL: `max(duckgres_org_storage_tracked_bytes$ORG)`, Unit: "bytes", AllowedLabels: labelSet(),
	},
}

type trinoMonitoringStore interface {
	Snapshot() *configstore.Snapshot
}

type trinoMonitoringState struct {
	State    string     `json:"state"`
	ReadyAt  *time.Time `json:"ready_at"`
	FailedAt *time.Time `json:"failed_at"`
}

type trinoMonitoringLimits struct {
	MaxRunningQueries int `json:"max_running_queries"`
	MaxQueuedQueries  int `json:"max_queued_queries"`
}

type trinoMonitoringTotals struct {
	InFlight int `json:"in_flight"`
	// Running is every in-flight query past the queue, which is what the
	// tier's concurrency limit counts. InFlight = Running + Queued.
	Running int `json:"running"`
	Queued  int `json:"queued"`
	// Blocked is the subset of Running whose drivers are all blocked.
	Blocked int `json:"blocked"`
	// LongestRunningMS is the longest time since submission among all in-flight queries, queued ones included.
	LongestRunningMS   int64 `json:"longest_running_ms"`
	PhysicalInputBytes int64 `json:"physical_input_bytes"`
}

type trinoMonitoringQuery struct {
	QueryID            string     `json:"query_id"`
	State              string     `json:"state"`
	User               string     `json:"user"`
	Source             string     `json:"source"`
	Query              string     `json:"query"`
	CreatedAt          *time.Time `json:"created_at"`
	ElapsedMS          int64      `json:"elapsed_ms"`
	QueuedMS           int64      `json:"queued_ms"`
	CPUMS              int64      `json:"cpu_ms"`
	PhysicalInputBytes int64      `json:"physical_input_bytes"`
	PeakMemoryBytes    int64      `json:"peak_memory_bytes"`
	ProcessedInputRows int64      `json:"processed_input_rows"`
	ProgressPercentage *float64   `json:"progress_percentage"`
	Blocked            bool       `json:"blocked"`
}

type trinoMonitoringSnapshotResponse struct {
	SchemaVersion int                  `json:"schema_version"`
	OrgID         string               `json:"org_id"`
	AsOf          time.Time            `json:"as_of"`
	Trino         trinoMonitoringState `json:"trino"`
	// Available is false when the coordinator or the org index could not be
	// read, or no configured cell owns the org. Totals are then zero, and a consumer must not present them as an idle warehouse.
	Available        bool                   `json:"available"`
	Limits           trinoMonitoringLimits  `json:"limits"`
	Totals           trinoMonitoringTotals  `json:"totals"`
	Queries          []trinoMonitoringQuery `json:"queries"`
	QueriesTruncated bool                   `json:"queries_truncated"`
}

type trinoMonitoringHandler struct {
	store   trinoMonitoringStore
	trino   *TrinoAPI
	metrics *MetricsProxy
}

// registerTrinoMonitoringAPI mounts the tenant-safe Trino monitoring contract
// that the PostHog backend reads. A nil trino means the deployment has no
// Trino cell; the snapshot then reports every org as not enabled.
func registerTrinoMonitoringAPI(r *gin.RouterGroup, store trinoMonitoringStore, trino *TrinoAPI, metrics *MetricsProxy) {
	h := &trinoMonitoringHandler{store: store, trino: trino, metrics: metrics}
	group := r.Group("/orgs/:id/monitoring/trino", requireInternalSecret())
	group.GET("/snapshot", h.snapshot)
	group.GET("/series", h.series)
}

// orgCell returns the cell API that owns orgID's Trino assignment, with the
// org's Trino row. The API is nil when the org is not enabled or no
// configured cell owns it.
func (a *TrinoAPI) orgCell(orgID string) (*TrinoAPI, *configstore.ManagedWarehouseTrino, error) {
	row, err := a.orgs.GetManagedWarehouseTrino(orgID)
	if err != nil {
		return nil, nil, err
	}
	if row == nil || !row.Enabled {
		return nil, row, nil
	}
	if a.fleet == nil {
		// Production always builds the fleet API, so this branch serves a single-cell API built directly with NewTrinoAPI.
		return a, row, nil
	}
	for _, candidate := range a.fleet {
		if row.TrinoCellID != "" && candidate.cell.storedID() == row.TrinoCellID {
			return candidate, row, nil
		}
	}
	return nil, row, nil
}

func (h *trinoMonitoringHandler) warehouseExists(c *gin.Context) bool {
	if h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "monitoring unavailable"})
		return false
	}
	snapshot := h.store.Snapshot()
	if snapshot == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration snapshot unavailable"})
		return false
	}
	org, ok := snapshot.Orgs[c.Param("id")]
	if !ok || org == nil || org.Warehouse == nil {
		monitoringWarehouseNotFound(c)
		return false
	}
	return true
}

func (h *trinoMonitoringHandler) snapshot(c *gin.Context) {
	if !h.warehouseExists(c) {
		return
	}
	orgID := c.Param("id")
	response := trinoMonitoringSnapshotResponse{
		SchemaVersion: trinoMonitoringSchemaVersion,
		OrgID:         orgID,
		AsOf:          time.Now().UTC(),
		Trino:         trinoMonitoringState{State: trinoMonitoringStateNotEnabled},
		Queries:       []trinoMonitoringQuery{},
	}
	if h.trino == nil {
		c.JSON(http.StatusOK, response)
		return
	}

	cell, row, err := h.trino.orgCell(orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "monitoring snapshot unavailable"})
		return
	}
	if row == nil || !row.Enabled {
		c.JSON(http.StatusOK, response)
		return
	}
	response.Trino = trinoMonitoringState{
		State:    trinoMonitoringLifecycleState(row.State),
		ReadyAt:  utcTimePointer(row.ReadyAt),
		FailedAt: utcTimePointer(row.FailedAt),
	}
	response.Limits.MaxRunningQueries, response.Limits.MaxQueuedQueries = provisioner.TrinoTierLimits(row.Tier)
	if cell == nil {
		c.JSON(http.StatusOK, response)
		return
	}

	// Without the principal index no query can be attributed to the org, and
	// an empty list would read as an idle warehouse. Report it as unavailable.
	idx, err := cell.index()
	if err != nil {
		slog.Warn("admin: trino monitoring snapshot unavailable: org index unreadable", "org", orgID, "error", err)
		c.JSON(http.StatusOK, response)
		return
	}
	queries, _, err := cell.liveQueries(c.Request.Context(), idx)
	if err != nil {
		slog.Warn("admin: trino monitoring snapshot unavailable: coordinator unreadable", "org", orgID, "error", err)
		c.JSON(http.StatusOK, response)
		return
	}
	response.Available = true

	inFlight := make([]TrinoQuery, 0)
	for _, q := range queries {
		if q.Org != orgID {
			continue
		}
		state, ok := TrinoInFlightState(q)
		if !ok {
			continue
		}
		response.Totals.InFlight++
		switch state {
		case TrinoInFlightQueued:
			response.Totals.Queued++
		case TrinoInFlightBlocked:
			response.Totals.Blocked++
			response.Totals.Running++
		default:
			response.Totals.Running++
		}
		if q.ElapsedMS > response.Totals.LongestRunningMS {
			response.Totals.LongestRunningMS = q.ElapsedMS
		}
		response.Totals.PhysicalInputBytes += q.PhysicalInputBytes
		inFlight = append(inFlight, q)
	}

	sort.SliceStable(inFlight, func(i, j int) bool { return inFlight[i].ElapsedMS > inFlight[j].ElapsedMS })
	if len(inFlight) > trinoMonitoringQueryLimit {
		inFlight = inFlight[:trinoMonitoringQueryLimit]
		response.QueriesTruncated = true
	}
	for _, q := range inFlight {
		owner, _ := idx.orgByPrincipal.Resolve(q.Principal)
		var createdAt *time.Time
		if !q.Created.IsZero() {
			createdAt = utcTimePointer(&q.Created)
		}
		response.Queries = append(response.Queries, trinoMonitoringQuery{
			QueryID:            q.QueryID,
			State:              strings.ToLower(q.State),
			User:               owner.Username,
			Source:             q.Source,
			Query:              maskTrinoSQLLiterals(q.Query),
			CreatedAt:          createdAt,
			ElapsedMS:          q.ElapsedMS,
			QueuedMS:           q.QueuedMS,
			CPUMS:              q.CPUMS,
			PhysicalInputBytes: q.PhysicalInputBytes,
			PeakMemoryBytes:    q.PeakMemoryBytes,
			ProcessedInputRows: q.ProcessedInputRows,
			ProgressPercentage: q.ProgressPercentage,
			Blocked:            q.State == trinoStateRunning && q.FullyBlocked,
		})
	}
	c.JSON(http.StatusOK, response)
}

func (h *trinoMonitoringHandler) series(c *gin.Context) {
	metric := c.Query("metric")
	spec, ok := trinoMonitoringMetrics[metric]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unknown monitoring metric"})
		return
	}
	window, ok := monitoringWindows[c.DefaultQuery("window", "24h")]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported monitoring window"})
		return
	}
	if !h.warehouseExists(c) {
		return
	}
	if h.metrics == nil || h.metrics.promURL == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metrics not configured"})
		return
	}

	response, err := h.metrics.queryMonitoringRange(c, c.Param("id"), metric, spec, window)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "metrics unavailable"})
		return
	}
	response.SchemaVersion = trinoMonitoringSchemaVersion
	c.JSON(http.StatusOK, response)
}

// trinoMonitoringLifecycleState maps the Trino row's state onto the four
// lifecycle states the contract documents. An empty or unexpected state is
// pending, which is what the row means before its first reconcile.
func trinoMonitoringLifecycleState(state configstore.ManagedWarehouseProvisioningState) string {
	switch state {
	case configstore.ManagedWarehouseStateProvisioning,
		configstore.ManagedWarehouseStateReady,
		configstore.ManagedWarehouseStateFailed:
		return string(state)
	default:
		return string(configstore.ManagedWarehouseStatePending)
	}
}

func utcTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}
