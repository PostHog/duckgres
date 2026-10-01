//go:build kubernetes

package admin

import (
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
	Blocked            int   `json:"blocked"`
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
	// Available is false when the coordinator could not be read. Totals are
	// then zero, and a consumer must not present them as an idle warehouse.
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
		c.JSON(http.StatusOK, response)
		return
	}
	queries, _, err := cell.liveQueries(c.Request.Context(), idx)
	if err != nil {
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
