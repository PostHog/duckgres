//go:build kubernetes

package admin

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	monitoringSchemaVersion         = 1
	monitoringWarehouseNotFoundCode = "managed_warehouse_not_found"
)

type monitoringStore interface {
	Snapshot() *configstore.Snapshot
	ListWorkerRecordsForOrg(orgID string) ([]configstore.WorkerRecord, error)
	OrgConnectionMonitoringState(orgID string) (configstore.OrgConnectionMonitoringStatus, error)
}

// MonitoringWorkerDefaults resolves empty default-profile worker records to
// the deployment shape that the Kubernetes worker pool actually provisions.
type MonitoringWorkerDefaults struct {
	CPU    string
	Memory string
	TTL    time.Duration
}

type monitoringWarehouse struct {
	State string `json:"state"`
}

type monitoringLimits struct {
	MaxWorkers              int    `json:"max_workers"`
	MaxVCPUs                int    `json:"max_vcpus"`
	DefaultWorkerCPU        string `json:"default_worker_cpu"`
	DefaultWorkerMemory     string `json:"default_worker_memory"`
	DefaultWorkerTTLSeconds int    `json:"default_worker_ttl_seconds"`
	DefaultWorkerMinHotIdle int    `json:"default_worker_min_hot_idle"`
}

type monitoringTotals struct {
	Workers              int     `json:"workers"`
	AllocatedCPUCores    float64 `json:"allocated_cpu_cores"`
	AllocatedMemoryBytes int64   `json:"allocated_memory_bytes"`
	ActiveSessions       int64   `json:"active_sessions"`
	RunningQueries       int     `json:"running_queries"`
	QueuedConnections    int64   `json:"queued_connections"`
}

type monitoringSession struct {
	Protocol   string   `json:"protocol"`
	State      string   `json:"state"`
	ElapsedMS  int64    `json:"elapsed_ms"`
	Percentage *float64 `json:"percentage"`
	Rows       uint64   `json:"rows"`
	TotalRows  uint64   `json:"total_rows"`
	Stalled    bool     `json:"stalled"`
}

type monitoringWorker struct {
	ID              int                `json:"id"`
	State           string             `json:"state"`
	CPU             string             `json:"cpu"`
	Memory          string             `json:"memory"`
	TTLSeconds      int                `json:"ttl_seconds"`
	CreatedAt       time.Time          `json:"created_at"`
	LastHeartbeatAt time.Time          `json:"last_heartbeat_at"`
	Session         *monitoringSession `json:"session,omitempty"`
}

type monitoringCoverage struct {
	CPResponders int  `json:"cp_responders"`
	CPTotal      int  `json:"cp_total"`
	Partial      bool `json:"partial"`
}

type monitoringSnapshotResponse struct {
	SchemaVersion int                 `json:"schema_version"`
	OrgID         string              `json:"org_id"`
	AsOf          time.Time           `json:"as_of"`
	Warehouse     monitoringWarehouse `json:"warehouse"`
	Limits        monitoringLimits    `json:"limits"`
	Totals        monitoringTotals    `json:"totals"`
	Workers       []monitoringWorker  `json:"workers"`
	Coverage      monitoringCoverage  `json:"coverage"`
}

type monitoringMetricSpec struct {
	PromQL        string
	Unit          string
	AllowedLabels map[string]struct{}
}

var monitoringMetrics = map[string]monitoringMetricSpec{
	"query_rate": {
		PromQL: rangePanels["query_rate"], Unit: "queries_per_second",
		AllowedLabels: labelSet("status", "reason"),
	},
	"error_ratio": {
		PromQL: `(sum(rate(duckgres_query_total$ORGERR[$WIN])) or vector(0)) / clamp_min((sum(rate(duckgres_query_total$ORG[$WIN])) or vector(0)), 1e-9)`,
		Unit:   "ratio", AllowedLabels: labelSet(),
	},
	"duration_p95": {PromQL: rangePanels["duration_p95"], Unit: "seconds", AllowedLabels: labelSet()},
	"duration_p50": {PromQL: rangePanels["duration_p50"], Unit: "seconds", AllowedLabels: labelSet()},
	"sessions_active": {
		PromQL: rangePanels["sessions_active"], Unit: "sessions", AllowedLabels: labelSet(),
	},
	"acquire_p95": {
		PromQL: rangePanels["acquire_p95"], Unit: "seconds", AllowedLabels: labelSet("source"),
	},
	"acquire_by_source": {
		PromQL: rangePanels["acquire_by_source"], Unit: "acquisitions_per_second", AllowedLabels: labelSet("source"),
	},
	"storage_bytes": {
		PromQL: `max(duckgres_org_storage_tracked_bytes$ORG)`, Unit: "bytes", AllowedLabels: labelSet(),
	},
	"worker_crash_rate": {
		PromQL: `(sum(rate(duckgres_org_worker_crashes_total$ORG[$WIN])) or vector(0))`, Unit: "crashes_per_second", AllowedLabels: labelSet(),
	},
}

var monitoringWindows = map[string]time.Duration{
	"1h":  time.Hour,
	"6h":  6 * time.Hour,
	"24h": 24 * time.Hour,
	"7d":  7 * 24 * time.Hour,
	"30d": 30 * 24 * time.Hour,
}

type monitoringPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

type monitoringSeries struct {
	Labels map[string]string `json:"labels"`
	Points []monitoringPoint `json:"points"`
}

type monitoringSeriesResponse struct {
	SchemaVersion int                `json:"schema_version"`
	OrgID         string             `json:"org_id"`
	Metric        string             `json:"metric"`
	Unit          string             `json:"unit"`
	Start         time.Time          `json:"start"`
	End           time.Time          `json:"end"`
	StepSeconds   int64              `json:"step_seconds"`
	Series        []monitoringSeries `json:"series"`
}

type prometheusRangeResponse struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string   `json:"metric"`
			Values [][]json.RawMessage `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func labelSet(labels ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		set[label] = struct{}{}
	}
	return set
}

func registerMonitoringAPI(r *gin.RouterGroup, store monitoringStore, live LiveInfo, fetcher PeerFetcher, metrics *MetricsProxy, defaults MonitoringWorkerDefaults) {
	h := &monitoringHandler{store: store, live: live, fetcher: fetcher, metrics: metrics, defaults: defaults}
	group := r.Group("/orgs/:id/monitoring", requireInternalSecret())
	group.GET("/snapshot", h.snapshot)
	group.GET("/series", h.series)
}

func requireInternalSecret() gin.HandlerFunc {
	return func(c *gin.Context) {
		identity := IdentityFromContext(c)
		if identity == nil || identity.Source != "internal-secret" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "internal secret required"})
			return
		}
		c.Next()
	}
}

type monitoringHandler struct {
	store    monitoringStore
	live     LiveInfo
	fetcher  PeerFetcher
	metrics  *MetricsProxy
	defaults MonitoringWorkerDefaults
}

func (h *monitoringHandler) snapshot(c *gin.Context) {
	if h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "monitoring unavailable"})
		return
	}

	orgID := c.Param("id")
	snapshot := h.store.Snapshot()
	if snapshot == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration snapshot unavailable"})
		return
	}
	org, ok := snapshot.Orgs[orgID]
	if !ok || org == nil || org.Warehouse == nil {
		monitoringWarehouseNotFound(c)
		return
	}

	workerRecords, err := h.store.ListWorkerRecordsForOrg(orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "monitoring snapshot unavailable"})
		return
	}
	connections, err := h.store.OrgConnectionMonitoringState(orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "monitoring snapshot unavailable"})
		return
	}

	queries, coverage := h.orgQueries(c, orgID)
	queriesByWorker := make(map[int]QueryStatus, len(queries))
	runningQueries := 0
	for _, query := range queries {
		queriesByWorker[query.WorkerID] = query
		if query.State == "active" {
			runningQueries++
		}
	}

	workers := make([]monitoringWorker, 0, len(workerRecords))
	var allocatedCPUCores float64
	var allocatedMemoryBytes int64
	deploymentDefaults := normalizedMonitoringWorkerDefaults(h.defaults)
	configuredDefaults := configuredMonitoringWorkerDefaults(org, deploymentDefaults)
	for _, record := range workerRecords {
		cpu := firstNonEmptyMonitoring(record.ProfileCPU, deploymentDefaults.CPU)
		memory := firstNonEmptyMonitoring(record.ProfileMemory, deploymentDefaults.Memory)
		allocatedCPUCores += cpuCores(cpu)
		allocatedMemoryBytes += memoryBytes(memory)

		ttlSeconds := record.TTLMinutes * 60
		if ttlSeconds == 0 {
			ttlSeconds = int(deploymentDefaults.TTL / time.Second)
		}
		worker := monitoringWorker{
			ID:              record.WorkerID,
			State:           string(record.State),
			CPU:             cpu,
			Memory:          memory,
			TTLSeconds:      ttlSeconds,
			CreatedAt:       record.CreatedAt.UTC(),
			LastHeartbeatAt: record.LastHeartbeatAt.UTC(),
		}
		if query, ok := queriesByWorker[record.WorkerID]; ok {
			worker.Session = &monitoringSession{
				Protocol:   query.Protocol,
				State:      query.State,
				ElapsedMS:  query.ElapsedMS,
				Percentage: monitoringPercentage(query.Percentage),
				Rows:       query.Rows,
				TotalRows:  query.TotalRows,
				Stalled:    query.Stalled,
			}
		}
		workers = append(workers, worker)
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })

	c.JSON(http.StatusOK, monitoringSnapshotResponse{
		SchemaVersion: monitoringSchemaVersion,
		OrgID:         orgID,
		AsOf:          time.Now().UTC(),
		Warehouse:     monitoringWarehouse{State: string(org.Warehouse.State)},
		Limits: monitoringLimits{
			MaxWorkers:              org.MaxWorkers,
			MaxVCPUs:                org.MaxVCPUs,
			DefaultWorkerCPU:        configuredDefaults.CPU,
			DefaultWorkerMemory:     configuredDefaults.Memory,
			DefaultWorkerTTLSeconds: int(configuredDefaults.TTL / time.Second),
			DefaultWorkerMinHotIdle: org.DefaultWorkerMinHotIdle,
		},
		Totals: monitoringTotals{
			Workers:              len(workers),
			AllocatedCPUCores:    allocatedCPUCores,
			AllocatedMemoryBytes: allocatedMemoryBytes,
			ActiveSessions:       connections.ActiveLeases,
			RunningQueries:       runningQueries,
			QueuedConnections:    connections.QueuedConns,
		},
		Workers:  workers,
		Coverage: coverage,
	})
}

func (h *monitoringHandler) orgQueries(c *gin.Context, orgID string) ([]QueryStatus, monitoringCoverage) {
	queries := make([]QueryStatus, 0)
	if h.live != nil {
		queries = append(queries, h.live.RunningQueries()...)
	}
	responders, total := 1, 1
	if h.fetcher != nil {
		peerResult := h.fetcher.FetchPeers(c.Request.Context(), "/api/v1/queries")
		type envelope struct {
			Queries []QueryStatus `json:"queries"`
		}
		coverage := peerReadCoverage(peerResult, mergePeer(&queries, peerResult.Bodies, func(e envelope) []QueryStatus { return e.Queries }))
		responders, total = coverage.Responders, coverage.Total
		queries = dedupeBy(queries, func(q QueryStatus) int { return q.WorkerID })
	}
	filtered := make([]QueryStatus, 0, len(queries))
	for _, query := range queries {
		if query.Org == orgID {
			filtered = append(filtered, query)
		}
	}
	return filtered, monitoringCoverage{CPResponders: responders, CPTotal: total, Partial: responders < total}
}

func (h *monitoringHandler) series(c *gin.Context) {
	metric := c.Query("metric")
	spec, ok := monitoringMetrics[metric]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unknown monitoring metric"})
		return
	}
	windowKey := c.DefaultQuery("window", "24h")
	window, ok := monitoringWindows[windowKey]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported monitoring window"})
		return
	}
	if h.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "monitoring unavailable"})
		return
	}
	snapshot := h.store.Snapshot()
	var org *configstore.OrgConfig
	found := false
	if snapshot != nil {
		org, found = snapshot.Orgs[c.Param("id")]
	}
	if !found || org == nil || org.Warehouse == nil {
		monitoringWarehouseNotFound(c)
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
	c.JSON(http.StatusOK, response)
}

func (m *MetricsProxy) queryMonitoringRange(c *gin.Context, orgID, metric string, spec monitoringMetricSpec, window time.Duration) (monitoringSeriesResponse, error) {
	step := window / 240
	if step < 15*time.Second {
		step = 15 * time.Second
	}
	end := time.Now().UTC().Truncate(step)
	start := end.Add(-window)
	rateWindow := monitoringRateWindow(window)
	orgSelector := fmt.Sprintf(`{org=%q}`, orgID)
	orgErrorSelector := fmt.Sprintf(`{org=%q,status="error"}`, orgID)
	promQL := renderPanel(spec.PromQL, orgSelector, orgErrorSelector, rateWindow)

	query := url.Values{}
	query.Set("query", promQL)
	query.Set("start", strconv.FormatInt(start.Unix(), 10))
	query.Set("end", strconv.FormatInt(end.Unix(), 10))
	query.Set("step", strconv.FormatInt(int64(step/time.Second), 10)+"s")
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, m.promURL+"/api/v1/query_range?"+query.Encode(), nil)
	if err != nil {
		return monitoringSeriesResponse{}, err
	}
	client := m.client
	if client == nil {
		client = http.DefaultClient
	}
	upstream, err := client.Do(req)
	if err != nil {
		return monitoringSeriesResponse{}, err
	}
	defer func() { _ = upstream.Body.Close() }()
	if upstream.StatusCode != http.StatusOK {
		return monitoringSeriesResponse{}, fmt.Errorf("prometheus returned status %d", upstream.StatusCode)
	}

	var promResponse prometheusRangeResponse
	decoder := json.NewDecoder(io.LimitReader(upstream.Body, 10<<20))
	if err := decoder.Decode(&promResponse); err != nil {
		return monitoringSeriesResponse{}, fmt.Errorf("decode prometheus response: %w", err)
	}
	if promResponse.Status != "success" || promResponse.Data.ResultType != "matrix" {
		return monitoringSeriesResponse{}, fmt.Errorf("unexpected prometheus response")
	}

	series := make([]monitoringSeries, 0, len(promResponse.Data.Result))
	for _, result := range promResponse.Data.Result {
		labels := make(map[string]string, len(spec.AllowedLabels))
		for key := range spec.AllowedLabels {
			if value, ok := result.Metric[key]; ok {
				labels[key] = value
			}
		}
		points := make([]monitoringPoint, 0, len(result.Values))
		for _, pair := range result.Values {
			point, ok := decodePrometheusPoint(pair)
			if ok {
				points = append(points, point)
			}
		}
		series = append(series, monitoringSeries{Labels: labels, Points: points})
	}

	return monitoringSeriesResponse{
		SchemaVersion: monitoringSchemaVersion,
		OrgID:         orgID,
		Metric:        metric,
		Unit:          spec.Unit,
		Start:         start,
		End:           end,
		StepSeconds:   int64(step / time.Second),
		Series:        series,
	}, nil
}

func decodePrometheusPoint(pair []json.RawMessage) (monitoringPoint, bool) {
	if len(pair) != 2 {
		return monitoringPoint{}, false
	}
	var timestamp float64
	var rawValue string
	if json.Unmarshal(pair[0], &timestamp) != nil || json.Unmarshal(pair[1], &rawValue) != nil {
		return monitoringPoint{}, false
	}
	value, err := strconv.ParseFloat(rawValue, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return monitoringPoint{}, false
	}
	seconds, fraction := math.Modf(timestamp)
	return monitoringPoint{
		Timestamp: time.Unix(int64(seconds), int64(fraction*float64(time.Second))).UTC(),
		Value:     value,
	}, true
}

func monitoringRateWindow(window time.Duration) string {
	switch {
	case window >= 30*24*time.Hour:
		return "2h"
	case window >= 7*24*time.Hour:
		return "30m"
	default:
		return "5m"
	}
}

func monitoringWarehouseNotFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, gin.H{
		"code":  monitoringWarehouseNotFoundCode,
		"error": "managed warehouse not found",
	})
}

func monitoringPercentage(value float64) *float64 {
	if value < 0 {
		return nil
	}
	return &value
}

func normalizedMonitoringWorkerDefaults(deployment MonitoringWorkerDefaults) MonitoringWorkerDefaults {
	effective := MonitoringWorkerDefaults{
		CPU:    strings.TrimSpace(deployment.CPU),
		Memory: strings.TrimSpace(deployment.Memory),
		TTL:    deployment.TTL,
	}
	if cpuCores(effective.CPU) <= 0 {
		effective.CPU = ""
	}
	if memoryBytes(effective.Memory) <= 0 {
		effective.Memory = ""
	}
	if effective.TTL <= 0 {
		effective.TTL = 0
	}
	return effective
}

func configuredMonitoringWorkerDefaults(org *configstore.OrgConfig, deployment MonitoringWorkerDefaults) MonitoringWorkerDefaults {
	effective := deployment
	if org == nil {
		return deployment
	}
	if orgCPU := strings.TrimSpace(org.DefaultWorkerCPU); cpuCores(orgCPU) > 0 {
		effective.CPU = orgCPU
	}
	if orgMemory := strings.TrimSpace(org.DefaultWorkerMemory); memoryBytes(orgMemory) > 0 {
		effective.Memory = orgMemory
	}
	if orgTTL, err := time.ParseDuration(strings.TrimSpace(org.DefaultWorkerTTL)); err == nil && orgTTL > 0 {
		effective.TTL = orgTTL
	}
	return effective
}

func firstNonEmptyMonitoring(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func cpuCores(raw string) float64 {
	quantity, err := resource.ParseQuantity(raw)
	if err != nil || quantity.Sign() <= 0 {
		return 0
	}
	return quantity.AsApproximateFloat64()
}

func memoryBytes(raw string) int64 {
	quantity, err := resource.ParseQuantity(raw)
	if err != nil || quantity.Sign() <= 0 {
		return 0
	}
	return quantity.Value()
}
