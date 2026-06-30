//go:build kubernetes

package admin

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// QueryStatus is a currently-running query/session with its live progress,
// sliceable by org and user.
type QueryStatus struct {
	Org      string `json:"org"`
	User     string `json:"user"`
	PID      int32  `json:"pid"`
	WorkerID int    `json:"worker_id"`
	Protocol string `json:"protocol"`
	// StartedAt is when the SESSION was created (session age). ElapsedMS is how
	// long the CURRENT statement has been running (0 when idle); computed on the
	// owning CP from the connection's query-start, so it survives the cross-CP
	// /queries merge unchanged. The Live view shows query-elapsed when a query is
	// in flight and falls back to session age otherwise.
	StartedAt  time.Time `json:"started_at,omitempty"`
	ElapsedMS  int64     `json:"elapsed_ms"`
	Percentage float64   `json:"percentage"`
	Rows       uint64    `json:"rows"`
	TotalRows  uint64    `json:"total_rows"`
	Stalled    bool      `json:"stalled"`
}

// QueryDetail is the expanded, single-session view behind a running query: the
// (already-redacted) SQL text plus connection metadata and live progress. It is
// served on demand when an operator opens a query row, so the polled list
// payload stays small. Query is redacted upstream (usersecrets.RedactForLog) —
// raw SQL never reaches this struct.
type QueryDetail struct {
	Org             string  `json:"org"`
	User            string  `json:"user"`
	PID             int32   `json:"pid"`
	WorkerID        int     `json:"worker_id"`
	WorkerPod       string  `json:"worker_pod"`
	Protocol        string  `json:"protocol"`
	Database        string  `json:"database"`
	ApplicationName string  `json:"application_name"`
	ClientAddr      string  `json:"client_addr"`
	ClientPort      int32   `json:"client_port"`
	State           string  `json:"state"`
	Query           string  `json:"query"`
	BackendStart    string  `json:"backend_start"` // RFC3339, "" if unknown
	QueryStart      string  `json:"query_start"`   // RFC3339, "" if idle
	ElapsedMS       int64   `json:"elapsed_ms"`    // since query_start, 0 if idle
	Percentage      float64 `json:"percentage"`
	Rows            uint64  `json:"rows"`
	TotalRows       uint64  `json:"total_rows"`
	Stalled         bool    `json:"stalled"`
}

// FleetStat is a cluster-wide worker count grouped by image/state/binding,
// from the durable runtime store (the source of truth for hot-idle/spawning/
// draining counts the in-memory session map cannot see).
type FleetStat struct {
	Image       string  `json:"image"`
	State       string  `json:"state"`
	Binding     string  `json:"binding"`
	Count       int64   `json:"count"`
	CPUCores    float64 `json:"cpu_cores"`
	MemoryBytes int64   `json:"memory_bytes"`
}

// CPInstance is a live control-plane replica.
type CPInstance struct {
	ID   string `json:"id"`
	Self bool   `json:"self"`
}

// LiveInfo surfaces live cluster state beyond the basic OrgStackInfo summary.
// Implemented by the controlplane adapter.
type LiveInfo interface {
	// RunningQueries returns every active session with its cached progress.
	RunningQueries() []QueryStatus
	// QueryDetailForWorkerID returns the expanded detail (redacted SQL +
	// connection metadata + progress) for the in-flight query on the given
	// cluster-unique worker id, or ok=false if no live session/conn for that
	// worker is owned by this control-plane replica. Addressed by worker id, not
	// pid: pids are per-org and not unique, so a pid key can return the wrong
	// org's query.
	QueryDetailForWorkerID(workerID int) (QueryDetail, bool)
	// WorkerFleet returns worker counts by lifecycle state across the cluster.
	WorkerFleet() ([]FleetStat, error)
	// ControlPlaneInstances returns the live CP replicas.
	ControlPlaneInstances() ([]CPInstance, error)
	// KillSession tears down the session (and its exclusive worker) for pid.
	// Returns an error if no such session exists.
	KillSession(pid int32) error
}

// registerLiveAPI wires the live-state read endpoints + the session-kill action.
// fetcher (may be nil) aggregates per-CP in-memory state across replicas.
func registerLiveAPI(r *gin.RouterGroup, live LiveInfo, fetcher PeerFetcher) {
	if live == nil {
		return
	}
	r.GET("/queries", func(c *gin.Context) {
		queries := live.RunningQueries()
		responders, total := 1, 1
		// Aggregate every other CP's in-memory view (a query lives on exactly
		// one CP, so the union is disjoint; dedupeBy makes it idempotent anyway).
		if !localScope(c) && fetcher != nil {
			bodies, peers := fetcher.FetchPeers(c.Request.Context(), "/api/v1/queries")
			type env struct {
				Queries []QueryStatus `json:"queries"`
			}
			responders += mergePeer(&queries, bodies, func(e env) []QueryStatus { return e.Queries })
			total += peers
			queries = dedupeBy(queries, func(q QueryStatus) int { return q.WorkerID })
		}
		// Optional org/user slicing (applied AFTER the merge).
		org, user := c.Query("org"), c.Query("user")
		if org != "" || user != "" {
			filtered := queries[:0:0]
			for _, q := range queries {
				if org != "" && q.Org != org {
					continue
				}
				if user != "" && q.User != user {
					continue
				}
				filtered = append(filtered, q)
			}
			queries = filtered
		}
		c.JSON(http.StatusOK, gin.H{"queries": queries, "cp_responders": responders, "cp_total": total})
	})

	// Expanded detail for a single in-flight query (redacted SQL + metadata),
	// addressed by CLUSTER-UNIQUE worker id (pid is per-org, not a safe key).
	// Served on demand so the polled list stays light. The SQL text lives only
	// on the CP replica that owns the connection, so this scatter-gathers like
	// /queries: check locally first, and if this replica doesn't own the worker,
	// fan out to peers (scope=local guard prevents re-fanning) and return the one
	// owner's 200 (FetchPeers drops every peer's 404).
	r.GET("/queries/by-worker/:wid", func(c *gin.Context) {
		wid, err := strconv.Atoi(c.Param("wid"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid worker id"})
			return
		}
		if detail, ok := live.QueryDetailForWorkerID(wid); ok {
			c.JSON(http.StatusOK, detail)
			return
		}
		// Not owned locally. A peer fan-out call (scope=local) stops here so a
		// missing worker can't recurse across the cluster.
		if !localScope(c) && fetcher != nil {
			bodies, _ := fetcher.FetchPeers(c.Request.Context(), "/api/v1/queries/by-worker/"+strconv.Itoa(wid))
			for _, b := range bodies {
				var d QueryDetail
				if json.Unmarshal(b, &d) == nil && d.WorkerID == wid {
					c.JSON(http.StatusOK, d)
					return
				}
			}
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "no live query on that worker"})
	})

	r.GET("/workers/fleet", func(c *gin.Context) {
		stats, err := live.WorkerFleet()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"fleet": stats})
	})

	r.GET("/cluster/instances", func(c *gin.Context) {
		instances, err := live.ControlPlaneInstances()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"instances": instances})
	})

	// Kill a session (admin-only via RoleGate — it's a POST).
	r.POST("/sessions/:pid/cancel", func(c *gin.Context) {
		pid64, err := strconv.ParseInt(c.Param("pid"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid pid"})
			return
		}
		if err := live.KillSession(int32(pid64)); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"killed": pid64})
	})
}
