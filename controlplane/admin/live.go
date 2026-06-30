//go:build kubernetes

package admin

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// QueryStatus is a currently-running query/session with its live progress,
// sliceable by org and user.
type QueryStatus struct {
	Org        string  `json:"org"`
	User       string  `json:"user"`
	PID        int32   `json:"pid"`
	WorkerID   int     `json:"worker_id"`
	Protocol   string  `json:"protocol"`
	Percentage float64 `json:"percentage"`
	Rows       uint64  `json:"rows"`
	TotalRows  uint64  `json:"total_rows"`
	Stalled    bool    `json:"stalled"`
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
	Image   string `json:"image"`
	State   string `json:"state"`
	Binding string `json:"binding"`
	Count   int64  `json:"count"`
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
	// QueryDetailForPID returns the expanded detail (redacted SQL + connection
	// metadata + progress) for one in-flight query, or ok=false if no live
	// session with that pid is owned by this control-plane replica.
	QueryDetailForPID(pid int32) (QueryDetail, bool)
	// WorkerFleet returns worker counts by lifecycle state across the cluster.
	WorkerFleet() ([]FleetStat, error)
	// ControlPlaneInstances returns the live CP replicas.
	ControlPlaneInstances() ([]CPInstance, error)
	// KillSession tears down the session (and its exclusive worker) for pid.
	// Returns an error if no such session exists.
	KillSession(pid int32) error
}

// registerLiveAPI wires the live-state read endpoints + the session-kill action.
func registerLiveAPI(r *gin.RouterGroup, live LiveInfo) {
	if live == nil {
		return
	}
	r.GET("/queries", func(c *gin.Context) {
		queries := live.RunningQueries()
		// Optional org/user slicing.
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
		c.JSON(http.StatusOK, gin.H{"queries": queries})
	})

	// Expanded detail for a single in-flight query (redacted SQL + metadata).
	// Served on demand so the polled list stays light.
	r.GET("/queries/:pid", func(c *gin.Context) {
		pid64, err := strconv.ParseInt(c.Param("pid"), 10, 32)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid pid"})
			return
		}
		detail, ok := live.QueryDetailForPID(int32(pid64))
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "no live query with that pid on this replica"})
			return
		}
		c.JSON(http.StatusOK, detail)
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
