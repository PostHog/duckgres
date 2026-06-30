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
		// one CP, so the union is disjoint — concatenate, no dedup).
		if !localScope(c) && fetcher != nil {
			bodies, peers := fetcher.FetchPeers(c.Request.Context(), "/api/v1/queries")
			type env struct {
				Queries []QueryStatus `json:"queries"`
			}
			responders += mergePeer(&queries, bodies, func(e env) []QueryStatus { return e.Queries })
			total += peers
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
