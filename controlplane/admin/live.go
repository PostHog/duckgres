//go:build kubernetes

package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
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
	StartedAt time.Time `json:"started_at,omitempty"`
	ElapsedMS int64     `json:"elapsed_ms"`
	// State is the pg_stat_activity-style connection state: "active" when a query
	// is in flight, else "idle" / "idle in transaction" / "idle in transaction
	// (aborted)". A non-active session holds a worker but has no in-flight query —
	// surfaced in the Live view so idle sessions are visible at a glance.
	State      string  `json:"state"`
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
	// Returns an error if no such session exists. NOTE: pid is per-CP, not
	// cluster-unique — prefer KillSessionByWorkerID for cross-CP cancel.
	KillSession(pid int32) error
	// KillSessionByWorkerID tears down the session bound to the cluster-unique
	// worker id on THIS replica (0 or 1). The handler fans it out so a cancel
	// hits whichever replica owns the session — pid can't be used for the
	// fan-out because pids collide across CPs.
	KillSessionByWorkerID(workerID int) int
	// KillUserSessions tears down every active session for (orgID, username) owned
	// by THIS control-plane replica and returns the count destroyed. The handler
	// fans it out to peers so the kill is cluster-wide. 0 (not an error) when the
	// org/user has no live sessions on this replica.
	KillUserSessions(orgID, username string) int
}

// UserAdmin persists the per-user kill switch (the disabled flag) and forces a
// config-snapshot reload so a flip takes effect immediately rather than one poll
// interval later. *configstore.ConfigStore satisfies it. nil disables the
// disable/enable endpoints (they 503).
type UserAdmin interface {
	SetOrgUserDisabled(orgID, username string, disabled bool) error
	ReloadSnapshot() error
}

// registerLiveAPI wires the live-state read endpoints + the session-kill and
// per-user kill-switch actions. fetcher (may be nil) aggregates / fans out
// per-CP in-memory state across replicas; users (may be nil) persists the
// disabled flag for the disable/enable endpoints.
func registerLiveAPI(r *gin.RouterGroup, live LiveInfo, fetcher PeerFetcher, users UserAdmin) {
	if live == nil {
		return
	}

	// userActionPath builds the peer fan-out path for a per-user action, escaping
	// org/user so names with slashes or reserved chars route correctly.
	userActionPath := func(org, user, action string) string {
		return "/api/v1/orgs/" + url.PathEscape(org) + "/users/" + url.PathEscape(user) + "/" + action
	}
	// sumKilled parses peer {"killed":N} bodies, returning the total and how many
	// peers responded with a parseable 200.
	sumKilled := func(bodies [][]byte) (killed, responders int) {
		for _, b := range bodies {
			var e struct {
				Killed int `json:"killed"`
			}
			if json.Unmarshal(b, &e) == nil {
				killed += e.Killed
			}
			responders++
		}
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

	// Kill a session by pid (admin-only via RoleGate — it's a POST). LOCAL ONLY:
	// pid is per-CP, so this only finds a session the serving replica owns.
	// Prefer /sessions/by-worker/:wid/cancel, which fans out.
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

	// Kill a session addressed by CLUSTER-UNIQUE worker id (admin-only POST).
	// A session lives on exactly one CP, so this kills locally and, unless it's
	// a scope=local peer call, fans out to peer replicas (?scope=local stops the
	// recursion). Worker id — not pid — is the address because pids collide
	// across CPs, so a pid fan-out could kill the wrong replica's session.
	r.POST("/sessions/by-worker/:wid/cancel", func(c *gin.Context) {
		wid, err := strconv.Atoi(c.Param("wid"))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid worker id"})
			return
		}
		killed := live.KillSessionByWorkerID(wid)
		responders, total := 1, 1
		// A worker hosts exactly one session on exactly one CP: only fan out when
		// this replica didn't own it (avoids needless peer POSTs on the hit path).
		if killed == 0 && !localScope(c) && fetcher != nil {
			bodies, peers := fetcher.PostPeers(c.Request.Context(), "/api/v1/sessions/by-worker/"+strconv.Itoa(wid)+"/cancel")
			total += peers
			k, r := sumKilled(bodies)
			killed += k
			responders += r
		}
		if killed == 0 {
			c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("no active session on worker %d", wid)})
			return
		}
		c.JSON(http.StatusOK, gin.H{"killed": killed, "cp_responders": responders, "cp_total": total})
	})

	// Per-user kill switch (admin-only via RoleGate — all POSTs). A user's
	// sessions live on whichever CP replica owns each connection, so every action
	// kills locally and fans out to peers (?scope=local stops the recursion); the
	// per-CP killed counts are summed. org/user come from the path params, so the
	// audit middleware records Org + TargetUser automatically.

	// One-shot terminate: kill all of the user's sessions + in-flight queries
	// cluster-wide. Does NOT block reconnects (use /disable for that).
	r.POST("/orgs/:id/users/:username/kill", func(c *gin.Context) {
		org, user := c.Param("id"), c.Param("username")
		killed := live.KillUserSessions(org, user)
		responders, total := 1, 1
		if !localScope(c) && fetcher != nil {
			bodies, peers := fetcher.PostPeers(c.Request.Context(), userActionPath(org, user, "kill"))
			total += peers
			k, r := sumKilled(bodies)
			killed += k
			responders += r
		}
		c.JSON(http.StatusOK, gin.H{"killed": killed, "cp_responders": responders, "cp_total": total})
	})

	// Disable (block + kill): persist disabled=true, reload every replica's
	// snapshot so new connections are refused cluster-wide immediately, and kill
	// the user's live sessions. A scope=local peer call only reloads + kills (the
	// DB write already happened on the primary).
	r.POST("/orgs/:id/users/:username/disable", func(c *gin.Context) {
		org, user := c.Param("id"), c.Param("username")
		if users == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "user administration unavailable"})
			return
		}
		if localScope(c) {
			if err := users.ReloadSnapshot(); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			killed := live.KillUserSessions(org, user)
			c.JSON(http.StatusOK, gin.H{"killed": killed})
			return
		}
		if err := users.SetOrgUserDisabled(org, user, true); err != nil {
			if errors.Is(err, configstore.ErrOrgUserNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// Reload + kill locally, then fan out to peers (which reload + kill too).
		if err := users.ReloadSnapshot(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		killed := live.KillUserSessions(org, user)
		responders, total := 1, 1
		if fetcher != nil {
			bodies, peers := fetcher.PostPeers(c.Request.Context(), userActionPath(org, user, "disable"))
			total += peers
			k, r := sumKilled(bodies)
			killed += k
			responders += r
		}
		c.JSON(http.StatusOK, gin.H{"disabled": true, "killed": killed, "cp_responders": responders, "cp_total": total})
	})

	// Enable (unblock): persist disabled=false and reload every replica's snapshot
	// so the user can reconnect immediately. No session kill.
	r.POST("/orgs/:id/users/:username/enable", func(c *gin.Context) {
		org, user := c.Param("id"), c.Param("username")
		if users == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "user administration unavailable"})
			return
		}
		if localScope(c) {
			if err := users.ReloadSnapshot(); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"enabled": true})
			return
		}
		if err := users.SetOrgUserDisabled(org, user, false); err != nil {
			if errors.Is(err, configstore.ErrOrgUserNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if err := users.ReloadSnapshot(); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		responders, total := 1, 1
		if fetcher != nil {
			bodies, peers := fetcher.PostPeers(c.Request.Context(), userActionPath(org, user, "enable"))
			total += peers
			responders += len(bodies)
		}
		c.JSON(http.StatusOK, gin.H{"disabled": false, "cp_responders": responders, "cp_total": total})
	})
}
