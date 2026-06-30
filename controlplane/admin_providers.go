//go:build kubernetes

package controlplane

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/server"
)

// clusterInfoProvider implements admin.LiveInfo over the org router + config
// store. It is the source of live state for the admin UI's monitoring views.
// srv is the local PG wire server, the only place the (redacted) in-flight SQL
// text lives — so QueryDetailForPID is scoped to queries this replica owns.
type clusterInfoProvider struct {
	router   *OrgRouter
	store    *configstore.ConfigStore
	srv      *server.Server
	selfCPID string
}

var _ admin.LiveInfo = (*clusterInfoProvider)(nil)

// RunningQueries flattens every org's active sessions with their cached
// progress, attributed to org + user.
func (p *clusterInfoProvider) RunningQueries() []admin.QueryStatus {
	stacks := p.router.AllStacks()
	// Running-query duration comes from the owning connection's query-start.
	// Key it by cluster-unique worker id, not pid: pids are allocated per-org
	// (every stack starts at 1000), so a pid-keyed lookup can read the wrong
	// org's connection. Snapshot once per poll.
	var queryStarts map[int]time.Time
	if p.srv != nil {
		queryStarts = p.srv.QueryStartsByWorkerID()
	}
	var out []admin.QueryStatus
	for org, stack := range stacks {
		for _, s := range stack.Sessions.AllSessions() {
			q := admin.QueryStatus{
				Org:       org,
				User:      s.Username,
				PID:       s.PID,
				WorkerID:  s.WorkerID,
				Protocol:  s.Protocol,
				StartedAt: s.StartedAt,
			}
			if prog := stack.Sessions.GetProgress(s.PID); prog != nil {
				q.Percentage = prog.Percentage
				q.Rows = prog.Rows
				q.TotalRows = prog.TotalRows
				q.Stalled = prog.Stalled
			}
			if qs, ok := queryStarts[s.WorkerID]; ok {
				q.ElapsedMS = time.Since(qs).Milliseconds()
			}
			out = append(out, q)
		}
	}
	return out
}

// QueryDetailForWorkerID expands one in-flight query, addressed by its
// cluster-unique worker id: the redacted SQL text + conn metadata from the local
// PG server, joined with org + protocol + live progress from the session
// manager. ok=false when no live session/conn for that worker is owned by this
// replica (the SQL text only exists on the CP that owns the connection).
//
// Worker id, NOT pid, is the address: pids are per-org (every stack starts at
// 1000), so a pid lookup can stitch one org's SQL onto another org's identity.
func (p *clusterInfoProvider) QueryDetailForWorkerID(workerID int) (admin.QueryDetail, bool) {
	if p.srv == nil {
		return admin.QueryDetail{}, false
	}
	cd, connOK := p.srv.ConnDetailByWorkerID(workerID)

	d := admin.QueryDetail{WorkerID: workerID}
	if connOK {
		d.Org = cd.OrgID
		d.User = cd.Username
		d.PID = cd.PID
		d.WorkerPod = cd.WorkerPod
		d.Database = cd.Database
		d.ApplicationName = cd.ApplicationName
		d.ClientAddr = cd.ClientAddr
		d.ClientPort = cd.ClientPort
		d.State = cd.State
		d.Query = cd.Query
		if !cd.BackendStart.IsZero() {
			d.BackendStart = cd.BackendStart.UTC().Format(time.RFC3339)
		}
		if !cd.QueryStart.IsZero() {
			d.QueryStart = cd.QueryStart.UTC().Format(time.RFC3339)
			d.ElapsedMS = time.Since(cd.QueryStart).Milliseconds()
		}
	}

	// Org (stack-keyed, consistent with the list), protocol, pid, and cached
	// progress from the owning session manager — located by worker id.
	sessOK := false
	for org, stack := range p.router.AllStacks() {
		s, ok := stack.Sessions.SessionForWorker(workerID)
		if !ok {
			continue
		}
		sessOK = true
		d.Org = org
		d.PID = s.PID
		d.Protocol = s.Protocol
		if prog := stack.Sessions.GetProgress(s.PID); prog != nil {
			d.Percentage = prog.Percentage
			d.Rows = prog.Rows
			d.TotalRows = prog.TotalRows
			d.Stalled = prog.Stalled
		}
		break
	}

	// Found neither a live conn nor a session for this worker on this CP.
	if !connOK && !sessOK {
		return admin.QueryDetail{}, false
	}
	return d, true
}

// WorkerFleet returns cluster-wide worker counts by lifecycle state from the
// durable runtime store — the only source that sees hot-idle/spawning/draining
// workers that hold no session.
func (p *clusterInfoProvider) WorkerFleet() ([]admin.FleetStat, error) {
	stats, err := p.store.ListWorkerLifecycleStats()
	if err != nil {
		return nil, err
	}
	out := make([]admin.FleetStat, 0, len(stats))
	for _, s := range stats {
		out = append(out, admin.FleetStat{
			Image:       s.Image,
			State:       string(s.State),
			Binding:     s.Binding,
			Count:       s.Count,
			CPUCores:    s.CPUCores,
			MemoryBytes: s.MemoryBytes,
		})
	}
	return out, nil
}

// ControlPlaneInstances returns the live CP replicas, flagging this one.
func (p *clusterInfoProvider) ControlPlaneInstances() ([]admin.CPInstance, error) {
	ids, err := p.store.ListLiveControlPlaneInstanceIDs()
	if err != nil {
		return nil, err
	}
	out := make([]admin.CPInstance, 0, len(ids))
	for _, id := range ids {
		out = append(out, admin.CPInstance{ID: id, Self: id == p.selfCPID})
	}
	return out, nil
}

// KillSession tears down the session (and its exclusive worker) for pid by
// locating the owning org stack.
func (p *clusterInfoProvider) KillSession(pid int32) error {
	for _, stack := range p.router.AllStacks() {
		if stack.Sessions.WorkerIDForPID(pid) >= 0 {
			stack.Sessions.DestroySession(pid)
			return nil
		}
	}
	return fmt.Errorf("no active session with pid %d", pid)
}

// KillUserSessions tears down every active session for (orgID, username) owned by
// THIS control-plane replica and returns the count destroyed. It is the local
// half of the cluster-wide per-user kill switch: the admin handler fans this out
// to every CP replica (sessions live in-memory on whichever replica owns the
// connection) and sums the counts. An org with no live stack on this replica
// (returns 0, not an error) is normal — its sessions, if any, live elsewhere.
func (p *clusterInfoProvider) KillUserSessions(orgID, username string) int {
	stack, ok := p.router.AllStacks()[orgID]
	if !ok {
		return 0
	}
	return stack.Sessions.DestroySessionsForUser(username)
}

// impersonator implements admin.Impersonator. It opens a real session as the
// target org+user on that org's worker, runs the SQL, and ALWAYS destroys the
// session. The session is a real session: it acquires a worker exclusively
// (one-session-per-worker), counts against the org's connection limits, and
// appears in the org's session accounting — this is intended and audited.
type impersonator struct {
	router *OrgRouter
}

var _ admin.Impersonator = (*impersonator)(nil)

// impersonationPIDSeq hands out unique NEGATIVE pids for admin-initiated
// sessions so they can never collide with real (positive) Postgres pids in the
// session map.
var impersonationPIDSeq atomic.Int32

// maxImpersonationRows caps the rows returned to the UI to bound memory; the
// result is flagged Truncated when exceeded.
const maxImpersonationRows = 5000

func (im *impersonator) Impersonate(c *gin.Context, org, username, sql string, allowWrite bool) (*admin.QueryResult, error) {
	stack, ok := im.router.StackForOrg(org)
	if !ok {
		return nil, fmt.Errorf("unknown or inactive org %q", org)
	}
	ctx := c.Request.Context()

	pid := impersonationPIDSeq.Add(-1)
	// Empty memoryLimit/threads + nil profile: the worker auto-sizes DuckDB to
	// its own pod and the default org worker shape is acquired/reused.
	_, executor, err := stack.Sessions.CreateSessionWithProtocol(ctx, username, pid, "", 0, "flight", nil)
	if err != nil {
		return nil, fmt.Errorf("open session as %s@%s: %w", username, org, err)
	}
	defer stack.Sessions.DestroySession(pid)

	rs, err := executor.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	cols, err := rs.Columns()
	if err != nil {
		return nil, err
	}
	result := &admin.QueryResult{Columns: cols, Rows: [][]any{}}
	for rs.Next() {
		if result.RowCount >= maxImpersonationRows {
			result.Truncated = true
			break
		}
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, vals)
		result.RowCount++
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
