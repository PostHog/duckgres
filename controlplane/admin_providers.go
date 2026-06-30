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
	var out []admin.QueryStatus
	for org, stack := range stacks {
		for _, s := range stack.Sessions.AllSessions() {
			q := admin.QueryStatus{
				Org:      org,
				User:     s.Username,
				PID:      s.PID,
				WorkerID: s.WorkerID,
				Protocol: s.Protocol,
			}
			if prog := stack.Sessions.GetProgress(s.PID); prog != nil {
				q.Percentage = prog.Percentage
				q.Rows = prog.Rows
				q.TotalRows = prog.TotalRows
				q.Stalled = prog.Stalled
			}
			out = append(out, q)
		}
	}
	return out
}

// QueryDetailForPID expands one in-flight query: the redacted SQL text + conn
// metadata from the local PG server, joined with protocol + live progress from
// the session manager. ok=false when no live session with that pid is owned by
// this replica (the SQL text only exists on the CP that owns the connection).
func (p *clusterInfoProvider) QueryDetailForPID(pid int32) (admin.QueryDetail, bool) {
	if p.srv == nil {
		return admin.QueryDetail{}, false
	}
	cd, ok := p.srv.ConnDetailByPID(pid)
	if !ok {
		return admin.QueryDetail{}, false
	}
	d := admin.QueryDetail{
		Org:             cd.OrgID,
		User:            cd.Username,
		PID:             cd.PID,
		WorkerID:        cd.WorkerID,
		WorkerPod:       cd.WorkerPod,
		Database:        cd.Database,
		ApplicationName: cd.ApplicationName,
		ClientAddr:      cd.ClientAddr,
		ClientPort:      cd.ClientPort,
		State:           cd.State,
		Query:           cd.Query,
	}
	if !cd.BackendStart.IsZero() {
		d.BackendStart = cd.BackendStart.UTC().Format(time.RFC3339)
	}
	if !cd.QueryStart.IsZero() {
		d.QueryStart = cd.QueryStart.UTC().Format(time.RFC3339)
		d.ElapsedMS = time.Since(cd.QueryStart).Milliseconds()
	}
	// Org (consistent with the list view's stack-keyed attribution), protocol,
	// and cached progress come from the session manager. Locate the owning stack
	// by pid, like KillSession.
	for org, stack := range p.router.AllStacks() {
		if stack.Sessions.WorkerIDForPID(pid) < 0 {
			continue
		}
		d.Org = org
		for _, s := range stack.Sessions.AllSessions() {
			if s.PID == pid {
				d.Protocol = s.Protocol
				break
			}
		}
		if prog := stack.Sessions.GetProgress(pid); prog != nil {
			d.Percentage = prog.Percentage
			d.Rows = prog.Rows
			d.TotalRows = prog.TotalRows
			d.Stalled = prog.Stalled
		}
		break
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
			Image:   s.Image,
			State:   string(s.State),
			Binding: s.Binding,
			Count:   s.Count,
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
