package server

import "github.com/posthog/duckgres/server/sqlcore"

// installPortal transfers ownership of an already-validated Bind frame and
// compact metadata to the portal.
func (c *clientConn) installPortal(name string, p *portal) {
	if c.portals == nil {
		c.portals = make(map[string]*portal)
	}
	c.portals[name] = p
}

// releasePortalPayload is idempotent. It releases the Bind frame and every
// Bind-derived slice, while retaining cached RowDescription metadata needed by
// terminal Describe(P).
func (c *clientConn) releasePortalPayload(p *portal, _ string) {
	if p == nil || p.payloadReleased {
		return
	}
	p.bindBody = nil
	p.params = nil
	p.paramFormats = nil
	p.resultFormats = nil
	p.payloadReleased = true
}

// finishPortal records a terminal Execute result and releases the heavy Bind
// payload. A future PortalSuspended implementation can keep state Ready until
// the portal actually reaches a terminal response.
func (c *clientConn) finishPortal(p *portal, state portalState, reason string) {
	if p == nil || p.state != portalStateReady {
		return
	}
	p.state = state
	c.releasePortalPayload(p, reason)
	// Terminal Describe(P) replays rowDescription and Close(S) matches
	// stmtName, so the shell must not pin the prepared statement.
	p.stmt = nil
}

func (c *clientConn) dropPortal(name, reason string) {
	if c == nil || c.portals == nil {
		return
	}
	p, ok := c.portals[name]
	if !ok {
		return
	}
	delete(c.portals, name)
	c.releasePortalPayload(p, reason)
}

func (c *clientConn) dropAllPortals(reason string) {
	for name := range c.portals {
		c.dropPortal(name, reason)
	}
}

func (c *clientConn) dropPortalsForStatement(stmt *preparedStmt, stmtName, reason string) {
	for name, p := range c.portals {
		if (stmt != nil && p.stmt == stmt) || p.stmtName == stmtName {
			c.dropPortal(name, reason)
		}
	}
}

// execPortal/queryPortal keep multi-statement rewrites on the same compact
// Flight path as ordinary extended execution. Local executors retain their
// existing database/sql []any behavior.
func (c *clientConn) execPortal(p *portal, query string, args []interface{}) (ExecResult, error) {
	if executor, ok := c.executor.(sqlcore.BoundQueryExecutor); ok {
		return executor.ExecWithBoundParams(query, p)
	}
	return c.executor.Exec(query, args...)
}

func (c *clientConn) queryPortal(p *portal, query string, args []interface{}) (RowSet, error) {
	if executor, ok := c.executor.(sqlcore.BoundQueryExecutor); ok {
		return executor.QueryWithBoundParams(query, p)
	}
	return c.executor.Query(query, args...)
}
