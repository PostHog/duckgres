package server

import (
	"github.com/posthog/duckgres/server/observe"
	"github.com/posthog/duckgres/server/sqlcore"
)

// DefaultMaxRetainedBindBytes limits the bytes a single connection can retain
// in unexecuted named portals. It comfortably admits a normal 27,000-parameter
// bulk Bind while bounding abandoned portal memory.
const DefaultMaxRetainedBindBytes int64 = 64 << 20

// DefaultMaxOpenPortals bounds lightweight named portal shells per connection.
// Payload is released immediately after terminal execution, but shells still
// consume map/accounting resources until Close, transaction end, or idle Sync.
const DefaultMaxOpenPortals = 1024

// These fixed charges cover compact slice backing arrays without unsafe. A
// bindParam contains two int32 fields; charging eight bytes is exact for its
// portable representation. Format codes are int16 values. Slice headers live
// in the portal struct and do not grow with the Bind's parameter count.
const (
	bindParamRetainedBytes      = 8
	bindFormatCodeRetainedBytes = 2
)

func retainedBindStorageBytes(bodyBytes, params, paramFormats, resultFormats int) int {
	return bodyBytes +
		params*bindParamRetainedBytes +
		(paramFormats+resultFormats)*bindFormatCodeRetainedBytes
}

func (c *clientConn) maxRetainedBindBytes() int64 {
	if c != nil && c.server != nil && c.server.cfg.MaxRetainedBindBytes > 0 {
		return c.server.cfg.MaxRetainedBindBytes
	}
	return DefaultMaxRetainedBindBytes
}

func (c *clientConn) maxOpenPortals() int {
	if c != nil && c.server != nil && c.server.cfg.MaxOpenPortals > 0 {
		return c.server.cfg.MaxOpenPortals
	}
	return DefaultMaxOpenPortals
}

// installPortal transfers ownership of an already-validated Bind body and its
// compact metadata to p. Callers must check duplicate names and budgets first
// so no accounting is mutated for a rejected Bind.
func (c *clientConn) installPortal(name string, p *portal) {
	if c.portals == nil {
		c.portals = make(map[string]*portal)
	}
	c.portals[name] = p
	retained := p.retainedPayloadBytes()
	c.retainedBindBytes += int64(retained)
	observe.AddRetainedBindBytes(retained)
	observe.AddOpenPortals(1)
}

// releasePortalPayload is idempotent. It releases the Bind backing body and
// every Bind-derived compact slice, while retaining only statement identity and
// an already-encoded RowDescription body needed by Describe and Close.
func (c *clientConn) releasePortalPayload(p *portal, reason string) {
	if p == nil || p.payloadReleased {
		return
	}
	retained := p.retainedPayloadBytes()
	p.bindBody = nil
	p.params = nil
	p.paramFormats = nil
	p.resultFormats = nil
	p.payloadReleased = true
	if retained > 0 {
		c.retainedBindBytes -= int64(retained)
		if c.retainedBindBytes < 0 {
			c.retainedBindBytes = 0
		}
		observe.AddRetainedBindBytes(-retained)
	}
	observe.IncPortalPayloadRelease(reason)
}

// finishPortal marks an Execute terminal and releases its heavy payload. A
// future true PortalSuspended implementation can keep state Ready and defer
// this call until the portal genuinely reaches a terminal response.
func (c *clientConn) finishPortal(p *portal, state portalState, reason string) {
	if p == nil || p.state != portalStateReady {
		return
	}
	p.state = state
	c.releasePortalPayload(p, reason)
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
	observe.AddOpenPortals(-1)
}

func (c *clientConn) dropAllPortals(reason string) {
	for name := range c.portals {
		c.dropPortal(name, reason)
	}
}

func (c *clientConn) dropPortalsForStatement(stmt *preparedStmt, stmtName, reason string) {
	for name, p := range c.portals {
		if p.stmt == stmt || p.stmtName == stmtName {
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
