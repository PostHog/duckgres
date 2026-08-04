package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/posthog/duckgres/server/wire"
)

// pgStatActivityRegex matches queries that reference pg_stat_activity:
//
//	SELECT ... FROM pg_stat_activity ...
//	SELECT ... FROM pg_catalog.pg_stat_activity ...
//
// Limitation: this intercepts any query containing FROM pg_stat_activity,
// so WHERE clauses, JOINs, and column selection are ignored — all rows
// with all columns are always returned. This is acceptable because most
// tools simply do SELECT * FROM pg_stat_activity.
var pgStatActivityRegex = regexp.MustCompile(
	`(?i)\bFROM\s+(?:pg_catalog\s*\.\s*)?pg_stat_activity\b`,
)

// matchPgStatActivityQuery returns true if a query references pg_stat_activity.
func matchPgStatActivityQuery(query string) bool {
	if !strings.Contains(query, "pg_stat_activity") {
		return false
	}
	return pgStatActivityRegex.MatchString(query)
}

// pg_stat_activity column definitions
var pgStatActivityColumns = []struct {
	name    string
	oid     int32
	typSize int16
}{
	{"datid", 23, 4},                 // int4
	{"datname", 25, -1},              // text
	{"pid", 23, 4},                   // int4
	{"usesysid", 23, 4},              // int4
	{"usename", 25, -1},              // text
	{"application_name", 25, -1},     // text
	{"client_addr", 25, -1},          // text (inet in PG, text here)
	{"client_port", 23, 4},           // int4
	{"backend_start", 1184, 8},       // timestamptz
	{"xact_start", 1184, 8},          // timestamptz (NULL)
	{"query_start", 1184, 8},         // timestamptz (NULL)
	{"state_change", 1184, 8},        // timestamptz (NULL)
	{"wait_event_type", 25, -1},      // text (NULL)
	{"wait_event", 25, -1},           // text (NULL)
	{"state", 25, -1},                // text
	{"backend_xid", 28, 4},           // xid (NULL)
	{"backend_xmin", 28, 4},          // xid (NULL)
	{"query", 25, -1},                // text
	{"backend_type", 25, -1},         // text
	{"leader_pid", 23, 4},            // int4 (NULL)
	{"worker_id", 23, 4},             // int4 (duckgres extension)
	{"query_progress", 701, 8},       // float8 (percentage, -1 if not tracked)
	{"rows_processed", 20, 8},        // int8
	{"total_rows_to_process", 20, 8}, // int8
}

// pgStatActivityTypeOIDs is precomputed from pgStatActivityColumns to avoid per-row allocation.
var pgStatActivityTypeOIDs = func() []int32 {
	oids := make([]int32, len(pgStatActivityColumns))
	for i, col := range pgStatActivityColumns {
		oids[i] = col.oid
	}
	return oids
}()

// handlePgStatActivity handles SELECT FROM pg_stat_activity in the Simple Query protocol.
func (c *clientConn) handlePgStatActivity() error {
	if err := c.sendPgStatActivityRowDescriptionWithFormats(nil); err != nil {
		return err
	}

	conns := c.visiblePgStatActivityConns()
	sort.Slice(conns, func(i, j int) bool { return conns[i].pid < conns[j].pid })

	for _, conn := range conns {
		if err := c.sendPgStatActivityDataRow(conn, nil); err != nil {
			return err
		}
	}

	_ = c.writeCommandComplete(fmt.Sprintf("SELECT %d", len(conns)))
	_ = c.writeReadyForQuery(c.txStatus)
	_ = c.flushWriter()
	return nil
}

// handlePgStatActivityExtended handles SELECT FROM pg_stat_activity in the Extended Query protocol.
func (c *clientConn) handlePgStatActivityExtended(p *portal) {
	if !p.stmt.described && !p.described {
		_ = c.sendPgStatActivityRowDescriptionWithFormats(p.resultFormats)
	}

	conns := c.visiblePgStatActivityConns()
	sort.Slice(conns, func(i, j int) bool { return conns[i].pid < conns[j].pid })

	for _, conn := range conns {
		_ = c.sendPgStatActivityDataRow(conn, p.resultFormats)
	}

	_ = c.writeCommandComplete(fmt.Sprintf("SELECT %d", len(conns)))
}

func (c *clientConn) visiblePgStatActivityConns() []*clientConn {
	conns := c.server.listConns()
	if c.queryAccessPolicy == nil {
		return conns
	}

	visible := make([]*clientConn, 0, len(conns))
	for _, conn := range conns {
		// Cluster project readers have one distinct username per project.
		if conn.orgID == c.orgID && conn.username == c.username {
			visible = append(visible, conn)
		}
	}
	return visible
}

// sendPgStatActivityRowDescription sends a RowDescription for pg_stat_activity.
func (c *clientConn) sendPgStatActivityRowDescriptionWithFormats(formatCodes []int16) error {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int16(len(pgStatActivityColumns)))
	for i, col := range pgStatActivityColumns {
		buf.WriteString(col.name)
		buf.WriteByte(0)
		_ = binary.Write(&buf, binary.BigEndian, int32(0))    // table OID
		_ = binary.Write(&buf, binary.BigEndian, int16(0))    // column attr
		_ = binary.Write(&buf, binary.BigEndian, col.oid)     // type OID
		_ = binary.Write(&buf, binary.BigEndian, col.typSize) // type size
		_ = binary.Write(&buf, binary.BigEndian, int32(-1))   // typmod
		var format int16
		if len(formatCodes) == 1 {
			format = formatCodes[0]
		} else if i < len(formatCodes) {
			format = formatCodes[i]
		}
		_ = binary.Write(&buf, binary.BigEndian, format)
	}
	return wire.WriteMessage(c.writer, wire.MsgRowDescription, buf.Bytes())
}

// sendPgStatActivityDataRow sends a DataRow for a single connection in pg_stat_activity.
func (c *clientConn) sendPgStatActivityDataRow(conn *clientConn, formatCodes []int16) error {
	// Determine state.
	// NOTE: conn.txStatus is read without synchronization. This is a benign race —
	// txStatus is a single byte written only by the owning goroutine, and a stale
	// read just means a briefly inaccurate state string. Making txStatus atomic
	// would require changing dozens of write sites for negligible benefit.
	var state string
	q, _ := conn.currentQuery.Load().(string)
	if q != "" {
		state = "active"
	} else {
		switch conn.txStatus {
		case txStatusTransaction:
			state = "idle in transaction"
		case txStatusError:
			state = "idle in transaction (aborted)"
		default:
			state = "idle"
		}
	}

	// Extract client IP and port from RemoteAddr
	var clientAddr string
	var clientPort int32
	if conn.conn != nil {
		if addr, ok := conn.conn.RemoteAddr().(*net.TCPAddr); ok {
			clientAddr = addr.IP.String()
			clientPort = int32(addr.Port)
		}
	}

	// query_start: populated from atomic.Value when a query is active.
	var queryStart interface{}
	if qs, ok := conn.queryStart.Load().(time.Time); ok && !qs.IsZero() {
		queryStart = qs
	}

	// Query progress columns: populated from cached worker health check data
	// (control plane mode) or nil (standalone mode).
	var queryProgress, rowsProcessed, totalRowsToProcess interface{}
	if c.server.progressFn != nil {
		pct, rows, total, stalled := c.server.progressFn(conn.pid)
		queryProgress = pct
		rowsProcessed = int64(rows)
		totalRowsToProcess = int64(total)
		// Override state to "active (stuck)" when the worker detects no progress.
		if stalled && state == "active" {
			state = "active (stuck)"
		}
	} else {
		queryProgress = float64(-1)
		rowsProcessed = int64(0)
		totalRowsToProcess = int64(0)
	}

	values := []interface{}{
		int32(0),             // datid
		conn.database,        // datname
		conn.pid,             // pid
		int32(10),            // usesysid
		conn.username,        // usename
		conn.applicationName, // application_name
		clientAddr,           // client_addr
		clientPort,           // client_port
		conn.backendStart,    // backend_start
		nil,                  // xact_start (NULL)
		queryStart,           // query_start
		nil,                  // state_change (NULL)
		nil,                  // wait_event_type (NULL)
		nil,                  // wait_event (NULL)
		state,                // state
		nil,                  // backend_xid (NULL)
		nil,                  // backend_xmin (NULL)
		q,                    // query
		"client backend",     // backend_type
		nil,                  // leader_pid (NULL)
		int32(conn.workerID), // worker_id
		queryProgress,        // query_progress (float8)
		rowsProcessed,        // rows_processed (int8)
		totalRowsToProcess,   // total_rows_to_process (int8)
	}

	return c.sendDataRowWithFormats(values, formatCodes, pgStatActivityTypeOIDs)
}

// connDetail builds a redacted ConnDetail snapshot of this connection for the
// admin live-query detail view. Query is the ALREADY-redacted currentQuery
// (usersecrets.RedactForLog, the same value pg_stat_activity exposes) — this
// path must never surface raw SQL, or a CREATE PERSISTENT SECRET credential
// would leak into the admin UI. The state string mirrors the pg_stat_activity
// derivation above (active iff a query is in flight, else the txn idle state).
// connState returns the pg_stat_activity-style state string: "active" when a
// query is in flight, else the idle / idle-in-transaction state from the txn
// status. An "active" session has query data; anything else is idle (no
// in-flight query) — the smell the Live view flags.
func (c *clientConn) connState() string {
	if q, _ := c.currentQuery.Load().(string); q != "" {
		return "active"
	}
	switch c.txStatus {
	case txStatusTransaction:
		return "idle in transaction"
	case txStatusError:
		return "idle in transaction (aborted)"
	default:
		return "idle"
	}
}

func (c *clientConn) connDetail() ConnDetail {
	q, _ := c.currentQuery.Load().(string)
	state := c.connState()
	d := ConnDetail{
		PID:             c.pid,
		OrgID:           c.orgID,
		Username:        c.username,
		Database:        c.database,
		ApplicationName: c.applicationName,
		WorkerID:        c.workerID,
		WorkerPod:       c.workerPod,
		State:           state,
		Query:           q,
		BackendStart:    c.backendStart,
	}
	if c.conn != nil {
		if addr, ok := c.conn.RemoteAddr().(*net.TCPAddr); ok {
			d.ClientAddr = addr.IP.String()
			d.ClientPort = int32(addr.Port)
		}
	}
	if qs, ok := c.queryStart.Load().(time.Time); ok && !qs.IsZero() {
		d.QueryStart = qs
	}
	return d
}
