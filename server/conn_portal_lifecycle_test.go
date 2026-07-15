package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/server/flightclient"
	"github.com/posthog/duckgres/server/sqlcore"
	"github.com/posthog/duckgres/server/wire"
)

// portalLifecycleBindValue represents one client-provided Bind value. null is
// deliberately separate from data so tests exercise PostgreSQL's distinction
// between NULL and a zero-length value.
type portalLifecycleBindValue struct {
	null bool
	data []byte
}

// compactFlightRecordingExecutor proves extended execution selects the compact
// Flight capability instead of converting every text value to []any first.
type compactFlightRecordingExecutor struct {
	lifecycleExecutor
	interpolated string
	boundExecs   int
}

// cursorFormatCountingRowSet exposes a fixed two-column cursor schema while
// recording whether extended FETCH tried to advance it.
type cursorFormatCountingRowSet struct {
	nextCalls int
}

func (*cursorFormatCountingRowSet) Columns() ([]string, error) {
	return []string{"id", "name"}, nil
}

func (*cursorFormatCountingRowSet) ColumnTypes() ([]ColumnTyper, error) {
	return []ColumnTyper{describeColumnType("INTEGER"), describeColumnType("VARCHAR")}, nil
}

func (r *cursorFormatCountingRowSet) Next() bool {
	r.nextCalls++
	return false
}

func (*cursorFormatCountingRowSet) Scan(...any) error { return nil }
func (*cursorFormatCountingRowSet) Close() error      { return nil }
func (*cursorFormatCountingRowSet) Err() error        { return nil }

func (e *compactFlightRecordingExecutor) QueryWithBoundParams(query string, params sqlcore.SQLLiteralAppender) (RowSet, error) {
	e.boundExecs++
	interpolated, err := flightclient.InterpolateBoundArgs(query, params)
	if err != nil {
		return nil, err
	}
	e.interpolated = interpolated
	return e.queryRows, e.queryErr
}

func (e *compactFlightRecordingExecutor) ExecWithBoundParams(query string, params sqlcore.SQLLiteralAppender) (ExecResult, error) {
	e.boundExecs++
	interpolated, err := flightclient.InterpolateBoundArgs(query, params)
	if err != nil {
		return nil, err
	}
	e.interpolated = interpolated
	return e.execResult, e.execErr
}

func portalLifecycleBindBody(portalName, stmtName string, paramFormats []int16, values []portalLifecycleBindValue, resultFormats []int16) []byte {
	var body bytes.Buffer
	body.WriteString(portalName)
	body.WriteByte(0)
	body.WriteString(stmtName)
	body.WriteByte(0)
	_ = binary.Write(&body, binary.BigEndian, int16(len(paramFormats)))
	for _, format := range paramFormats {
		_ = binary.Write(&body, binary.BigEndian, format)
	}
	_ = binary.Write(&body, binary.BigEndian, int16(len(values)))
	for _, value := range values {
		if value.null {
			_ = binary.Write(&body, binary.BigEndian, int32(-1))
			continue
		}
		_ = binary.Write(&body, binary.BigEndian, int32(len(value.data)))
		body.Write(value.data)
	}
	_ = binary.Write(&body, binary.BigEndian, int16(len(resultFormats)))
	for _, format := range resultFormats {
		_ = binary.Write(&body, binary.BigEndian, format)
	}
	return body.Bytes()
}

func portalLifecycleExecuteBody(portalName string) []byte {
	var body bytes.Buffer
	body.WriteString(portalName)
	body.WriteByte(0)
	_ = binary.Write(&body, binary.BigEndian, int32(0))
	return body.Bytes()
}

func newPortalLifecycleConn(t *testing.T, executor QueryExecutor) (*clientConn, *bytes.Buffer, func()) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	out := &bytes.Buffer{}
	ctx, cancel := context.WithCancel(context.Background())
	c := &clientConn{
		server:   &Server{activeQueries: make(map[BackendKey]context.CancelFunc)},
		conn:     serverSide,
		reader:   bufio.NewReader(serverSide),
		writer:   bufio.NewWriter(out),
		executor: executor,
		stmts:    make(map[string]*preparedStmt),
		portals:  make(map[string]*portal),
		cursors:  make(map[string]*cursorState),
		txStatus: txStatusIdle,
		ctx:      ctx,
		cancel:   cancel,
	}
	return c, out, func() {
		cancel()
		_ = serverSide.Close()
		_ = clientSide.Close()
	}
}

func bindPortalForLifecycle(t *testing.T, c *clientConn, portalName, stmtName string, paramFormats []int16, values []portalLifecycleBindValue) *portal {
	t.Helper()
	c.handleBind(portalLifecycleBindBody(portalName, stmtName, paramFormats, values, nil))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush BindComplete: %v", err)
	}
	p, ok := c.portals[portalName]
	if !ok {
		t.Fatalf("Bind did not install portal %q", portalName)
	}
	return p
}

func requirePortalPayload(t *testing.T, p *portal) int {
	t.Helper()
	if got := p.retainedPayloadBytes(); got <= 0 {
		t.Fatalf("portal retained payload bytes = %d, want > 0 after Bind", got)
	} else {
		return got
	}
	return 0
}

func requireReleasedPortalPayload(t *testing.T, p *portal) {
	t.Helper()
	if got := p.retainedPayloadBytes(); got != 0 {
		t.Fatalf("portal retained payload bytes = %d after release, want 0", got)
	}
}

func requirePortalState(t *testing.T, p *portal, want portalState) {
	t.Helper()
	if got := p.state; got != want {
		t.Fatalf("portal state = %v, want %v", got, want)
	}
}

func TestPortalLifecycleTerminalExecuteReleasesPayloadAndRejectsReuse(t *testing.T) {
	c, out, cleanup := newPortalLifecycleConn(t, &lifecycleExecutor{execResult: emptyExecResult{}})
	defer cleanup()
	c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}

	p := bindPortalForLifecycle(t, c, "bulk", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}})
	requirePortalState(t, p, portalStateReady)
	requirePortalPayload(t, p)

	c.handleExecute(portalLifecycleExecuteBody("bulk"))
	requirePortalState(t, p, portalStateDone)
	requireReleasedPortalPayload(t, p)
	if got := c.portals["bulk"]; got != p {
		t.Fatalf("terminal Execute must retain a lightweight portal shell: got %p, want %p", got, p)
	}

	out.Reset()
	c.handleExecute(portalLifecycleExecuteBody("bulk"))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush repeated Execute error: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("55000")) {
		t.Fatalf("second Execute of a completed portal must return 55000, got %q", out.Bytes())
	}
}

func TestPortalLifecycleMalformedExecuteReleasesIdentifiedPortal(t *testing.T) {
	c, out, cleanup := newPortalLifecycleConn(t, nil)
	defer cleanup()
	c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
	p := bindPortalForLifecycle(t, c, "malformed-execute", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}})
	requirePortalPayload(t, p)

	// The portal name is complete but maxRows is missing. This is a terminal
	// Execute protocol error, so it must not leave the Bind body retained until
	// a later Sync or connection close.
	out.Reset()
	c.handleExecute([]byte("malformed-execute\x00"))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush malformed Execute error: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("08P01")) {
		t.Fatalf("malformed Execute response = %q, want 08P01", out.Bytes())
	}
	requirePortalState(t, p, portalStateFailed)
	requireReleasedPortalPayload(t, p)
}

func TestPortalLifecycleTerminalDescribeUsesCachedRowDescription(t *testing.T) {
	executor := &lifecycleExecutor{queryRows: &describeStaticRowSet{
		cols:     []string{"value"},
		colTypes: []ColumnTyper{describeColumnType("VARCHAR")},
	}}
	c, out, cleanup := newPortalLifecycleConn(t, executor)
	defer cleanup()
	c.stmts["select"] = &preparedStmt{
		query:          "SELECT $1",
		convertedQuery: "SELECT ?",
		numParams:      1,
	}

	p := bindPortalForLifecycle(t, c, "describe-after-execute", "select", nil, []portalLifecycleBindValue{{data: []byte("value")}})
	c.handleExecute(portalLifecycleExecuteBody("describe-after-execute"))
	requirePortalState(t, p, portalStateDone)
	requireReleasedPortalPayload(t, p)
	if got := executor.queryCalls.Load(); got != 1 {
		t.Fatalf("Execute query calls = %d, want 1", got)
	}

	out.Reset()
	c.handleDescribe([]byte{'P', 'd', 'e', 's', 'c', 'r', 'i', 'b', 'e', '-', 'a', 'f', 't', 'e', 'r', '-', 'e', 'x', 'e', 'c', 'u', 't', 'e', 0})
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush terminal Describe: %v", err)
	}
	if got := executor.queryCalls.Load(); got != 1 {
		t.Fatalf("terminal Describe must use cached metadata, query calls = %d, want 1", got)
	}
	frames := scanWireFrames(t, out.Bytes())
	if len(frames) != 1 || frames[0].msgType != wire.MsgRowDescription {
		t.Fatalf("terminal Describe frames = %q, want RowDescription", frameTypes(frames))
	}
}

func TestPortalLifecycleTerminalReleaseDropsResultFormatsAfterCachingDescription(t *testing.T) {
	executor := &lifecycleExecutor{queryRows: &describeStaticRowSet{
		cols:     []string{"value"},
		colTypes: []ColumnTyper{describeColumnType("VARCHAR")},
	}}
	c, _, cleanup := newPortalLifecycleConn(t, executor)
	defer cleanup()
	c.stmts["select"] = &preparedStmt{
		query:          "SELECT $1",
		convertedQuery: "SELECT ?",
		numParams:      1,
	}
	// A single result format applies to every result column. Terminal cleanup
	// must not keep the vector just because the portal shell remains.
	formats := []int16{1}
	c.handleBind(portalLifecycleBindBody("formats", "select", nil, []portalLifecycleBindValue{{data: []byte("value")}}, formats))
	p := c.portals["formats"]
	if p == nil {
		t.Fatal("Bind did not install format portal")
	}
	c.handleExecute(portalLifecycleExecuteBody("formats"))

	requirePortalState(t, p, portalStateDone)
	requireReleasedPortalPayload(t, p)
	if len(p.resultFormats) != 0 {
		t.Fatalf("terminal portal retained %d result format codes, want 0", len(p.resultFormats))
	}
	if len(p.rowDescription) < 2 {
		t.Fatal("terminal portal did not retain serialized RowDescription")
	}
	if got := int16(binary.BigEndian.Uint16(p.rowDescription[len(p.rowDescription)-2:])); got != 1 {
		t.Fatalf("cached RowDescription format = %d, want first Bind format 1", got)
	}
}

func TestPortalLifecycleRejectsInvalidResultFormatCountWhenColumnsKnown(t *testing.T) {
	executor := &lifecycleExecutor{queryRows: &describeStaticRowSet{
		cols:     []string{"value"},
		colTypes: []ColumnTyper{describeColumnType("VARCHAR")},
	}}
	c, out, cleanup := newPortalLifecycleConn(t, executor)
	defer cleanup()
	c.stmts["select"] = &preparedStmt{
		query:          "SELECT $1",
		convertedQuery: "SELECT ?",
		numParams:      1,
	}

	// Arbitrary SQL has no result-column count at Bind time. Once Execute sees
	// this one-column result, two result formats are a protocol violation.
	c.handleBind(portalLifecycleBindBody("invalid-formats", "select", nil, []portalLifecycleBindValue{{data: []byte("value")}}, []int16{0, 1}))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush BindComplete: %v", err)
	}
	p := c.portals["invalid-formats"]
	if p == nil {
		t.Fatal("Bind did not install portal with deferred result-format validation")
	}
	requirePortalPayload(t, p)

	out.Reset()
	c.handleExecute(portalLifecycleExecuteBody("invalid-formats"))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush Execute error: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("08P01")) {
		t.Fatalf("invalid result format count response = %q, want 08P01", out.Bytes())
	}
	if got := executor.queryCalls.Load(); got != 1 {
		t.Fatalf("Execute query calls = %d, want 1 to discover result columns", got)
	}
	requirePortalState(t, p, portalStateFailed)
	requireReleasedPortalPayload(t, p)
}

func TestPortalLifecycleCursorFetchRejectsMismatchedFormatsBeforeRows(t *testing.T) {
	c, out, cleanup := newPortalLifecycleConn(t, nil)
	defer cleanup()
	c.stmts["fetch"] = &preparedStmt{
		query:      "FETCH 4 FROM cursor_name",
		cursorOp:   cursorOpFetch,
		cursorName: "cursor_name",
	}
	rows := &cursorFormatCountingRowSet{}
	c.cursors["cursor_name"] = &cursorState{
		rows:     rows,
		cols:     []string{"id", "name"},
		colTypes: []ColumnTyper{describeColumnType("INTEGER"), describeColumnType("VARCHAR")},
		typeOIDs: []int32{OidInt4, OidText},
	}

	// FETCH accepts the multi-code Bind until it knows the cursor schema. Three
	// formats do not match the two output columns, so Execute must reject it
	// before calling Next and release the terminal portal payload.
	c.handleBind(portalLifecycleBindBody("fetch-portal", "fetch", nil, nil, []int16{0, 1, 0}))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush cursor FETCH Bind: %v", err)
	}
	p := c.portals["fetch-portal"]
	if p == nil {
		t.Fatal("cursor FETCH Bind must install a portal before schema validation")
	}
	requirePortalPayload(t, p)
	out.Reset()

	c.handleExecute(portalLifecycleExecuteBody("fetch-portal"))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush cursor FETCH Execute: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("08P01")) {
		t.Fatalf("cursor FETCH mismatched formats response = %q, want 08P01", out.Bytes())
	}
	if rows.nextCalls != 0 {
		t.Fatalf("cursor FETCH called Next %d times before rejecting formats, want 0", rows.nextCalls)
	}
	requirePortalState(t, p, portalStateFailed)
	requireReleasedPortalPayload(t, p)
	if got := c.retainedBindBytes; got != 0 {
		t.Fatalf("retained Bind bytes = %d after terminal cursor FETCH error, want 0", got)
	}
}

func TestPortalLifecycleRejectsInvalidDMLReturningResultFormatsBeforeExecute(t *testing.T) {
	executor := &lifecycleExecutor{queryRows: &describeStaticRowSet{
		cols:     []string{"first", "second"},
		colTypes: []ColumnTyper{describeColumnType("VARCHAR"), describeColumnType("VARCHAR")},
	}}
	c, out, cleanup := newPortalLifecycleConn(t, executor)
	defer cleanup()
	c.stmts["returning"] = &preparedStmt{
		query:          "INSERT INTO t VALUES ($1) RETURNING first, second",
		convertedQuery: "INSERT INTO t VALUES (?) RETURNING first, second",
		numParams:      1,
	}

	// DML RETURNING cannot be schema-probed safely. A three-code vector cannot
	// describe its two explicit output targets, so Bind must reject it before
	// Execute gets an opportunity to mutate anything.
	c.handleBind(portalLifecycleBindBody("invalid-returning", "returning", nil, []portalLifecycleBindValue{{data: []byte("value")}}, []int16{0, 1, 0}))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush Bind error: %v", err)
	}
	if _, ok := c.portals["invalid-returning"]; ok {
		t.Fatal("invalid DML RETURNING formats must not install a portal")
	}
	if !bytes.Contains(out.Bytes(), []byte("08P01")) {
		t.Fatalf("invalid DML RETURNING result format response = %q, want 08P01", out.Bytes())
	}
	if got := executor.queryCalls.Load(); got != 0 {
		t.Fatalf("invalid Bind must not invoke Query, got %d calls", got)
	}
	if got := executor.execCalls.Load(); got != 0 {
		t.Fatalf("invalid Bind must not invoke Exec, got %d calls", got)
	}
}

func TestPortalLifecycleAllowsMatchingExplicitDMLReturningResultFormats(t *testing.T) {
	c, _, cleanup := newPortalLifecycleConn(t, nil)
	defer cleanup()
	c.stmts["returning"] = &preparedStmt{
		query:          "INSERT INTO t VALUES ($1) RETURNING first, second",
		convertedQuery: "INSERT INTO t VALUES (?) RETURNING first, second",
		numParams:      1,
	}

	c.handleBind(portalLifecycleBindBody("matching-returning", "returning", nil, []portalLifecycleBindValue{{data: []byte("value")}}, []int16{0, 1}))
	p := c.portals["matching-returning"]
	if p == nil {
		t.Fatal("matching explicit DML RETURNING formats must install a portal")
	}
	requirePortalPayload(t, p)
}

func TestPortalLifecycleRejectsMultiCodeFormatsForExplainAnalyzeWriteBeforeExecute(t *testing.T) {
	executor := &lifecycleExecutor{queryRows: &describeStaticRowSet{
		cols:     []string{"explain_value"},
		colTypes: []ColumnTyper{describeColumnType("VARCHAR")},
	}}
	c, out, cleanup := newPortalLifecycleConn(t, executor)
	defer cleanup()
	c.stmts["explain-write"] = &preparedStmt{
		query:          "EXPLAIN ANALYZE INSERT INTO t VALUES ($1)",
		convertedQuery: "EXPLAIN ANALYZE INSERT INTO t VALUES (?)",
		numParams:      1,
	}

	// EXPLAIN has exactly one plan column. More than one result format must be
	// rejected at Bind so EXPLAIN ANALYZE cannot run the wrapped write first.
	c.handleBind(portalLifecycleBindBody("invalid-explain-write", "explain-write", nil, []portalLifecycleBindValue{{data: []byte("value")}}, []int16{0, 1}))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush Bind error: %v", err)
	}
	if _, ok := c.portals["invalid-explain-write"]; ok {
		t.Fatal("invalid EXPLAIN ANALYZE formats must not install a portal")
	}
	if !bytes.Contains(out.Bytes(), []byte("08P01")) {
		t.Fatalf("invalid EXPLAIN ANALYZE result format response = %q, want 08P01", out.Bytes())
	}
	if got := executor.queryCalls.Load(); got != 0 {
		t.Fatalf("invalid Bind must not invoke Query, got %d calls", got)
	}
	if got := executor.execCalls.Load(); got != 0 {
		t.Fatalf("invalid Bind must not invoke Exec, got %d calls", got)
	}
}

func TestPortalLifecycleRejectsMultiCodeFormatsForWildcardDMLReturning(t *testing.T) {
	c, out, cleanup := newPortalLifecycleConn(t, nil)
	defer cleanup()
	c.stmts["returning-star"] = &preparedStmt{
		query:          "INSERT INTO t VALUES ($1) RETURNING *",
		convertedQuery: "INSERT INTO t VALUES (?) RETURNING *",
		numParams:      1,
	}

	// The table schema determines the cardinality of RETURNING *. A multi-code
	// vector must be rejected rather than execute a write to discover it.
	c.handleBind(portalLifecycleBindBody("invalid-returning-star", "returning-star", nil, []portalLifecycleBindValue{{data: []byte("value")}}, []int16{0, 1}))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush Bind error: %v", err)
	}
	if _, ok := c.portals["invalid-returning-star"]; ok {
		t.Fatal("wildcard DML RETURNING formats must not install a portal")
	}
	if !bytes.Contains(out.Bytes(), []byte("08P01")) {
		t.Fatalf("wildcard DML RETURNING result format response = %q, want 08P01", out.Bytes())
	}
}

func TestPortalLifecycleRejectsMultiCodeFormatsForWritableCTERewrite(t *testing.T) {
	c, out, cleanup := newPortalLifecycleConn(t, nil)
	defer cleanup()
	c.stmts["writable-cte"] = &preparedStmt{
		query:          "WITH changed AS (UPDATE t SET value = $1 RETURNING id) SELECT id FROM changed",
		convertedQuery: "SELECT id FROM changed",
		numParams:      1,
		statements: []string{
			"UPDATE t SET value = ?",
			"SELECT id FROM changed",
		},
	}

	// Rewritten writable CTEs perform setup writes before their final SELECT, so
	// a multi-code vector cannot wait for runtime output metadata.
	c.handleBind(portalLifecycleBindBody("invalid-writable-cte", "writable-cte", nil, []portalLifecycleBindValue{{data: []byte("value")}}, []int16{0, 1}))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush Bind error: %v", err)
	}
	if _, ok := c.portals["invalid-writable-cte"]; ok {
		t.Fatal("writable CTE result formats must not install a portal")
	}
	if !bytes.Contains(out.Bytes(), []byte("08P01")) {
		t.Fatalf("writable CTE result format response = %q, want 08P01", out.Bytes())
	}
}

func TestPortalLifecycleFlightUsesCompactLiteralAppender(t *testing.T) {
	executor := &compactFlightRecordingExecutor{lifecycleExecutor: lifecycleExecutor{execResult: emptyExecResult{}}}
	c, _, cleanup := newPortalLifecycleConn(t, executor)
	defer cleanup()
	c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}

	p := bindPortalForLifecycle(t, c, "flight", "insert", nil, []portalLifecycleBindValue{{data: []byte("o'brien")}})
	c.handleExecute(portalLifecycleExecuteBody("flight"))
	if executor.boundExecs != 1 {
		t.Fatalf("compact Flight Exec calls = %d, want 1", executor.boundExecs)
	}
	if got, want := executor.interpolated, "INSERT INTO t VALUES ('o''brien')"; got != want {
		t.Fatalf("compact Flight SQL = %q, want %q", got, want)
	}
	requireReleasedPortalPayload(t, p)
}

func TestPortalLifecycleEmptyExecuteReleasesPayload(t *testing.T) {
	c, _, cleanup := newPortalLifecycleConn(t, nil)
	defer cleanup()
	c.stmts["empty"] = &preparedStmt{}

	p := bindPortalForLifecycle(t, c, "empty-portal", "empty", nil, nil)
	c.handleExecute(portalLifecycleExecuteBody("empty-portal"))

	requirePortalState(t, p, portalStateDone)
	requireReleasedPortalPayload(t, p)
}

func TestPortalLifecycleDecodeErrorReleasesPayloadAndRejectsReuse(t *testing.T) {
	c, out, cleanup := newPortalLifecycleConn(t, &lifecycleExecutor{execResult: emptyExecResult{}})
	defer cleanup()
	c.stmts["binary"] = &preparedStmt{
		query:          "INSERT INTO t VALUES ($1)",
		convertedQuery: "INSERT INTO t VALUES (?)",
		paramTypes:     []int32{OidInt4},
		numParams:      1,
	}

	p := bindPortalForLifecycle(t, c, "bad-binary", "binary", []int16{1}, []portalLifecycleBindValue{{data: []byte{1}}})
	c.handleExecute(portalLifecycleExecuteBody("bad-binary"))
	requirePortalState(t, p, portalStateFailed)
	requireReleasedPortalPayload(t, p)

	out.Reset()
	c.handleExecute(portalLifecycleExecuteBody("bad-binary"))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush repeated Execute error: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("55000")) {
		t.Fatalf("second Execute of a failed portal must return 55000, got %q", out.Bytes())
	}
}

func TestPortalLifecycleExecutorErrorReleasesPayloadAndRejectsReuse(t *testing.T) {
	c, out, cleanup := newPortalLifecycleConn(t, &lifecycleExecutor{execErr: errors.New("executor failed")})
	defer cleanup()
	c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}

	p := bindPortalForLifecycle(t, c, "executor-failure", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}})
	c.handleExecute(portalLifecycleExecuteBody("executor-failure"))
	requirePortalState(t, p, portalStateFailed)
	requireReleasedPortalPayload(t, p)

	out.Reset()
	c.handleExecute(portalLifecycleExecuteBody("executor-failure"))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush repeated Execute error: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("55000")) {
		t.Fatalf("second Execute of an executor-failed portal must return 55000, got %q", out.Bytes())
	}
}

func TestPortalLifecycleUnnamedRebindReleasesPriorPayload(t *testing.T) {
	c, _, cleanup := newPortalLifecycleConn(t, nil)
	defer cleanup()
	c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}

	first := bindPortalForLifecycle(t, c, "", "insert", nil, []portalLifecycleBindValue{{data: []byte("first")}})
	requirePortalPayload(t, first)
	second := bindPortalForLifecycle(t, c, "", "insert", nil, []portalLifecycleBindValue{{data: []byte("second")}})

	if first == second {
		t.Fatal("unnamed Bind must replace the prior portal shell")
	}
	requireReleasedPortalPayload(t, first)
	if got := c.portals[""]; got != second {
		t.Fatalf("unnamed portal = %p, want replacement %p", got, second)
	}
	requirePortalState(t, second, portalStateReady)
	requirePortalPayload(t, second)
}

func TestPortalLifecycleDuplicateNamedBindPreservesExistingPortal(t *testing.T) {
	c, out, cleanup := newPortalLifecycleConn(t, nil)
	defer cleanup()
	c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}

	first := bindPortalForLifecycle(t, c, "named", "insert", nil, []portalLifecycleBindValue{{data: []byte("first")}})
	firstBytes := requirePortalPayload(t, first)

	out.Reset()
	c.handleBind(portalLifecycleBindBody("named", "insert", nil, []portalLifecycleBindValue{{data: []byte("second")}}, nil))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush duplicate Bind error: %v", err)
	}
	if !bytes.Contains(out.Bytes(), []byte("42P03")) {
		t.Fatalf("duplicate named Bind must return duplicate_cursor (42P03), got %q", out.Bytes())
	}
	if got := c.portals["named"]; got != first {
		t.Fatalf("duplicate named Bind replaced original portal: got %p, want %p", got, first)
	}
	if got := first.retainedPayloadBytes(); got != firstBytes {
		t.Fatalf("duplicate named Bind changed original retained bytes: got %d, want %d", got, firstBytes)
	}
}

func TestPortalLifecycleBindBudgetRejectionLeavesExistingPortalsUntouched(t *testing.T) {
	t.Run("retained bytes", func(t *testing.T) {
		c, out, cleanup := newPortalLifecycleConn(t, nil)
		defer cleanup()
		c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
		firstBody := portalLifecycleBindBody("p1", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}}, nil)
		c.server.cfg.MaxRetainedBindBytes = int64(len(firstBody) + bindParamRetainedBytes)

		first := bindPortalForLifecycle(t, c, "p1", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}})
		retained := first.retainedPayloadBytes()
		out.Reset()
		c.handleBind(portalLifecycleBindBody("p2", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}}, nil))
		if err := c.writer.Flush(); err != nil {
			t.Fatalf("flush rejected Bind: %v", err)
		}
		if !bytes.Contains(out.Bytes(), []byte("54000")) {
			t.Fatalf("retained-byte budget rejection must return 54000, got %q", out.Bytes())
		}
		if got := c.portals["p1"]; got != first {
			t.Fatalf("budget rejection changed existing portal: got %p, want %p", got, first)
		}
		if got := first.retainedPayloadBytes(); got != retained {
			t.Fatalf("budget rejection changed existing payload: got %d, want %d", got, retained)
		}
		if _, ok := c.portals["p2"]; ok {
			t.Fatal("budget-rejected Bind installed a partial portal")
		}
	})

	t.Run("compact metadata is charged", func(t *testing.T) {
		c, out, cleanup := newPortalLifecycleConn(t, nil)
		defer cleanup()
		c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
		body := portalLifecycleBindBody("p1", "insert", nil, []portalLifecycleBindValue{{null: true}}, nil)
		// The raw frame alone fits, but the descriptor backing storage must be
		// admitted too or a large NULL/empty Bind can bypass the byte budget.
		c.server.cfg.MaxRetainedBindBytes = int64(len(body))

		c.handleBind(body)
		if err := c.writer.Flush(); err != nil {
			t.Fatalf("flush metadata-budget Bind: %v", err)
		}
		if !bytes.Contains(out.Bytes(), []byte("54000")) {
			t.Fatalf("compact metadata budget rejection must return 54000, got %q", out.Bytes())
		}
		if len(c.portals) != 0 || c.retainedBindBytes != 0 {
			t.Fatalf("metadata-budget rejection changed portal/accounting state: portals=%d retained=%d", len(c.portals), c.retainedBindBytes)
		}
	})

	t.Run("open portals", func(t *testing.T) {
		c, out, cleanup := newPortalLifecycleConn(t, nil)
		defer cleanup()
		c.server.cfg.MaxOpenPortals = 1
		c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
		first := bindPortalForLifecycle(t, c, "p1", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}})

		out.Reset()
		c.handleBind(portalLifecycleBindBody("p2", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}}, nil))
		if err := c.writer.Flush(); err != nil {
			t.Fatalf("flush rejected Bind: %v", err)
		}
		if !bytes.Contains(out.Bytes(), []byte("54000")) {
			t.Fatalf("open-portal budget rejection must return 54000, got %q", out.Bytes())
		}
		if got := c.portals["p1"]; got != first {
			t.Fatalf("open-portal rejection changed existing portal: got %p, want %p", got, first)
		}
		if _, ok := c.portals["p2"]; ok {
			t.Fatal("open-portal-rejected Bind installed a partial portal")
		}
	})
}

func TestPortalLifecycleCloseReleasesPortalPayloads(t *testing.T) {
	t.Run("Close(P)", func(t *testing.T) {
		c, _, cleanup := newPortalLifecycleConn(t, nil)
		defer cleanup()
		c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
		p := bindPortalForLifecycle(t, c, "close-me", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}})

		c.handleClose([]byte{'P', 'c', 'l', 'o', 's', 'e', '-', 'm', 'e', 0})
		if _, ok := c.portals["close-me"]; ok {
			t.Fatal("Close(P) must remove the portal")
		}
		requireReleasedPortalPayload(t, p)
	})

	t.Run("Close(S)", func(t *testing.T) {
		c, _, cleanup := newPortalLifecycleConn(t, nil)
		defer cleanup()
		c.stmts["s1"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
		c.stmts["s2"] = &preparedStmt{query: "INSERT INTO u VALUES ($1)", convertedQuery: "INSERT INTO u VALUES (?)", numParams: 1}
		p1 := bindPortalForLifecycle(t, c, "p1", "s1", nil, []portalLifecycleBindValue{{data: []byte("one")}})
		p2 := bindPortalForLifecycle(t, c, "p2", "s1", nil, []portalLifecycleBindValue{{data: []byte("two")}})
		other := bindPortalForLifecycle(t, c, "other", "s2", nil, []portalLifecycleBindValue{{data: []byte("three")}})

		// A re-Parse replaces the named statement object, but Close(S) must
		// still release portals originally derived from that statement name.
		c.stmts["s1"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
		c.handleClose([]byte{'S', 's', '1', 0})
		if _, ok := c.portals["p1"]; ok {
			t.Fatal("Close(S) must remove portals derived from the statement")
		}
		if _, ok := c.portals["p2"]; ok {
			t.Fatal("Close(S) must remove every portal derived from the statement")
		}
		if got := c.portals["other"]; got != other {
			t.Fatalf("Close(S) removed unrelated portal: got %p, want %p", got, other)
		}
		requireReleasedPortalPayload(t, p1)
		requireReleasedPortalPayload(t, p2)
		requirePortalPayload(t, other)
	})
}

type portalLifecycleOutput struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	writes chan struct{}
}

func newPortalLifecycleOutput() *portalLifecycleOutput {
	return &portalLifecycleOutput{writes: make(chan struct{}, 8)}
}

func (o *portalLifecycleOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	n, err := o.buf.Write(p)
	o.mu.Unlock()
	select {
	case o.writes <- struct{}{}:
	default:
	}
	return n, err
}

func (o *portalLifecycleOutput) snapshot() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return bytes.Clone(o.buf.Bytes())
}

func newPortalLifecycleLoopConn(t *testing.T, status byte) (*clientConn, net.Conn, *portalLifecycleOutput, <-chan error, func()) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	out := newPortalLifecycleOutput()
	c := &clientConn{
		server:   &Server{cfg: Config{IdleTimeout: -1}, activeQueries: make(map[BackendKey]context.CancelFunc)},
		conn:     serverSide,
		reader:   bufio.NewReader(serverSide),
		writer:   bufio.NewWriter(out),
		stmts:    make(map[string]*preparedStmt),
		portals:  make(map[string]*portal),
		cursors:  make(map[string]*cursorState),
		txStatus: status,
		ctx:      ctx,
		cancel:   cancel,
	}
	done := make(chan error, 1)
	go func() { done <- c.messageLoop() }()
	return c, clientSide, out, done, func() {
		cancel()
		_ = clientSide.Close()
		_ = serverSide.Close()
	}
}

func syncPortalLifecycleConn(t *testing.T, client net.Conn, out *portalLifecycleOutput) {
	t.Helper()
	if err := wire.WriteMessage(client, wire.MsgSync, nil); err != nil {
		t.Fatalf("write Sync: %v", err)
	}
	select {
	case <-out.writes:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Sync response")
	}
	frames := scanWireFrames(t, out.snapshot())
	if len(frames) == 0 || frames[len(frames)-1].msgType != wire.MsgReadyForQuery {
		t.Fatalf("Sync response frames = %q, want final ReadyForQuery", frameTypes(frames))
	}
}

func stopPortalLifecycleLoop(t *testing.T, client net.Conn, done <-chan error) {
	t.Helper()
	if err := client.Close(); err != nil {
		t.Fatalf("close client side: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("message loop returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("message loop did not stop after client close")
	}
}

func TestPortalLifecycleSyncDropsIdlePortalsButPreservesTransactionPortals(t *testing.T) {
	t.Run("idle", func(t *testing.T) {
		c, client, out, done, cleanup := newPortalLifecycleLoopConn(t, txStatusIdle)
		defer cleanup()
		c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
		c.handleBind(portalLifecycleBindBody("p", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}}, nil))
		p := c.portals["p"]
		requirePortalPayload(t, p)

		syncPortalLifecycleConn(t, client, out)
		if len(c.portals) != 0 {
			t.Fatalf("idle Sync must drop all portal shells, got %d", len(c.portals))
		}
		requireReleasedPortalPayload(t, p)
		stopPortalLifecycleLoop(t, client, done)
	})

	for _, status := range []byte{txStatusTransaction, txStatusError} {
		status := status
		t.Run("transaction_"+string(status), func(t *testing.T) {
			c, client, out, done, cleanup := newPortalLifecycleLoopConn(t, status)
			defer cleanup()
			c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
			c.handleBind(portalLifecycleBindBody("p", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}}, nil))
			p := c.portals["p"]
			payloadBytes := requirePortalPayload(t, p)

			syncPortalLifecycleConn(t, client, out)
			if got := c.portals["p"]; got != p {
				t.Fatalf("Sync in transaction state %c dropped portal: got %p, want %p", status, got, p)
			}
			if got := p.retainedPayloadBytes(); got != payloadBytes {
				t.Fatalf("Sync in transaction state %c changed retained payload bytes: got %d, want %d", status, got, payloadBytes)
			}
			stopPortalLifecycleLoop(t, client, done)
		})
	}
}

func TestPortalLifecycleManyNamedBindExecuteBeforeSyncReleasePayloads(t *testing.T) {
	c, out, cleanup := newPortalLifecycleConn(t, &lifecycleExecutor{execResult: emptyExecResult{}})
	defer cleanup()
	c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}

	const portals = 12
	for i := 0; i < portals; i++ {
		name := "bulk-" + string(rune('a'+i))
		p := bindPortalForLifecycle(t, c, name, "insert", nil, []portalLifecycleBindValue{{data: bytes.Repeat([]byte("v"), 1024)}})
		c.handleExecute(portalLifecycleExecuteBody(name))
		requirePortalState(t, p, portalStateDone)
		requireReleasedPortalPayload(t, p)
		if got := c.retainedBindBytes; got != 0 {
			t.Fatalf("retained Bind bytes after terminal Execute %q = %d, want 0", name, got)
		}
	}
	if got := len(c.portals); got != portals {
		t.Fatalf("named terminal portal shells = %d, want %d before Sync", got, portals)
	}

	// Process an actual idle Sync after the whole Bind/Execute pipeline. The
	// message-loop connection-close defer is harmless here because Sync itself
	// must remove the lightweight shells first.
	_ = runPipeline(t, c, out, pgFrame(wire.MsgSync, nil))
	if got := len(c.portals); got != 0 {
		t.Fatalf("idle Sync left %d portal shells, want 0", got)
	}
}

func TestPortalLifecycleTransactionEndAndSimpleQueryDropPortals(t *testing.T) {
	for _, command := range []string{"COMMIT", "ROLLBACK"} {
		command := command
		t.Run(command, func(t *testing.T) {
			c, _, cleanup := newPortalLifecycleConn(t, nil)
			defer cleanup()
			c.txStatus = txStatusTransaction
			c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
			p := bindPortalForLifecycle(t, c, "p", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}})

			c.updateTxStatus(command)
			if len(c.portals) != 0 {
				t.Fatalf("successful %s must drop all portals, got %d", command, len(c.portals))
			}
			requireReleasedPortalPayload(t, p)
		})
	}

	t.Run("simple query", func(t *testing.T) {
		c, _, cleanup := newPortalLifecycleConn(t, &lifecycleExecutor{execResult: emptyExecResult{}})
		defer cleanup()
		c.stmts["insert"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
		p := bindPortalForLifecycle(t, c, "", "insert", nil, []portalLifecycleBindValue{{data: []byte("value")}})

		if err := c.handleQuery([]byte("UPDATE t SET v = 1\x00")); err != nil {
			t.Fatalf("handle simple query: %v", err)
		}
		if _, ok := c.portals[""]; ok {
			t.Fatal("simple Query must destroy the unnamed portal")
		}
		requireReleasedPortalPayload(t, p)
	})
}

func TestPortalLifecycleBindPreservesNullEmptyAndFormatConventions(t *testing.T) {
	c, _, cleanup := newPortalLifecycleConn(t, nil)
	defer cleanup()
	c.stmts["values"] = &preparedStmt{
		query:          "SELECT $1, $2, $3, $4",
		convertedQuery: "SELECT ?, ?, ?, ?",
		paramTypes:     []int32{OidText, OidText, OidInt4, 0},
		numParams:      4,
	}

	p := bindPortalForLifecycle(t, c, "formats", "values", []int16{0, 0, 1, 1}, []portalLifecycleBindValue{
		{null: true},
		{data: []byte{}},
		{data: []byte{0, 0, 0, 42}},
		{data: []byte("unknown-as-text")},
	})
	args, err := p.decodeParams()
	if err != nil {
		t.Fatalf("decode params: %v", err)
	}
	if args[0] != nil {
		t.Fatalf("NULL parameter decoded as %#v, want nil", args[0])
	}
	if got, ok := args[1].(string); !ok || got != "" {
		t.Fatalf("empty text parameter decoded as %#v, want empty string", args[1])
	}
	if got, ok := args[2].(int32); !ok || got != 42 {
		t.Fatalf("binary int4 decoded as %#v, want int32(42)", args[2])
	}
	if got, ok := args[3].(string); !ok || got != "unknown-as-text" {
		t.Fatalf("unknown OID with binary format decoded as %#v, want text", args[3])
	}

	interpolated, err := flightclient.InterpolateBoundArgs("SELECT ?, ?, ?, ?", p)
	if err != nil {
		t.Fatalf("direct Flight interpolation: %v", err)
	}
	const want = "SELECT NULL, '', 42, 'unknown-as-text'"
	if interpolated != want {
		t.Fatalf("direct Flight interpolation = %q, want %q", interpolated, want)
	}
}

// BenchmarkHandleBind27000Params models a 1,000-row x 27-column bulk INSERT.
// The production implementation should keep allocation growth close to the
// final contiguous Bind buffer plus compact descriptors, not one heap object
// per parameter value.
func BenchmarkHandleBind27000Params(b *testing.B) {
	const (
		rows    = 1000
		columns = 27
	)
	values := make([]portalLifecycleBindValue, rows*columns)
	for i := range values {
		values[i] = portalLifecycleBindValue{data: []byte("value")}
	}
	body := portalLifecycleBindBody("", "bulk", nil, values, nil)
	stmt := &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: len(values)}

	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := &clientConn{
			writer:  bufio.NewWriter(io.Discard),
			stmts:   map[string]*preparedStmt{"bulk": stmt},
			portals: make(map[string]*portal),
		}
		c.handleBind(body)
		if len(c.portals) != 1 {
			b.Fatalf("Bind did not install a portal")
		}
	}
}

// BenchmarkBindToFlightInterpolation27000Params covers the production remote
// path after framing: compact Bind parsing plus direct text literal append into
// the final Flight SQL buffer. It intentionally does not call decodeParams for
// text values—the compact appender replaces those transient strings.
func BenchmarkBindToFlightInterpolation27000Params(b *testing.B) {
	const paramCount = 27_000
	values := make([]portalLifecycleBindValue, paramCount)
	placeholders := make([]byte, 0, paramCount*2)
	for i := range values {
		values[i] = portalLifecycleBindValue{data: []byte("value")}
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
	}
	body := portalLifecycleBindBody("", "bulk", nil, values, nil)
	stmt := &preparedStmt{
		query:          "INSERT INTO t VALUES (" + string(placeholders) + ")",
		convertedQuery: "INSERT INTO t VALUES (" + string(placeholders) + ")",
		numParams:      paramCount,
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := &clientConn{
			writer:  bufio.NewWriter(io.Discard),
			stmts:   map[string]*preparedStmt{"bulk": stmt},
			portals: make(map[string]*portal),
		}
		c.handleBind(body)
		p := c.portals[""]
		if p == nil {
			b.Fatal("Bind did not install a portal")
		}
		if _, err := flightclient.InterpolateBoundArgs(stmt.convertedQuery, p); err != nil {
			b.Fatal(err)
		}
	}
}
