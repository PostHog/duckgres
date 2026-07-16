package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/posthog/duckgres/server/flightclient"
)

type bindTestValue struct {
	null bool
	data []byte
}

var bindTestInterpolatedSQLSink string

// buildBindBody constructs a Bind message body (without the 'B' type byte and
// length header): portal name, statement name, no format codes, one parameter
// with the given declared length and actual data, no result format codes.
func buildBindBody(portal, stmt string, declaredLen int32, data []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString(portal)
	buf.WriteByte(0)
	buf.WriteString(stmt)
	buf.WriteByte(0)
	_ = binary.Write(&buf, binary.BigEndian, int16(0)) // no param format codes
	_ = binary.Write(&buf, binary.BigEndian, int16(1)) // one parameter
	_ = binary.Write(&buf, binary.BigEndian, declaredLen)
	buf.Write(data)
	_ = binary.Write(&buf, binary.BigEndian, int16(0)) // no result format codes
	return buf.Bytes()
}

// bindMessageBody builds a Bind message body (without the type byte and
// length prefix) for portal/statement names with raw trailing fields.
func bindMessageBody(portal, stmt string, fields ...any) []byte {
	var buf bytes.Buffer
	buf.WriteString(portal)
	buf.WriteByte(0)
	buf.WriteString(stmt)
	buf.WriteByte(0)
	for _, f := range fields {
		_ = binary.Write(&buf, binary.BigEndian, f)
	}
	return buf.Bytes()
}

func bindTestBody(portal, stmt string, paramFormats []int16, values []bindTestValue, resultFormats []int16) []byte {
	var body bytes.Buffer
	body.WriteString(portal)
	body.WriteByte(0)
	body.WriteString(stmt)
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

func executeTestBody(portal string) []byte {
	return executeTestBodyWithMaxRows(portal, 0)
}

func executeTestBodyWithMaxRows(portal string, maxRows int32) []byte {
	var body bytes.Buffer
	body.WriteString(portal)
	body.WriteByte(0)
	_ = binary.Write(&body, binary.BigEndian, maxRows)
	return body.Bytes()
}

func parseTestBody(stmt, query string, paramTypes []int32) []byte {
	var body bytes.Buffer
	body.WriteString(stmt)
	body.WriteByte(0)
	body.WriteString(query)
	body.WriteByte(0)
	_ = binary.Write(&body, binary.BigEndian, int16(len(paramTypes)))
	for _, oid := range paramTypes {
		_ = binary.Write(&body, binary.BigEndian, oid)
	}
	return body.Bytes()
}

func bindPortalForTest(t *testing.T, c *clientConn, portal, stmt string, paramFormats []int16, values []bindTestValue, resultFormats []int16) *portal {
	t.Helper()
	c.handleBind(bindTestBody(portal, stmt, paramFormats, values, resultFormats))
	if err := c.writer.Flush(); err != nil {
		t.Fatalf("flush Bind: %v", err)
	}
	p := c.portals[portal]
	if p == nil {
		t.Fatalf("Bind did not install portal %q", portal)
	}
	return p
}

func requireBindPayload(t *testing.T, p *portal) {
	t.Helper()
	if p == nil || p.payloadReleased || p.bindBody == nil {
		t.Fatalf("portal did not retain its Bind frame: portal=%p released=%v body=%v", p, p != nil && p.payloadReleased, p != nil && p.bindBody != nil)
	}
}

func requireReleasedBindPayload(t *testing.T, p *portal) {
	t.Helper()
	if p == nil || !p.payloadReleased || p.bindBody != nil || p.params != nil || p.paramFormats != nil || p.resultFormats != nil {
		t.Fatalf("portal Bind payload was not released: portal=%p released=%v body=%v params=%v paramFormats=%v resultFormats=%v", p, p != nil && p.payloadReleased, p != nil && p.bindBody != nil, p != nil && p.params != nil, p != nil && p.paramFormats != nil, p != nil && p.resultFormats != nil)
	}
}

func newBindTestConn(out *bytes.Buffer) *clientConn {
	return &clientConn{
		writer: bufio.NewWriter(out),
		stmts: map[string]*preparedStmt{
			"s1": {query: "SELECT $1", convertedQuery: "SELECT $1", numParams: 1},
		},
		portals: map[string]*portal{},
	}
}

// Regression test for #717: a Bind parameter length is client-controlled and
// must be bounded by the remaining message bytes before allocating — a client
// could otherwise reserve multi-GiB per parameter.
func TestHandleBindRejectsOversizedParamLength(t *testing.T) {
	for _, tc := range []struct {
		name        string
		declaredLen int32
		data        []byte
	}{
		{"max int32 with no data", 0x7FFFFFFF, nil},
		{"slightly exceeds remaining bytes", 8, []byte("abc")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			c := newBindTestConn(&out)

			c.handleBind(buildBindBody("p1", "s1", tc.declaredLen, tc.data))

			if !bytes.Contains(out.Bytes(), []byte("08P01")) {
				t.Fatalf("expected 08P01 protocol_violation error, got: %q", out.Bytes())
			}
			if !bytes.Contains(out.Bytes(), []byte("exceeds remaining message size")) {
				t.Fatalf("expected oversized-length rejection before allocation, got: %q", out.Bytes())
			}
			if _, ok := c.portals["p1"]; ok {
				t.Fatal("portal must not be created for a rejected Bind")
			}
		})
	}
}

// Regression test for #720: malformed counts/lengths in a Bind message used
// to reach make() unvalidated and panic with "makeslice: len out of range"
// (recovered per-connection). They must instead produce a clean 08P01
// protocol_violation error.
func TestHandleBindMalformedFieldsReturnProtocolViolation(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{
			name: "negative param format count",
			body: bindMessageBody("p1", "s1", int16(-1)),
		},
		{
			name: "negative param count",
			body: bindMessageBody("p1", "s1", int16(0), int16(-1)),
		},
		{
			name: "negative non-NULL param value length",
			body: bindMessageBody("p1", "s1", int16(0), int16(1), int32(-2)),
		},
		{
			name: "negative result format count",
			body: bindMessageBody("p1", "s1", int16(0), int16(0), int16(-1)),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			c := newBindTestConn(&out)

			c.handleBind(tc.body) // must not panic

			if !bytes.Contains(out.Bytes(), []byte("08P01")) {
				t.Fatalf("expected 08P01 ErrorResponse, got output: %q", out.Bytes())
			}
			if _, ok := c.portals["p1"]; ok {
				t.Fatal("portal must not be created from a malformed Bind message")
			}
		})
	}
}

func TestHandleBindAcceptsValidParam(t *testing.T) {
	var out bytes.Buffer
	c := newBindTestConn(&out)

	c.handleBind(buildBindBody("p1", "s1", 3, []byte("abc")))
	_ = c.writer.Flush()

	if out.Len() == 0 || out.Bytes()[0] != '2' {
		t.Fatalf("expected BindComplete ('2'), got: %q", out.Bytes())
	}
	p, ok := c.portals["p1"]
	if !ok {
		t.Fatal("expected portal to be created")
	}
	if p.payloadReleased || p.bindBody == nil || len(p.params) != 1 || string(p.paramValue(0)) != "abc" {
		t.Fatalf("unexpected compact parameter view: released=%v body=%v params=%v value=%q", p.payloadReleased, p.bindBody, p.params, p.paramValue(0))
	}
}

// Sanity-check the happy path still binds: one NULL param (length -1),
// default format codes everywhere.
func TestHandleBindValidMessageStillBinds(t *testing.T) {
	var out bytes.Buffer
	c := newBindTestConn(&out)

	c.handleBind(bindMessageBody("p1", "s1", int16(0), int16(1), int32(-1), int16(0)))

	if bytes.Contains(out.Bytes(), []byte("08P01")) {
		t.Fatalf("unexpected protocol error for valid Bind message: %q", out.Bytes())
	}
	p, ok := c.portals["p1"]
	if !ok {
		t.Fatal("expected portal to be created")
	}
	if len(p.params) != 1 || !p.params[0].isNull() || p.paramValue(0) != nil {
		t.Fatalf("expected one NULL compact parameter, got %#v", p.params)
	}
}

// FETCH exposes the columns of an already-declared cursor, so its output
// cardinality is only known once the cursor schema is available during
// Describe or Execute. A client may nevertheless provide one result-format
// code per output column in Bind.
func TestHandleBindDefersCursorFetchResultFormatValidation(t *testing.T) {
	var out bytes.Buffer
	c := newBindTestConn(&out)
	c.stmts["fetch"] = &preparedStmt{
		query:      "FETCH 4 FROM cursor_name",
		cursorOp:   cursorOpFetch,
		cursorName: "cursor_name",
	}

	// No parameters, then two result format codes for the cursor's two
	// columns. Their exact cardinality is validated once the schema is known.
	c.handleBind(bindMessageBody("fetch-portal", "fetch", int16(0), int16(0), int16(2), int16(0), int16(1)))
	_ = c.writer.Flush()

	if bytes.Contains(out.Bytes(), []byte("08P01")) {
		t.Fatalf("cursor FETCH Bind must defer result format validation, got: %q", out.Bytes())
	}
	if _, ok := c.portals["fetch-portal"]; !ok {
		t.Fatal("expected cursor FETCH portal to be created")
	}
}

func TestHandleBindResultFormatCompatibilityForMutations(t *testing.T) {
	value := []bindTestValue{{data: []byte("value")}}
	tests := []struct {
		name      string
		stmt      *preparedStmt
		formats   []int16
		wantError bool
	}{
		{
			name: "explicit returning exact count",
			stmt: &preparedStmt{
				query:          "INSERT INTO t VALUES ($1) RETURNING first, second",
				convertedQuery: "INSERT INTO t VALUES (?) RETURNING first, second",
				numParams:      1,
			},
			formats: []int16{0, 1},
		},
		{
			name: "explicit returning mismatched count",
			stmt: &preparedStmt{
				query:          "INSERT INTO t VALUES ($1) RETURNING first, second",
				convertedQuery: "INSERT INTO t VALUES (?) RETURNING first, second",
				numParams:      1,
			},
			formats:   []int16{0, 1, 0},
			wantError: true,
		},
		{
			name: "wildcard returning deferred",
			stmt: &preparedStmt{
				query:          "INSERT INTO t VALUES ($1) RETURNING *",
				convertedQuery: "INSERT INTO t VALUES (?) RETURNING *",
				numParams:      1,
			},
			formats: []int16{0, 1},
		},
		{
			name: "writable cte deferred",
			stmt: &preparedStmt{
				query:          "WITH changed AS (UPDATE t SET value = $1 RETURNING id) SELECT id FROM changed",
				convertedQuery: "SELECT id FROM changed",
				numParams:      1,
				statements:     []string{"UPDATE t SET value = ?", "SELECT id FROM changed"},
			},
			formats: []int16{0, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			executor := &lifecycleExecutor{}
			c := newBindTestConn(&out)
			c.executor = executor
			c.stmts["mutation"] = tt.stmt
			c.handleBind(bindTestBody("p", "mutation", nil, value, tt.formats))
			if err := c.writer.Flush(); err != nil {
				t.Fatalf("flush Bind: %v", err)
			}
			gotError := bytes.Contains(out.Bytes(), []byte("08P01"))
			if gotError != tt.wantError {
				t.Fatalf("Bind error = %v, want %v; output=%q", gotError, tt.wantError, out.Bytes())
			}
			p := c.portals["p"]
			if tt.wantError {
				if p != nil {
					t.Fatal("mismatched explicit RETURNING formats installed a portal")
				}
			} else {
				if p == nil || p.state != portalStateReady {
					t.Fatalf("compatible Bind portal = %#v, want ready portal", p)
				}
				requireBindPayload(t, p)
			}
			if got := executor.queryCalls.Load(); got != 0 {
				t.Fatalf("Bind invoked Query %d times", got)
			}
			if got := executor.execCalls.Load(); got != 0 {
				t.Fatalf("Bind invoked Exec %d times", got)
			}
		})
	}
}

func TestHandleBindOwnershipCleanup(t *testing.T) {
	var out bytes.Buffer
	c := newBindTestConn(&out)
	c.stmts["s1"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}

	first := bindPortalForTest(t, c, "", "s1", nil, []bindTestValue{{data: []byte("first")}}, nil)
	second := bindPortalForTest(t, c, "", "s1", nil, []bindTestValue{{data: []byte("second")}}, nil)
	if c.portals[""] != second {
		t.Fatal("unnamed Bind did not replace the previous portal")
	}
	requireReleasedBindPayload(t, first)
	requireBindPayload(t, second)

	c.handleClose([]byte{'P', 0})
	if _, ok := c.portals[""]; ok {
		t.Fatal("Close(P) did not remove unnamed portal")
	}
	requireReleasedBindPayload(t, second)

	named := bindPortalForTest(t, c, "named", "s1", nil, []bindTestValue{{data: []byte("value")}}, nil)
	c.stmts["s1"] = &preparedStmt{query: "INSERT INTO t VALUES ($1)", convertedQuery: "INSERT INTO t VALUES (?)", numParams: 1}
	c.handleClose([]byte{'S', 's', '1', 0})
	if _, ok := c.portals["named"]; ok {
		t.Fatal("Close(S) did not remove portal after statement re-Parse")
	}
	requireReleasedBindPayload(t, named)
}

func TestCompactBindPreservesNullEmptyTextAndBinary(t *testing.T) {
	var out bytes.Buffer
	c := newBindTestConn(&out)
	c.stmts["values"] = &preparedStmt{
		query:          "SELECT $1, $2, $3, $4",
		convertedQuery: "SELECT ?, ?, ?, ?",
		paramTypes:     []int32{OidText, OidText, OidInt4, 0},
		numParams:      4,
	}
	p := bindPortalForTest(t, c, "values", "values", []int16{0, 0, 1, 1}, []bindTestValue{
		{null: true},
		{data: []byte{}},
		{data: []byte{0, 0, 0, 42}},
		{data: []byte("unknown-as-text")},
	}, nil)
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
		t.Fatalf("compact Flight interpolation: %v", err)
	}
	if want := "SELECT NULL, '', 42, 'unknown-as-text'"; interpolated != want {
		t.Fatalf("compact Flight interpolation = %q, want %q", interpolated, want)
	}
}

func compactBindBenchmarkInput(count int, withPlaceholders bool) ([]byte, *preparedStmt) {
	values := make([]bindTestValue, count)
	for i := range values {
		values[i] = bindTestValue{data: []byte("value")}
	}
	query := "INSERT INTO t VALUES ($1)"
	if withPlaceholders {
		query = "INSERT INTO t VALUES (" + strings.TrimSuffix(strings.Repeat("?,", count), ",") + ")"
	}
	return bindTestBody("", "bulk", nil, values, nil), &preparedStmt{
		query:          query,
		convertedQuery: query,
		numParams:      count,
	}
}

func TestCompactBind27000ParamAllocationsAreConstant(t *testing.T) {
	measure := func(body []byte, stmt *preparedStmt) float64 {
		return testing.AllocsPerRun(10, func() {
			c := &clientConn{
				writer:  bufio.NewWriter(io.Discard),
				stmts:   map[string]*preparedStmt{"bulk": stmt},
				portals: make(map[string]*portal),
			}
			c.handleBind(body)
			if c.portals[""] == nil {
				t.Fatal("Bind did not install a portal")
			}
		})
	}
	smallBody, smallStmt := compactBindBenchmarkInput(1, false)
	largeBody, largeStmt := compactBindBenchmarkInput(27_000, false)
	small := measure(smallBody, smallStmt)
	large := measure(largeBody, largeStmt)
	if large > small+2 {
		t.Fatalf("27,000 Bind parameters allocated %.0f objects versus %.0f for one parameter; want O(1) growth", large, small)
	}
}

func TestBindToFlightInterpolation27000TextAllocationsAreConstant(t *testing.T) {
	measure := func(body []byte, stmt *preparedStmt) float64 {
		return testing.AllocsPerRun(10, func() {
			c := &clientConn{
				writer:  bufio.NewWriter(io.Discard),
				stmts:   map[string]*preparedStmt{"bulk": stmt},
				portals: make(map[string]*portal),
			}
			c.handleBind(body)
			p := c.portals[""]
			if p == nil {
				t.Fatal("Bind did not install a portal")
			}
			var err error
			bindTestInterpolatedSQLSink, err = flightclient.InterpolateBoundArgs(stmt.convertedQuery, p)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	smallBody, smallStmt := compactBindBenchmarkInput(1, true)
	largeBody, largeStmt := compactBindBenchmarkInput(27_000, true)
	small := measure(smallBody, smallStmt)
	large := measure(largeBody, largeStmt)
	if large > small+2 {
		t.Fatalf("27,000 text Bind parameters allocated %.0f objects versus %.0f for one parameter; want O(1) growth", large, small)
	}
}

func BenchmarkHandleBind27000Params(b *testing.B) {
	body, stmt := compactBindBenchmarkInput(27_000, false)
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
		if c.portals[""] == nil {
			b.Fatal("Bind did not install a portal")
		}
	}
}

func BenchmarkBindToFlightInterpolation27000Params(b *testing.B) {
	body, stmt := compactBindBenchmarkInput(27_000, true)
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
		interpolated, err := flightclient.InterpolateBoundArgs(stmt.convertedQuery, p)
		if err != nil {
			b.Fatal(err)
		}
		bindTestInterpolatedSQLSink = interpolated
	}
}
