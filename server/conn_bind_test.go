package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"testing"
)

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
	if p.retainedPayloadBytes() == 0 || len(p.params) != 1 || string(p.paramValue(0)) != "abc" {
		t.Fatalf("unexpected compact parameter view: payload=%d params=%v value=%q", p.retainedPayloadBytes(), p.params, p.paramValue(0))
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
