package server

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/posthog/duckgres/server/flightclient"
	"github.com/posthog/duckgres/server/sqlcore"
	"github.com/posthog/duckgres/server/wire"
)

type recordingCopyFromStdinExecutor struct {
	*LocalExecutor

	attempts                int
	calls                   int
	copySQL                 string
	payload                 []byte
	rows                    int64
	copyErr                 error
	readErr                 error
	binaryDatabaseTypeNames []string
}

type exactTypeRowSet struct {
	RowSet
}

func (r *exactTypeRowSet) ColumnTypes() ([]ColumnTyper, error) {
	columnTypes, err := r.RowSet.ColumnTypes()
	if err != nil {
		return nil, err
	}
	exactTypes := make([]ColumnTyper, len(columnTypes))
	for i, columnType := range columnTypes {
		typeName := columnType.DatabaseTypeName()
		exactTypes[i] = exactStaticColumnType{
			reported: typeName,
			exact:    typeName,
			present:  true,
		}
	}
	return exactTypes, nil
}

func (e *recordingCopyFromStdinExecutor) Query(query string, args ...any) (RowSet, error) {
	rowSet, err := e.LocalExecutor.Query(query, args...)
	if err != nil {
		return nil, err
	}
	return &exactTypeRowSet{RowSet: rowSet}, nil
}

type exactStaticColumnType struct {
	reported string
	exact    string
	present  bool
}

func (c exactStaticColumnType) DatabaseTypeName() string { return c.reported }

func (c exactStaticColumnType) ExactDatabaseTypeName() (string, bool) {
	return c.exact, c.present
}

func (e *recordingCopyFromStdinExecutor) CopyFromStdin(
	_ context.Context,
	request sqlcore.CopyFromStdinRequest,
	r io.Reader,
) (int64, error) {
	e.attempts++
	payload, err := io.ReadAll(r)
	if err != nil {
		e.readErr = err
		return 0, err
	}
	e.calls++
	e.copySQL = request.SQLTemplate
	e.binaryDatabaseTypeNames = append([]string(nil), request.PostgresBinaryDatabaseTypeNames...)
	e.payload = payload
	return e.rows, e.copyErr
}

func postgresBinaryCopyOneField(field []byte) (prefix, suffix, payload []byte) {
	var beforeField bytes.Buffer
	beforeField.Write([]byte{'P', 'G', 'C', 'O', 'P', 'Y', '\n', 0xff, '\r', '\n', 0})
	_ = binary.Write(&beforeField, binary.BigEndian, uint32(0)) // flags
	_ = binary.Write(&beforeField, binary.BigEndian, uint32(0)) // header extension length
	_ = binary.Write(&beforeField, binary.BigEndian, int16(1))
	_ = binary.Write(&beforeField, binary.BigEndian, int32(len(field)))

	var afterField bytes.Buffer
	_ = binary.Write(&afterField, binary.BigEndian, int16(-1))

	prefix = beforeField.Bytes()
	suffix = afterField.Bytes()
	payload = append(append(append([]byte{}, prefix...), field...), suffix...)
	return prefix, suffix, payload
}

func TestBuildDuckDBBinaryInsertSQLUsesNativeReader(t *testing.T) {
	got, databaseTypeNames, ok := buildDuckDBBinaryInsertSQL(
		"events",
		"(id)",
		flightclient.CopyFromStdinPathPlaceholder,
		[]ColumnTyper{exactStaticColumnType{reported: "BIGINT", exact: "BIGINT", present: true}},
	)
	if !ok {
		t.Fatal("buildDuckDBBinaryInsertSQL() rejected BIGINT")
	}
	want := "INSERT INTO events (id) SELECT * FROM read_postgres_binary('" +
		flightclient.CopyFromStdinPathPlaceholder +
		"', columns = {c0: 'BIGINT'}, buffer_size = " +
		flightclient.CopyFromStdinSizePlaceholder + ")"
	if got != want {
		t.Fatalf("buildDuckDBBinaryInsertSQL() =\n  %q\nwant:\n  %q", got, want)
	}
	if got, want := strings.Join(databaseTypeNames, ","), "BIGINT"; got != want {
		t.Fatalf("database type names = %q, want %q", got, want)
	}
}

func TestBuildDuckDBBinaryInsertSQLRequiresLosslessDatabaseTypes(t *testing.T) {
	tests := []struct {
		name       string
		columnType ColumnTyper
		wantType   string
		supported  bool
	}{
		{
			name:       "metadata absent fails closed",
			columnType: staticColumnType("BIGINT"),
		},
		{
			name: "UUID hidden behind Arrow string remains on legacy path",
			columnType: exactStaticColumnType{
				reported: "VARCHAR", exact: "UUID", present: true,
			},
		},
		{
			name: "HUGEINT hidden behind Arrow decimal remains on legacy path",
			columnType: exactStaticColumnType{
				reported: "DECIMAL(38,0)", exact: "HUGEINT", present: true,
			},
		},
		{
			name: "remote BIGINT is compatible",
			columnType: exactStaticColumnType{
				reported: "BIGINT", exact: "BIGINT", present: true,
			},
			wantType: "BIGINT", supported: true,
		},
		{
			name: "remote TEXT uses the compatible VARCHAR reader",
			columnType: exactStaticColumnType{
				reported: "VARCHAR", exact: "TEXT", present: true,
			},
			wantType: "VARCHAR", supported: true,
		},
		{
			name: "remote NUMERIC keeps precision and scale",
			columnType: exactStaticColumnType{
				reported: "DECIMAL(18,4)", exact: "DECIMAL(18,4)", present: true,
			},
			wantType: "DECIMAL(18,4)", supported: true,
		},
		{
			name: "remote BOOLEAN is compatible",
			columnType: exactStaticColumnType{
				reported: "BOOLEAN", exact: "BOOLEAN", present: true,
			},
			wantType: "BOOLEAN", supported: true,
		},
		{
			name: "remote DATE is compatible",
			columnType: exactStaticColumnType{
				reported: "DATE", exact: "DATE", present: true,
			},
			wantType: "DATE", supported: true,
		},
		{
			name: "remote TIMESTAMP is compatible",
			columnType: exactStaticColumnType{
				reported: "TIMESTAMP", exact: "TIMESTAMP", present: true,
			},
			wantType: "TIMESTAMP", supported: true,
		},
		{
			name: "remote TIMESTAMPTZ is compatible",
			columnType: exactStaticColumnType{
				reported: "TIMESTAMPTZ", exact: "TIMESTAMP WITH TIME ZONE", present: true,
			},
			wantType: "TIMESTAMPTZ", supported: true,
		},
		{
			name: "remote BLOB is compatible",
			columnType: exactStaticColumnType{
				reported: "BLOB", exact: "BLOB", present: true,
			},
			wantType: "BLOB", supported: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, databaseTypeNames, supported := buildDuckDBBinaryInsertSQL(
				"events",
				"(value)",
				flightclient.CopyFromStdinPathPlaceholder,
				[]ColumnTyper{tt.columnType},
			)
			if supported != tt.supported {
				t.Fatalf("supported = %v, want %v; SQL = %q", supported, tt.supported, got)
			}
			if !tt.supported {
				if got != "" {
					t.Fatalf("unsupported type produced SQL %q", got)
				}
				return
			}
			if got, want := strings.Join(databaseTypeNames, ","), tt.columnType.(exactStaticColumnType).exact; got != want {
				t.Fatalf("database type names = %q, want %q", got, want)
			}
			wantFragment := "columns = {c0: '" + tt.wantType + "'}"
			if !strings.Contains(got, wantFragment) {
				t.Fatalf("SQL %q does not contain %q", got, wantFragment)
			}
		})
	}
}

func TestPostgresBinaryReaderType(t *testing.T) {
	tests := []struct {
		name      string
		typeName  string
		want      string
		supported bool
	}{
		{name: "big integer has a stable eight byte encoding", typeName: "BIGINT", want: "BIGINT", supported: true},
		{name: "double has a stable eight byte encoding", typeName: "DOUBLE", want: "DOUBLE", supported: true},
		{name: "narrow integer uses legacy width tolerant decoder", typeName: "INTEGER", supported: false},
		{name: "unsigned integer uses legacy width tolerant decoder", typeName: "UINTEGER", supported: false},
		{name: "huge integer uses legacy width tolerant decoder", typeName: "HUGEINT", supported: false},
		{name: "float uses legacy width tolerant decoder", typeName: "FLOAT", supported: false},
		{name: "time uses legacy encoder specific decoder", typeName: "TIME", supported: false},
		{name: "interval uses legacy encoder specific decoder", typeName: "INTERVAL", supported: false},
		{name: "UUID uses legacy encoder specific decoder", typeName: "UUID", supported: false},
		{name: "decimal keeps precision and scale", typeName: "DECIMAL(18,4)", want: "DECIMAL(18,4)", supported: true},
		{name: "array uses legacy fallback", typeName: "UTINYINT[]", supported: false},
		{name: "nested type uses legacy fallback", typeName: "MAP(VARCHAR, INTEGER)", supported: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, supported := postgresBinaryReaderType(tt.typeName)
			if supported != tt.supported {
				t.Fatalf("postgresBinaryReaderType(%q) supported = %v, want %v", tt.typeName, supported, tt.supported)
			}
			if got != tt.want {
				t.Fatalf("postgresBinaryReaderType(%q) = %q, want %q", tt.typeName, got, tt.want)
			}
		})
	}
}

func TestBuildDuckDBBinaryInsertSQLRejectsUnsupportedType(t *testing.T) {
	if got, databaseTypeNames, ok := buildDuckDBBinaryInsertSQL(
		"events",
		"(properties)",
		flightclient.CopyFromStdinPathPlaceholder,
		[]ColumnTyper{staticColumnType("MAP(VARCHAR, INTEGER)")},
	); ok || got != "" || databaseTypeNames != nil {
		t.Fatalf("buildDuckDBBinaryInsertSQL() = (%q, %v, %v), want legacy fallback", got, databaseTypeNames, ok)
	}
}

func TestHandleCopyInBinaryRemoteStreamsRawPayload(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE events (payload BLOB)"); err != nil {
		t.Fatal(err)
	}

	exec := &recordingCopyFromStdinExecutor{
		LocalExecutor: NewLocalExecutor(db),
		rows:          1,
	}
	// A CopyData frame equal to `\.` is a text-mode end marker, but in binary
	// mode it is ordinary field data and must not be removed by the wire reader.
	prefix, suffix, payload := postgresBinaryCopyOneField([]byte(`\.`))

	// Split one PostgreSQL binary stream over multiple CopyData messages and
	// deliberately isolate the `\.` field value in its own frame.
	var input bytes.Buffer
	writePGMessage(&input, wire.MsgCopyData, prefix)
	writePGMessage(&input, wire.MsgCopyData, []byte(`\.`))
	writePGMessage(&input, wire.MsgCopyData, suffix)
	writePGMessage(&input, wire.MsgCopyDone, nil)

	var output bytes.Buffer
	c := &clientConn{
		reader:   bufio.NewReader(&input),
		writer:   bufio.NewWriter(&output),
		executor: exec,
		server:   &Server{},
		txStatus: txStatusIdle,
		ctx:      context.Background(),
	}

	query := "COPY public.events (payload) FROM STDIN (FORMAT binary)"
	if err := c.handleCopyIn(query, "COPY PUBLIC.EVENTS (PAYLOAD) FROM STDIN (FORMAT BINARY)"); err != nil {
		t.Fatalf("handleCopyIn: %v", err)
	}

	if exec.calls != 1 {
		t.Fatalf("CopyFromStdin calls = %d, want 1", exec.calls)
	}
	wantSQL := "INSERT INTO events (payload) SELECT * FROM read_postgres_binary('" +
		flightclient.CopyFromStdinPathPlaceholder +
		"', columns = {c0: 'BLOB'}, buffer_size = " +
		flightclient.CopyFromStdinSizePlaceholder + ")"
	if exec.copySQL != wantSQL {
		t.Errorf("CopyFromStdin SQL = %q, want %q", exec.copySQL, wantSQL)
	}
	if got, want := strings.Join(exec.binaryDatabaseTypeNames, ","), "BLOB"; got != want {
		t.Errorf("CopyFromStdin database types = %q, want %q", got, want)
	}
	if !bytes.Equal(exec.payload, payload) {
		t.Errorf("CopyFromStdin payload changed: got %x, want %x", exec.payload, payload)
	}
}

func TestHandleCopyInBinaryRemoteKeepsNativeLoadTelemetryGenerated(t *testing.T) {
	logs := captureCopyFallbackTelemetryLogs(t)
	tracker := installCopyFallbackTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE native_copy_target (payload BLOB)"); err != nil {
		t.Fatal(err)
	}

	exec := &recordingCopyFromStdinExecutor{
		LocalExecutor: NewLocalExecutor(db),
		rows:          1,
	}
	_, _, payload := postgresBinaryCopyOneField([]byte("native-load-value"))
	var input bytes.Buffer
	writePGMessage(&input, wire.MsgCopyData, payload)
	writePGMessage(&input, wire.MsgCopyDone, nil)
	c.reader = bufio.NewReader(&input)
	c.writer = bufio.NewWriter(&bytes.Buffer{})
	c.executor = exec
	sink := &copyFallbackTelemetryQueryLogSink{}
	c.server.queryLogSink = sink

	const query = "COPY native_copy_target (payload) FROM STDIN (FORMAT binary)"
	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}

	output := logs.String()
	clientStarts := assertCopyFallbackTelemetryLogCount(t, output, "Client query started.", 1)
	clientFinishes := assertCopyFallbackTelemetryLogCount(t, output, "Client query finished.", 1)
	assertCopyFallbackTelemetryAttrs(t, clientStarts[0], "scope=client", "protocol=simple")
	assertCopyFallbackTelemetryAttrs(t, clientFinishes[0], "scope=client", "outcome=success")

	workerStarts := assertCopyFallbackTelemetryLogCount(t, output, "Worker statement started.", 2)
	workerFinishes := assertCopyFallbackTelemetryLogCount(t, output, "Worker statement finished.", 2)
	for _, line := range append(workerStarts, workerFinishes...) {
		for _, forbidden := range []string{
			"query=",
			"read_postgres_binary",
			flightclient.CopyFromStdinPathPlaceholder,
			flightclient.CopyFromStdinSizePlaceholder,
		} {
			if strings.Contains(line, forbidden) {
				t.Errorf("native COPY worker event leaked generated request SQL %q:\n%s", forbidden, line)
			}
		}
	}
	if got := len(copyFallbackTelemetryLogLinesWithAttr(output, "Worker statement started.", "operation=copy_schema_probe")); got != 1 {
		t.Errorf("native COPY schema-probe worker starts = %d, want 1", got)
	}
	if got := len(copyFallbackTelemetryLogLinesWithAttr(output, "Worker statement started.", "operation=copy_from_stdin_stream")); got != 1 {
		t.Errorf("native COPY stream worker starts = %d, want 1", got)
	}
	if got := copyFallbackTelemetryEventCount(tracker, "query_initiated"); got != 1 {
		t.Errorf("query_initiated analytics events = %d, want 1", got)
	}
	if got := copyFallbackTelemetryEventCount(tracker, "query_failed"); got != 0 {
		t.Errorf("query_failed analytics events = %d, want 0", got)
	}
	if got := len(sink.entries); got != 1 {
		t.Fatalf("durable query-log entries = %d, want 1", got)
	}
	if entry := sink.entries[0]; entry.Query != query || entry.Protocol != "simple" || entry.ExceptionCode != "" {
		t.Errorf("durable query-log entry = %#v, want one successful outer COPY", entry)
	}
}

func TestHandleCopyInBinaryRemoteLoadFailureDoesNotLeakGeneratedDetails(t *testing.T) {
	logs := captureCopyFallbackTelemetryLogs(t)
	tracker := installCopyFallbackTelemetryTracker(t)
	c, cleanup := newLifecycleClientConn(t)
	defer cleanup()

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE native_copy_failure_target (payload BLOB)"); err != nil {
		t.Fatal(err)
	}

	const leakMarker = "generated INSERT private-value /tmp/duckgres-worker-copy-secret.copy"
	exec := &recordingCopyFromStdinExecutor{
		LocalExecutor: NewLocalExecutor(db),
		copyErr:       errors.New(leakMarker),
	}
	_, _, payload := postgresBinaryCopyOneField([]byte("private-value"))
	var input bytes.Buffer
	writePGMessage(&input, wire.MsgCopyData, payload)
	writePGMessage(&input, wire.MsgCopyDone, nil)
	var output bytes.Buffer
	c.reader = bufio.NewReader(&input)
	c.writer = bufio.NewWriter(&output)
	c.executor = exec
	sink := &copyFallbackTelemetryQueryLogSink{}
	c.server.queryLogSink = sink

	const query = "COPY native_copy_failure_target (payload) FROM STDIN (FORMAT binary)"
	if err := c.handleQuery([]byte(query + "\x00")); err != nil {
		t.Fatalf("handleQuery: %v", err)
	}
	_ = c.writer.Flush()

	if got := extractErrorResponseField(t, output.Bytes(), 'C'); got != "22P02" {
		t.Fatalf("ErrorResponse SQLSTATE = %q, want 22P02", got)
	}
	if got := extractErrorResponseField(t, output.Bytes(), 'M'); got != "COPY failed: worker load failed" {
		t.Fatalf("ErrorResponse message = %q, want fixed generated COPY failure", got)
	}
	for surface, content := range map[string]string{
		"pgwire":    output.String(),
		"logs":      logs.String(),
		"durable":   fmt.Sprint(sink.entries),
		"analytics": fmt.Sprint(tracker.events),
	} {
		for _, forbidden := range []string{leakMarker, "private-value", "/tmp/duckgres-worker-copy-secret.copy"} {
			if strings.Contains(content, forbidden) {
				t.Errorf("%s leaked %q: %s", surface, forbidden, content)
			}
		}
	}
	if got := copyFallbackTelemetryEventCount(tracker, "query_initiated"); got != 1 {
		t.Errorf("query_initiated analytics events = %d, want 1", got)
	}
	if got := copyFallbackTelemetryEventCount(tracker, "query_failed"); got != 1 {
		t.Errorf("query_failed analytics events = %d, want 1", got)
	}
	if got := len(sink.entries); got != 1 {
		t.Fatalf("durable query-log entries = %d, want 1", got)
	}
	if entry := sink.entries[0]; entry.ExceptionCode != "22P02" || strings.Contains(entry.Exception, leakMarker) {
		t.Errorf("durable query-log entry = %#v, want sanitized 22P02 failure", entry)
	}
}

func TestHandleCopyInBinaryRemoteStreamsProtocolCompletedPayloadWithoutTrailer(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE events (payload BLOB)"); err != nil {
		t.Fatal(err)
	}

	exec := &recordingCopyFromStdinExecutor{
		LocalExecutor: NewLocalExecutor(db),
		rows:          1,
	}
	field := []byte("protocol-completed")
	prefix, _, canonical := postgresBinaryCopyOneField(field)
	trailerless := canonical[:len(canonical)-2]

	var input bytes.Buffer
	writePGMessage(&input, wire.MsgCopyData, prefix)
	writePGMessage(&input, wire.MsgCopyData, field)
	writePGMessage(&input, wire.MsgCopyDone, nil)

	var output bytes.Buffer
	c := &clientConn{
		reader:   bufio.NewReader(&input),
		writer:   bufio.NewWriter(&output),
		executor: exec,
		server:   &Server{},
		txStatus: txStatusIdle,
		ctx:      context.Background(),
	}

	query := "COPY public.events (payload) FROM STDIN (FORMAT binary)"
	if err := c.handleCopyIn(query, "COPY PUBLIC.EVENTS (PAYLOAD) FROM STDIN (FORMAT BINARY)"); err != nil {
		t.Fatalf("handleCopyIn: %v", err)
	}
	if exec.calls != 1 {
		t.Fatalf("CopyFromStdin calls = %d, want 1", exec.calls)
	}
	if !bytes.Equal(exec.payload, trailerless) {
		t.Fatalf("CopyFromStdin payload changed: got %x, want trailerless %x", exec.payload, trailerless)
	}
}

func TestHandleCopyInBinaryRemoteAbortsOnTransportEOFBeforeCopyDone(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE events (payload BLOB)"); err != nil {
		t.Fatal(err)
	}

	exec := &recordingCopyFromStdinExecutor{
		LocalExecutor: NewLocalExecutor(db),
		rows:          1,
	}
	_, _, payload := postgresBinaryCopyOneField([]byte("complete binary file"))

	// The PostgreSQL binary trailer is present, but the frontend transport ends
	// without the CopyDone message that successfully terminates COPY-in mode.
	var input bytes.Buffer
	writePGMessage(&input, wire.MsgCopyData, payload)

	var output bytes.Buffer
	c := &clientConn{
		reader:   bufio.NewReader(&input),
		writer:   bufio.NewWriter(&output),
		executor: exec,
		server:   &Server{},
		txStatus: txStatusIdle,
		ctx:      context.Background(),
	}

	query := "COPY public.events (payload) FROM STDIN (FORMAT binary)"
	if err := c.handleCopyIn(query, "COPY PUBLIC.EVENTS (PAYLOAD) FROM STDIN (FORMAT BINARY)"); err != nil {
		t.Fatalf("handleCopyIn: %v", err)
	}
	_ = c.writer.Flush()

	if exec.calls != 0 {
		t.Fatalf("CopyFromStdin completed %d times without frontend CopyDone, want 0", exec.calls)
	}
	if got, want := frameTypes(scanWireFrames(t, output.Bytes())), "GEZ"; got != want {
		t.Fatalf("response frame sequence = %q, want %q", got, want)
	}
}

func TestHandleCopyInBinaryRemotePropagatesFrontendCopyFail(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE events (payload BLOB)"); err != nil {
		t.Fatal(err)
	}

	exec := &recordingCopyFromStdinExecutor{
		LocalExecutor: NewLocalExecutor(db),
		rows:          1,
	}
	prefix, _, _ := postgresBinaryCopyOneField([]byte("partial payload"))

	var input bytes.Buffer
	writePGMessage(&input, wire.MsgCopyData, prefix)
	writePGMessage(&input, wire.MsgCopyFail, []byte("client stopped producing rows\x00"))

	var output bytes.Buffer
	c := &clientConn{
		reader:   bufio.NewReader(&input),
		writer:   bufio.NewWriter(&output),
		executor: exec,
		server:   &Server{},
		txStatus: txStatusTransaction,
		ctx:      context.Background(),
	}

	query := "COPY public.events (payload) FROM STDIN (FORMAT binary)"
	if err := c.handleCopyIn(query, "COPY PUBLIC.EVENTS (PAYLOAD) FROM STDIN (FORMAT BINARY)"); err != nil {
		t.Fatalf("handleCopyIn: %v", err)
	}
	_ = c.writer.Flush()

	if exec.attempts != 1 {
		t.Fatalf("CopyFromStdin attempts = %d, want 1", exec.attempts)
	}
	if exec.calls != 0 {
		t.Fatalf("CopyFromStdin completed %d times after CopyFail, want 0", exec.calls)
	}
	if !errors.Is(exec.readErr, errCopyAborted) {
		t.Fatalf("CopyFromStdin read error = %v, want errCopyAborted", exec.readErr)
	}
	if c.txStatus != txStatusError {
		t.Fatalf("transaction status = %c, want failed transaction status %c", c.txStatus, txStatusError)
	}
	if got, want := frameTypes(scanWireFrames(t, output.Bytes())), "GEZ"; got != want {
		t.Fatalf("response frame sequence = %q, want %q", got, want)
	}
	if got, want := extractErrorResponseField(t, output.Bytes(), 'C'), "57014"; got != want {
		t.Fatalf("ErrorResponse SQLSTATE = %q, want %q", got, want)
	}
	if got := extractErrorResponseField(t, output.Bytes(), 'M'); !strings.Contains(got, "client stopped producing rows") {
		t.Fatalf("ErrorResponse message = %q, want client CopyFail reason", got)
	}
}
