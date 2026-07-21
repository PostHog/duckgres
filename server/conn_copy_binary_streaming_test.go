package server

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"io"
	"testing"

	"github.com/posthog/duckgres/server/flightclient"
	"github.com/posthog/duckgres/server/wire"
)

type recordingCopyFromStdinExecutor struct {
	*LocalExecutor

	calls   int
	copySQL string
	payload []byte
	rows    int64
}

func (e *recordingCopyFromStdinExecutor) CopyFromStdin(
	_ context.Context,
	copySQL string,
	r io.Reader,
) (int64, error) {
	payload, err := io.ReadAll(r)
	if err != nil {
		return 0, err
	}
	e.calls++
	e.copySQL = copySQL
	e.payload = payload
	return e.rows, nil
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
	got, ok := buildDuckDBBinaryInsertSQL(
		"events",
		"(id)",
		flightclient.CopyFromStdinPathPlaceholder,
		[]ColumnTyper{staticColumnType("BIGINT")},
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
	if got, ok := buildDuckDBBinaryInsertSQL(
		"events",
		"(properties)",
		flightclient.CopyFromStdinPathPlaceholder,
		[]ColumnTyper{staticColumnType("MAP(VARCHAR, INTEGER)")},
	); ok || got != "" {
		t.Fatalf("buildDuckDBBinaryInsertSQL() = (%q, %v), want legacy fallback", got, ok)
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
	if !bytes.Equal(exec.payload, payload) {
		t.Errorf("CopyFromStdin payload changed: got %x, want %x", exec.payload, payload)
	}
}
