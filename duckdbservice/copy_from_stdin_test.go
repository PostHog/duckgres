package duckdbservice

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/posthog/duckgres/server/flightclient"
	"github.com/posthog/duckgres/server/sqlcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestIsCopyFromStdinDescriptor(t *testing.T) {
	if IsCopyFromStdinDescriptor(nil) {
		t.Error("nil descriptor should not match")
	}
	if IsCopyFromStdinDescriptor(&flight.FlightDescriptor{Type: flight.DescriptorCMD, Cmd: []byte("anything")}) {
		t.Error("DescriptorCMD should not match")
	}
	if IsCopyFromStdinDescriptor(&flight.FlightDescriptor{Type: flight.DescriptorPATH}) {
		t.Error("PATH descriptor with empty path should not match")
	}
	if IsCopyFromStdinDescriptor(&flight.FlightDescriptor{
		Type: flight.DescriptorPATH, Path: []string{"some-other-path"},
	}) {
		t.Error("PATH descriptor with wrong path should not match")
	}
	if !IsCopyFromStdinDescriptor(&flight.FlightDescriptor{
		Type: flight.DescriptorPATH, Path: []string{flightclient.CopyFromStdinDescriptorPath},
	}) {
		t.Error("PATH descriptor with our path SHOULD match")
	}
}

// fakeDoPutStream simulates a DoPut stream by replaying preloaded inbound
// FlightData frames and capturing the server's PutResult sends. Tests run
// single-goroutine, so no synchronization is needed.
type fakeDoPutStream struct {
	grpc.ServerStream

	ctx context.Context

	inbound  []*flight.FlightData
	idx      int
	outbound []*flight.PutResult

	// recvErrAt and recvErr simulate transport-level errors mid-stream
	// (e.g. a cancelled gRPC stream after the CP returned early on
	// CopyFail). When recvErr is non-nil and idx has reached recvErrAt,
	// Recv returns recvErr instead of the next frame / io.EOF.
	recvErrAt int
	recvErr   error
}

func (f *fakeDoPutStream) Context() context.Context     { return f.ctx }
func (f *fakeDoPutStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeDoPutStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeDoPutStream) SetTrailer(metadata.MD)       {}
func (f *fakeDoPutStream) SendMsg(m any) error          { return nil }
func (f *fakeDoPutStream) RecvMsg(m any) error          { return nil }
func (f *fakeDoPutStream) Send(r *pb.PutResult) error {
	f.outbound = append(f.outbound, r)
	return nil
}
func (f *fakeDoPutStream) Recv() (*flight.FlightData, error) {
	if f.recvErr != nil && f.idx >= f.recvErrAt {
		return nil, f.recvErr
	}
	if f.idx >= len(f.inbound) {
		return nil, io.EOF
	}
	fd := f.inbound[f.idx]
	f.idx++
	return fd, nil
}

type blockingDoPutStream struct {
	fakeDoPutStream
	recvStarted chan struct{}
	unblock     chan struct{}
	once        sync.Once
}

func (b *blockingDoPutStream) Recv() (*flight.FlightData, error) {
	b.once.Do(func() { close(b.recvStarted) })
	<-b.unblock
	return nil, status.Error(codes.Canceled, "test stream canceled")
}

func newSessionWithInMemoryDuckDB(t *testing.T) (*Session, func()) {
	t.Helper()
	db, err := sql.Open("duckdb", ":memory:?allow_unsigned_extensions=true")
	if err != nil {
		t.Fatalf("sql.Open duckdb: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		t.Fatalf("db.Conn: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE TABLE t (a INTEGER, b TEXT)"); err != nil {
		_ = conn.Close()
		_ = db.Close()
		t.Fatalf("create table: %v", err)
	}
	s := &Session{
		ID:        "test-session",
		DB:        db,
		Conn:      conn,
		Username:  "tester",
		CreatedAt: time.Now(),
	}
	s.lastUsed.Store(time.Now().UnixNano())
	cleanup := func() {
		_ = conn.Close()
		_ = db.Close()
	}
	return s, cleanup
}

func postgresBinaryCopyFixture(t *testing.T, extension []byte, rows ...[][]byte) []byte {
	t.Helper()
	var payload bytes.Buffer
	payload.Write([]byte("PGCOPY\n\xff\r\n\x00"))
	if err := binary.Write(&payload, binary.BigEndian, uint32(0)); err != nil {
		t.Fatal(err)
	}
	if err := binary.Write(&payload, binary.BigEndian, uint32(len(extension))); err != nil {
		t.Fatal(err)
	}
	payload.Write(extension)
	for _, row := range rows {
		if err := binary.Write(&payload, binary.BigEndian, int16(len(row))); err != nil {
			t.Fatal(err)
		}
		for _, field := range row {
			if field == nil {
				if err := binary.Write(&payload, binary.BigEndian, int32(-1)); err != nil {
					t.Fatal(err)
				}
				continue
			}
			if err := binary.Write(&payload, binary.BigEndian, int32(len(field))); err != nil {
				t.Fatal(err)
			}
			payload.Write(field)
		}
	}
	if err := binary.Write(&payload, binary.BigEndian, int16(-1)); err != nil {
		t.Fatal(err)
	}
	return payload.Bytes()
}

func postgresBinaryInt64Fixture(value int64) []byte {
	var payload [8]byte
	binary.BigEndian.PutUint64(payload[:], uint64(value))
	return payload[:]
}

func postgresBinaryInt32Fixture(value int32) []byte {
	var payload [4]byte
	binary.BigEndian.PutUint32(payload[:], uint32(value))
	return payload[:]
}

func postgresBinaryFloat64Fixture(value float64) []byte {
	return postgresBinaryInt64Fixture(int64(math.Float64bits(value)))
}

func postgresBinaryNumericFixture(t *testing.T, weight int16, sign, scale uint16, digits ...uint16) []byte {
	t.Helper()
	var payload bytes.Buffer
	for _, value := range []any{uint16(len(digits)), weight, sign, scale} {
		if err := binary.Write(&payload, binary.BigEndian, value); err != nil {
			t.Fatal(err)
		}
	}
	for _, digit := range digits {
		if err := binary.Write(&payload, binary.BigEndian, digit); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func runPostgresBinaryCopyFixture(
	t *testing.T,
	handler *FlightSQLHandler,
	ctx context.Context,
	copySQL string,
	databaseTypeNames []string,
	payload []byte,
) int64 {
	t.Helper()
	requestBytes, err := json.Marshal(sqlcore.CopyFromStdinRequest{
		SQLTemplate:                     copySQL,
		PostgresBinaryDatabaseTypeNames: databaseTypeNames,
	})
	if err != nil {
		t.Fatal(err)
	}
	first := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{
				flightclient.CopyFromStdinDescriptorPath,
				flightclient.CopyFromStdinPostgresBinaryPathVersion,
			},
			Cmd: requestBytes,
		},
	}
	stream := &fakeDoPutStream{
		ctx:     ctx,
		inbound: []*flight.FlightData{{DataBody: payload}},
	}
	if err := handler.doCopyFromStdin(ctx, first, stream); err != nil {
		t.Fatalf("doCopyFromStdin: %v", err)
	}
	if len(stream.outbound) != 1 {
		t.Fatalf("PutResult count = %d, want 1", len(stream.outbound))
	}
	var result pb.DoPutUpdateResult
	if err := proto.Unmarshal(stream.outbound[0].AppMetadata, &result); err != nil {
		t.Fatal(err)
	}
	return result.RecordCount
}

func TestDoCopyFromStdinIngestsCSVAndRunsCOPY(t *testing.T) {
	session, cleanup := newSessionWithInMemoryDuckDB(t)
	defer cleanup()

	pool := &SessionPool{sessions: map[string]*Session{session.ID: session}}
	handler := &FlightSQLHandler{pool: pool}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	copySQL := "COPY t (a, b) FROM '" + flightclient.CopyFromStdinPathPlaceholder +
		"' (FORMAT CSV, HEADER false)"

	csv := []byte("1,one\n2,two\n3,three\n")
	first := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{flightclient.CopyFromStdinDescriptorPath},
			Cmd:  []byte(copySQL),
		},
	}
	chunks := []*flight.FlightData{
		{DataBody: csv[:7]},
		{DataBody: csv[7:]},
	}

	stream := &fakeDoPutStream{
		ctx:     ctx,
		inbound: chunks,
	}

	if err := handler.doCopyFromStdin(ctx, first, stream); err != nil {
		t.Fatalf("doCopyFromStdin: %v", err)
	}

	if len(stream.outbound) != 1 {
		t.Fatalf("expected 1 PutResult, got %d", len(stream.outbound))
	}
	var got pb.DoPutUpdateResult
	if err := proto.Unmarshal(stream.outbound[0].AppMetadata, &got); err != nil {
		t.Fatalf("unmarshal DoPutUpdateResult: %v", err)
	}
	if got.RecordCount != 3 {
		t.Errorf("RecordCount = %d, want 3", got.RecordCount)
	}

	// Confirm rows actually landed.
	row := session.Conn.QueryRowContext(ctx, "SELECT count(*) FROM t")
	var n int
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Errorf("table row count = %d, want 3", n)
	}
}

func TestDoCopyFromStdinIngestsPostgresBinaryWithBundledScanner(t *testing.T) {
	extensionDirectory := os.Getenv("DUCKGRES_TEST_DUCKDB_EXTENSION_DIRECTORY")
	if extensionDirectory == "" {
		t.Skip("set DUCKGRES_TEST_DUCKDB_EXTENSION_DIRECTORY to test the bundled postgres_scanner")
	}

	session, cleanup := newSessionWithInMemoryDuckDB(t)
	defer cleanup()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))
	for _, statement := range []string{
		"SET autoinstall_known_extensions = false",
		"SET autoload_known_extensions = false",
		"SET extension_directory = '" + strings.ReplaceAll(extensionDirectory, "'", "''") + "'",
		"LOAD postgres_scanner",
	} {
		if _, err := session.Conn.ExecContext(ctx, statement); err != nil {
			t.Fatalf("configure postgres_scanner test: %v", err)
		}
	}

	pool := &SessionPool{sessions: map[string]*Session{session.ID: session}}
	handler := &FlightSQLHandler{pool: pool}

	t.Run("canonical subset preserves values and nulls", func(t *testing.T) {
		if _, err := session.Conn.ExecContext(ctx, `
			CREATE TABLE binary_canonical (
				id BIGINT,
				untouched VARCHAR DEFAULT 'kept',
				label VARCHAR,
				payload BLOB,
				enabled BOOLEAN,
				ratio DOUBLE,
				event_date DATE,
				event_time TIMESTAMP,
				received_at TIMESTAMPTZ
			)`); err != nil {
			t.Fatal(err)
		}
		payload := postgresBinaryCopyFixture(t, nil,
			[][]byte{
				[]byte("alpha"),
				postgresBinaryInt64Fixture(42),
				{0x00, '\\', '.', 0xff},
				{1},
				postgresBinaryFloat64Fixture(-123.5),
				postgresBinaryInt32Fixture(-1),
				postgresBinaryInt64Fixture(-1),
				postgresBinaryInt64Fixture(1_000_001),
			},
			[][]byte{nil, postgresBinaryInt64Fixture(-7), nil, nil, nil, nil, nil, nil},
		)
		copySQL := "INSERT INTO binary_canonical " +
			"(label, id, payload, enabled, ratio, event_date, event_time, received_at) " +
			"SELECT * FROM read_postgres_binary('" + flightclient.CopyFromStdinPathPlaceholder +
			"', columns = {c0: 'VARCHAR', c1: 'BIGINT', c2: 'BLOB', c3: 'BOOLEAN', " +
			"c4: 'DOUBLE', c5: 'DATE', c6: 'TIMESTAMP', c7: 'TIMESTAMPTZ'}, buffer_size = " +
			flightclient.CopyFromStdinSizePlaceholder + ")"
		if rows := runPostgresBinaryCopyFixture(t, handler, ctx, copySQL,
			[]string{"VARCHAR", "BIGINT", "BLOB", "BOOLEAN", "DOUBLE", "DATE", "TIMESTAMP", "TIMESTAMPTZ"},
			payload); rows != 2 {
			t.Fatalf("row count = %d, want 2", rows)
		}

		var untouched string
		var labelNull, blobNull, enabledNull, ratioNull, dateNull, timestampNull, timestamptzNull bool
		if err := session.Conn.QueryRowContext(ctx, `
			SELECT untouched, label IS NULL, payload IS NULL, enabled IS NULL,
			       ratio IS NULL, event_date IS NULL, event_time IS NULL, received_at IS NULL
			FROM binary_canonical WHERE id = -7`).Scan(
			&untouched, &labelNull, &blobNull, &enabledNull,
			&ratioNull, &dateNull, &timestampNull, &timestamptzNull,
		); err != nil {
			t.Fatal(err)
		}
		if untouched != "kept" || !labelNull || !blobNull || !enabledNull ||
			!ratioNull || !dateNull || !timestampNull || !timestamptzNull {
			t.Fatalf("NULL/default row was not preserved")
		}

		var label string
		var blob []byte
		var enabled, ratioMatches, dateMatches, timestampMatches, timestamptzMatches bool
		if err := session.Conn.QueryRowContext(ctx, `
			SELECT untouched, label, payload, enabled,
			       ratio = -123.5,
			       event_date = DATE '1999-12-31',
			       event_time = TIMESTAMP '1999-12-31 23:59:59.999999',
			       received_at = TIMESTAMPTZ '2000-01-01 00:00:01.000001+00'
			FROM binary_canonical WHERE id = 42`).Scan(
			&untouched, &label, &blob, &enabled,
			&ratioMatches, &dateMatches, &timestampMatches, &timestamptzMatches,
		); err != nil {
			t.Fatal(err)
		}
		wantBlob := []byte{0x00, '\\', '.', 0xff}
		if untouched != "kept" || label != "alpha" || !bytes.Equal(blob, wantBlob) || !enabled ||
			!ratioMatches || !dateMatches || !timestampMatches || !timestamptzMatches {
			t.Fatalf("typed row values were not preserved")
		}
		var count int
		if err := session.Conn.QueryRowContext(ctx, "SELECT count(*) FROM binary_canonical").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Fatalf("stored row count = %d, want 2", count)
		}
	})

	t.Run("header-only rewrite reaches scanner", func(t *testing.T) {
		if _, err := session.Conn.ExecContext(ctx, "CREATE TABLE binary_header (label VARCHAR)"); err != nil {
			t.Fatal(err)
		}
		payload := postgresBinaryCopyFixture(t, []byte{0xde, 0xad, 0xbe, 0xef},
			[][]byte{[]byte("header-only")},
		)
		copySQL := "INSERT INTO binary_header " +
			"SELECT * FROM read_postgres_binary('" + flightclient.CopyFromStdinPathPlaceholder +
			"', columns = {c0: 'VARCHAR'}, buffer_size = " + flightclient.CopyFromStdinSizePlaceholder + ")"
		if rows := runPostgresBinaryCopyFixture(t, handler, ctx, copySQL, []string{"VARCHAR"}, payload); rows != 1 {
			t.Fatalf("row count = %d, want 1", rows)
		}
		var label string
		if err := session.Conn.QueryRowContext(ctx, "SELECT label FROM binary_header").Scan(&label); err != nil {
			t.Fatal(err)
		}
		if label != "header-only" {
			t.Fatalf("label = %q, want header-only", label)
		}
	})

	t.Run("decimal rewrite reaches scanner", func(t *testing.T) {
		if _, err := session.Conn.ExecContext(ctx, `
			CREATE TABLE binary_rewrite (
				untouched VARCHAR DEFAULT 'kept',
				amount DECIMAL(10,2),
				label VARCHAR
			)`); err != nil {
			t.Fatal(err)
		}
		payload := postgresBinaryCopyFixture(t, nil,
			[][]byte{[]byte("positive"), postgresBinaryNumericFixture(t, 0, 0x0000, 3, 1, 2350)},
			[][]byte{[]byte("negative"), postgresBinaryNumericFixture(t, 0, 0x4000, 3, 1, 2350)},
		)
		copySQL := "INSERT INTO binary_rewrite (label, amount) " +
			"SELECT * FROM read_postgres_binary('" + flightclient.CopyFromStdinPathPlaceholder +
			"', columns = {c0: 'VARCHAR', c1: 'DECIMAL(10,2)'}, buffer_size = " +
			flightclient.CopyFromStdinSizePlaceholder + ")"
		if rows := runPostgresBinaryCopyFixture(t, handler, ctx, copySQL,
			[]string{"VARCHAR", "DECIMAL(10,2)"}, payload); rows != 2 {
			t.Fatalf("row count = %d, want 2", rows)
		}

		rows, err := session.Conn.QueryContext(ctx,
			"SELECT label, CAST(amount AS VARCHAR), untouched FROM binary_rewrite ORDER BY label")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		for _, want := range []struct {
			label  string
			amount string
		}{{"negative", "-1.24"}, {"positive", "1.24"}} {
			if !rows.Next() {
				t.Fatalf("missing %s row: %v", want.label, rows.Err())
			}
			var label, amount, untouched string
			if err := rows.Scan(&label, &amount, &untouched); err != nil {
				t.Fatal(err)
			}
			if label != want.label || amount != want.amount || untouched != "kept" {
				t.Fatalf("row = (%q, %q, %q), want (%q, %q, kept)", label, amount, untouched, want.label, want.amount)
			}
		}
		if rows.Next() || rows.Err() != nil {
			t.Fatalf("unexpected rows/error after expected values: %v", rows.Err())
		}
	})
}

func TestPreparePostgresBinaryCopyReusesCanonicalSpool(t *testing.T) {
	payload := postgresBinaryCopyFixture(t, nil,
		[][]byte{postgresBinaryInt64Fixture(42)},
	)
	path := filepath.Join(t.TempDir(), "canonical.copy")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}

	loadPath, loadBytes, cleanup, err := preparePostgresBinaryCopy(
		context.Background(), path, []string{"BIGINT"},
	)
	if err != nil {
		t.Fatal(err)
	}
	cleanup()
	if loadPath != path {
		t.Fatalf("canonical load path = %q, want original spool %q", loadPath, path)
	}
	if loadBytes != int64(len(payload)) {
		t.Fatalf("canonical load size = %d, want %d", loadBytes, len(payload))
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("canonical cleanup removed caller-owned spool: %v", err)
	}
}

func TestPreparePostgresBinaryCopyNormalizesHeaderExtension(t *testing.T) {
	extension := []byte{0xde, 0xad, 0xbe, 0xef}
	payload := postgresBinaryCopyFixture(t, extension,
		[][]byte{[]byte("header-only")},
	)
	path := filepath.Join(t.TempDir(), "header-extension.copy")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}

	loadPath, loadBytes, cleanup, err := preparePostgresBinaryCopy(
		context.Background(), path, []string{"VARCHAR"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if loadPath == path {
		cleanup()
		t.Fatal("header extension reused the original spool, want normalized spool")
	}
	if loadBytes != int64(len(payload)-len(extension)) {
		cleanup()
		t.Fatalf("normalized load size = %d, want %d", loadBytes, len(payload)-len(extension))
	}
	if _, err := os.Stat(loadPath); err != nil {
		cleanup()
		t.Fatalf("normalized spool missing before cleanup: %v", err)
	}
	cleanup()
	if _, err := os.Stat(loadPath); !os.IsNotExist(err) {
		t.Fatalf("normalized spool still exists after cleanup: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("normalization removed caller-owned spool: %v", err)
	}
}

func TestDoCopyFromStdinRejectsBinaryTupleWidthBeforeScannerExecution(t *testing.T) {
	session, cleanup := newSessionWithInMemoryDuckDB(t)
	defer cleanup()

	pool := &SessionPool{sessions: map[string]*Session{session.ID: session}}
	handler := &FlightSQLHandler{pool: pool}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	var payload bytes.Buffer
	payload.Write([]byte("PGCOPY\n\xff\r\n\x00"))
	_ = binary.Write(&payload, binary.BigEndian, uint32(0))
	_ = binary.Write(&payload, binary.BigEndian, uint32(0))
	_ = binary.Write(&payload, binary.BigEndian, int16(2))
	for _, value := range []int64{1, 2} {
		_ = binary.Write(&payload, binary.BigEndian, int32(8))
		_ = binary.Write(&payload, binary.BigEndian, value)
	}
	_ = binary.Write(&payload, binary.BigEndian, int16(-1))

	copySQL := "INSERT INTO t (a) SELECT * FROM read_postgres_binary('" +
		flightclient.CopyFromStdinPathPlaceholder +
		"', columns = {c0: 'BIGINT'}, buffer_size = " +
		flightclient.CopyFromStdinSizePlaceholder + ")"
	requestBytes, err := json.Marshal(sqlcore.CopyFromStdinRequest{
		SQLTemplate:                     copySQL,
		PostgresBinaryDatabaseTypeNames: []string{"BIGINT"},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{
				flightclient.CopyFromStdinDescriptorPath,
				flightclient.CopyFromStdinPostgresBinaryPathVersion,
			},
			Cmd: requestBytes,
		},
	}
	stream := &fakeDoPutStream{
		ctx:     ctx,
		inbound: []*flight.FlightData{{DataBody: payload.Bytes()}},
	}

	err = handler.doCopyFromStdin(ctx, first, stream)
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "2 fields, expected 1") {
		t.Fatalf("doCopyFromStdin() error = %v, want tuple-width validation error", err)
	}
	if len(stream.outbound) != 0 {
		t.Fatalf("worker sent %d results after validation failure", len(stream.outbound))
	}
	var rows int
	if err := session.Conn.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("rows inserted after validation failure = %d, want 0", rows)
	}
}

func TestDoCopyFromStdinRejectsNativeBinarySQLWithoutSchemaMetadata(t *testing.T) {
	session, cleanup := newSessionWithInMemoryDuckDB(t)
	defer cleanup()

	pool := &SessionPool{sessions: map[string]*Session{session.ID: session}}
	handler := &FlightSQLHandler{pool: pool}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))
	first := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{flightclient.CopyFromStdinDescriptorPath},
			Cmd: []byte("SELECT * FROM read_postgres_binary('" +
				flightclient.CopyFromStdinPathPlaceholder + "', columns = {c0: 'BIGINT'})"),
		},
	}
	err := handler.doCopyFromStdin(ctx, first, &fakeDoPutStream{ctx: ctx})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "schema metadata") {
		t.Fatalf("doCopyFromStdin() error = %v, want missing schema metadata error", err)
	}
}

func TestCopyFromStdinDescriptorRequestAllowsLegacyTextCopyTargetContainingNativeReaderName(t *testing.T) {
	copySQL := "COPY read_postgres_binary_staging FROM '" +
		flightclient.CopyFromStdinPathPlaceholder + "' (FORMAT CSV)"
	desc := &flight.FlightDescriptor{
		Type: flight.DescriptorPATH,
		Path: []string{flightclient.CopyFromStdinDescriptorPath},
		Cmd:  []byte(copySQL),
	}

	gotSQL, databaseTypeNames, err := copyFromStdinDescriptorRequest(desc)
	if err != nil {
		t.Fatalf("copyFromStdinDescriptorRequest() error = %v", err)
	}
	if gotSQL != copySQL {
		t.Fatalf("copyFromStdinDescriptorRequest() SQL = %q, want %q", gotSQL, copySQL)
	}
	if databaseTypeNames != nil {
		t.Fatalf("copyFromStdinDescriptorRequest() database types = %v, want nil", databaseTypeNames)
	}
}

func TestPreparePostgresBinaryCopyHonorsCanceledContext(t *testing.T) {
	var payload bytes.Buffer
	payload.Write([]byte("PGCOPY\n\xff\r\n\x00"))
	_ = binary.Write(&payload, binary.BigEndian, uint32(0))
	_ = binary.Write(&payload, binary.BigEndian, uint32(0))
	_ = binary.Write(&payload, binary.BigEndian, int16(1))
	_ = binary.Write(&payload, binary.BigEndian, int32(8))
	_ = binary.Write(&payload, binary.BigEndian, int64(1))
	_ = binary.Write(&payload, binary.BigEndian, int16(-1))
	path := filepath.Join(t.TempDir(), "copy.bin")
	if err := os.WriteFile(path, payload.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, cleanup, err := preparePostgresBinaryCopy(ctx, path, []string{"BIGINT"})
	defer cleanup()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("preparePostgresBinaryCopy() error = %v, want context.Canceled", err)
	}
}

func TestDoCopyFromStdinRejectsConcurrentSessionOperation(t *testing.T) {
	session, cleanup := newSessionWithInMemoryDuckDB(t)
	defer cleanup()
	finishOperation, ok := session.beginOperation()
	if !ok {
		t.Fatal("expected operation to start")
	}
	defer finishOperation()

	pool := &SessionPool{sessions: map[string]*Session{session.ID: session}}
	handler := &FlightSQLHandler{pool: pool}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	first := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{flightclient.CopyFromStdinDescriptorPath},
			Cmd:  []byte("COPY t FROM '" + flightclient.CopyFromStdinPathPlaceholder + "' (FORMAT CSV)"),
		},
	}
	err := handler.doCopyFromStdin(ctx, first, &fakeDoPutStream{ctx: ctx})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected concurrent COPY to be rejected, got %v", err)
	}
}

func TestDoCopyFromStdinRejectsMissingPlaceholder(t *testing.T) {
	session, cleanup := newSessionWithInMemoryDuckDB(t)
	defer cleanup()

	pool := &SessionPool{sessions: map[string]*Session{session.ID: session}}
	handler := &FlightSQLHandler{pool: pool}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	first := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{flightclient.CopyFromStdinDescriptorPath},
			Cmd:  []byte("COPY t FROM '/no-placeholder' (FORMAT CSV)"),
		},
	}
	stream := &fakeDoPutStream{ctx: ctx}

	err := handler.doCopyFromStdin(ctx, first, stream)
	if err == nil {
		t.Fatal("expected error when placeholder missing, got nil")
	}
	if !strings.Contains(err.Error(), "placeholder") {
		t.Errorf("error should mention placeholder, got: %v", err)
	}
}

func TestDoCopyFromStdinRequiresSession(t *testing.T) {
	pool := &SessionPool{sessions: map[string]*Session{}}
	handler := &FlightSQLHandler{pool: pool}

	// No x-duckgres-session metadata.
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})

	first := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{flightclient.CopyFromStdinDescriptorPath},
			Cmd:  []byte("COPY t FROM '" + flightclient.CopyFromStdinPathPlaceholder + "' (FORMAT CSV)"),
		},
	}
	stream := &fakeDoPutStream{ctx: ctx}

	err := handler.doCopyFromStdin(ctx, first, stream)
	if err == nil {
		t.Fatal("expected auth error when session metadata missing")
	}
	// Don't pin to specific gRPC code spelling — just confirm we didn't
	// silently no-op or panic.
	if errors.Is(err, io.EOF) {
		t.Fatalf("expected non-EOF auth error, got: %v", err)
	}
}

// TestDoCopyFromStdinAbortsOnStreamCancellation pins down the corrected
// cancellation path: when the CP cancels the gRPC stream mid-CSV (because
// the wire client sent CopyFail), the worker's Recv returns Canceled
// before EOF; the worker MUST return the error and MUST NOT run COPY on
// the partial bytes already buffered to the tempfile.
func TestDoCopyFromStdinAbortsOnStreamCancellation(t *testing.T) {
	session, cleanup := newSessionWithInMemoryDuckDB(t)
	defer cleanup()

	pool := &SessionPool{sessions: map[string]*Session{session.ID: session}}
	handler := &FlightSQLHandler{pool: pool}

	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-duckgres-session", session.ID))

	copySQL := "COPY t (a, b) FROM '" + flightclient.CopyFromStdinPathPlaceholder +
		"' (FORMAT CSV, HEADER false)"

	first := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{flightclient.CopyFromStdinDescriptorPath},
			Cmd:  []byte(copySQL),
		},
	}
	// One legitimate chunk, then the stream errors as if the gRPC client
	// cancelled (which is what happens after the CP's reader returned
	// errCopyAborted and the deferred cancel() fired).
	chunks := []*flight.FlightData{
		{DataBody: []byte("1,one\n")},
	}
	stream := &fakeDoPutStream{
		ctx:       ctx,
		inbound:   chunks,
		recvErrAt: 1,
		recvErr:   status.Error(codes.Canceled, "client cancelled"),
	}

	err := handler.doCopyFromStdin(ctx, first, stream)
	if err == nil {
		t.Fatal("expected error from cancelled stream, got nil")
	}
	if status.Code(err) != codes.Aborted {
		t.Errorf("expected codes.Aborted, got %v: %v", status.Code(err), err)
	}

	// The smoking gun: even though one row's worth of CSV was buffered to
	// the worker tempfile, the COPY must NOT have run.
	row := session.Conn.QueryRowContext(ctx, "SELECT count(*) FROM t")
	var n int
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("rows landed despite cancellation: got %d, want 0", n)
	}

	// And the worker did not send a PutResult.
	if len(stream.outbound) != 0 {
		t.Errorf("worker should not send PutResult on cancellation, got %d", len(stream.outbound))
	}
}

func TestDoCopyFromStdinKeepsRawSQLTransactionDrainOpenWhileReceiving(t *testing.T) {
	session, cleanup := newSessionWithInMemoryDuckDB(t)
	defer cleanup()

	pool := &SessionPool{
		sessions:    map[string]*Session{session.ID: session},
		stopRefresh: make(map[string]func()),
	}
	handler := &FlightSQLHandler{pool: pool}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-duckgres-session", session.ID))

	if _, err := handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "BEGIN"}); err != nil {
		t.Fatalf("raw BEGIN: %v", err)
	}
	session.sqlTxLastUsed.Store(time.Now().Add(-txnIdleTimeout - time.Minute).UnixNano())

	copySQL := "COPY t (a, b) FROM '" + flightclient.CopyFromStdinPathPlaceholder +
		"' (FORMAT CSV, HEADER false)"
	first := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{flightclient.CopyFromStdinDescriptorPath},
			Cmd:  []byte(copySQL),
		},
	}
	stream := &blockingDoPutStream{
		fakeDoPutStream: fakeDoPutStream{ctx: ctx},
		recvStarted:     make(chan struct{}),
		unblock:         make(chan struct{}),
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- handler.doCopyFromStdin(ctx, first, stream)
	}()

	select {
	case <-stream.recvStarted:
	case <-time.After(time.Second):
		t.Fatal("expected COPY stream receive to start")
	}
	session.sqlTxLastUsed.Store(time.Now().Add(-txnIdleTimeout - time.Minute).UnixNano())

	pool.BeginDrain()
	pool.reapIdle(time.Now())
	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	drained := pool.WaitForDrain(shortCtx)

	close(stream.unblock)
	err := <-errCh
	if err == nil {
		t.Fatal("expected stream cancellation error")
	}
	if drained {
		t.Fatal("COPY FROM STDIN in a raw SQL transaction must keep drain work open while receiving")
	}

	if _, err := handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "ROLLBACK"}); err != nil {
		t.Fatalf("raw ROLLBACK after canceled COPY: %v", err)
	}
}
