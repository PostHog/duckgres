package duckdbservice

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	"github.com/posthog/duckgres/server/pgbinary"
	"github.com/posthog/duckgres/server/sqlcore"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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

func useIsolatedWorkerCopyTempDir(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	t.Setenv("TMPDIR", tmpDir)
	t.Cleanup(func() { assertNoWorkerCopyTempfiles(t, tmpDir) })
	return tmpDir
}

func assertNoWorkerCopyTempfiles(t *testing.T, tmpDir string) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(tmpDir, "duckgres-worker-copy-*.copy"))
	if err != nil {
		t.Fatalf("glob worker COPY tempfiles: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("worker COPY tempfiles leaked: %v", matches)
	}
}

type failingCopyPreparationReader struct {
	err error
}

func (r failingCopyPreparationReader) Read([]byte) (int, error) { return 0, r.err }

type failingCopyPreparationWriter struct {
	err error
}

func (w failingCopyPreparationWriter) Write([]byte) (int, error) { return 0, w.err }

func assertCopyFromStdinRPCError(
	t *testing.T,
	err error,
	wantCode codes.Code,
	wantReason string,
	wantMessage string,
	forbidden ...string,
) {
	t.Helper()
	if err == nil {
		t.Fatalf("doCopyFromStdin() error = nil, want %s", wantCode)
	}
	if got := status.Code(err); got != wantCode {
		t.Fatalf("doCopyFromStdin() code = %s, want %s: %v", got, wantCode, err)
	}
	if got := status.Convert(err).Message(); got != wantMessage {
		t.Fatalf("doCopyFromStdin() message = %q, want %q", got, wantMessage)
	}
	var gotInfo *errdetails.ErrorInfo
	for _, detail := range status.Convert(err).Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			gotInfo = info
			break
		}
	}
	if gotInfo == nil {
		t.Fatalf("doCopyFromStdin() details = %v, want google.rpc.ErrorInfo", status.Convert(err).Details())
	}
	if gotInfo.Domain != "duckgres.copy" || gotInfo.Reason != wantReason {
		t.Errorf("doCopyFromStdin() ErrorInfo = {domain:%q reason:%q}, want {domain:%q reason:%q}",
			gotInfo.Domain, gotInfo.Reason, "duckgres.copy", wantReason)
	}
	for _, marker := range forbidden {
		if strings.Contains(err.Error(), marker) {
			t.Errorf("doCopyFromStdin() error leaked %q: %v", marker, err)
		}
	}
}

func TestSanitizeCopyFromStdinRPCErrorPreservesCodesAndUsesFixedTaxonomy(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		class       copyFromStdinErrorClass
		wantCode    codes.Code
		wantReason  string
		wantMessage string
	}{
		{
			name:        "invalid request",
			err:         status.Error(codes.InvalidArgument, "request leaked __DUCKGRES_COPY_PATH__"),
			class:       copyFromStdinErrorInvalidRequest,
			wantCode:    codes.InvalidArgument,
			wantReason:  "INVALID_REQUEST",
			wantMessage: "copy-from-stdin: invalid request",
		},
		{
			name:        "invalid input",
			err:         status.Error(codes.InvalidArgument, "row value leaked"),
			class:       copyFromStdinErrorInvalidInput,
			wantCode:    codes.InvalidArgument,
			wantReason:  "INVALID_INPUT",
			wantMessage: "copy-from-stdin: invalid input",
		},
		{
			name:        "load rejected",
			err:         status.Error(codes.InvalidArgument, "INSERT INTO private_table FROM '/tmp/private.copy'"),
			class:       copyFromStdinErrorLoadRejected,
			wantCode:    codes.InvalidArgument,
			wantReason:  "LOAD_REJECTED",
			wantMessage: "copy-from-stdin: load rejected",
		},
		{
			name:        "canceled context",
			err:         fmt.Errorf("wrapped cancellation detail: %w", context.Canceled),
			class:       copyFromStdinErrorInternal,
			wantCode:    codes.Canceled,
			wantReason:  "CANCELED",
			wantMessage: "copy-from-stdin: canceled",
		},
		{
			name:        "deadline context",
			err:         fmt.Errorf("wrapped deadline detail: %w", context.DeadlineExceeded),
			class:       copyFromStdinErrorInternal,
			wantCode:    codes.DeadlineExceeded,
			wantReason:  "DEADLINE_EXCEEDED",
			wantMessage: "copy-from-stdin: deadline exceeded",
		},
		{
			name:        "unavailable",
			err:         status.Error(codes.NotFound, "worker path leaked"),
			class:       copyFromStdinErrorUnavailable,
			wantCode:    codes.NotFound,
			wantReason:  "UNAVAILABLE",
			wantMessage: "copy-from-stdin: unavailable",
		},
		{
			name:        "internal",
			err:         errors.New("/tmp/duckgres-worker-copy-private.copy"),
			class:       copyFromStdinErrorInternal,
			wantCode:    codes.Unknown,
			wantReason:  "INTERNAL",
			wantMessage: "copy-from-stdin: internal error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sanitizeCopyFromStdinRPCError(tt.err, tt.class)
			assertCopyFromStdinRPCError(t, err, tt.wantCode, tt.wantReason, tt.wantMessage,
				"__DUCKGRES_COPY_PATH__", "row value leaked", "private_table", "/tmp/", "worker path leaked")
		})
	}
}

func TestPreparePostgresBinaryCopySeparatesInputAndFilesystemFailures(t *testing.T) {
	t.Run("missing worker spool is internal", func(t *testing.T) {
		_, _, cleanup, err := preparePostgresBinaryCopy(
			context.Background(),
			filepath.Join(t.TempDir(), "missing.copy"),
			[]string{"BIGINT"},
		)
		defer cleanup()
		if err == nil {
			t.Fatal("preparePostgresBinaryCopy() error = nil, want missing-spool error")
		}
		if got := copyFromStdinPreparationErrorClass(err); got != copyFromStdinErrorInternal {
			t.Fatalf("preparation error class = %v, want internal", got)
		}
	})

	t.Run("malformed client payload is invalid input", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "malformed.copy")
		if err := os.WriteFile(path, []byte("not a PostgreSQL binary COPY stream"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, _, cleanup, err := preparePostgresBinaryCopy(context.Background(), path, []string{"BIGINT"})
		defer cleanup()
		if err == nil {
			t.Fatal("preparePostgresBinaryCopy() error = nil, want malformed-input error")
		}
		if got := copyFromStdinPreparationErrorClass(err); got != copyFromStdinErrorInvalidInput {
			t.Fatalf("preparation error class = %v, want invalid input", got)
		}
	})

	t.Run("worker spool read failure is internal", func(t *testing.T) {
		schema, err := pgbinary.SchemaFromDatabaseTypes([]string{"BIGINT"})
		if err != nil {
			t.Fatal(err)
		}
		_, err = pgbinary.InspectProtocolCompleted(&copyFromStdinPreparationReader{
			r: failingCopyPreparationReader{err: errors.New("synthetic spool read failure")},
		}, schema)
		if err == nil {
			t.Fatal("InspectProtocolCompleted() error = nil, want read failure")
		}
		if got := copyFromStdinPreparationErrorClass(err); got != copyFromStdinErrorInternal {
			t.Fatalf("preparation error class = %v, want internal", got)
		}
	})

	t.Run("worker spool write failure is internal", func(t *testing.T) {
		schema, err := pgbinary.SchemaFromDatabaseTypes([]string{"BIGINT"})
		if err != nil {
			t.Fatal(err)
		}
		payload := postgresBinaryCopyFixture(t, nil, [][]byte{postgresBinaryInt64Fixture(1)})
		_, err = pgbinary.RewriteProtocolCompleted(
			&copyFromStdinPreparationWriter{w: failingCopyPreparationWriter{err: errors.New("synthetic spool write failure")}},
			bytes.NewReader(payload),
			schema,
		)
		if err == nil {
			t.Fatal("RewriteProtocolCompleted() error = nil, want write failure")
		}
		if got := copyFromStdinPreparationErrorClass(err); got != copyFromStdinErrorInternal {
			t.Fatalf("preparation error class = %v, want internal", got)
		}
	})
}

func newPostgresBinaryCopyFixture(
	t *testing.T,
	ctx context.Context,
	copySQL string,
	databaseTypeNames []string,
	payload []byte,
) (*flight.FlightData, *fakeDoPutStream) {
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
	return first, stream
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
	first, stream := newPostgresBinaryCopyFixture(t, ctx, copySQL, databaseTypeNames, payload)
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
	previousLogger := slog.Default()
	var logs bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	tmpDir := useIsolatedWorkerCopyTempDir(t)
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
	for _, forbidden := range []string{tmpDir, "duckgres-worker-copy-"} {
		if strings.Contains(logs.String(), forbidden) {
			t.Errorf("COPY worker telemetry leaked generated tempfile metadata %q:\n%s", forbidden, logs.String())
		}
	}
	assertNoWorkerCopyTempfiles(t, tmpDir)
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

	t.Run("protocol-completed subset without trailer preserves values and nulls", func(t *testing.T) {
		if _, err := session.Conn.ExecContext(ctx, `
			CREATE TABLE binary_protocol_completed (
				id BIGINT,
				label VARCHAR,
				untouched VARCHAR DEFAULT 'kept'
			)`); err != nil {
			t.Fatal(err)
		}
		payload := postgresBinaryCopyFixture(t, nil,
			[][]byte{postgresBinaryInt64Fixture(1), []byte("one")},
			[][]byte{postgresBinaryInt64Fixture(2), nil},
		)
		payload = payload[:len(payload)-2]
		copySQL := "INSERT INTO binary_protocol_completed (id, label) " +
			"SELECT * FROM read_postgres_binary('" + flightclient.CopyFromStdinPathPlaceholder +
			"', columns = {c0: 'BIGINT', c1: 'VARCHAR'}, buffer_size = " +
			flightclient.CopyFromStdinSizePlaceholder + ")"
		if rows := runPostgresBinaryCopyFixture(t, handler, ctx, copySQL,
			[]string{"BIGINT", "VARCHAR"}, payload); rows != 2 {
			t.Fatalf("row count = %d, want 2", rows)
		}

		var valuesMatch bool
		if err := session.Conn.QueryRowContext(ctx, `
			SELECT count(*) = 2
			   AND count(*) FILTER (WHERE id = 1 AND label = 'one' AND untouched = 'kept') = 1
			   AND count(*) FILTER (WHERE id = 2 AND label IS NULL AND untouched = 'kept') = 1
			FROM binary_protocol_completed`).Scan(&valuesMatch); err != nil {
			t.Fatal(err)
		}
		if !valuesMatch {
			t.Fatal("protocol-completed rows, NULL, or default value were not preserved")
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
		payload = payload[:len(payload)-2]
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

	t.Run("zero-row stream returns zero and cleans its spool", func(t *testing.T) {
		tmpDir := useIsolatedWorkerCopyTempDir(t)
		if _, err := session.Conn.ExecContext(ctx, "CREATE TABLE binary_empty (id BIGINT)"); err != nil {
			t.Fatal(err)
		}
		payload := postgresBinaryCopyFixture(t, nil)
		payload = payload[:len(payload)-2]
		copySQL := "INSERT INTO binary_empty " +
			"SELECT * FROM read_postgres_binary('" + flightclient.CopyFromStdinPathPlaceholder +
			"', columns = {c0: 'BIGINT'}, buffer_size = " + flightclient.CopyFromStdinSizePlaceholder + ")"
		if rows := runPostgresBinaryCopyFixture(t, handler, ctx, copySQL, []string{"BIGINT"}, payload); rows != 0 {
			t.Fatalf("row count = %d, want 0", rows)
		}
		var count int
		if err := session.Conn.QueryRowContext(ctx, "SELECT count(*) FROM binary_empty").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("stored row count = %d, want 0", count)
		}
		assertNoWorkerCopyTempfiles(t, tmpDir)
	})

	t.Run("blob larger than scanner default buffer preserves bytes", func(t *testing.T) {
		tmpDir := useIsolatedWorkerCopyTempDir(t)
		if _, err := session.Conn.ExecContext(ctx, "CREATE TABLE binary_large_blob (payload BLOB)"); err != nil {
			t.Fatal(err)
		}

		const blobSize = 32<<20 + 1
		blob := make([]byte, blobSize)
		copy(blob[:4], []byte{0x00, 0xff, 0x5a, 0xa5})
		middle := len(blob) / 2
		copy(blob[middle:middle+4], []byte{0xde, 0xad, 0xbe, 0xef})
		copy(blob[len(blob)-4:], []byte{0x13, 0x37, 0xc0, 0xde})

		payload := postgresBinaryCopyFixture(t, nil, [][]byte{blob})
		if len(payload) <= 32<<20 {
			t.Fatalf("test binary COPY size = %d, want more than 32 MiB", len(payload))
		}
		copySQL := "INSERT INTO binary_large_blob " +
			"SELECT * FROM read_postgres_binary('" + flightclient.CopyFromStdinPathPlaceholder +
			"', columns = {c0: 'BLOB'}, buffer_size = " + flightclient.CopyFromStdinSizePlaceholder + ")"
		if rows := runPostgresBinaryCopyFixture(t, handler, ctx, copySQL, []string{"BLOB"}, payload); rows != 1 {
			t.Fatalf("row count = %d, want 1", rows)
		}

		var stored []byte
		if err := session.Conn.QueryRowContext(ctx, "SELECT payload FROM binary_large_blob").Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if len(stored) != blobSize {
			t.Fatalf("stored BLOB length = %d, want %d", len(stored), blobSize)
		}
		if !bytes.Equal(stored, blob) {
			t.Fatal("stored BLOB differs from the PostgreSQL binary COPY payload")
		}
		assertNoWorkerCopyTempfiles(t, tmpDir)
	})

	t.Run("raw transaction controls native copy commit and rollback", func(t *testing.T) {
		tmpDir := useIsolatedWorkerCopyTempDir(t)
		t.Cleanup(func() {
			if session.sqlTxActive.Load() {
				_, _ = handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "ROLLBACK"})
			}
		})
		if _, err := session.Conn.ExecContext(ctx, "CREATE TABLE binary_transaction (id BIGINT)"); err != nil {
			t.Fatal(err)
		}
		copySQL := "INSERT INTO binary_transaction " +
			"SELECT * FROM read_postgres_binary('" + flightclient.CopyFromStdinPathPlaceholder +
			"', columns = {c0: 'BIGINT'}, buffer_size = " + flightclient.CopyFromStdinSizePlaceholder + ")"

		if _, err := handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "BEGIN"}); err != nil {
			t.Fatalf("BEGIN before rollback case: %v", err)
		}
		if rows := runPostgresBinaryCopyFixture(t, handler, ctx, copySQL, []string{"BIGINT"},
			postgresBinaryCopyFixture(t, nil, [][]byte{postgresBinaryInt64Fixture(11)})); rows != 1 {
			t.Fatalf("rollback case row count = %d, want 1", rows)
		}
		var inTransactionCount int
		if err := session.Conn.QueryRowContext(ctx,
			"SELECT count(*) FROM binary_transaction WHERE id = 11").Scan(&inTransactionCount); err != nil {
			t.Fatal(err)
		}
		if inTransactionCount != 1 {
			t.Fatalf("rows visible before rollback = %d, want 1", inTransactionCount)
		}
		if _, err := handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "ROLLBACK"}); err != nil {
			t.Fatalf("ROLLBACK after native COPY: %v", err)
		}
		var count int
		if err := session.Conn.QueryRowContext(ctx, "SELECT count(*) FROM binary_transaction").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("rows after rollback = %d, want 0", count)
		}

		if _, err := handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "BEGIN"}); err != nil {
			t.Fatalf("BEGIN before commit case: %v", err)
		}
		if rows := runPostgresBinaryCopyFixture(t, handler, ctx, copySQL, []string{"BIGINT"},
			postgresBinaryCopyFixture(t, nil, [][]byte{postgresBinaryInt64Fixture(22)})); rows != 1 {
			t.Fatalf("commit case row count = %d, want 1", rows)
		}
		if _, err := handler.DoPutCommandStatementUpdate(ctx, testStatementUpdate{query: "COMMIT"}); err != nil {
			t.Fatalf("COMMIT after native COPY: %v", err)
		}
		var id int64
		if err := session.Conn.QueryRowContext(ctx, "SELECT id FROM binary_transaction").Scan(&id); err != nil {
			t.Fatal(err)
		}
		if id != 22 {
			t.Fatalf("committed id = %d, want 22", id)
		}
		assertNoWorkerCopyTempfiles(t, tmpDir)
	})

	t.Run("rewritten copy failure is atomic and cleans both spools", func(t *testing.T) {
		tmpDir := useIsolatedWorkerCopyTempDir(t)
		if _, err := session.Conn.ExecContext(ctx,
			"CREATE TABLE binary_atomic_failure (amount DECIMAL(10,2) CHECK (amount > 0))"); err != nil {
			t.Fatal(err)
		}
		payload := postgresBinaryCopyFixture(t, nil,
			[][]byte{postgresBinaryNumericFixture(t, 0, 0x0000, 3, 1, 2350)},
			[][]byte{postgresBinaryNumericFixture(t, 0, 0x4000, 3, 1, 2350)},
		)
		copySQL := "INSERT INTO binary_atomic_failure " +
			"SELECT * FROM read_postgres_binary('" + flightclient.CopyFromStdinPathPlaceholder +
			"', columns = {c0: 'DECIMAL(10,2)'}, buffer_size = " + flightclient.CopyFromStdinSizePlaceholder + ")"
		first, stream := newPostgresBinaryCopyFixture(t, ctx, copySQL, []string{"DECIMAL(10,2)"}, payload)
		err := handler.doCopyFromStdin(ctx, first, stream)
		assertCopyFromStdinRPCError(t, err, codes.InvalidArgument, "LOAD_REJECTED",
			"copy-from-stdin: load rejected",
			"CHECK constraint", "-23.5", "read_postgres_binary", copySQL,
			flightclient.CopyFromStdinPathPlaceholder, flightclient.CopyFromStdinSizePlaceholder, tmpDir)
		if len(stream.outbound) != 0 {
			t.Fatalf("worker sent %d results after failed INSERT", len(stream.outbound))
		}
		var count int
		if err := session.Conn.QueryRowContext(ctx, "SELECT count(*) FROM binary_atomic_failure").Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("rows after failed atomic INSERT = %d, want 0", count)
		}
		assertNoWorkerCopyTempfiles(t, tmpDir)
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

func TestPreparePostgresBinaryCopyCanonicalizesProtocolCompletedSpool(t *testing.T) {
	canonical := postgresBinaryCopyFixture(t, nil,
		[][]byte{postgresBinaryInt64Fixture(42)},
	)
	trailerless := canonical[:len(canonical)-2]
	path := filepath.Join(t.TempDir(), "protocol-completed.copy")
	if err := os.WriteFile(path, trailerless, 0o600); err != nil {
		t.Fatal(err)
	}

	loadPath, loadBytes, cleanup, err := preparePostgresBinaryCopy(
		context.Background(), path, []string{"BIGINT"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if loadPath == path {
		cleanup()
		t.Fatal("protocol-completed spool reused the trailerless input, want normalized spool")
	}
	if loadBytes != int64(len(canonical)) {
		cleanup()
		t.Fatalf("normalized load size = %d, want %d", loadBytes, len(canonical))
	}
	got, err := os.ReadFile(loadPath)
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	if !bytes.Equal(got, canonical) {
		cleanup()
		t.Fatalf("normalized spool differs\n got: %x\nwant: %x", got, canonical)
	}
	cleanup()
	if _, err := os.Stat(loadPath); !os.IsNotExist(err) {
		t.Fatalf("normalized spool still exists after cleanup: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("normalization removed caller-owned spool: %v", err)
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
	tmpDir := useIsolatedWorkerCopyTempDir(t)
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
	assertCopyFromStdinRPCError(t, err, codes.InvalidArgument, "INVALID_INPUT",
		"copy-from-stdin: invalid input", "2 fields, expected 1", copySQL,
		flightclient.CopyFromStdinPathPlaceholder, flightclient.CopyFromStdinSizePlaceholder, tmpDir)
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
	assertNoWorkerCopyTempfiles(t, tmpDir)
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
	assertCopyFromStdinRPCError(t, err, codes.InvalidArgument, "INVALID_REQUEST",
		"copy-from-stdin: invalid request", "schema metadata", "read_postgres_binary",
		flightclient.CopyFromStdinPathPlaceholder)
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
	assertCopyFromStdinRPCError(t, err, codes.InvalidArgument, "INVALID_REQUEST",
		"copy-from-stdin: invalid request", "placeholder", "/no-placeholder",
		flightclient.CopyFromStdinPathPlaceholder)
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
	tmpDir := useIsolatedWorkerCopyTempDir(t)
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
	assertCopyFromStdinRPCError(t, err, codes.Aborted, "CANCELED",
		"copy-from-stdin: canceled", "client cancelled", copySQL,
		flightclient.CopyFromStdinPathPlaceholder)

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
	assertNoWorkerCopyTempfiles(t, tmpDir)
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
