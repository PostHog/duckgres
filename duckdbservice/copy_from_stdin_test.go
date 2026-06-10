package duckdbservice

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/posthog/duckgres/server/flightclient"
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
	db, err := sql.Open("duckdb", "")
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

func TestDoCopyFromStdinDoesNotUseSessionOperationGate(t *testing.T) {
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

	copySQL := "COPY t (a, b) FROM '" + flightclient.CopyFromStdinPathPlaceholder +
		"' (FORMAT CSV, HEADER false)"
	first := &flight.FlightData{
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorPATH,
			Path: []string{flightclient.CopyFromStdinDescriptorPath},
			Cmd:  []byte(copySQL),
		},
		DataBody: []byte("1,one\n"),
	}
	err := handler.doCopyFromStdin(ctx, first, &fakeDoPutStream{ctx: ctx})
	if err != nil {
		t.Fatalf("expected COPY to rely on connection guards instead of session admission, got %v", err)
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
