package flightclient

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/posthog/duckgres/server/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestFlightExecutorWithSessionAddsOwnerEpochHeader(t *testing.T) {
	exec := NewFlightExecutorFromClient(nil, "session-1")
	exec.SetOwnerEpoch(7)
	exec.SetControlMetadata(17, "cp-live:boot-a", 7)

	ctx := exec.withSession(context.Background())
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	if got := md.Get("x-duckgres-session"); len(got) != 1 || got[0] != "session-1" {
		t.Fatalf("unexpected session metadata: %#v", got)
	}
	if got := md.Get("x-duckgres-owner-epoch"); len(got) != 1 || got[0] != "7" {
		t.Fatalf("unexpected owner epoch metadata: %#v", got)
	}
	if got := md.Get("x-duckgres-worker-id"); len(got) != 1 || got[0] != "17" {
		t.Fatalf("unexpected worker id metadata: %#v", got)
	}
	if got := md.Get("x-duckgres-cp-instance-id"); len(got) != 1 || got[0] != "cp-live:boot-a" {
		t.Fatalf("unexpected cp instance metadata: %#v", got)
	}
}

type closeWaitFlightServer struct {
	pb.UnimplementedFlightServiceServer

	schema *arrow.Schema

	doGetStarted         chan struct{}
	doGetContextCanceled chan struct{}
	allowDoGetReturn     chan struct{}
	doGetDone            chan struct{}
	doActionCalled       chan struct{}
	doActionOnce         sync.Once
	releaseActionCalled  chan struct{}
	releaseActionOnce    sync.Once
	releaseTicket        []byte
	closeAfterFirstBatch bool
	doGetErr             error
	doActionErr          error
}

func newCloseWaitFlightServer() *closeWaitFlightServer {
	return &closeWaitFlightServer{
		schema:               arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil),
		doGetStarted:         make(chan struct{}),
		doGetContextCanceled: make(chan struct{}),
		allowDoGetReturn:     make(chan struct{}),
		doGetDone:            make(chan struct{}),
		doActionCalled:       make(chan struct{}),
		releaseActionCalled:  make(chan struct{}),
	}
}

func (s *closeWaitFlightServer) GetFlightInfo(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	ticket, err := flightsql.CreateStatementQueryTicket([]byte("query-1"))
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{
		Schema: flight.SerializeSchema(s.schema, memory.DefaultAllocator),
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: ticket},
		}},
	}, nil
}

func (s *closeWaitFlightServer) DoGet(_ *flight.Ticket, stream pb.FlightService_DoGetServer) error {
	close(s.doGetStarted)
	if s.doGetErr != nil {
		close(s.doGetDone)
		return s.doGetErr
	}

	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(1)
	arr := builder.NewArray()
	builder.Release()
	defer arr.Release()

	record := array.NewRecordBatch(s.schema, []arrow.Array{arr}, 1)
	defer record.Release()

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(s.schema), ipc.WithAllocator(memory.DefaultAllocator))
	if err := writer.Write(record); err != nil {
		return err
	}
	if s.closeAfterFirstBatch {
		close(s.doGetDone)
		return nil
	}

	<-stream.Context().Done()
	close(s.doGetContextCanceled)
	<-s.allowDoGetReturn
	close(s.doGetDone)
	return stream.Context().Err()
}

func (s *closeWaitFlightServer) DoAction(action *flight.Action, stream pb.FlightService_DoActionServer) error {
	switch action.Type {
	case releaseQueryHandleAction:
		s.releaseActionOnce.Do(func() {
			close(s.releaseActionCalled)
		})
		var payload wire.WorkerReleaseQueryHandlePayload
		if err := json.Unmarshal(action.Body, &payload); err != nil {
			return err
		}
		s.releaseTicket = payload.Ticket
		return stream.Send(&flight.Result{Body: []byte(`{"ok":true}`)})
	case waitSessionIdleAction:
	default:
		return s.UnimplementedFlightServiceServer.DoAction(action, stream)
	}
	s.doActionOnce.Do(func() {
		close(s.doActionCalled)
	})
	var payload wire.WorkerWaitSessionIdlePayload
	if err := json.Unmarshal(action.Body, &payload); err != nil {
		return err
	}
	if s.doActionErr != nil {
		return s.doActionErr
	}
	<-s.doGetDone
	return stream.Send(&flight.Result{Body: []byte(`{"ok":true}`)})
}

func newCloseWaitExecutor(t *testing.T, srv *closeWaitFlightServer) (*FlightExecutor, context.Context) {
	t.Helper()
	grpcSrv := grpc.NewServer()
	pb.RegisterFlightServiceServer(grpcSrv, srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	flightCli, err := flight.NewClientWithMiddlewareCtx(
		ctx, lis.Addr().String(), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("flight client: %v", err)
	}
	t.Cleanup(func() { _ = flightCli.Close() })

	exec := NewFlightExecutorFromClient(&flightsql.Client{Client: flightCli}, "session-1")
	t.Cleanup(func() { _ = exec.Close() })
	return exec, ctx
}

func TestFlightRowSetCloseWaitsForWorkerDoGetCleanup(t *testing.T) {
	srv := newCloseWaitFlightServer()
	exec, ctx := newCloseWaitExecutor(t, srv)

	rows, err := exec.QueryContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	select {
	case <-srv.doGetStarted:
	case <-time.After(time.Second):
		t.Fatal("DoGet did not start")
	}

	closeReturned := make(chan error, 1)
	go func() {
		closeReturned <- rows.Close()
	}()

	select {
	case err := <-closeReturned:
		t.Fatalf("Close returned before worker DoGet cleanup completed: %v", err)
	case <-srv.doGetContextCanceled:
	case <-time.After(time.Second):
		t.Fatal("worker DoGet did not observe cancellation")
	}

	select {
	case err := <-closeReturned:
		t.Fatalf("Close returned while worker DoGet cleanup was still blocked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(srv.allowDoGetReturn)
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after worker DoGet cleanup completed")
	}
}

func TestFlightRowSetCloseSkipsWaitAfterCleanEOF(t *testing.T) {
	srv := newCloseWaitFlightServer()
	srv.closeAfterFirstBatch = true
	exec, ctx := newCloseWaitExecutor(t, srv)

	rows, err := exec.QueryContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	if !rows.Next() {
		t.Fatalf("expected first row, err=%v", rows.Err())
	}
	if rows.Next() {
		t.Fatal("expected EOF after first row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rowset error: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-srv.doActionCalled:
		t.Fatal("Close called WaitSessionIdle after a clean EOF")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestFlightRowSetCloseSkipsWaitAfterExecutorMarkedDead(t *testing.T) {
	srv := newCloseWaitFlightServer()
	exec, ctx := newCloseWaitExecutor(t, srv)

	rows, err := exec.QueryContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	select {
	case <-srv.doGetStarted:
	case <-time.After(time.Second):
		t.Fatal("DoGet did not start")
	}

	exec.MarkDead()
	closeReturned := make(chan error, 1)
	go func() {
		closeReturned <- rows.Close()
	}()

	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		close(srv.allowDoGetReturn)
		t.Fatal("Close waited for worker idle after executor was marked dead")
	}
	select {
	case <-srv.doActionCalled:
		t.Fatal("Close called WaitSessionIdle after executor was marked dead")
	default:
	}
	close(srv.allowDoGetReturn)
}

func TestFlightRowSetCloseTreatsTerminalWaitFailureAsBestEffort(t *testing.T) {
	srv := newCloseWaitFlightServer()
	srv.doActionErr = status.Error(codes.Unavailable, "worker is gone")
	exec, ctx := newCloseWaitExecutor(t, srv)

	rows, err := exec.QueryContext(ctx, "SELECT 1")
	if err != nil {
		t.Fatalf("QueryContext: %v", err)
	}
	select {
	case <-srv.doGetStarted:
	case <-time.After(time.Second):
		t.Fatal("DoGet did not start")
	}

	closeReturned := make(chan error, 1)
	go func() {
		closeReturned <- rows.Close()
	}()
	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("Close returned terminal wait failure: %v", err)
		}
	case <-time.After(time.Second):
		close(srv.allowDoGetReturn)
		t.Fatal("Close did not return after terminal WaitSessionIdle failure")
	}
	close(srv.allowDoGetReturn)
}

func TestQueryContextReleasesHandleWhenDoGetFails(t *testing.T) {
	srv := newCloseWaitFlightServer()
	srv.doGetErr = status.Error(codes.Canceled, "context canceled")
	exec, ctx := newCloseWaitExecutor(t, srv)

	rows, err := exec.QueryContext(ctx, "SELECT 1")
	if err == nil {
		if rows != nil {
			_ = rows.Close()
		}
		t.Fatal("expected QueryContext to return DoGet error")
	}

	select {
	case <-srv.releaseActionCalled:
	case <-time.After(time.Second):
		t.Fatal("QueryContext did not release the abandoned query handle")
	}
	ticket, err := flightsql.GetStatementQueryTicket(&flight.Ticket{Ticket: srv.releaseTicket})
	if err != nil {
		t.Fatalf("release ticket did not decode: %v", err)
	}
	if got := string(ticket.GetStatementHandle()); got != "query-1" {
		t.Fatalf("released handle %q, want query-1", got)
	}

	select {
	case <-srv.doActionCalled:
	case <-time.After(time.Second):
		t.Fatal("QueryContext did not wait for the session to become idle after handle release")
	}
}
