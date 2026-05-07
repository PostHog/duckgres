package flightclient

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// fakeFlightServer implements just the DoPut method so we can verify the
// gRPC trailer flows through. Anything else returns Unimplemented via the
// embedded UnimplementedFlightServiceServer.
type fakeFlightServer struct {
	pb.UnimplementedFlightServiceServer
	rowsAffected int64
	trailer      metadata.MD
}

func (s *fakeFlightServer) DoPut(stream pb.FlightService_DoPutServer) error {
	// Consume the FlightDescriptor sent by the client. We don't bother
	// decoding it — the test only cares about the trailer round-trip.
	if _, err := stream.Recv(); err != nil {
		return err
	}

	updateResult := &pb.DoPutUpdateResult{RecordCount: s.rowsAffected}
	metaBytes, err := proto.Marshal(updateResult)
	if err != nil {
		return err
	}
	if err := stream.Send(&pb.PutResult{AppMetadata: metaBytes}); err != nil {
		return err
	}

	if s.trailer != nil {
		stream.SetTrailer(s.trailer)
	}
	return nil
}

// TestExecuteUpdateWithTrailerCapturesProfilingMetadata locks in the fix
// for the bidi-streaming trailer bug: ExecuteUpdate is a DoPut stream and
// grpc.Trailer(&md) as a CallOption only works for unary RPCs, so the
// previous implementation silently dropped the worker's per-query
// profiling metadata. executeUpdateWithTrailer drains the stream to EOF
// and reads stream.Trailer() instead.
func TestExecuteUpdateWithTrailerCapturesProfilingMetadata(t *testing.T) {
	expected := metadata.Pairs("x-duckgres-profiling", `{"latency": 0.001}`)
	srv := &fakeFlightServer{rowsAffected: 7, trailer: expected}

	grpcSrv := grpc.NewServer()
	pb.RegisterFlightServiceServer(grpcSrv, srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	flightCli, err := flight.NewClientWithMiddlewareCtx(
		ctx, lis.Addr().String(), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("flight client: %v", err)
	}
	defer func() { _ = flightCli.Close() }()
	sqlClient := &flightsql.Client{Client: flightCli}

	affected, trailer, err := executeUpdateWithTrailer(ctx, sqlClient, "INSERT INTO t VALUES (1)")
	if err != nil {
		t.Fatalf("executeUpdateWithTrailer: %v", err)
	}
	if affected != 7 {
		t.Errorf("rows affected: got %d, want 7", affected)
	}
	got := trailer.Get("x-duckgres-profiling")
	if len(got) != 1 || got[0] != `{"latency": 0.001}` {
		t.Fatalf("trailer x-duckgres-profiling: got %#v, want one entry %q", got, `{"latency": 0.001}`)
	}
}
