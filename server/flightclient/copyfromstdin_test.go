package flightclient

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/posthog/duckgres/server/sqlcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// fakeCopyFromStdinServer captures the descriptor and assembled CSV bytes
// from a CopyFromStdin DoPut and replies with a fixed RecordCount.
type fakeCopyFromStdinServer struct {
	pb.UnimplementedFlightServiceServer

	rowsAffected int64

	gotDescriptor *flight.FlightDescriptor
	gotBytes      bytes.Buffer
	frames        int
}

func (s *fakeCopyFromStdinServer) DoPut(stream pb.FlightService_DoPutServer) error {
	for {
		fd, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if d := fd.GetFlightDescriptor(); d != nil {
			s.gotDescriptor = d
		}
		body := fd.GetDataBody()
		if len(body) > 0 {
			s.gotBytes.Write(body)
			s.frames++
		}
	}

	updateResult := &pb.DoPutUpdateResult{RecordCount: s.rowsAffected}
	metaBytes, err := proto.Marshal(updateResult)
	if err != nil {
		return err
	}
	return stream.Send(&pb.PutResult{AppMetadata: metaBytes})
}

func TestCopyFromStdinStreamsBytesAndDescriptor(t *testing.T) {
	srv := &fakeCopyFromStdinServer{rowsAffected: 1234}

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

	exec := NewFlightExecutorFromClient(sqlClient, "test-session")
	defer func() { _ = exec.Close() }()

	// Build a CSV payload bigger than one chunk so we exercise multi-frame send.
	payload := bytes.Repeat([]byte("a,b,c\n"), CopyFromStdinChunkSize/3)
	copySQL := "COPY t FROM '" + CopyFromStdinPathPlaceholder + "' (FORMAT CSV)"

	request := sqlcore.CopyFromStdinRequest{
		SQLTemplate:                     copySQL,
		PostgresBinaryDatabaseTypeNames: []string{"BIGINT", "DECIMAL(18,4)"},
	}
	rows, err := exec.CopyFromStdin(ctx, request, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("CopyFromStdin: %v", err)
	}
	if rows != 1234 {
		t.Errorf("rows = %d, want 1234", rows)
	}

	if srv.gotDescriptor == nil {
		t.Fatal("server did not receive FlightDescriptor")
	}
	if srv.gotDescriptor.Type != flight.DescriptorPATH ||
		len(srv.gotDescriptor.Path) == 0 ||
		srv.gotDescriptor.Path[0] != CopyFromStdinDescriptorPath {
		t.Errorf("descriptor routing wrong: %+v", srv.gotDescriptor)
	}
	if string(srv.gotDescriptor.Cmd) == copySQL {
		t.Error("binary descriptor.Cmd contains executable SQL instead of a versioned request")
	}
	var gotRequest sqlcore.CopyFromStdinRequest
	if err := json.Unmarshal(srv.gotDescriptor.Cmd, &gotRequest); err != nil {
		t.Fatalf("descriptor.Cmd is not a structured binary request: %v", err)
	}
	if gotRequest.SQLTemplate != request.SQLTemplate ||
		strings.Join(gotRequest.PostgresBinaryDatabaseTypeNames, "|") != strings.Join(request.PostgresBinaryDatabaseTypeNames, "|") {
		t.Errorf("descriptor request = %#v, want %#v", gotRequest, request)
	}
	wantPath := []string{
		CopyFromStdinDescriptorPath,
		CopyFromStdinPostgresBinaryPathVersion,
	}
	if strings.Join(srv.gotDescriptor.Path, "|") != strings.Join(wantPath, "|") {
		t.Errorf("descriptor.Path = %q, want %q", srv.gotDescriptor.Path, wantPath)
	}
	if !bytes.Equal(srv.gotBytes.Bytes(), payload) {
		t.Errorf("payload mismatch: got %d bytes, want %d", srv.gotBytes.Len(), len(payload))
	}
	if srv.frames < 2 {
		t.Errorf("expected multiple frames for multi-MB payload, got %d", srv.frames)
	}
	if !strings.Contains(copySQL, CopyFromStdinPathPlaceholder) {
		t.Fatalf("test bug: copySQL must contain placeholder")
	}
}
