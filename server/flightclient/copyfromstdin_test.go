package flightclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/posthog/duckgres/server/sqlcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

type scriptedCopyFromStdinServer struct {
	pb.UnimplementedFlightServiceServer

	results     []*pb.PutResult
	terminalErr error
}

func (s *scriptedCopyFromStdinServer) DoPut(stream pb.FlightService_DoPutServer) error {
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	for _, result := range s.results {
		if err := stream.Send(result); err != nil {
			return err
		}
	}
	return s.terminalErr
}

type sourceFailureCopyFromStdinServer struct {
	pb.UnimplementedFlightServiceServer

	chunkSeen chan struct{}
	recvErr   chan error
	once      sync.Once
}

func (s *sourceFailureCopyFromStdinServer) DoPut(stream pb.FlightService_DoPutServer) error {
	for {
		data, err := stream.Recv()
		if err != nil {
			s.recvErr <- err
			return nil
		}
		if len(data.GetDataBody()) > 0 {
			s.once.Do(func() { close(s.chunkSeen) })
		}
	}
}

type rejectCopyFromStdinServer struct {
	pb.UnimplementedFlightServiceServer
}

func (s *rejectCopyFromStdinServer) DoPut(stream pb.FlightService_DoPutServer) error {
	// Accept the descriptor before rejecting the upload. Without this handshake,
	// scheduler timing decides whether the client observes a descriptor-send or
	// chunk-send failure, making the phase-specific assertion flaky.
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return status.Error(codes.ResourceExhausted, "reject COPY upload")
}

type errorAfterSignalReader struct {
	payload []byte
	signal  <-chan struct{}
	err     error
	sent    bool
}

func (r *errorAfterSignalReader) Read(dst []byte) (int, error) {
	if !r.sent {
		r.sent = true
		return copy(dst, r.payload), nil
	}
	<-r.signal
	return 0, r.err
}

type boundedZeroReader struct {
	remaining int64
}

func (r *boundedZeroReader) Read(dst []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := int64(len(dst))
	if n > r.remaining {
		n = r.remaining
	}
	r.remaining -= n
	return int(n), nil
}

func newCopyFromStdinTestExecutor(t *testing.T, server pb.FlightServiceServer) (*FlightExecutor, context.Context) {
	t.Helper()
	grpcServer := grpc.NewServer()
	pb.RegisterFlightServiceServer(grpcServer, server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = grpcServer.Serve(listener) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	flightClient, err := flight.NewClientWithMiddlewareCtx(
		ctx,
		listener.Addr().String(),
		nil,
		nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		cancel()
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("flight client: %v", err)
	}
	executor := NewFlightExecutorFromClient(&flightsql.Client{Client: flightClient}, "test-session")
	t.Cleanup(func() {
		_ = executor.Close()
		_ = flightClient.Close()
		cancel()
		grpcServer.Stop()
		_ = listener.Close()
	})
	return executor, ctx
}

func copyFromStdinAck(t *testing.T, rows int64) *pb.PutResult {
	t.Helper()
	metadata, err := proto.Marshal(&pb.DoPutUpdateResult{RecordCount: rows})
	if err != nil {
		t.Fatal(err)
	}
	return &pb.PutResult{AppMetadata: metadata}
}

func copyFromStdinTestRequest() sqlcore.CopyFromStdinRequest {
	return sqlcore.CopyFromStdinRequest{
		SQLTemplate: "COPY t FROM '" + CopyFromStdinPathPlaceholder + "' (FORMAT CSV)",
	}
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

func TestCopyFromStdinKeepsLegacyCSVDescriptorCompatible(t *testing.T) {
	server := &fakeCopyFromStdinServer{rowsAffected: 1}
	executor, ctx := newCopyFromStdinTestExecutor(t, server)
	request := copyFromStdinTestRequest()

	rows, err := executor.CopyFromStdin(ctx, request, strings.NewReader("one\n"))
	if err != nil || rows != 1 {
		t.Fatalf("CopyFromStdin() = (%d, %v), want (1, nil)", rows, err)
	}
	if server.gotDescriptor == nil {
		t.Fatal("server did not receive FlightDescriptor")
	}
	wantPath := []string{CopyFromStdinDescriptorPath}
	if strings.Join(server.gotDescriptor.Path, "|") != strings.Join(wantPath, "|") {
		t.Fatalf("descriptor.Path = %q, want %q", server.gotDescriptor.Path, wantPath)
	}
	if got := string(server.gotDescriptor.Cmd); got != request.SQLTemplate {
		t.Fatalf("descriptor.Cmd = %q, want raw SQL %q", got, request.SQLTemplate)
	}
}

func TestCopyFromStdinCancelsUploadOnSourceFailure(t *testing.T) {
	server := &sourceFailureCopyFromStdinServer{
		chunkSeen: make(chan struct{}),
		recvErr:   make(chan error, 1),
	}
	executor, ctx := newCopyFromStdinTestExecutor(t, server)
	sourceErr := errors.New("source failed")
	reader := &errorAfterSignalReader{
		payload: []byte("partial COPY payload"),
		signal:  server.chunkSeen,
		err:     sourceErr,
	}

	rows, err := executor.CopyFromStdin(ctx, copyFromStdinTestRequest(), reader)
	if rows != 0 || !errors.Is(err, sourceErr) || !strings.Contains(err.Error(), "read COPY stream") {
		t.Fatalf("CopyFromStdin() = (%d, %v), want wrapped source error", rows, err)
	}

	select {
	case recvErr := <-server.recvErr:
		if status.Code(recvErr) != codes.Canceled {
			t.Fatalf("server Recv() error = %v, want canceled upload", recvErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe canceled upload")
	}
}

func TestCopyFromStdinReportsChunkSendFailure(t *testing.T) {
	executor, ctx := newCopyFromStdinTestExecutor(t, &rejectCopyFromStdinServer{})
	reader := &boundedZeroReader{remaining: 64 * CopyFromStdinChunkSize}

	rows, err := executor.CopyFromStdin(ctx, copyFromStdinTestRequest(), reader)
	if rows != 0 || err == nil || !strings.Contains(err.Error(), "flight doput send chunk") {
		t.Fatalf("CopyFromStdin() = (%d, %v), want chunk-send error", rows, err)
	}
}

func TestCopyFromStdinReportsAckFailures(t *testing.T) {
	for _, tt := range []struct {
		name        string
		server      *scriptedCopyFromStdinServer
		wantMessage string
	}{
		{
			name:        "missing acknowledgement",
			server:      &scriptedCopyFromStdinServer{},
			wantMessage: "flight doput recv",
		},
		{
			name: "malformed acknowledgement",
			server: &scriptedCopyFromStdinServer{results: []*pb.PutResult{
				{AppMetadata: []byte{0xff}},
			}},
			wantMessage: "unmarshal DoPutUpdateResult",
		},
		{
			name: "server error before acknowledgement",
			server: &scriptedCopyFromStdinServer{
				terminalErr: status.Error(codes.Internal, "COPY failed before acknowledgement"),
			},
			wantMessage: "flight doput recv",
		},
		{
			name: "server error after acknowledgement",
			server: &scriptedCopyFromStdinServer{
				results:     []*pb.PutResult{copyFromStdinAck(t, 7)},
				terminalErr: status.Error(codes.Internal, "COPY failed after acknowledgement"),
			},
			wantMessage: "COPY failed after acknowledgement",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			executor, ctx := newCopyFromStdinTestExecutor(t, tt.server)
			rows, err := executor.CopyFromStdin(ctx, copyFromStdinTestRequest(), strings.NewReader("one\n"))
			if rows != 0 || err == nil || !strings.Contains(err.Error(), tt.wantMessage) {
				t.Fatalf("CopyFromStdin() = (%d, %v), want error containing %q", rows, err, tt.wantMessage)
			}
		})
	}
}
