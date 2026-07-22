package flightclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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

func TestCopyFromStdinSanitizesChunkSendFailure(t *testing.T) {
	executor, ctx := newCopyFromStdinTestExecutor(t, &rejectCopyFromStdinServer{})
	reader := &boundedZeroReader{remaining: 64 * CopyFromStdinChunkSize}

	rows, err := executor.CopyFromStdin(ctx, copyFromStdinTestRequest(), reader)
	if rows != 0 || err == nil || err.Error() != "COPY worker failed" {
		t.Fatalf("CopyFromStdin() = (%d, %v), want sanitized chunk-send error", rows, err)
	}
	if strings.Contains(err.Error(), "reject COPY upload") {
		t.Fatalf("chunk-send error leaked worker status: %q", err)
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
			wantMessage: "COPY worker failed",
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
			wantMessage: "COPY worker failed",
		},
		{
			name: "server error after acknowledgement",
			server: &scriptedCopyFromStdinServer{
				results:     []*pb.PutResult{copyFromStdinAck(t, 7)},
				terminalErr: status.Error(codes.Internal, "COPY failed after acknowledgement"),
			},
			wantMessage: "COPY worker failed",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			executor, ctx := newCopyFromStdinTestExecutor(t, tt.server)
			rows, err := executor.CopyFromStdin(ctx, copyFromStdinTestRequest(), strings.NewReader("one\n"))
			if rows != 0 || err == nil || !strings.Contains(err.Error(), tt.wantMessage) {
				t.Fatalf("CopyFromStdin() = (%d, %v), want safe error containing %q", rows, err, tt.wantMessage)
			}
		})
	}
}

func TestCopyFromStdinSanitizesLegacyWorkerStatus(t *testing.T) {
	const leakMarker = "generated COPY SQL VALUES ('private-value') /tmp/duckgres-copy-secret"
	executor, ctx := newCopyFromStdinTestExecutor(t, &scriptedCopyFromStdinServer{
		terminalErr: status.Error(codes.Internal, leakMarker),
	})

	rows, err := executor.CopyFromStdin(ctx, copyFromStdinTestRequest(), strings.NewReader("one\n"))
	if rows != 0 || err == nil {
		t.Fatalf("CopyFromStdin() = (%d, %v), want a sanitized worker error", rows, err)
	}
	if got := err.Error(); got != "COPY worker failed" {
		t.Fatalf("error = %q, want fixed worker failure text", got)
	}
	if strings.Contains(err.Error(), leakMarker) {
		t.Fatalf("sanitized error leaked worker status: %q", err)
	}
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("status.Code(error) = %s, want %s", got, codes.Internal)
	}
}

func TestCopyFromStdinUsesSafeWorkerErrorClassification(t *testing.T) {
	const leakMarker = "generated COPY SQL with private-value and /tmp/private-copy"
	rpcStatus, err := status.New(codes.InvalidArgument, leakMarker).WithDetails(&errdetails.ErrorInfo{
		Domain: "duckgres.copy",
		Reason: "LOAD_REJECTED",
	})
	if err != nil {
		t.Fatalf("attach ErrorInfo: %v", err)
	}
	executor, ctx := newCopyFromStdinTestExecutor(t, &scriptedCopyFromStdinServer{
		terminalErr: rpcStatus.Err(),
	})

	rows, copyErr := executor.CopyFromStdin(ctx, copyFromStdinTestRequest(), strings.NewReader("one\n"))
	if rows != 0 || copyErr == nil {
		t.Fatalf("CopyFromStdin() = (%d, %v), want classified worker error", rows, copyErr)
	}
	if got := copyErr.Error(); got != "COPY input was rejected by the destination" {
		t.Fatalf("error = %q, want fixed load-rejected message", got)
	}
	if strings.Contains(copyErr.Error(), leakMarker) {
		t.Fatalf("classified worker error leaked status text: %q", copyErr)
	}
	if got := status.Code(copyErr); got != codes.InvalidArgument {
		t.Fatalf("status.Code(error) = %s, want %s", got, codes.InvalidArgument)
	}
}

func TestCopyFromStdinPreservesSafeCancellationStatus(t *testing.T) {
	for _, tt := range []struct {
		name        string
		code        codes.Code
		wantMessage string
		wantTarget  error
	}{
		{
			name:        "canceled",
			code:        codes.Canceled,
			wantMessage: "COPY load canceled",
			wantTarget:  context.Canceled,
		},
		{
			name:        "deadline exceeded",
			code:        codes.DeadlineExceeded,
			wantMessage: "COPY load deadline exceeded",
			wantTarget:  context.DeadlineExceeded,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			executor, ctx := newCopyFromStdinTestExecutor(t, &scriptedCopyFromStdinServer{
				terminalErr: status.Error(tt.code, "private worker detail"),
			})

			rows, err := executor.CopyFromStdin(ctx, copyFromStdinTestRequest(), strings.NewReader("one\n"))
			if rows != 0 || err == nil {
				t.Fatalf("CopyFromStdin() = (%d, %v), want cancellation error", rows, err)
			}
			if got := err.Error(); got != tt.wantMessage {
				t.Fatalf("error = %q, want %q", got, tt.wantMessage)
			}
			if got := status.Code(err); got != tt.code {
				t.Fatalf("status.Code(error) = %s, want %s", got, tt.code)
			}
			if !errors.Is(err, tt.wantTarget) {
				t.Fatalf("errors.Is(%v, %v) = false", err, tt.wantTarget)
			}
		})
	}
}

func TestCopyFromStdinUsesWorkerCancellationReasonWithAbortedCode(t *testing.T) {
	for _, tt := range []struct {
		name        string
		reason      string
		wantMessage string
		wantTarget  error
	}{
		{
			name:        "canceled",
			reason:      "CANCELED",
			wantMessage: "COPY load canceled",
			wantTarget:  context.Canceled,
		},
		{
			name:        "deadline exceeded",
			reason:      "DEADLINE_EXCEEDED",
			wantMessage: "COPY load deadline exceeded",
			wantTarget:  context.DeadlineExceeded,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rpcStatus, detailsErr := status.New(codes.Aborted, "legacy private worker detail").WithDetails(&errdetails.ErrorInfo{
				Domain: "duckgres.copy",
				Reason: tt.reason,
			})
			if detailsErr != nil {
				t.Fatalf("attach ErrorInfo: %v", detailsErr)
			}

			err := sanitizeCopyFromStdinRPCError(rpcStatus.Err())

			if got := err.Error(); got != tt.wantMessage {
				t.Fatalf("error = %q, want %q", got, tt.wantMessage)
			}
			if got := status.Code(err); got != codes.Aborted {
				t.Fatalf("status.Code(error) = %s, want preserved %s", got, codes.Aborted)
			}
			if !errors.Is(err, tt.wantTarget) {
				t.Fatalf("errors.Is(%v, %v) = false", err, tt.wantTarget)
			}
		})
	}
}

func TestCopyFromStdinAcceptsWorkerUnavailableReasonForEmittedCodes(t *testing.T) {
	for _, code := range []codes.Code{codes.FailedPrecondition, codes.Aborted} {
		t.Run(code.String(), func(t *testing.T) {
			rpcStatus, detailsErr := status.New(code, "private worker state").WithDetails(&errdetails.ErrorInfo{
				Domain: "duckgres.copy",
				Reason: "UNAVAILABLE",
			})
			if detailsErr != nil {
				t.Fatalf("attach ErrorInfo: %v", detailsErr)
			}

			err := sanitizeCopyFromStdinRPCError(rpcStatus.Err())

			if got := err.Error(); got != "COPY worker is unavailable" {
				t.Fatalf("error = %q, want fixed unavailable message", got)
			}
			if got := status.Code(err); got != code {
				t.Fatalf("status.Code(error) = %s, want preserved %s", got, code)
			}
		})
	}
}

func TestSanitizeCopyFromStdinRPCErrorPreservesWrappedContextCancellation(t *testing.T) {
	for _, tt := range []struct {
		name        string
		err         error
		wantCode    codes.Code
		wantMessage string
		wantTarget  error
	}{
		{
			name:        "direct cancellation",
			err:         context.Canceled,
			wantCode:    codes.Canceled,
			wantMessage: "COPY load canceled",
			wantTarget:  context.Canceled,
		},
		{
			name:        "wrapped deadline",
			err:         fmt.Errorf("transport stopped: %w", context.DeadlineExceeded),
			wantCode:    codes.DeadlineExceeded,
			wantMessage: "COPY load deadline exceeded",
			wantTarget:  context.DeadlineExceeded,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := sanitizeCopyFromStdinRPCError(tt.err)
			if got := err.Error(); got != tt.wantMessage {
				t.Fatalf("error = %q, want %q", got, tt.wantMessage)
			}
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("status.Code(error) = %s, want %s", got, tt.wantCode)
			}
			if !errors.Is(err, tt.wantTarget) {
				t.Fatalf("errors.Is(%v, %v) = false", err, tt.wantTarget)
			}
		})
	}
}
