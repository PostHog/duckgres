package sqlcore

import (
	"context"
	"net"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type captureStatsHandler struct {
	rpc stats.RPCStats
}

func (h *captureStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *captureStatsHandler) HandleRPC(_ context.Context, event stats.RPCStats) {
	h.rpc = event
}

func (h *captureStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *captureStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

func TestSanitizingClientStatsHandlerRedactsRPCStatusDescription(t *testing.T) {
	const leakMarker = "generated SQL private-value /tmp/worker-copy-secret"
	delegate := &captureStatsHandler{}
	handler := sanitizingClientStatsHandler{delegate: delegate}
	original := &stats.End{
		Client: true,
		Error:  status.Error(codes.InvalidArgument, leakMarker),
	}

	handler.HandleRPC(context.Background(), original)

	end, ok := delegate.rpc.(*stats.End)
	if !ok {
		t.Fatalf("delegate event = %T, want *stats.End", delegate.rpc)
	}
	if got := status.Code(end.Error); got != codes.InvalidArgument {
		t.Fatalf("status code = %s, want %s", got, codes.InvalidArgument)
	}
	if got := status.Convert(end.Error).Message(); got != "worker RPC failed" {
		t.Fatalf("status message = %q, want fixed redacted message", got)
	}
	if got := status.Convert(original.Error).Message(); got != leakMarker {
		t.Fatalf("original stats event was mutated: message = %q, want %q", got, leakMarker)
	}
}

func TestSanitizingClientStatsHandlerPassesNonTerminalEventsThrough(t *testing.T) {
	delegate := &captureStatsHandler{}
	handler := sanitizingClientStatsHandler{delegate: delegate}
	event := &stats.OutPayload{Client: true, Length: 42}

	handler.HandleRPC(context.Background(), event)

	if delegate.rpc != event {
		t.Fatalf("delegate event = %p, want original %p", delegate.rpc, event)
	}
}

func TestOTELGRPCClientHandlerRedactsLegacyStatusInFinishedSpan(t *testing.T) {
	const leakMarker = "generated SQL private-value /tmp/legacy-worker-copy-secret"
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer(grpc.UnknownServiceHandler(func(any, grpc.ServerStream) error {
		return status.Error(codes.InvalidArgument, leakMarker)
	}))
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		_ = provider.Shutdown(context.Background())
	})

	conn, err := grpc.NewClient(
		"passthrough:///worker",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		OTELGRPCClientHandler(),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	callErr := conn.Invoke(
		context.Background(),
		"/test.Privacy/DoPut",
		&emptypb.Empty{},
		&emptypb.Empty{},
	)
	if callErr == nil || !strings.Contains(callErr.Error(), leakMarker) {
		t.Fatalf("application error = %v, want original legacy status", callErr)
	}

	var doPutSpan sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if strings.Contains(span.Name(), "DoPut") {
			doPutSpan = span
			break
		}
	}
	if doPutSpan == nil {
		t.Fatalf("ended spans = %d, want a DoPut client span", len(recorder.Ended()))
	}
	if got := doPutSpan.Status().Description; got != "worker RPC failed" {
		t.Fatalf("span status description = %q, want redacted status", got)
	}
	if strings.Contains(doPutSpan.Status().Description, leakMarker) {
		t.Fatalf("span status leaked legacy worker detail: %q", doPutSpan.Status().Description)
	}
	for _, attr := range doPutSpan.Attributes() {
		switch string(attr.Key) {
		case "rpc.grpc.status_code":
			if got := codes.Code(attr.Value.AsInt64()); got != codes.InvalidArgument {
				t.Fatalf("span gRPC status code = %s, want %s", got, codes.InvalidArgument)
			}
			return
		case "rpc.response.status_code":
			if got := attr.Value.AsString(); got != "INVALID_ARGUMENT" {
				t.Fatalf("span RPC status code = %q, want INVALID_ARGUMENT", got)
			}
			return
		}
	}
	t.Fatalf("DoPut span is missing a gRPC status-code attribute; attributes = %v", doPutSpan.Attributes())
}
