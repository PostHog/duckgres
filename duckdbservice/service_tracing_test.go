package duckdbservice

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/posthog/duckgres/server/sqlcore"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type traceContextFlightServer struct {
	pb.UnimplementedFlightServiceServer
	contexts chan context.Context
}

func (s *traceContextFlightServer) GetFlightInfo(ctx context.Context, _ *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	s.contexts <- ctx
	return &flight.FlightInfo{}, nil
}

func TestFlightServerOptionsExtractQueryTraceContext(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := trace.NewTracerProvider(trace.WithSpanProcessor(recorder))
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	})

	opts, err := flightServerOptions(ServiceConfig{}, nil)
	if err != nil {
		t.Fatalf("flightServerOptions: %v", err)
	}
	grpcServer := grpc.NewServer(opts...)
	server := &traceContextFlightServer{contexts: make(chan context.Context, 1)}
	pb.RegisterFlightServiceServer(grpcServer, server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		sqlcore.OTELGRPCClientHandler(),
	)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, parent := otel.Tracer("test").Start(context.Background(), "parent")
	defer parent.End()
	if _, err := pb.NewFlightServiceClient(conn).GetFlightInfo(ctx, &flight.FlightDescriptor{}); err != nil {
		t.Fatalf("GetFlightInfo: %v", err)
	}

	select {
	case handlerCtx := <-server.contexts:
		handlerSpanContext := oteltrace.SpanContextFromContext(handlerCtx)
		if !handlerSpanContext.IsValid() {
			t.Fatal("Flight handler did not receive an active span context")
		}
		if handlerSpanContext.TraceID() != oteltrace.SpanContextFromContext(ctx).TraceID() {
			t.Fatalf("handler trace ID = %s, want %s", handlerSpanContext.TraceID(), oteltrace.SpanContextFromContext(ctx).TraceID())
		}
		for _, span := range recorder.Ended() {
			spanContext := span.SpanContext()
			if spanContext.TraceID() == handlerSpanContext.TraceID() && spanContext.SpanID() == handlerSpanContext.SpanID() {
				if !span.Parent().IsRemote() {
					t.Fatal("Flight server span did not retain its remote parent")
				}
				return
			}
		}
		t.Fatal("Flight server span was not recorded")
	case <-time.After(time.Second):
		t.Fatal("Flight handler was not called")
	}
}
