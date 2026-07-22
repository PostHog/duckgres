package sqlcore

import (
	"context"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// sanitizingClientStatsHandler prevents a legacy worker's arbitrary gRPC
// status description from becoming an OpenTelemetry span status. The
// application still receives the original error; only the stats event passed
// to the telemetry delegate is copied and redacted.
type sanitizingClientStatsHandler struct {
	delegate stats.Handler
}

func (h sanitizingClientStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return h.delegate.TagRPC(ctx, info)
}

func (h sanitizingClientStatsHandler) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if end, ok := event.(*stats.End); ok && end.Error != nil {
		redacted := *end
		code := status.Code(end.Error)
		if code == codes.OK {
			code = codes.Unknown
		}
		redacted.Error = status.Error(code, "worker RPC failed")
		event = &redacted
	}
	h.delegate.HandleRPC(ctx, event)
}

func (h sanitizingClientStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return h.delegate.TagConn(ctx, info)
}

func (h sanitizingClientStatsHandler) HandleConn(ctx context.Context, event stats.ConnStats) {
	h.delegate.HandleConn(ctx, event)
}

// OTELGRPCClientHandler returns a gRPC StatsHandler that creates spans only
// for query-related Flight SQL RPCs (GetFlightInfo, DoGet, DoPut), filtering
// out noisy control-plane calls (DoAction for health checks, session
// management). DoPut covers ExecuteUpdate (non-result-returning queries:
// CTAS, INSERT, UPDATE, DELETE, DDL) — without it those queries had no
// Flight wall-time visibility in traces.
func OTELGRPCClientHandler() grpc.DialOption {
	delegate := otelgrpc.NewClientHandler(
		otelgrpc.WithFilter(func(info *stats.RPCTagInfo) bool {
			return strings.Contains(info.FullMethodName, "GetFlightInfo") ||
				strings.Contains(info.FullMethodName, "DoGet") ||
				strings.Contains(info.FullMethodName, "DoPut")
		}),
	)
	return grpc.WithStatsHandler(sanitizingClientStatsHandler{delegate: delegate})
}
