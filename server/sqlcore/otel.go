package sqlcore

import (
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// OTELGRPCClientHandler returns a gRPC StatsHandler that creates spans only
// for query-related Flight SQL RPCs (GetFlightInfo, DoGet, DoPut), filtering
// out noisy control-plane calls (DoAction for health checks, session
// management). DoPut covers ExecuteUpdate (non-result-returning queries:
// CTAS, INSERT, UPDATE, DELETE, DDL) — without it those queries had no
// Flight wall-time visibility in traces.
func OTELGRPCClientHandler() grpc.DialOption {
	return grpc.WithStatsHandler(otelgrpc.NewClientHandler(
		otelgrpc.WithFilter(func(info *stats.RPCTagInfo) bool {
			return strings.Contains(info.FullMethodName, "GetFlightInfo") ||
				strings.Contains(info.FullMethodName, "DoGet") ||
				strings.Contains(info.FullMethodName, "DoPut")
		}),
	))
}
