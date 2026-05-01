package sqlcore

import (
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// OTELGRPCClientHandler returns a gRPC StatsHandler that creates spans only
// for query-related Flight SQL RPCs (GetFlightInfo, DoGet), filtering out
// noisy control-plane calls (DoAction for health checks, session management).
func OTELGRPCClientHandler() grpc.DialOption {
	return grpc.WithStatsHandler(otelgrpc.NewClientHandler(
		otelgrpc.WithFilter(func(info *stats.RPCTagInfo) bool {
			// Only trace query execution RPCs, not session/health management.
			return strings.Contains(info.FullMethodName, "GetFlightInfo") ||
				strings.Contains(info.FullMethodName, "DoGet")
		}),
	))
}
