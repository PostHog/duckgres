package duckdbservice

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// profilingMetadataKey is the gRPC metadata key used to pass DuckDB profiling
// output from the worker back to the control plane.
const profilingMetadataKey = "x-duckgres-profiling"

// sendProfilingMetadata queries the last profiling output from the session's
// DuckDB connection and sends it as gRPC trailing metadata.
func sendProfilingMetadata(ctx context.Context, session *Session) {
	var output string
	err := session.Conn.QueryRowContext(ctx, "SELECT json(profiling_output) FROM pragma_last_profiling_output()").Scan(&output)
	if err != nil || output == "" {
		return
	}
	_ = grpc.SetTrailer(ctx, metadata.Pairs(profilingMetadataKey, output))
}
