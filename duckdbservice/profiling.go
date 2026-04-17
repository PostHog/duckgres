package duckdbservice

import (
	"context"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// profilingMetadataKey is the gRPC metadata key used to pass DuckDB profiling
// output from the worker back to the control plane.
const profilingMetadataKey = "x-duckgres-profiling"

// profilingOutputPath is the fixed file path where DuckDB writes profiling
// output. Only one query runs per worker at a time (control plane enforces
// this), so a single file is safe.
const profilingOutputPath = "/tmp/duckgres-profiling.json"

// sendProfilingMetadata reads the profiling output file written by DuckDB
// and sends it as gRPC trailing metadata so the control plane can attach
// it to the trace span.
func sendProfilingMetadata(ctx context.Context, session *Session) {
	data, err := os.ReadFile(profilingOutputPath)
	if err != nil || len(data) == 0 {
		return
	}
	_ = grpc.SetTrailer(ctx, metadata.Pairs(profilingMetadataKey, string(data)))
}
