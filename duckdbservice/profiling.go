package duckdbservice

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// profilingMetadataKey is the gRPC metadata key used to pass DuckDB profiling
// output from the worker back to the control plane.
const profilingMetadataKey = "x-duckgres-profiling"

// profilingOutputPath is the fixed file path where DuckDB writes profiling
// output. Only one query runs per worker at a time (control plane enforces
// this), so a single file is safe.
var profilingOutputPath = "/tmp/duckgres-profiling.json"

// clearProfilingOutput removes the previous statement's profile before a
// statement begins. The returned timestamp is used to reject a profile that
// was not produced by this execution.
func clearProfilingOutput() time.Time {
	_ = os.Remove(profilingOutputPath)
	return time.Now()
}

func profilingMetadataSince(startedAt time.Time) string {
	info, err := os.Stat(profilingOutputPath)
	if err != nil || !info.ModTime().After(startedAt) {
		return ""
	}
	data, err := os.ReadFile(profilingOutputPath)
	if err != nil || len(data) == 0 {
		return ""
	}
	// Compact JSON to a single line — gRPC metadata values cannot contain newlines.
	var compact bytes.Buffer
	if json.Compact(&compact, data) != nil {
		return ""
	}
	return compact.String()
}

// sendProfilingMetadataSince sends the profile written by the execution that
// began at startedAt. It deliberately ignores absent, stale, and malformed
// files so canceled or failed statements cannot reuse an earlier profile.
func sendProfilingMetadataSince(ctx context.Context, startedAt time.Time) {
	if profile := profilingMetadataSince(startedAt); profile != "" {
		_ = grpc.SetTrailer(ctx, metadata.Pairs(profilingMetadataKey, profile))
	}
}
