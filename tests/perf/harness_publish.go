package perf

import (
	"context"
	"fmt"

	"github.com/posthog/duckgres/tests/perf/publisher"
)

// publishRunDir is the seam tests use to mock out publisher.PublishRunDir.
// It must live in a non-test file because harness_artifacts.go calls it
// (via publishArtifactsIfConfigured) under a regular `go build` — declaring
// it in *_test.go would make the build fail for non-test consumers.
var publishRunDir = publisher.PublishRunDir

// publishArtifactsIfConfigured invokes publishRunDir for cfg if it's
// enabled, returning a wrapped error so callers see context. A no-op when
// the publisher is disabled.
func publishArtifactsIfConfigured(ctx context.Context, cfg publisher.Config, runDir string) error {
	if !cfg.Enabled() {
		return nil
	}
	if err := publishRunDir(ctx, cfg, runDir); err != nil {
		return fmt.Errorf("publish perf artifacts: %w", err)
	}
	return nil
}
