package main

import "github.com/posthog/duckgres/internal/cliboot"

// version, commit and date are populated at link time via -ldflags -X by the
// Dockerfile, justfile and release workflow. Each duckgres binary
// (root, cmd/duckgres-worker, cmd/duckgres-controlplane) holds its own
// package-main copy because ldflags can only target package-main symbols.
// The actual build-info logic lives in internal/cliboot.
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func buildInfo() cliboot.BuildInfo {
	bi := cliboot.BuildInfo{Version: version, Commit: commit, Date: date}
	bi.Enrich()
	return bi
}
