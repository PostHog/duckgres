// Package cliboot owns the cross-binary boot scaffolding shared by the
// all-in-one duckgres binary, cmd/duckgres-worker, and cmd/duckgres-controlplane.
// It centralizes build-info reporting, slog/redaction wiring, and OTLP
// log/trace exporters so production observability stays identical regardless
// of which entry point runs.
package cliboot

import (
	"fmt"
	"log/slog"
	"runtime/debug"
)

// BuildInfo carries the version/commit/date triple that each binary
// receives via -ldflags -X main.* at link time. Each package main owns
// its own var version/commit/date because ldflags can only target
// package-main symbols; cliboot just reads them via a value-typed struct.
type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

// Enrich backfills Commit/Date from runtime/debug VCS settings when
// ldflags did not provide them — useful for local `go run` / `go build`
// where the linker flags aren't passed. ldflags-supplied values always
// win; the dirty marker only applies to VCS-backfilled commits because
// the release pipeline owns ldflags-injected values.
func (b *BuildInfo) Enrich() {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	commitFromVCS := b.Commit == "unknown"
	var dirty bool
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			if commitFromVCS && s.Value != "" {
				b.Commit = s.Value
				if len(b.Commit) > 12 {
					b.Commit = b.Commit[:12]
				}
			}
		case "vcs.time":
			if b.Date == "unknown" && s.Value != "" {
				b.Date = s.Value
			}
		case "vcs.modified":
			if s.Value == "true" {
				dirty = true
			}
		}
	}
	if dirty && commitFromVCS && b.Commit != "unknown" {
		b.Commit += "-dirty"
	}
}

// String renders the human-readable single-line version string used by
// `--version` / `-v`.
func (b BuildInfo) String() string {
	return fmt.Sprintf("duckgres version %s (commit: %s, built: %s)", b.Version, b.Commit, b.Date)
}

// Log emits the canonical startup line tagging the binary's mode and
// build identity. Call once per process at startup so log readers can
// correlate behavior to a specific build, especially in K8s where
// worker pods often inherit the control plane image.
func (b BuildInfo) Log(mode string) {
	slog.Info("Duckgres build info.",
		"mode", mode,
		"version", b.Version,
		"commit", b.Commit,
		"built", b.Date,
	)
}
