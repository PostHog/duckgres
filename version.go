package main

import (
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
)

// version, commit and date are populated at link time via -ldflags -X by the
// Dockerfile, justfile and release workflow. When the binary is built without
// those flags (local `go run`/`go build`), enrichBuildInfo backfills commit
// and date from runtime/debug.BuildInfo VCS settings, which the go toolchain
// embeds automatically from the local git checkout (Go 1.18+). ldflags values
// always win — the fallback only fires when the default sentinel is still set.
var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

var enrichBuildInfoOnce sync.Once

func enrichBuildInfo() {
	enrichBuildInfoOnce.Do(func() {
		info, ok := debug.ReadBuildInfo()
		if !ok {
			return
		}
		// Only backfill commit from VCS when ldflags did not provide one.
		// The dirty marker is scoped to the VCS-backfilled case too — if
		// ldflags injected a commit, the release pipeline owns that value
		// and we should not mutate it.
		commitFromVCS := commit == "unknown"
		var dirty bool
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				if commitFromVCS && s.Value != "" {
					commit = s.Value
					if len(commit) > 12 {
						commit = commit[:12]
					}
				}
			case "vcs.time":
				if date == "unknown" && s.Value != "" {
					date = s.Value
				}
			case "vcs.modified":
				if s.Value == "true" {
					dirty = true
				}
			}
		}
		if dirty && commitFromVCS && commit != "unknown" {
			commit += "-dirty"
		}
	})
}

func versionString() string {
	enrichBuildInfo()
	return fmt.Sprintf("duckgres version %s (commit: %s, built: %s)", version, commit, date)
}

// logBuildInfo emits a single startup line identifying the binary's version,
// git commit and build time, tagged with the run mode. Call once per process
// at startup so log readers can quickly correlate behavior with a specific
// build, especially in K8s where worker pods inherit the control plane image.
func logBuildInfo(mode string) {
	enrichBuildInfo()
	slog.Info("Duckgres build info.",
		"mode", mode,
		"version", version,
		"commit", commit,
		"built", date,
	)
}
