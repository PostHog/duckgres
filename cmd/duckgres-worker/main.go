// duckgres-worker is the worker-only entry point that links the embedded
// DuckDB driver. It is the binary intended to ship in worker pods and the
// target of the per-DuckDB-version matrix build in CI.
//
// At runtime, this binary only supports `--mode duckdb-service` (the
// shape spawned by the control plane over Unix sockets / TCP). The
// standalone PG-wire path is not exposed here — for that, use the
// all-in-one duckgres binary in `--mode standalone`.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/posthog/duckgres/configloader"
	_ "github.com/posthog/duckgres/duckdbservice" // keep the import live so the worker binary actually links libduckdb via duckdbservice
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n", os.Args[0])
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "DuckDB-service-only duckgres binary. Links libduckdb.")
		fmt.Fprintln(os.Stderr, "Serves Arrow Flight SQL on a local Unix socket or TCP port,")
		fmt.Fprintln(os.Stderr, "spawned by the control plane.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "This binary is the target of the per-DuckDB-version matrix build")
		fmt.Fprintln(os.Stderr, "in CI: a worker pod ships exactly one DuckDB version, pinned via")
		fmt.Fprintln(os.Stderr, "go.mod + the Dockerfile DUCKDB_EXTENSION_VERSION build arg.")
		flag.PrintDefaults()
	}

	configPath := flag.String("config", "", "path to duckgres.yaml configuration")
	flag.Parse()

	if *configPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	cfg, err := configloader.LoadFile(*configPath)
	if err != nil {
		slog.Error("failed to load config", "path", *configPath, "error", err)
		os.Exit(1)
	}
	slog.Info("duckgres-worker: loaded config",
		"path", *configPath,
		"data_dir", cfg.DataDir,
		"memory_limit", cfg.MemoryLimit,
		"threads", cfg.Threads,
		"ducklake_metadata_store_set", cfg.DuckLake.MetadataStore != "",
	)

	// TODO: build duckdbservice.ServiceConfig from cfg, then call
	// duckdbservice.Run. Today resolveEffectiveConfig in the all-in-one
	// main.go does this — lifting it into a shared resolution package
	// both binaries can call is the follow-up to PR #505 (which
	// extracted the YAML schema). Until then, error out so no one
	// mistakes this for a working worker binary.
	slog.Error("duckgres-worker: config-resolution wiring is still in the all-in-one main.go — use the all-in-one duckgres binary in --mode duckdb-service while this is being built out.")
	os.Exit(1)
}
