// duckgres-worker is the worker-only entry point that links the embedded
// DuckDB driver. It is the binary intended to ship in worker pods and the
// target of the per-DuckDB-version matrix build in CI.
//
// At runtime, this binary only supports `--mode duckdb-service` (the
// shape spawned by the control plane over Unix sockets / TCP). The
// standalone PG-wire path is not exposed here — for that, use the
// all-in-one duckgres binary in `--mode standalone`.
//
// Configuration mirrors the all-in-one duckgres binary running in
// `--mode duckdb-service`. See the duckgres README for the full flag set;
// this stub accepts a single -config flag pointing at a YAML file for now.
// Wiring config resolution out of the all-in-one main package and into a
// shared package both binaries can use is a follow-up PR.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/posthog/duckgres/duckdbservice"
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

	// TODO: load YAML, resolve effective config, build duckdbservice.ServiceConfig.
	// For the first cut this binary only proves the worker entry point exists
	// and the duckdbservice import graph is healthy; wiring it through the
	// existing config-resolution code in main.go (which lives in the same
	// package as the all-in-one binary) is the follow-up. Until then, error
	// out clearly.
	slog.Error("duckgres-worker stub: config loading is not wired up yet — use the all-in-one duckgres binary in --mode duckdb-service while this is being built out.")
	_ = duckdbservice.Run // keep the import live so the worker binary actually links libduckdb via duckdbservice
	os.Exit(1)
}
