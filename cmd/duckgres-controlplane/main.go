// duckgres-controlplane is a control-plane-only entry point that does NOT
// link the embedded DuckDB driver. It accepts the same configuration as the
// all-in-one duckgres binary running in `--mode control-plane`, but routes
// queries exclusively to remote duckdb-service workers over Arrow Flight SQL.
//
// The build is verified to be duckdb-go-free via:
//
//	go list -deps ./cmd/duckgres-controlplane | grep duckdb-go   # empty
//
// At runtime, this binary only supports control-plane mode; standalone /
// duckdb-service modes belong in the all-in-one duckgres binary or
// cmd/duckgres-worker respectively.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/posthog/duckgres/configloader"
	_ "github.com/posthog/duckgres/controlplane" // keep the import live so go list -deps reflects the real CP graph
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n", os.Args[0])
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Control-plane-only duckgres binary. Does NOT link libduckdb.")
		fmt.Fprintln(os.Stderr, "Routes all SQL execution to remote duckdb-service worker pods")
		fmt.Fprintln(os.Stderr, "via Arrow Flight SQL.")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Configuration mirrors the all-in-one duckgres binary running in")
		fmt.Fprintln(os.Stderr, "`--mode control-plane --worker-backend remote`. The CLI/env")
		fmt.Fprintln(os.Stderr, "flag plumbing currently lives in the all-in-one main.go and")
		fmt.Fprintln(os.Stderr, "will be lifted into a shared package in a follow-up PR; until")
		fmt.Fprintln(os.Stderr, "then this binary loads -config <path> as the source of truth.")
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
	slog.Info("duckgres-controlplane: loaded config",
		"path", *configPath,
		"data_dir", cfg.DataDir,
		"port", cfg.Port,
		"flight_port", cfg.FlightPort,
		"worker_backend", cfg.WorkerBackend,
		"k8s_worker_image", cfg.K8s.WorkerImage,
		"ducklake_default_spec_version", cfg.DuckLake.DefaultSpecVersion,
	)

	// TODO: build controlplane.ControlPlaneConfig from cfg, then call
	// controlplane.RunControlPlane(cpCfg). Today resolveEffectiveConfig in
	// the all-in-one main.go does this — lifting it into a shared
	// resolution package both binaries can call is the follow-up to PR
	// #505 (which extracted the YAML schema). Until then, error out so
	// no one mistakes this for a working CP binary.
	slog.Error("duckgres-controlplane: config-resolution wiring is still in the all-in-one main.go — use the all-in-one duckgres binary in --mode control-plane while this is being built out.")
	os.Exit(1)
}
