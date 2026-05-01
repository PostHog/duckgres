// duckgres-controlplane is a control-plane-only entry point that does NOT
// link the embedded DuckDB driver. It accepts the same configuration as the
// all-in-one duckgres binary running in `--mode control-plane`, but routes
// queries exclusively to remote duckdb-service workers over Arrow Flight SQL.
//
// The build is verified to be duckdb-go-free via:
//
//	go list -deps ./cmd/duckgres-controlplane | grep duckdb-go   # empty
//
// At runtime, attempts to use standalone or worker modes are rejected at
// startup; this binary only supports `--mode control-plane`.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/posthog/duckgres/controlplane"
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
		fmt.Fprintln(os.Stderr, "`--mode control-plane --worker-backend remote`. See the duckgres")
		fmt.Fprintln(os.Stderr, "README for the full flag set; this stub accepts a single -config")
		fmt.Fprintln(os.Stderr, "flag pointing at a YAML file for now.")
		flag.PrintDefaults()
	}

	configPath := flag.String("config", "", "path to duckgres.yaml configuration")
	flag.Parse()

	if *configPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	// TODO: load YAML, resolve effective config, build ControlPlaneConfig.
	// For the first cut this binary only proves the import graph stays
	// duckdb-free; wiring it through the existing config-resolution code in
	// main.go (which lives in the same package as the all-in-one binary) is
	// the follow-up. Until then, error out clearly.
	slog.Error("duckgres-controlplane stub: config loading is not wired up yet — use the all-in-one duckgres binary in --mode control-plane while this is being built out.")
	_ = controlplane.RunControlPlane // keep the import live so go list -deps reflects the real CP graph
	os.Exit(1)
}
