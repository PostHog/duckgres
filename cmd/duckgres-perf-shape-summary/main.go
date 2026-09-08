package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/posthog/duckgres/tests/perf/shapecompare"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("duckgres-perf-shape-summary", flag.ContinueOnError)
	dir := flags.String("artifacts-dir", "", "required directory containing one artifact directory per Trino shape")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *dir == "" || flags.NArg() != 0 {
		return fmt.Errorf("usage: duckgres-perf-shape-summary --artifacts-dir <directory>")
	}
	report, err := shapecompare.Generate(*dir)
	if _, writeErr := io.WriteString(stdout, report); writeErr != nil {
		return writeErr
	}
	return err
}
