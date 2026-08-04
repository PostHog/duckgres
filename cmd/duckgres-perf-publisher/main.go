package main

import (
	"context"
	"fmt"
	"os"

	"github.com/posthog/duckgres/tests/perf/publishercli"
)

func main() {
	if err := publishercli.Run(context.Background(), os.Args[1:], publishercli.Dependencies{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
	}); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
