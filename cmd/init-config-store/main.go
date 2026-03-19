package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func main() {
	configStoreConn := flag.String("config-store", "postgres://duckgres:duckgres@127.0.0.1:5434/duckgres_config?sslmode=disable", "PostgreSQL config-store connection string")
	flag.Parse()

	if _, err := configstore.NewConfigStore(*configStoreConn, time.Hour); err != nil {
		exitf("initialize config store: %v", err)
	}
}

func exitf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
