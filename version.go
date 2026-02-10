package main

import "fmt"

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func versionString() string {
	return fmt.Sprintf("duckgres version %s (commit: %s, built: %s)", version, commit, date)
}
