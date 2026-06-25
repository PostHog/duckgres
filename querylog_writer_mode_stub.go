//go:build !kubernetes

package main

import (
	"context"
	"errors"

	"github.com/posthog/duckgres/configresolve"
	"github.com/posthog/duckgres/server"
)

func runQueryLogWriter(context.Context, server.Config, configresolve.Resolved) error {
	return errors.New("query-log-writer mode requires a kubernetes build")
}
