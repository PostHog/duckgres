//go:build kubernetes

package main

import (
	"context"
	"errors"
	"testing"

	"github.com/posthog/duckgres/configresolve"
	"github.com/posthog/duckgres/server"
)

func TestRunQueryLogWriterBootstrapsBundledExtensions(t *testing.T) {
	prevBootstrap := queryLogWriterBootstrapBundledExtensions
	t.Cleanup(func() {
		queryLogWriterBootstrapBundledExtensions = prevBootstrap
	})

	bootstrapErr := errors.New("bootstrap failed")
	var gotDataDir string
	queryLogWriterBootstrapBundledExtensions = func(dataDir string) error {
		gotDataDir = dataDir
		return bootstrapErr
	}

	cfg := server.Config{
		DataDir: "/tmp/query-log-writer-test-data",
		QueryLog: server.QueryLogConfig{
			Enabled: true,
			Kafka: server.QueryLogKafkaConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "query-log",
			},
		},
	}

	err := runQueryLogWriter(context.Background(), cfg, configresolve.Resolved{})
	if !errors.Is(err, bootstrapErr) {
		t.Fatalf("expected bootstrap error, got %v", err)
	}
	if gotDataDir != cfg.DataDir {
		t.Fatalf("bootstrap data dir = %q, want %q", gotDataDir, cfg.DataDir)
	}
}
