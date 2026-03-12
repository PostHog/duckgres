package perf

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/posthog/duckgres/tests/perf/publisher"
)

func TestPublishArtifactsIfConfiguredSkipsWhenDisabled(t *testing.T) {
	called := false
	orig := publishRunDir
	publishRunDir = func(context.Context, publisher.Config, string) error {
		called = true
		return nil
	}
	t.Cleanup(func() { publishRunDir = orig })

	err := publishArtifactsIfConfigured(context.Background(), publisher.Config{}, "/tmp/run")
	if err != nil {
		t.Fatalf("publishArtifactsIfConfigured returned error: %v", err)
	}
	if called {
		t.Fatalf("expected publisher not to be called")
	}
}

func TestPublishArtifactsIfConfiguredInvokesPublisher(t *testing.T) {
	called := false
	gotRunDir := ""
	orig := publishRunDir
	publishRunDir = func(_ context.Context, cfg publisher.Config, runDir string) error {
		called = true
		gotRunDir = runDir
		if cfg.Schema != "duckgres_perf" {
			t.Fatalf("schema = %q", cfg.Schema)
		}
		return nil
	}
	t.Cleanup(func() { publishRunDir = orig })

	err := publishArtifactsIfConfigured(context.Background(), publisher.Config{
		DSN:    "host=127.0.0.1 user=duckgres dbname=perf sslmode=require",
		Schema: "duckgres_perf",
	}, "/tmp/run")
	if err != nil {
		t.Fatalf("publishArtifactsIfConfigured returned error: %v", err)
	}
	if !called {
		t.Fatalf("expected publisher to be called")
	}
	if gotRunDir != "/tmp/run" {
		t.Fatalf("runDir = %q", gotRunDir)
	}
}

func TestPublishArtifactsIfConfiguredReturnsClearPublisherError(t *testing.T) {
	orig := publishRunDir
	publishRunDir = func(context.Context, publisher.Config, string) error {
		return errors.New("boom")
	}
	t.Cleanup(func() { publishRunDir = orig })

	err := publishArtifactsIfConfigured(context.Background(), publisher.Config{
		DSN:    "host=127.0.0.1 user=duckgres dbname=perf sslmode=require",
		Schema: "duckgres_perf",
	}, "/tmp/run")
	if err == nil {
		t.Fatal("expected publishArtifactsIfConfigured to return an error")
	}
	if !strings.Contains(err.Error(), "publish perf artifacts") {
		t.Fatalf("expected clear publish error, got %v", err)
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected wrapped publisher error, got %v", err)
	}
}
