package main

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// multiHandler fans out slog records to multiple handlers.
type multiHandler struct {
	handlers []slog.Handler
}

func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range m.handlers {
		if h.Enabled(ctx, r.Level) {
			_ = h.Handle(ctx, r)
		}
	}
	return nil
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		handlers[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: handlers}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		handlers[i] = h.WithGroup(name)
	}
	return &multiHandler{handlers: handlers}
}

// initLogging configures slog to send logs to PostHog via OTLP when
// POSTHOG_API_KEY is set. Logs always go to stderr; PostHog is additive.
// Returns a shutdown function that flushes the OTLP batch processor.
func initLogging() func() {
	apiKey := os.Getenv("POSTHOG_API_KEY")
	if apiKey == "" {
		return func() {}
	}

	host := os.Getenv("POSTHOG_HOST")
	if host == "" {
		host = "us.i.posthog.com"
	}

	ctx := context.Background()

	exporter, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpoint(host),
		otlploghttp.WithURLPath("/i/v1/logs"),
		otlploghttp.WithHeaders(map[string]string{
			"Authorization": "Bearer " + apiKey,
		}),
	)
	if err != nil {
		slog.Error("Failed to create PostHog log exporter, continuing with stderr only.", "error", err)
		return func() {}
	}

	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)

	otelHandler := otelslog.NewHandler("duckgres", otelslog.WithLoggerProvider(provider))
	textHandler := slog.NewTextHandler(os.Stderr, nil)

	slog.SetDefault(slog.New(&multiHandler{
		handlers: []slog.Handler{textHandler, otelHandler},
	}))

	slog.Info("PostHog logging enabled.", "host", host)

	return func() {
		_ = provider.Shutdown(context.Background())
	}
}
