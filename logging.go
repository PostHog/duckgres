package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// multiHandler fans out slog records to multiple handlers.
type multiHandler struct {
	handlers []slog.Handler
}

// Enabled returns true if any underlying handler is enabled for the given level.
func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

// Handle dispatches the log record to each underlying handler that is enabled for the record's level.
func (m *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range m.handlers {
		if h.Enabled(ctx, r.Level) {
			_ = h.Handle(ctx, r)
		}
	}
	return nil
}

// WithAttrs returns a new multiHandler with the given attributes applied to all underlying handlers.
func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		handlers[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: handlers}
}

// WithGroup returns a new multiHandler with the given group name applied to all underlying handlers.
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
		fmt.Fprintln(os.Stderr, "PostHog logging disabled (POSTHOG_API_KEY not set)")
		return func() {}
	}
	fmt.Fprintln(os.Stderr, "PostHog logging enabled, configuring OTLP exporter...")

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

	serviceName := "duckgres"
	if id := os.Getenv("DUCKGRES_IDENTIFIER"); id != "" {
		serviceName = serviceName + "-" + id
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
	)

	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		sdklog.WithResource(res),
	)

	otelHandler := otelslog.NewHandler("duckgres", otelslog.WithLoggerProvider(provider))
	textHandler := slog.NewTextHandler(os.Stderr, nil)

	slog.SetDefault(slog.New(&multiHandler{
		handlers: []slog.Handler{textHandler, otelHandler},
	}))

	slog.Info("PostHog logging enabled.", "host", host)

	var once sync.Once
	return func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = provider.Shutdown(ctx)
		})
	}
}
