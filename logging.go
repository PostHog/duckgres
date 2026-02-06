package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
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

// newPostHogExporter creates an OTLP log exporter for a single PostHog API key.
// Returns nil if the exporter cannot be created.
func newPostHogExporter(ctx context.Context, host, apiKey string) *otlploghttp.Exporter {
	exporter, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpoint(host),
		otlploghttp.WithURLPath("/i/v1/logs"),
		otlploghttp.WithHeaders(map[string]string{
			"Authorization": "Bearer " + apiKey,
		}),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create PostHog log exporter for key %s…: %v\n", apiKey[:min(8, len(apiKey))], err)
		return nil
	}
	return exporter
}

// initLogging configures slog to send logs to PostHog via OTLP when
// POSTHOG_API_KEY is set. Additional PostHog projects can be targeted by
// setting ADDITIONAL_POSTHOG_API_KEYS to a comma-separated list of API keys.
// Logs always go to stderr; PostHog is additive.
// Returns a shutdown function that flushes all OTLP batch processors.
func initLogging() func() {
	apiKey := os.Getenv("POSTHOG_API_KEY")
	if apiKey == "" {
		if os.Getenv("ADDITIONAL_POSTHOG_API_KEYS") != "" {
			fmt.Fprintln(os.Stderr, "ADDITIONAL_POSTHOG_API_KEYS is set but POSTHOG_API_KEY is not; ignoring additional keys")
		}
		fmt.Fprintln(os.Stderr, "PostHog logging disabled (POSTHOG_API_KEY not set)")
		return func() {}
	}
	fmt.Fprintln(os.Stderr, "PostHog logging enabled, configuring OTLP exporter...")

	host := os.Getenv("POSTHOG_HOST")
	if host == "" {
		host = "us.i.posthog.com"
	}

	ctx := context.Background()

	// Collect all API keys: primary + additional.
	apiKeys := []string{apiKey}
	if additional := os.Getenv("ADDITIONAL_POSTHOG_API_KEYS"); additional != "" {
		seen := map[string]bool{apiKey: true}
		for _, raw := range strings.Split(additional, ",") {
			k := strings.TrimSpace(raw)
			if k == "" {
				continue
			}
			if seen[k] {
				fmt.Fprintf(os.Stderr, "Ignoring duplicate PostHog API key %s…\n", k[:min(8, len(k))])
				continue
			}
			seen[k] = true
			apiKeys = append(apiKeys, k)
		}
	}

	// The primary exporter must succeed; additional ones are best-effort.
	primaryExp := newPostHogExporter(ctx, host, apiKey)
	if primaryExp == nil {
		fmt.Fprintln(os.Stderr, "Primary PostHog exporter failed to initialize, continuing with stderr only")
		return func() {}
	}
	processors := []sdklog.LoggerProviderOption{
		sdklog.WithProcessor(sdklog.NewBatchProcessor(primaryExp)),
	}
	for _, k := range apiKeys[1:] {
		exp := newPostHogExporter(ctx, host, k)
		if exp == nil {
			continue
		}
		processors = append(processors, sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)))
	}

	serviceName := "duckgres"
	if id := os.Getenv("DUCKGRES_IDENTIFIER"); id != "" {
		serviceName = serviceName + "-" + id
	}

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
	)

	opts := append(processors, sdklog.WithResource(res))
	provider := sdklog.NewLoggerProvider(opts...)

	otelHandler := otelslog.NewHandler("duckgres", otelslog.WithLoggerProvider(provider))
	textHandler := slog.NewTextHandler(os.Stderr, nil)

	slog.SetDefault(slog.New(&multiHandler{
		handlers: []slog.Handler{textHandler, otelHandler},
	}))

	slog.Info("PostHog logging enabled.", "host", host, "exporters", len(processors))

	var once sync.Once
	return func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = provider.Shutdown(ctx)
		})
	}
}
