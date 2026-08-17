package cliboot

import (
	"context"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

const dropStartingMetricsServer = "Starting metrics server"

// newPostHogBranch is the OTLP-only stack: level → INFO sample → drop → strip.
func newPostHogBranch(inner slog.Handler, level slog.Level, infoSample float64, queryText string) slog.Handler {
	return &posthogLevelHandler{
		level: level,
		inner: &infoSampleHandler{
			sample: infoSample,
			inner: &dropMessageHandler{
				drop: map[string]struct{}{dropStartingMetricsServer: {}},
				inner: &QueryStripHandler{
					Inner:     inner,
					QueryText: queryText,
				},
			},
		},
	}
}

type posthogLevelHandler struct {
	level slog.Level
	inner slog.Handler
}

func (h *posthogLevelHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= h.level && h.inner.Enabled(ctx, l)
}

func (h *posthogLevelHandler) Handle(ctx context.Context, r slog.Record) error {
	if !h.Enabled(ctx, r.Level) {
		return nil
	}
	return h.inner.Handle(ctx, r)
}

func (h *posthogLevelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &posthogLevelHandler{level: h.level, inner: h.inner.WithAttrs(attrs)}
}

func (h *posthogLevelHandler) WithGroup(name string) slog.Handler {
	return &posthogLevelHandler{level: h.level, inner: h.inner.WithGroup(name)}
}

type infoSampleHandler struct {
	sample float64
	inner  slog.Handler
}

func (h *infoSampleHandler) Enabled(ctx context.Context, l slog.Level) bool {
	if l >= slog.LevelWarn {
		return h.inner.Enabled(ctx, l)
	}
	if l < slog.LevelInfo {
		return h.inner.Enabled(ctx, l)
	}
	if h.sample <= 0 {
		return false
	}
	if h.sample >= 1 {
		return h.inner.Enabled(ctx, l)
	}
	return rand.Float64() < h.sample && h.inner.Enabled(ctx, l)
}

func (h *infoSampleHandler) Handle(ctx context.Context, r slog.Record) error {
	if !h.Enabled(ctx, r.Level) {
		return nil
	}
	return h.inner.Handle(ctx, r)
}

func (h *infoSampleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &infoSampleHandler{sample: h.sample, inner: h.inner.WithAttrs(attrs)}
}

func (h *infoSampleHandler) WithGroup(name string) slog.Handler {
	return &infoSampleHandler{sample: h.sample, inner: h.inner.WithGroup(name)}
}

type dropMessageHandler struct {
	drop  map[string]struct{}
	inner slog.Handler
}

func (h *dropMessageHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *dropMessageHandler) Handle(ctx context.Context, r slog.Record) error {
	if _, ok := h.drop[r.Message]; ok {
		return nil
	}
	return h.inner.Handle(ctx, r)
}

func (h *dropMessageHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &dropMessageHandler{drop: h.drop, inner: h.inner.WithAttrs(attrs)}
}

func (h *dropMessageHandler) WithGroup(name string) slog.Handler {
	return &dropMessageHandler{drop: h.drop, inner: h.inner.WithGroup(name)}
}

func parsePostHogLogLevel() slog.Level {
	val := strings.ToLower(os.Getenv("DUCKGRES_POSTHOG_LOG_LEVEL"))
	switch val {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "error":
		return slog.LevelError
	case "warn", "warning", "":
		return slog.LevelWarn
	default:
		return slog.LevelWarn
	}
}

func parsePostHogInfoSample() float64 {
	raw := strings.TrimSpace(os.Getenv("DUCKGRES_POSTHOG_LOG_INFO_SAMPLE"))
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil || n < 0 {
		return 0
	}
	if n > 1 {
		return 1
	}
	return n
}

func parsePostHogQueryText() string {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("DUCKGRES_POSTHOG_LOG_QUERY_TEXT"))) {
	case queryTextOff:
		return queryTextOff
	case queryTextOn:
		return queryTextOn
	default:
		return queryTextRedacted
	}
}
