package cliboot

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/posthog/duckgres/server/usersecrets"
)

const (
	createSecretSQL = "CREATE SECRET foo (TYPE s3, SECRET 'super-secret-credential')"
	selectSQL       = "SELECT id, email FROM events WHERE org = 'acme'"
	catalogErrText  = "Catalog Error: Table with name nope does not exist!\nLINE 1: SELECT * FROM nope"
	secretErrText   = "Binder Error: LINE 1: CREATE SECRET foo (TYPE s3, SECRET 'super-secret-credential')"
)

type captured struct {
	msg   string
	level slog.Level
	attrs map[string]string
}

type captureHandler struct {
	level slog.Level
	base  []slog.Attr
	recs  *[]captured
}

func newCapture(level slog.Level) *captureHandler {
	recs := []captured{}
	return &captureHandler{level: level, recs: &recs}
}

func (h *captureHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	m := map[string]string{}
	for _, a := range h.base {
		m[a.Key] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.String()
		return true
	})
	*h.recs = append(*h.recs, captured{msg: r.Message, level: r.Level, attrs: m})
	return nil
}
func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	nh := *h
	nh.base = append(append([]slog.Attr{}, h.base...), attrs...)
	return &nh
}
func (h *captureHandler) WithGroup(name string) slog.Handler { return h }
func (h *captureHandler) last() captured {
	if h.recs == nil || len(*h.recs) == 0 {
		return captured{attrs: map[string]string{}}
	}
	return (*h.recs)[len(*h.recs)-1]
}
func (h *captureHandler) all() []captured {
	if h.recs == nil {
		return nil
	}
	return *h.recs
}

func stripLogger(inner slog.Handler, queryText string) *slog.Logger {
	return slog.New(&QueryStripHandler{Inner: inner, QueryText: queryText})
}

func TestQueryStripHandlerKeepsRedactedSelect(t *testing.T) {
	cap := newCapture(slog.LevelDebug)
	stripLogger(cap, queryTextRedacted).Info("Query execution errored.", "query", selectSQL)
	got := cap.last().attrs["query"]
	if got != selectSQL {
		t.Fatalf("query = %q, want ordinary SELECT present", got)
	}
	if strings.Contains(got, "(…redacted)") {
		t.Fatalf("ordinary SELECT was placeholder-redacted: %q", got)
	}
}

func TestQueryStripHandlerSecretDDLIsPlaceholder(t *testing.T) {
	cap := newCapture(slog.LevelDebug)
	stripLogger(cap, queryTextRedacted).Info("x", "query", createSecretSQL)
	got := cap.last().attrs["query"]
	want := usersecrets.RedactForLog(createSecretSQL)
	if got != want {
		t.Fatalf("query = %q, want placeholder %q", got, want)
	}
	if strings.Contains(got, "super-secret-credential") {
		t.Fatalf("credential leaked: %q", got)
	}
}

func TestQueryStripHandlerRedactsSecretError(t *testing.T) {
	cap := newCapture(slog.LevelDebug)
	stripLogger(cap, queryTextRedacted).Error("failed",
		"query", createSecretSQL,
		"error", errors.New(secretErrText),
	)
	got := cap.last().attrs["error"]
	if strings.Contains(got, "super-secret-credential") {
		t.Fatalf("secret leaked in error: %q", got)
	}
	if got != usersecrets.RedactErrorForLog(createSecretSQL, secretErrText) {
		t.Fatalf("error = %q, want RedactErrorForLog result", got)
	}
}

func TestQueryStripHandlerPreservesNonSecretDuckDBError(t *testing.T) {
	cap := newCapture(slog.LevelDebug)
	stripLogger(cap, queryTextRedacted).Error("Query execution errored.",
		"query", "SELECT * FROM nope",
		"error", errors.New(catalogErrText),
	)
	got := cap.last().attrs["error"]
	if !strings.Contains(got, "Catalog Error") {
		t.Fatalf("Catalog Error was wiped: %q", got)
	}
	if strings.Contains(got, "error redacted") {
		t.Fatalf("LINE 1: was used as a redaction trigger: %q", got)
	}
}

func TestQueryStripHandlerOffDropsQuery(t *testing.T) {
	cap := newCapture(slog.LevelDebug)
	stripLogger(cap, queryTextOff).Info("x",
		"query", selectSQL,
		"sql", selectSQL,
		"transpiled", selectSQL,
		"org", "acme",
	)
	got := cap.last().attrs
	for _, k := range []string{"query", "sql", "transpiled"} {
		if _, ok := got[k]; ok {
			t.Errorf("QueryText=off kept %s = %q", k, got[k])
		}
	}
	if got["org"] != "acme" {
		t.Errorf("org missing: %+v", got)
	}
}

func TestQueryStripHandlerWithAttrsDoesNotLeakRawSecret(t *testing.T) {
	cap := newCapture(slog.LevelDebug)
	stripLogger(cap, queryTextRedacted).With("query", createSecretSQL).Info("x")
	got := cap.last().attrs["query"]
	if strings.Contains(got, "super-secret-credential") {
		t.Fatalf("With(query) leaked credential: %q", got)
	}
	if got != usersecrets.RedactForLog(createSecretSQL) {
		t.Fatalf("query = %q, want redacted form", got)
	}
}

func TestQueryStripHandlerWithAttrsRedactsLaterError(t *testing.T) {
	t.Run("secret error after With", func(t *testing.T) {
		cap := newCapture(slog.LevelDebug)
		stripLogger(cap, queryTextRedacted).
			With("query", createSecretSQL).
			Error("failed", "error", errors.New(secretErrText))
		rec := cap.last()
		if strings.Contains(rec.attrs["error"], "super-secret-credential") {
			t.Fatalf("later error leaked secret: %q", rec.attrs["error"])
		}
		if rec.attrs["query"] != usersecrets.RedactForLog(createSecretSQL) {
			t.Fatalf("query attr = %q", rec.attrs["query"])
		}
	})
	t.Run("catalog error after With is preserved", func(t *testing.T) {
		cap := newCapture(slog.LevelDebug)
		stripLogger(cap, queryTextRedacted).
			With("query", "SELECT 1").
			Error("failed", "error", errors.New(catalogErrText))
		got := cap.last().attrs["error"]
		if !strings.Contains(got, "Catalog Error") {
			t.Fatalf("catalog error wiped: %q", got)
		}
	})
}

func TestPostHogLevelIndependentOfStderr(t *testing.T) {
	var stderr bytes.Buffer
	cap := newCapture(slog.LevelDebug)
	logger := slog.New(&multiHandler{handlers: []slog.Handler{
		NewStampedHandler(&stderr, slog.LevelInfo),
		newPostHogBranch(cap, slog.LevelWarn, 0, queryTextRedacted),
	}})
	logger.Info("Client query received.", "query", selectSQL)
	logger.Error("Query execution errored.", "query", selectSQL)

	if !strings.Contains(stderr.String(), "Client query received.") {
		t.Fatalf("INFO missing from stderr:\n%s", stderr.String())
	}
	var sawInfo, sawError bool
	for _, rec := range cap.all() {
		if rec.msg == "Client query received." {
			sawInfo = true
		}
		if rec.msg == "Query execution errored." {
			sawError = true
		}
	}
	if sawInfo {
		t.Fatal("INFO reached PostHog branch at default warn")
	}
	if !sawError {
		t.Fatal("ERROR did not reach PostHog branch")
	}
}

func TestPostHogInfoSampleKeepsErrors(t *testing.T) {
	cap := newCapture(slog.LevelDebug)
	logger := slog.New(newPostHogBranch(cap, slog.LevelInfo, 0, queryTextRedacted))
	logger.Info("Client query received.")
	logger.Error("Query execution errored.")
	var sawInfo, sawError bool
	for _, rec := range cap.all() {
		if rec.msg == "Client query received." {
			sawInfo = true
		}
		if rec.msg == "Query execution errored." {
			sawError = true
		}
	}
	if sawInfo {
		t.Fatal("sample=0 kept INFO")
	}
	if !sawError {
		t.Fatal("sample=0 dropped ERROR")
	}
}

func TestDropFilterExactStartingMetricsServer(t *testing.T) {
	cap := newCapture(slog.LevelDebug)
	logger := slog.New(newPostHogBranch(cap, slog.LevelInfo, 1, queryTextRedacted))
	logger.Info("Starting metrics server", "addr", ":9090")
	logger.Info("Query execution failed.")
	var sawMetrics, sawFailed bool
	for _, rec := range cap.all() {
		if rec.msg == "Starting metrics server" {
			sawMetrics = true
		}
		if rec.msg == "Query execution failed." {
			sawFailed = true
		}
	}
	if sawMetrics {
		t.Fatal("Starting metrics server was not dropped")
	}
	if !sawFailed {
		t.Fatal("Query execution failed. was dropped by substring match")
	}
}
