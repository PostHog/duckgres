package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/internal/analytics"
	"github.com/posthog/duckgres/server/observe"
)

type capturedQueryEvent struct {
	event string
	orgID string
	props map[string]any
}

type fakeQueryTracker struct {
	events []capturedQueryEvent
}

func (f *fakeQueryTracker) Capture(event, orgID string, props map[string]any) {
	f.events = append(f.events, capturedQueryEvent{event: event, orgID: orgID, props: props})
}
func (f *fakeQueryTracker) Close() {}

func installFakeQueryTracker(t *testing.T) *fakeQueryTracker {
	t.Helper()
	fake := &fakeQueryTracker{}
	analytics.SetDefault(fake)
	t.Cleanup(func() { analytics.SetDefault(nil) })
	return fake
}

func TestLogClientQueryReceivedEmitsQueryInitiated(t *testing.T) {
	fake := installFakeQueryTracker(t)
	c := &clientConn{orgID: "acme", username: "root", teamID: 42, ctx: context.Background()}

	c.logClientQueryReceived(context.Background(), "simple", "SELECT 1")

	if len(fake.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(fake.events))
	}
	e := fake.events[0]
	if e.event != "query_initiated" {
		t.Errorf("event = %q, want query_initiated", e.event)
	}
	if e.orgID != "acme" {
		t.Errorf("orgID = %q, want acme", e.orgID)
	}
	if e.props["user"] != "root" {
		t.Errorf("user = %v, want root", e.props["user"])
	}
	if e.props["team_id"] != int64(42) {
		t.Errorf("team_id = %v, want 42", e.props["team_id"])
	}
}

func TestLogQueryErrorEmitsQueryFailed(t *testing.T) {
	fake := installFakeQueryTracker(t)
	c := &clientConn{orgID: "acme", username: "root", ctx: context.Background()}

	// A plain error matches no DuckDB prefix → system category, XX000.
	c.logQueryError("SELECT 1", errors.New("worker connection reset"))

	if len(fake.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(fake.events))
	}
	e := fake.events[0]
	if e.event != "query_failed" {
		t.Errorf("event = %q, want query_failed", e.event)
	}
	if e.orgID != "acme" {
		t.Errorf("orgID = %q, want acme", e.orgID)
	}
	if e.props["error_category"] != "system" {
		t.Errorf("error_category = %v, want system", e.props["error_category"])
	}
	if e.props["error_code"] != "XX000" {
		t.Errorf("error_code = %v, want XX000", e.props["error_code"])
	}
}

func TestLogQueryErrorClassifiesUserError(t *testing.T) {
	fake := installFakeQueryTracker(t)
	c := &clientConn{orgID: "acme", username: "root", ctx: context.Background()}

	// A Catalog Error is user-attributable (e.g. unknown table).
	c.logQueryError("SELECT * FROM nope", errors.New("Catalog Error: Table with name nope does not exist!"))

	e := fake.events[0]
	if e.props["error_category"] != "user" {
		t.Errorf("error_category = %v, want user", e.props["error_category"])
	}
}

func TestLogQueryEmitsQueryCompletedOnSuccess(t *testing.T) {
	fake := installFakeQueryTracker(t)
	// No server → queryLogSink is nil, so this exercises the analytics emission
	// independently of the query-log sink.
	c := &clientConn{orgID: "acme", username: "root", teamID: 42, ctx: context.Background()}
	c.lastProfilingSummary = observe.QueryProfilingSummary{CPUTimeSeconds: 2.5}

	c.logQuery(time.Now().Add(-100*time.Millisecond), "SELECT 1", "SELECT 1", "SELECT", 1, 0, "", "", "simple")

	if len(fake.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(fake.events))
	}
	e := fake.events[0]
	if e.event != "query_completed" {
		t.Errorf("event = %q, want query_completed", e.event)
	}
	if e.orgID != "acme" {
		t.Errorf("orgID = %q, want acme", e.orgID)
	}
	if e.props["team_id"] != int64(42) {
		t.Errorf("team_id = %v, want 42", e.props["team_id"])
	}
	if e.props["cpu_seconds"] != 2.5 {
		t.Errorf("cpu_seconds = %v, want 2.5", e.props["cpu_seconds"])
	}
	if e.props["query_kind"] != "Select" {
		t.Errorf("query_kind = %v, want Select", e.props["query_kind"])
	}
	if e.props["result_rows"] != int64(1) {
		t.Errorf("result_rows = %v, want 1", e.props["result_rows"])
	}
	if d, ok := e.props["duration_ms"].(int64); !ok || d <= 0 {
		t.Errorf("duration_ms = %v, want positive int64", e.props["duration_ms"])
	}
}

func TestLogQueryDoesNotEmitCompletedOnError(t *testing.T) {
	fake := installFakeQueryTracker(t)
	c := &clientConn{orgID: "acme", username: "root", ctx: context.Background()}

	// A failed query carries a non-empty errCode; query_completed is success-only
	// (the failure is covered by query_failed), so nothing should be emitted here.
	c.logQuery(time.Now(), "SELECT * FROM nope", "SELECT * FROM nope", "SELECT", 0, 0, "42P01", "table missing", "simple")

	for _, e := range fake.events {
		if e.event == "query_completed" {
			t.Fatalf("query_completed emitted for a failed query")
		}
	}
}
