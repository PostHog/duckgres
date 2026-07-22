package server

import (
	"context"
	"errors"
	"testing"

	"github.com/posthog/duckgres/internal/analytics"
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
	c := &clientConn{orgID: "acme", username: "root", ctx: context.Background()}

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
