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

func TestLogQueryErrorDoesNotEmitQueryFailedOutsideLogicalScope(t *testing.T) {
	fake := installFakeQueryTracker(t)
	c := &clientConn{orgID: "acme", username: "root", ctx: context.Background()}

	c.logQueryError("SELECT 1", errors.New("worker connection reset"))

	if len(fake.events) != 0 {
		t.Fatalf("logQueryError emitted %d analytics events; query_failed belongs to the terminal client lifecycle", len(fake.events))
	}
}

func TestLogQueryErrorDoesNotEmitUserFailureAnalyticsOutsideLogicalScope(t *testing.T) {
	fake := installFakeQueryTracker(t)
	c := &clientConn{orgID: "acme", username: "root", ctx: context.Background()}

	// A Catalog Error is user-attributable (e.g. unknown table).
	c.logQueryError("SELECT * FROM nope", errors.New("Catalog Error: Table with name nope does not exist!"))

	if len(fake.events) != 0 {
		t.Fatalf("logQueryError emitted %d analytics events; terminal client lifecycle must classify the failure", len(fake.events))
	}
}
