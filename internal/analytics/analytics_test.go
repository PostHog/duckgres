package analytics

import (
	"testing"

	"github.com/posthog/posthog-go"
)

// fakeEnqueuer captures enqueued messages for assertions.
type fakeEnqueuer struct {
	messages []posthog.Message
	closed   bool
}

func (f *fakeEnqueuer) Enqueue(m posthog.Message) error {
	f.messages = append(f.messages, m)
	return nil
}

func (f *fakeEnqueuer) Close() error {
	f.closed = true
	return nil
}

func TestCaptureUsesGroupAnalyticsForOrg(t *testing.T) {
	fake := &fakeEnqueuer{}
	tr := &posthogTracker{client: fake}

	tr.Capture("warehouse_provisioned", "acme", map[string]any{"database_name": "acme_db"})

	if len(fake.messages) != 1 {
		t.Fatalf("expected 1 enqueued message, got %d", len(fake.messages))
	}
	capture, ok := fake.messages[0].(posthog.Capture)
	if !ok {
		t.Fatalf("expected posthog.Capture, got %T", fake.messages[0])
	}
	if capture.Event != "warehouse_provisioned" {
		t.Errorf("event = %q, want warehouse_provisioned", capture.Event)
	}
	if capture.DistinctId != "acme" {
		t.Errorf("distinct_id = %q, want acme", capture.DistinctId)
	}
	if got := capture.Groups[GroupTypeOrg]; got != "acme" {
		t.Errorf("group %q = %v, want acme", GroupTypeOrg, got)
	}
	if got := capture.Properties["database_name"]; got != "acme_db" {
		t.Errorf("property database_name = %v, want acme_db", got)
	}
}

func TestCaptureWithoutOrgOmitsGroup(t *testing.T) {
	fake := &fakeEnqueuer{}
	tr := &posthogTracker{client: fake}

	tr.Capture("query_initiated", "", nil)

	capture := fake.messages[0].(posthog.Capture)
	if capture.DistinctId != distinctIDNoOrg {
		t.Errorf("distinct_id = %q, want %q", capture.DistinctId, distinctIDNoOrg)
	}
	if capture.Groups != nil {
		t.Errorf("expected no group for empty org, got %v", capture.Groups)
	}
}

func TestCloseClosesClient(t *testing.T) {
	fake := &fakeEnqueuer{}
	tr := &posthogTracker{client: fake}
	tr.Close()
	if !fake.closed {
		t.Error("Close did not close underlying client")
	}
}

func TestDefaultIsNoopUntilSet(t *testing.T) {
	t.Cleanup(func() { SetDefault(nil) })

	// Default must never be nil and must not panic.
	Default().Capture("evt", "org", nil)

	fake := &fakeEnqueuer{}
	SetDefault(&posthogTracker{client: fake})
	Default().Capture("evt", "org", nil)
	if len(fake.messages) != 1 {
		t.Fatalf("expected installed tracker to receive event, got %d", len(fake.messages))
	}

	// nil resets to no-op.
	SetDefault(nil)
	Default().Capture("evt", "org", nil)
	if len(fake.messages) != 1 {
		t.Errorf("expected no further events after reset, got %d", len(fake.messages))
	}
}
