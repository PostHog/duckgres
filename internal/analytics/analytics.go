// Package analytics emits product-analytics events to PostHog.
//
// This is distinct from the OTLP log export in internal/cliboot: that ships
// slog records to PostHog Logs, this captures discrete product events
// (provision, deprovision, password reset, query lifecycle) via the PostHog
// capture API. Both are gated on the same POSTHOG_API_KEY.
//
// Events are attributed to an org using PostHog group analytics: the
// distinct_id is the org name and the event carries a group of type
// "organization". This lets dashboards break down and aggregate by org.
//
// The default tracker is a no-op until SetDefault installs a real one (done by
// cliboot.InitAnalytics at startup when POSTHOG_API_KEY is set), so call sites
// can unconditionally call analytics.Default().Capture(...).
package analytics

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/posthog/posthog-go"
)

// GroupTypeOrg is the PostHog group type used to attribute events to an org.
const GroupTypeOrg = "organization"

// Tracker captures product-analytics events. Implementations must be safe for
// concurrent use and non-blocking: the query path calls Capture on every query,
// so an implementation must never block on network I/O in Capture.
type Tracker interface {
	// Capture records an event. orgID attributes the event to an org via group
	// analytics; an empty orgID falls back to a non-org distinct_id and omits
	// the group (e.g. standalone single-tenant mode). props may be nil.
	Capture(event, orgID string, props map[string]any)
	// Close flushes any buffered events. Safe to call multiple times.
	Close()
}

// distinctIDNoOrg is the distinct_id used when no org is known (standalone).
const distinctIDNoOrg = "standalone"

// noopTracker discards all events. It is the default until a real tracker is
// installed, and the tracker used whenever POSTHOG_API_KEY is unset.
type noopTracker struct{}

func (noopTracker) Capture(string, string, map[string]any) {}
func (noopTracker) Close()                                 {}

// enqueuer is the subset of posthog.Client that posthogTracker depends on.
// Narrowing the dependency keeps posthogTracker unit-testable with a small fake
// instead of having to implement PostHog's large feature-flag interface.
type enqueuer interface {
	Enqueue(posthog.Message) error
	Close() error
}

// posthogTracker captures events to a PostHog project via the capture API.
type posthogTracker struct {
	client enqueuer
}

// Capture enqueues a PostHog capture message. Enqueue is asynchronous (the
// posthog-go client batches and flushes on a background goroutine), so this
// does not block the caller on network I/O. Enqueue errors are logged at debug
// and otherwise swallowed: analytics must never break a query or admin request.
func (t *posthogTracker) Capture(event, orgID string, props map[string]any) {
	properties := posthog.NewProperties()
	for k, v := range props {
		properties.Set(k, v)
	}

	distinctID := orgID
	var groups posthog.Groups
	if orgID == "" {
		distinctID = distinctIDNoOrg
	} else {
		groups = posthog.NewGroups().Set(GroupTypeOrg, orgID)
	}

	if err := t.client.Enqueue(posthog.Capture{
		DistinctId: distinctID,
		Event:      event,
		Properties: properties,
		Groups:     groups,
	}); err != nil {
		slog.Debug("Failed to enqueue PostHog event.", "event", event, "error", err)
	}
}

func (t *posthogTracker) Close() { _ = t.client.Close() }

// NewPostHogTracker constructs a Tracker backed by a real PostHog client.
// host may be a bare host (e.g. "us.i.posthog.com") or a full URL; a bare host
// is upgraded to https. Returns an error if the client cannot be created.
func NewPostHogTracker(apiKey, host string) (Tracker, error) {
	endpoint := host
	if endpoint != "" && !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	client, err := posthog.NewWithConfig(apiKey, posthog.Config{Endpoint: endpoint})
	if err != nil {
		return nil, fmt.Errorf("create posthog client: %w", err)
	}
	return &posthogTracker{client: client}, nil
}

var (
	mu      sync.RWMutex
	current Tracker = noopTracker{}
)

// Default returns the installed Tracker, or a no-op tracker if none is set.
func Default() Tracker {
	mu.RLock()
	defer mu.RUnlock()
	return current
}

// SetDefault installs the global Tracker. Passing nil installs a no-op tracker.
func SetDefault(t Tracker) {
	mu.Lock()
	defer mu.Unlock()
	if t == nil {
		t = noopTracker{}
	}
	current = t
}
