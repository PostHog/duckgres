package cliboot

import (
	"fmt"
	"os"

	"github.com/posthog/duckgres/internal/analytics"
)

// InitAnalytics installs a PostHog product-analytics tracker when
// POSTHOG_API_KEY is set, reusing the same POSTHOG_API_KEY / POSTHOG_HOST env
// vars as the OTLP log export (see InitLogging). When the key is unset the
// global tracker stays a no-op and Capture calls are discarded.
//
// Returns a shutdown function that flushes buffered events; wire it alongside
// the InitLogging shutdown in each entrypoint.
func InitAnalytics() func() {
	apiKey := os.Getenv("POSTHOG_API_KEY")
	if apiKey == "" {
		return func() {}
	}

	host := os.Getenv("POSTHOG_HOST")
	if host == "" {
		host = "us.i.posthog.com"
	}

	tracker, err := analytics.NewPostHogTracker(apiKey, host)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize PostHog analytics, events disabled: %v\n", err)
		return func() {}
	}

	analytics.SetDefault(tracker)
	fmt.Fprintln(os.Stderr, "PostHog analytics events enabled.")

	return func() {
		tracker.Close()
		analytics.SetDefault(nil)
	}
}
