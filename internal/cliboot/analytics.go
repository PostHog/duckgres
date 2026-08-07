package cliboot

import (
	"fmt"
	"os"

	"github.com/posthog/duckgres/internal/analytics"
)

// analyticsAPIKey resolves the key used for product-analytics capture.
//
// POSTHOG_ANALYTICS_API_KEY enables analytics WITHOUT enabling the OTLP log
// export, which stays gated on POSTHOG_API_KEY alone (see InitLogging). The
// two exporters carry very different data: analytics events are metadata only
// (see the events table in README.md), while application logs include query
// text — `logQuery`/`logQueryError` attach the statement, and
// usersecrets.RedactForLog only rewrites secret DDL, so arbitrary SQL and its
// literals reach PostHog Logs. A deployment that must not ship customer SQL
// therefore sets ONLY POSTHOG_ANALYTICS_API_KEY.
//
// POSTHOG_API_KEY remains a fallback so existing single-key deployments keep
// both exporters with no config change. POSTHOG_HOST is shared by both and is
// read independently of which key is set.
func analyticsAPIKey() string {
	if key := os.Getenv("POSTHOG_ANALYTICS_API_KEY"); key != "" {
		return key
	}
	return os.Getenv("POSTHOG_API_KEY")
}

// InitAnalytics installs a PostHog product-analytics tracker when
// POSTHOG_ANALYTICS_API_KEY or POSTHOG_API_KEY is set (see analyticsAPIKey for
// which one to use), reading POSTHOG_HOST for the ingest host. When neither is
// set the global tracker stays a no-op and Capture calls are discarded.
//
// Returns a shutdown function that flushes buffered events; wire it alongside
// the InitLogging shutdown in each entrypoint.
func InitAnalytics() func() {
	apiKey := analyticsAPIKey()
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
