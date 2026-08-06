package cliboot

import "testing"

// TestAnalyticsAPIKeyPrefersDedicatedKey pins the split between the two
// PostHog exporters. POSTHOG_ANALYTICS_API_KEY must enable product analytics
// on its own, because InitLogging gates the OTLP log export on
// POSTHOG_API_KEY: a deployment that sets only the dedicated key gets
// metadata-only events and no query text exported. The POSTHOG_API_KEY
// fallback keeps existing single-key deployments on both exporters.
func TestAnalyticsAPIKeyPrefersDedicatedKey(t *testing.T) {
	tests := []struct {
		name         string
		analyticsKey string
		sharedKey    string
		want         string
	}{
		{
			name:         "dedicated key alone enables analytics without log export",
			analyticsKey: "phc_analytics",
			want:         "phc_analytics",
		},
		{
			name:      "shared key still enables analytics (existing deployments)",
			sharedKey: "phc_shared",
			want:      "phc_shared",
		},
		{
			name:         "dedicated key wins when both are set",
			analyticsKey: "phc_analytics",
			sharedKey:    "phc_shared",
			want:         "phc_analytics",
		},
		{
			name: "neither set leaves the tracker a no-op",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("POSTHOG_ANALYTICS_API_KEY", tt.analyticsKey)
			t.Setenv("POSTHOG_API_KEY", tt.sharedKey)

			if got := analyticsAPIKey(); got != tt.want {
				t.Errorf("analyticsAPIKey() = %q, want %q", got, tt.want)
			}
		})
	}
}
