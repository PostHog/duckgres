package configstore

import "testing"

func TestDucklingBucketName(t *testing.T) {
	cases := []struct {
		name      string
		orgID     string
		envSuffix string
		want      string
	}{
		{
			name:      "canonical UUID is hyphen-compacted to fit the 63-char S3 cap",
			orgID:     "0194d640-5db4-0000-6cde-48d6114c0f99",
			envSuffix: "mw-prod-us",
			want:      "posthog-duckling-0194d6405db400006cde48d6114c0f99-mw-prod-us",
		},
		{
			name:      "uppercase UUID is lowercased then compacted",
			orgID:     "0194D640-5DB4-0000-6CDE-48D6114C0F99",
			envSuffix: "mw-prod-us",
			want:      "posthog-duckling-0194d6405db400006cde48d6114c0f99-mw-prod-us",
		},
		{
			name:      "non-UUID name keeps its hyphens (existing buckets stay stable)",
			orgID:     "ben",
			envSuffix: "mw-prod-us",
			want:      "posthog-duckling-ben-mw-prod-us",
		},
		{
			name:      "non-UUID name with a hyphen is NOT compacted",
			orgID:     "perf-runner",
			envSuffix: "mw-dev",
			want:      "posthog-duckling-perf-runner-mw-dev",
		},
		{
			name:      "empty suffix disables CP naming",
			orgID:     "0194d640-5db4-0000-6cde-48d6114c0f99",
			envSuffix: "",
			want:      "",
		},
		{
			name:      "63-char cap: the compacted prod-us name fits",
			orgID:     "0194d640-5db4-0000-6cde-48d6114c0f99",
			envSuffix: "mw-prod-us",
			// 60 chars — guard that we stayed under the S3 limit.
			want: "posthog-duckling-0194d6405db400006cde48d6114c0f99-mw-prod-us",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DucklingBucketName(tc.orgID, tc.envSuffix)
			if got != tc.want {
				t.Fatalf("DucklingBucketName(%q, %q) = %q, want %q", tc.orgID, tc.envSuffix, got, tc.want)
			}
			if got != "" && len(got) > 63 {
				t.Fatalf("bucket name %q is %d chars, exceeds the S3 63-char limit", got, len(got))
			}
		})
	}
}
