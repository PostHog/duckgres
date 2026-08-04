package configstore

import (
	"fmt"
	"regexp"
	"strings"
)

// canonicalUUIDRe matches a lowercase canonical UUID (8-4-4-4-12). Only these
// get hyphen-compacted for the bucket suffix — see DucklingBucketName.
var canonicalUUIDRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// DucklingBucketName returns the per-org S3 bucket name for a type=s3bucket
// Duckling, mirroring the Crossplane composition's naming (charts
// crossplane-config, PR #12206):
//
//	posthog-duckling-<suffix>-<envSuffix>
//
// where <suffix> is the lowercased duckling name (= lowercased org ID, the same
// value the composition sees as $name) with hyphens stripped ONLY when it is a
// canonical UUID. The strip exists because an S3 bucket name is capped at 63
// chars and "posthog-duckling-<36-char-hyphenated-uuid>-mw-prod-us" is 64;
// dropping the four hyphens (UUID 36 → 32) brings it to 60. Non-UUID names
// (e.g. "ben") are left untouched so existing buckets stay byte-stable.
//
// This is the control plane's single source of truth for the name: it is
// written onto the Duckling CR's spec.dataStore.bucketName and returned to API
// callers, so nothing downstream re-derives it. The composition consumes the
// name verbatim when present (it only falls back to deriving the same string
// for ducklings provisioned before the CP started supplying it).
//
// Returns "" when envSuffix is empty — i.e. the deployment hasn't configured
// DUCKGRES_DUCKLING_BUCKET_SUFFIX, so the CP does not name buckets and the
// composition keeps deriving (legacy behavior).
func DucklingBucketName(orgID, envSuffix string) string {
	if envSuffix == "" {
		return ""
	}
	suffix := strings.ToLower(orgID)
	if canonicalUUIDRe.MatchString(suffix) {
		suffix = strings.ReplaceAll(suffix, "-", "")
	}
	return fmt.Sprintf("posthog-duckling-%s-%s", suffix, envSuffix)
}
