package main

import (
	"net/http"
	"strings"
)

// TenantScope extracts the stable, non-secret tenant dimension of a request:
// the access key ID from the SigV4 Authorization header
// ("AWS4-HMAC-SHA256 Credential=<ACCESS_KEY_ID>/<date>/<region>/s3/aws4_request, ...").
// STS access key IDs are unique per issued credential set, so the ID
// separates tenants that share a bucket with per-org path prefixes. Cache
// keys mix in this scope, so a warm entry for one tenant is never served to
// a request signed by another tenant's credentials.
//
// A request without a parseable SigV4 header gets the empty scope. Unsigned
// requests therefore share one cache namespace, which is correct for public
// objects. The scope is an identifier, not a secret: the secret access key
// and the signature never enter the cache key.
func TenantScope(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
		return ""
	}
	const marker = "Credential="
	i := strings.Index(auth, marker)
	if i < 0 {
		return ""
	}
	id, _, found := strings.Cut(auth[i+len(marker):], "/")
	if !found || id == "" {
		return ""
	}
	return id
}
