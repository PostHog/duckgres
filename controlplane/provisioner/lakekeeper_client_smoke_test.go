package provisioner

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
)

// Smoke-test against a real running Lakekeeper. Skipped unless
// LAKEKEEPER_SMOKE_URL is set (e.g. http://localhost:8181). Pair with
// LAKEKEEPER_SMOKE_BEARER if the server has OIDC enabled.
//
// To run against the tmp/lakekeeper-proto stack:
//
//	export LAKEKEEPER_SMOKE_URL=http://localhost:8181
//	export LAKEKEEPER_SMOKE_BEARER="$(docker exec lkproto-oidc python /srv/mkjwt.py 300)"
//	go test ./controlplane/provisioner/ -run TestSmoke -v
func TestSmoke_InfoAgainstLiveLakekeeper(t *testing.T) {
	base := os.Getenv("LAKEKEEPER_SMOKE_URL")
	if base == "" {
		t.Skip("LAKEKEEPER_SMOKE_URL not set; skipping live smoke test")
	}
	if _, err := url.Parse(base); err != nil {
		t.Fatalf("LAKEKEEPER_SMOKE_URL invalid: %v", err)
	}
	c := NewLakekeeperClient(strings.TrimRight(base, "/"))
	if tok := os.Getenv("LAKEKEEPER_SMOKE_BEARER"); tok != "" {
		c.WithBearer(tok)
	}
	info, err := c.Info(context.Background())
	if err != nil {
		// Distinguish auth from connectivity.
		var apiErr *APIError
		if asAPIErr(err, &apiErr) && apiErr.Status == http.StatusUnauthorized {
			t.Fatalf("Lakekeeper requires bearer (set LAKEKEEPER_SMOKE_BEARER): %v", err)
		}
		t.Fatalf("Info: %v", err)
	}
	if info.Version == "" {
		t.Fatalf("expected non-empty version, got %+v", info)
	}
	t.Logf("live lakekeeper info: version=%s authz=%s bootstrapped=%v", info.Version, info.AuthzBackend, info.Bootstrapped)
}

// helper so this file doesn't need errors.As import duplication
func asAPIErr(err error, target **APIError) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*APIError); ok {
		*target = e
		return true
	}
	return false
}
