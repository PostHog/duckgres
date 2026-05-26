package opa

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// PushOption tweaks PushBundle behaviour. Constructed via With... helpers
// so callers don't depend on the internal struct layout.
type PushOption func(*pushOptions)

type pushOptions struct {
	httpClient *http.Client
	bearer     string
	timeout    time.Duration
}

// WithHTTPClient overrides the HTTP client used to POST bundle bytes.
// Tests use this to inject an httptest.Server's client; production
// callers normally take the default.
func WithHTTPClient(c *http.Client) PushOption {
	return func(o *pushOptions) { o.httpClient = c }
}

// WithBearerToken sets an Authorization: Bearer token on the push request.
// OPA's bundle service supports bearer auth on the upload endpoint; we
// use this when the sidecar is configured with a shared secret.
func WithBearerToken(token string) PushOption {
	return func(o *pushOptions) { o.bearer = token }
}

// WithTimeout overrides the per-request timeout. Default is 10s, which
// is generous for a localhost POST of a kilobyte-scale bundle.
func WithTimeout(d time.Duration) PushOption {
	return func(o *pushOptions) { o.timeout = d }
}

// PushBundle POSTs the given bundle bytes to OPA's bundle service endpoint.
// The endpoint URL is the full path to the bundle resource, e.g.
// "http://localhost:8181/v1/bundles/trino" -- OPA will look for the bundle
// under data.bundles["trino"] when configured with discovery, or simply
// serve it as the named bundle.
//
// On non-2xx responses, PushBundle returns an error that includes the HTTP
// status and the first 4KB of the response body. The Trino provisioner
// invokes this from its reconcile loop; failures there should re-enqueue
// the provisioning state and retry on the next reconcile tick.
//
// Note: deploying and configuring OPA itself (sidecar manifest, bundle
// service config) lives in the customer-Trino Helm chart, not this package.
func PushBundle(ctx context.Context, endpoint string, bundleBytes []byte, opts ...PushOption) error {
	o := pushOptions{
		httpClient: http.DefaultClient,
		timeout:    10 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}

	if endpoint == "" {
		return fmt.Errorf("opa: push endpoint is empty")
	}
	if len(bundleBytes) == 0 {
		return fmt.Errorf("opa: bundle is empty")
	}

	reqCtx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPut, endpoint, bytes.NewReader(bundleBytes))
	if err != nil {
		return fmt.Errorf("opa: build push request: %w", err)
	}
	req.Header.Set("Content-Type", "application/gzip")
	if o.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+o.bearer)
	}

	resp, err := o.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("opa: push bundle: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return fmt.Errorf("opa: push bundle: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
