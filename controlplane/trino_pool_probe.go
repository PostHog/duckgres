//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var trinoPoolCoordinatorIDPattern = regexp.MustCompile(`^[a-zA-Z0-9]{5}$`)

type trinoPoolCoordinatorFacts struct {
	NodeID        string
	CoordinatorID string
}

func newTrinoPoolHTTPClient(tlsServerName string) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// These registry-owned cluster endpoints use a scoped direct transport.
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: tlsServerName}
	transport.MaxConnsPerHost = 4
	return &http.Client{Transport: transport, Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

type trinoPoolSQLClient struct {
	baseURL            string
	client             *http.Client
	username, password string
	// internalHTTP marks a pooled coordinator reached directly on its
	// in-cluster Service over plain HTTP. TLS terminates at the Gateway, and
	// the coordinator runs with http-server.process-forwarded=true, so an
	// authenticated request has to carry the forwarded-HTTPS metadata the
	// Gateway would supply. Authentication is NOT relaxed anywhere: without
	// these headers Trino refuses the credential rather than accepting it in
	// the clear. HTTPS endpoints leave this false and keep their
	// HTTPS-only checks byte for byte.
	internalHTTP bool
}

// forwardedScheme is what a coordinator behind a TLS-terminating proxy must be
// told about the original request. It is a statement about the Gateway hop,
// not a way to bypass the coordinator's own authentication.
const forwardedScheme = "https"

func (c trinoPoolSQLClient) scheme() string {
	if c.internalHTTP {
		return "http"
	}
	return "https"
}

func (c trinoPoolSQLClient) read(ctx context.Context, method, endpoint, sql string) ([]byte, error) {
	base, baseErr := url.Parse(c.baseURL)
	parsed, err := url.Parse(endpoint)
	if baseErr != nil || err != nil || base.Scheme != c.scheme() || !strings.EqualFold(parsed.Hostname(), base.Hostname()) || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || (parsed.Path != "/v1/statement" && !strings.HasPrefix(parsed.Path, "/v1/statement/") && parsed.Path != "/v1/info") {
		return nil, errors.New("invalid coordinator response endpoint")
	}
	internalContinuation := c.internalHTTP && method == http.MethodGet &&
		strings.HasPrefix(parsed.Path, "/v1/statement/") &&
		parsed.Scheme == forwardedScheme && trinoPoolHTTPSPort(parsed) == "443"
	if internalContinuation {
		// Trino returns the HTTPS origin declared by our forwarded headers.
		// Keep validated result paths on the configured internal coordinator transport.
		parsed.Scheme, parsed.Host = base.Scheme, base.Host
		endpoint = parsed.String()
	} else if parsed.Scheme != base.Scheme || trinoPoolHTTPSPort(parsed) != trinoPoolHTTPSPort(base) {
		return nil, errors.New("invalid coordinator response endpoint")
	}
	if c.username == "" || c.password == "" {
		return nil, errors.New("probe credential unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, strings.NewReader(sql))
	if err != nil {
		return nil, errors.New("invalid probe request")
	}
	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("X-Trino-User", c.username)
	req.Header.Set("X-Trino-Source", "rollout-readiness")
	req.Header.Set("Content-Type", "text/plain")
	if c.internalHTTP {
		// Declare the Gateway's terminated TLS. A coordinator with
		// process-forwarded=true reads these; without them it rejects an
		// authenticated request over plain HTTP, which is the behavior we
		// want to keep rather than disable.
		req.Header.Set("X-Forwarded-Proto", forwardedScheme)
		req.Header.Set("X-Forwarded-Port", "443")
	}
	response, err := c.client.Do(req)
	if err != nil {
		return nil, errors.New("coordinator request failed")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("coordinator rejected probe")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return nil, errors.New("coordinator response exceeds limit")
	}
	return body, nil
}

func trinoPoolHTTPSPort(endpoint *url.URL) string {
	if port := endpoint.Port(); port != "" {
		return port
	}
	if endpoint.Scheme == "http" {
		return "80"
	}
	return "443"
}

func (c trinoPoolSQLClient) info(ctx context.Context) (*trinoPoolCoordinatorFacts, error) {
	body, err := c.read(ctx, http.MethodGet, c.baseURL+"/v1/info", "")
	if err != nil {
		return nil, err
	}
	var value struct {
		Coordinator   bool   `json:"coordinator"`
		Starting      *bool  `json:"starting"`
		NodeID        string `json:"nodeId"`
		CoordinatorID string `json:"coordinatorId"`
	}
	if json.Unmarshal(body, &value) != nil || !value.Coordinator || value.Starting == nil || *value.Starting || value.NodeID == "" || !trinoPoolCoordinatorIDPattern.MatchString(value.CoordinatorID) {
		return nil, errors.New("invalid coordinator identity")
	}
	return &trinoPoolCoordinatorFacts{NodeID: value.NodeID, CoordinatorID: value.CoordinatorID}, nil
}

func (c trinoPoolSQLClient) statement(ctx context.Context, sql string) ([][]any, error) {
	endpoint, method := c.baseURL+"/v1/statement", http.MethodPost
	var rows [][]any
	totalBytes := 0
	for hop := 0; hop < 32; hop++ {
		body, err := c.read(ctx, method, endpoint, sql)
		if err != nil {
			return nil, err
		}
		totalBytes += len(body)
		if totalBytes > 4<<20 {
			return nil, errors.New("statement exceeds response budget")
		}
		var page struct {
			NextURI string          `json:"nextUri"`
			Data    [][]any         `json:"data"`
			Error   json.RawMessage `json:"error"`
		}
		if json.Unmarshal(body, &page) != nil || (len(page.Error) > 0 && string(page.Error) != "null") {
			return nil, errors.New("statement did not succeed")
		}
		rows = append(rows, page.Data...)
		if len(rows) > 4096 {
			return nil, errors.New("statement exceeds row budget")
		}
		if page.NextURI == "" {
			return rows, nil
		}
		endpoint, method, sql = page.NextURI, http.MethodGet, ""
	}
	return nil, errors.New("statement exceeds page budget")
}
