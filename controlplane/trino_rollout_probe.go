//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/provisioner"
)

var rolloutCoordinatorIDPattern = regexp.MustCompile(`^[a-zA-Z0-9]{5}$`)

type rolloutCanaryEligibility func(context.Context, rolloutCanaryCredential, string) (bool, error)

func newRolloutProbe(eligible rolloutCanaryEligibility) func(context.Context, rolloutReadinessSlot) (*rolloutCoordinatorFacts, error) {
	return func(ctx context.Context, slot rolloutReadinessSlot) (*rolloutCoordinatorFacts, error) {
		allowed, err := eligible(ctx, slot.canary, registeredTrinoCellPrefix+slot.cell)
		if err != nil || !allowed {
			return nil, errors.New("canary is not eligible")
		}
		user, password := slot.observer()
		client := rolloutSQLClient{baseURL: slot.coordinatorURL, client: slot.client, username: user, password: password}
		before, err := client.info(ctx)
		if err != nil {
			return nil, err
		}
		rows, err := client.statement(ctx, "SELECT node_id, http_uri, coordinator, state FROM system.runtime.nodes")
		if err != nil {
			return nil, err
		}
		seen := make(map[string]bool)
		seenIPs := make(map[string]bool)
		coordinators, workers := 0, 0
		if len(rows) > 1000 {
			return nil, errors.New("node inventory exceeds limit")
		}
		for _, row := range rows {
			if len(row) != 4 {
				return nil, errors.New("invalid node row")
			}
			id, idOK := row[0].(string)
			endpoint, endpointOK := row[1].(string)
			coordinator, coordinatorOK := row[2].(bool)
			state, stateOK := row[3].(string)
			parsed, parseErr := url.Parse(endpoint)
			if !idOK || id == "" || seen[id] || !endpointOK || parseErr != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") || (parsed.Scheme != "http" && parsed.Scheme != "https") || !coordinatorOK || !stateOK || state != "active" {
				return nil, errors.New("invalid or non-active node inventory")
			}
			ip, ipErr := netip.ParseAddr(parsed.Hostname())
			if ipErr != nil || seenIPs[ip.Unmap().String()] {
				return nil, errors.New("invalid or duplicate member pod IP")
			}
			seenIPs[ip.Unmap().String()] = true
			before.members = append(before.members, rolloutNodeMember{ip: ip.Unmap().String(), coordinator: coordinator})
			seen[id] = true
			if coordinator {
				coordinators++
				if id != before.NodeID {
					return nil, errors.New("coordinator node identity mismatch")
				}
			} else {
				workers++
			}
		}
		if coordinators != 1 || workers == 0 {
			return nil, errors.New("incomplete node inventory")
		}
		client.username, client.password = slot.canary.Principal, slot.canary.Password
		catalog := provisioner.TrinoCatalogName(slot.canary.Principal)
		schemas, err := client.statement(ctx, `SELECT schema_name FROM "`+strings.ReplaceAll(catalog, `"`, `""`)+`".information_schema.schemata`)
		if err != nil {
			return nil, err
		}
		if len(schemas) == 0 {
			return nil, errors.New("catalog metadata is empty")
		}
		for _, row := range schemas {
			if len(row) != 1 {
				return nil, errors.New("invalid catalog metadata row")
			}
			name, ok := row[0].(string)
			if !ok || strings.TrimSpace(name) == "" {
				return nil, errors.New("invalid catalog metadata name")
			}
		}
		client.username, client.password = user, password
		after, err := client.info(ctx)
		if err != nil || before.NodeID != after.NodeID || before.CoordinatorID != after.CoordinatorID {
			return nil, errors.New("coordinator changed during observation")
		}
		before.RegisteredWorkers = workers
		return before, nil
	}
}

func newRolloutHTTPClient(tlsServerName string) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// These registry-owned cluster endpoints use a scoped direct transport.
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: tlsServerName}
	transport.MaxConnsPerHost = 4
	return &http.Client{Transport: transport, Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

type rolloutSQLClient struct {
	baseURL            string
	client             *http.Client
	username, password string
}

func (c rolloutSQLClient) read(ctx context.Context, method, endpoint, sql string) ([]byte, error) {
	base, baseErr := url.Parse(c.baseURL)
	parsed, err := url.Parse(endpoint)
	if baseErr != nil || err != nil || parsed.Scheme != "https" || parsed.Scheme != base.Scheme || parsed.Host != base.Host || parsed.User != nil || parsed.Fragment != "" || (parsed.Path != "/v1/statement" && !strings.HasPrefix(parsed.Path, "/v1/statement/") && parsed.Path != "/v1/info") {
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

func (c rolloutSQLClient) info(ctx context.Context) (*rolloutCoordinatorFacts, error) {
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
	if json.Unmarshal(body, &value) != nil || !value.Coordinator || value.Starting == nil || *value.Starting || value.NodeID == "" || !rolloutCoordinatorIDPattern.MatchString(value.CoordinatorID) {
		return nil, errors.New("invalid coordinator identity")
	}
	return &rolloutCoordinatorFacts{NodeID: value.NodeID, CoordinatorID: value.CoordinatorID}, nil
}

func (c rolloutSQLClient) statement(ctx context.Context, sql string) ([][]any, error) {
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
