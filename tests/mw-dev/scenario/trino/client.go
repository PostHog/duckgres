package trino

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is the scenario runner's non-secret adapter to the control-plane Trino
// benchmark lifecycle. The API returns only a cluster ID, a lifecycle state, an
// in-cluster endpoint, worker counts, and the pinned image reference; no
// credential material crosses this boundary, so nothing the client stores or
// logs can leak one.
type Client struct {
	baseURL        string
	internalSecret string
	httpClient     *http.Client
}

type ClientConfig struct {
	BaseURL        string
	InternalSecret string
	HTTPClient     *http.Client
}

// Default polling controls. The scenario YAML normally sets its own; these keep
// a step that omits them from either hammering the API or hanging forever.
const (
	defaultWaitPollInterval = 10 * time.Second
	defaultWaitTimeout      = 15 * time.Minute
)

// ErrClusterFailed is the TERMINAL lifecycle outcome. pending is a polling
// state; failed is not, and the wait must stop on it rather than burn the
// scenario's whole readiness budget.
var ErrClusterFailed = errors.New("trino benchmark cluster failed")

// Client is the production Lifecycle the scenario runner uses.
var _ Lifecycle = (*Client)(nil)

func NewClient(cfg ClientConfig) (*Client, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("trino lifecycle API base URL is required")
	}
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("parse trino lifecycle API base URL: %w", err)
	}
	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &Client{baseURL: baseURL, internalSecret: cfg.InternalSecret, httpClient: client}, nil
}

func (c *Client) ProvisionTrino(ctx context.Context, request ProvisionRequest) (Cluster, error) {
	var cluster Cluster
	if err := c.doJSON(ctx, http.MethodPost, request.OrgID, "provision", request.Config, &cluster); err != nil {
		return Cluster{}, err
	}
	return cluster, nil
}

// WaitTrinoReady polls the status endpoint until the control plane reports a
// ready cluster with a usable endpoint, the cluster fails terminally, or the
// caller's budget (timeout, attempts, or context) runs out. A transient
// transport/status error is a polling state too — the control plane may still
// be converging — but the last one is reported if the wait ultimately fails.
func (c *Client) WaitTrinoReady(ctx context.Context, cluster Cluster, options WaitOptions) (Cluster, error) {
	interval := options.PollInterval
	if interval <= 0 {
		interval = defaultWaitPollInterval
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultWaitTimeout
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var (
		attempts  int
		lastErr   error
		lastState string
	)
	for {
		if options.MaxAttempts > 0 && attempts >= options.MaxAttempts {
			return Cluster{}, waitExhaustedError(cluster.ID, attempts, lastState, lastErr,
				fmt.Sprintf("after %d attempt(s)", attempts))
		}
		attempts++

		var status Cluster
		err := c.doJSON(deadlineCtx, http.MethodGet, "", "status/"+url.PathEscape(cluster.ID), nil, &status)
		switch {
		case err != nil:
			// A failure caused purely by the expiring budget would overwrite
			// the real reason polling never succeeded, so keep the earlier one.
			if deadlineCtx.Err() == nil || lastErr == nil {
				lastErr = err
			}
		default:
			lastErr = nil
			lastState = status.State
			if status.State == StateFailed {
				// Terminal: no amount of further polling changes this.
				return Cluster{}, fmt.Errorf("%w: cluster %s reported state %q", ErrClusterFailed, cluster.ID, status.State)
			}
			// A ready cluster without an endpoint is not usable yet, so it
			// stays a polling state rather than a false success.
			if status.State == StateReady && status.Endpoint != "" {
				if status.ID == "" {
					status.ID = cluster.ID
				}
				return status, nil
			}
		}

		select {
		case <-deadlineCtx.Done():
			return Cluster{}, waitExhaustedError(cluster.ID, attempts, lastState, lastErr,
				fmt.Sprintf("within %s", timeout))
		case <-time.After(interval):
		}
	}
}

func waitExhaustedError(clusterID string, attempts int, lastState string, lastErr error, budget string) error {
	state := lastState
	if state == "" {
		state = "unknown"
	}
	if lastErr != nil {
		return fmt.Errorf("trino benchmark cluster %s did not become ready %s (%d attempt(s), last state %q): %w",
			clusterID, budget, attempts, state, lastErr)
	}
	return fmt.Errorf("trino benchmark cluster %s did not become ready %s (%d attempt(s), last state %q)",
		clusterID, budget, attempts, state)
}

func (c *Client) DeprovisionTrino(ctx context.Context, cluster Cluster) error {
	return c.doJSON(ctx, http.MethodPost, "", "deprovision/"+url.PathEscape(cluster.ID), nil, nil)
}

func (c *Client) doJSON(ctx context.Context, method, orgID, action string, body, out any) error {
	path := "/api/v1/trino-benchmarks"
	if orgID != "" {
		path += "/orgs/" + url.PathEscape(orgID)
	}
	path += "/" + action
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode trino lifecycle request: %w", err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return fmt.Errorf("create trino lifecycle request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.internalSecret != "" {
		req.Header.Set("X-Duckgres-Internal-Secret", c.internalSecret)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute trino lifecycle request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		// Report method, path, and status only. The control plane already
		// sanitizes its own error bodies, and the request headers (which carry
		// the internal secret) are never part of this message.
		return fmt.Errorf("trino lifecycle request %s %s returned %s", method, path, resp.Status)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode trino lifecycle response: %w", err)
		}
	}
	return nil
}
