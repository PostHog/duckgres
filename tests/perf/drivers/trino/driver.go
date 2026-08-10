// Package trino implements the Trino HTTP statement protocol for benchmarks.
package trino

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/posthog/duckgres/tests/perf/core"
)

type Config struct {
	Endpoint   string
	User       string
	Catalog    string
	Schema     string
	TimeZone   string
	HTTPClient *http.Client
}

type Driver struct {
	endpoint   *url.URL
	user       string
	catalog    string
	schema     string
	timeZone   string
	httpClient *http.Client
}

func New(cfg Config) (*Driver, error) {
	endpoint, err := url.Parse(cfg.Endpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" {
		return nil, fmt.Errorf("trino endpoint must be an absolute HTTP URL")
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, fmt.Errorf("trino endpoint scheme must be http or https")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	user := cfg.User
	if user == "" {
		user = "duckgres-perf"
	}
	timeZone := cfg.TimeZone
	if timeZone == "" {
		timeZone = "UTC"
	}
	return &Driver{
		endpoint: endpoint, user: user, catalog: cfg.Catalog, schema: cfg.Schema,
		timeZone: timeZone, httpClient: client,
	}, nil
}

func (d *Driver) Protocol() core.Protocol { return core.ProtocolTrino }

func (d *Driver) Execute(ctx context.Context, query core.Query, args []any) (core.ExecutionResult, error) {
	if len(args) > 0 {
		return core.ExecutionResult{}, fmt.Errorf("trino driver does not support parameterized query %s", query.QueryID)
	}
	sql, err := query.SQLFor(core.ProtocolTrino)
	if err != nil {
		return core.ExecutionResult{}, err
	}
	started := time.Now()
	rows, err := d.executeStatement(ctx, sql)
	return core.ExecutionResult{Rows: rows, Duration: time.Since(started)}, err
}

func (d *Driver) Close() error { return nil }

// Environment reports the non-secret comparison metadata the artifact records
// for this protocol. The Trino version comes from the coordinator's own
// /v1/info endpoint, so the artifact states what actually answered the queries
// rather than what was expected to.
func (d *Driver) Environment(ctx context.Context) (core.ProtocolEnvironment, error) {
	env := core.ProtocolEnvironment{
		Protocol: core.ProtocolTrino,
		Engine:   "trino",
		Catalog:  d.catalog,
		Schema:   d.schema,
		TimeZone: d.timeZone,
	}
	version, err := d.serverVersion(ctx)
	if err != nil {
		// Best-effort metadata: report what is known rather than failing.
		return env, err
	}
	env.Version = version
	return env, nil
}

// serverVersion reads GET /v1/info, Trino's unauthenticated server-info
// endpoint: {"nodeVersion":{"version":"483"},...}.
func (d *Driver) serverVersion(ctx context.Context) (string, error) {
	infoURL := d.endpoint.ResolveReference(&url.URL{Path: "/v1/info"})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, infoURL.String(), nil)
	if err != nil {
		return "", fmt.Errorf("create Trino info request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("execute Trino info request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("trino info request returned %s", resp.Status)
	}
	var info struct {
		NodeVersion struct {
			Version string `json:"version"`
		} `json:"nodeVersion"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return "", fmt.Errorf("decode Trino info response: %w", err)
	}
	return info.NodeVersion.Version, nil
}

type statementResponse struct {
	NextURI string            `json:"nextUri"`
	Data    []json.RawMessage `json:"data"`
	Error   *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (d *Driver) executeStatement(ctx context.Context, sql string) (int64, error) {
	statementURL := d.endpoint.ResolveReference(&url.URL{Path: "/v1/statement"})
	response, err := d.request(ctx, http.MethodPost, statementURL.String(), strings.NewReader(sql))
	if err != nil {
		return 0, err
	}
	var rows int64
	for {
		rows += int64(len(response.Data))
		if response.Error != nil {
			return 0, fmt.Errorf("trino query failed: %s", response.Error.Message)
		}
		if response.NextURI == "" {
			return rows, nil
		}
		nextURL, err := d.endpoint.Parse(response.NextURI)
		if err != nil {
			return 0, fmt.Errorf("parse Trino next URI: %w", err)
		}
		response, err = d.request(ctx, http.MethodGet, nextURL.String(), nil)
		if err != nil {
			return 0, err
		}
	}
}

func (d *Driver) request(ctx context.Context, method, requestURL string, body io.Reader) (statementResponse, error) {
	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return statementResponse{}, fmt.Errorf("create Trino request: %w", err)
	}
	req.Header.Set("X-Trino-User", d.user)
	req.Header.Set("X-Trino-Time-Zone", d.timeZone)
	if d.catalog != "" {
		req.Header.Set("X-Trino-Catalog", d.catalog)
	}
	if d.schema != "" {
		req.Header.Set("X-Trino-Schema", d.schema)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "text/plain")
	}

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return statementResponse{}, fmt.Errorf("execute Trino request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return statementResponse{}, fmt.Errorf("trino request returned %s: %s", resp.Status, bytes.TrimSpace(body))
	}
	var result statementResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return statementResponse{}, fmt.Errorf("decode Trino response: %w", err)
	}
	return result, nil
}
