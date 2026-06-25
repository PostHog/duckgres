package provision

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	WarehouseStatePending      = "pending"
	WarehouseStateProvisioning = "provisioning"
	WarehouseStateReady        = "ready"
	WarehouseStateFailed       = "failed"
	WarehouseStateDeleting     = "deleting"
	WarehouseStateDeleted      = "deleted"

	defaultPollInterval = 10 * time.Second
)

type Config struct {
	BaseURL        string
	InternalSecret string
	HTTPClient     *http.Client
}

type Client struct {
	baseURL        string
	internalSecret string
	httpClient     *http.Client
}

type ProvisionResponse struct {
	Status   string `json:"status"`
	Org      string `json:"org"`
	Username string `json:"username"`
	Password string `json:"password"`
	Bucket   string `json:"bucket,omitempty"`
}

type DeprovisionResponse struct {
	Status string `json:"status"`
	Org    string `json:"org"`
}

type WarehouseStatus struct {
	OrgID              string             `json:"org_id"`
	State              string             `json:"state"`
	StatusMessage      string             `json:"status_message"`
	S3State            string             `json:"s3_state"`
	MetadataStoreState string             `json:"metadata_store_state"`
	IdentityState      string             `json:"identity_state"`
	SecretsState       string             `json:"secrets_state"`
	ReadyAt            *time.Time         `json:"ready_at,omitempty"`
	FailedAt           *time.Time         `json:"failed_at,omitempty"`
	Connection         *ConnectionDetails `json:"connection,omitempty"`
	Bucket             string             `json:"bucket,omitempty"`
}

type ConnectionDetails struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
}

type WaitOptions struct {
	PollInterval   time.Duration
	Timeout        time.Duration
	MaxAttempts    int
	AcceptNotFound bool
	Sleep          func(context.Context, time.Duration) error
}

func NewClient(cfg Config) (*Client, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, classified(ErrorClassConfig, fmt.Errorf("provisioning API base URL is required"))
	}
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, classified(ErrorClassConfig, fmt.Errorf("parse provisioning API base URL: %w", err))
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		baseURL:        baseURL,
		internalSecret: cfg.InternalSecret,
		httpClient:     httpClient,
	}, nil
}

func (c *Client) Provision(ctx context.Context, orgID string, request map[string]any) (ProvisionResponse, error) {
	var resp ProvisionResponse
	path := orgPath(orgID, "provision")
	if err := c.doJSON(ctx, http.MethodPost, path, request, &resp, http.StatusAccepted); err != nil {
		return ProvisionResponse{}, err
	}
	return resp, nil
}

func (c *Client) WarehouseStatus(ctx context.Context, orgID string) (WarehouseStatus, error) {
	var resp WarehouseStatus
	path := orgPath(orgID, "warehouse/status")
	if err := c.doJSON(ctx, http.MethodGet, path, nil, &resp, http.StatusOK); err != nil {
		return WarehouseStatus{}, err
	}
	return resp, nil
}

func (c *Client) Deprovision(ctx context.Context, orgID string) (DeprovisionResponse, error) {
	var resp DeprovisionResponse
	path := orgPath(orgID, "deprovision")
	if err := c.doJSON(ctx, http.MethodPost, path, nil, &resp, http.StatusAccepted); err != nil {
		return DeprovisionResponse{}, err
	}
	return resp, nil
}

func (c *Client) WaitWarehouseReady(ctx context.Context, orgID string, opts WaitOptions) (WarehouseStatus, error) {
	return c.waitForState(ctx, orgID, WarehouseStateReady, opts)
}

func (c *Client) WaitWarehouseDeleted(ctx context.Context, orgID string, opts WaitOptions) (WarehouseStatus, error) {
	opts.AcceptNotFound = true
	return c.waitForState(ctx, orgID, WarehouseStateDeleted, opts)
}

func (c *Client) doJSON(ctx context.Context, method, path string, body any, out any, expectedStatus int) error {
	var bodyReader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return classified(ErrorClassInvalidStepConfig, fmt.Errorf("encode request for %s %s: %w", method, path, err))
		}
		bodyReader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return classified(ErrorClassProvisionAPI, fmt.Errorf("create request for %s %s: %w", method, path, err))
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.internalSecret != "" {
		req.Header.Set("X-Duckgres-Internal-Secret", c.internalSecret)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return classified(ErrorClassProvisionAPI, fmt.Errorf("%s %s failed: %w", method, path, err))
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != expectedStatus {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16*1024))
		return &APIError{
			Method:     method,
			Path:       path,
			StatusCode: resp.StatusCode,
			Body:       redactHTTPBody(raw),
		}
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return classified(ErrorClassProvisionAPI, fmt.Errorf("decode response for %s %s: %w", method, path, err))
	}
	return nil
}

func (c *Client) waitForState(ctx context.Context, orgID, target string, opts WaitOptions) (WarehouseStatus, error) {
	waitCtx, cancel := contextWithOptionalTimeout(ctx, opts.Timeout)
	defer cancel()

	interval := opts.PollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}
	sleep := opts.Sleep
	if sleep == nil {
		sleep = func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
	}

	var last WarehouseStatus
	for attempts := 1; ; attempts++ {
		status, err := c.WarehouseStatus(waitCtx, orgID)
		if err != nil {
			var apiErr *APIError
			if opts.AcceptNotFound && errorsAs(err, &apiErr) && apiErr.NotFound() {
				return WarehouseStatus{OrgID: orgID, State: WarehouseStateDeleted}, nil
			}
			if waitCtx.Err() != nil {
				return last, waitContextError(waitCtx, target, orgID)
			}
			return last, err
		}
		last = status
		if status.State == target {
			return status, nil
		}
		if status.State == WarehouseStateFailed {
			return status, classified(ErrorClassProvisionFailed, fmt.Errorf("%w while waiting for %s to reach %s: %s", ErrWarehouseFailed, orgID, target, status.StatusMessage))
		}
		if opts.MaxAttempts > 0 && attempts >= opts.MaxAttempts {
			return status, waitTimeoutError(target, orgID)
		}
		if err := sleep(waitCtx, interval); err != nil {
			if waitCtx.Err() != nil {
				return status, waitContextError(waitCtx, target, orgID)
			}
			if errors.Is(err, context.Canceled) {
				return status, err
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return status, waitTimeoutError(target, orgID)
			}
			return status, classified(ErrorClassProvisionAPI, fmt.Errorf("sleep while waiting for %s to reach %s: %w", orgID, target, err))
		}
	}
}

func contextWithOptionalTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func waitTimeoutError(target, orgID string) error {
	return classified(ErrorClassWaitTimeout, fmt.Errorf("%w: warehouse %s did not reach %s", ErrWaitTimeout, orgID, target))
}

func waitContextError(ctx context.Context, target, orgID string) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return waitTimeoutError(target, orgID)
	}
	return ctx.Err()
}

func orgPath(orgID, suffix string) string {
	return "/api/v1/orgs/" + url.PathEscape(orgID) + "/" + suffix
}

func errorsAs(err error, target any) bool {
	return err != nil && errors.As(err, target)
}
