package provisioner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// LakekeeperClient is a thin HTTP client for the Lakekeeper management +
// catalog REST surface that the provisioner needs to drive: bootstrap the
// server, create the per-org warehouse, and check ready/bootstrapped state.
//
// One client instance addresses one Lakekeeper deployment. Per-org Lakekeeper
// deployments get their own client instances built ad-hoc by the provisioner.
//
// Authentication: a static bearer token can be set via WithBearer. In the
// current allowall + NetworkPolicy deployment model the token is unused; the
// field is kept so the OIDC follow-up (PR3) can plug in without changing
// callers.
type LakekeeperClient struct {
	baseURL string
	hc      *http.Client
	bearer  string
}

func NewLakekeeperClient(baseURL string) *LakekeeperClient {
	return &LakekeeperClient{
		baseURL: baseURL,
		hc:      &http.Client{Timeout: 30 * time.Second},
	}
}

// WithBearer sets the bearer token used on subsequent requests. The token
// must be a single-line string (no CR/LF) — Go's http.Header.Set won't error
// on construction but http.Client.Do will reject malformed headers at send.
//
// NOT safe to call concurrently with in-flight requests. Build the client
// once, configure it, then share read-only across goroutines. PR3 will add
// proper rotation primitives when OIDC refresh lands.
func (c *LakekeeperClient) WithBearer(token string) *LakekeeperClient {
	c.bearer = token
	return c
}

func (c *LakekeeperClient) WithHTTPClient(hc *http.Client) *LakekeeperClient {
	c.hc = hc
	return c
}

// ServerInfo is the subset of GET /management/v1/info the provisioner cares
// about. Bootstrapped flips to true after the first POST /management/v1/bootstrap.
type ServerInfo struct {
	Version          string `json:"version"`
	Bootstrapped     bool   `json:"bootstrapped"`
	ServerID         string `json:"server-id"`
	AuthzBackend     string `json:"authz-backend"`
	DefaultProjectID string `json:"default-project-id"`
}

// Info fetches the server info. Returns ErrServerNotReady if the endpoint is
// reachable but the server reports itself as not yet ready (rare; treated as
// transient by the caller).
func (c *LakekeeperClient) Info(ctx context.Context) (*ServerInfo, error) {
	var out ServerInfo
	if err := c.do(ctx, http.MethodGet, "/management/v1/info", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Bootstrap is the one-shot init of a fresh Lakekeeper server. Returns nil if
// the server was already bootstrapped (Lakekeeper responds 409 in that case;
// we treat it as success).
func (c *LakekeeperClient) Bootstrap(ctx context.Context) error {
	body := map[string]any{
		"accept-terms-of-use": true,
		"is-operator":         true,
	}
	err := c.do(ctx, http.MethodPost, "/management/v1/bootstrap", body, nil)
	if err == nil {
		return nil
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict {
		// Already bootstrapped — idempotent success.
		return nil
	}
	return err
}

// WarehouseStorageProfile is the subset of fields we send for an S3 / S3-compat
// warehouse. The Lakekeeper API accepts more; we only set what we need.
type WarehouseStorageProfile struct {
	Type                 string `json:"type"`                   // "s3"
	Bucket               string `json:"bucket"`
	KeyPrefix            string `json:"key-prefix"`
	Endpoint             string `json:"endpoint,omitempty"`     // e.g. http://minio:9000; omit for real AWS
	STSEndpoint          string `json:"sts-endpoint,omitempty"` // optional
	Region               string `json:"region"`
	PathStyleAccess      bool   `json:"path-style-access"`
	Flavor               string `json:"flavor"`                  // "s3-compat" for MinIO, "aws" for AWS
	STSEnabled           bool   `json:"sts-enabled"`
	RemoteSigningEnabled bool   `json:"remote-signing-enabled"`
	// STSRoleARN is the IAM role Lakekeeper assumes to mint vended (scoped,
	// short-lived) S3 credentials for clients. Lakekeeper requires it for the
	// AWS flavor when sts-enabled. Empty for s3-compat (MinIO). We set it to
	// the per-org duckling role, which the Lakekeeper pod already runs as via
	// EKS Pod Identity — so it assumes itself (the role trusts its own ARN).
	STSRoleARN string `json:"sts-role-arn,omitempty"`
}

// WarehouseStorageCredential supports access-key creds (dev/MinIO) and
// instance-profile / IRSA-style creds (prod AWS, no static key).
type WarehouseStorageCredential struct {
	Type              string `json:"type"`            // "s3"
	CredentialType    string `json:"credential-type"` // "access-key" or "aws-system-identity"
	AWSAccessKeyID    string `json:"aws-access-key-id,omitempty"`
	AWSSecretAccessKey string `json:"aws-secret-access-key,omitempty"`
}

// CreateWarehouseRequest is the body of POST /management/v1/warehouse.
type CreateWarehouseRequest struct {
	WarehouseName     string                     `json:"warehouse-name"`
	ProjectID         string                     `json:"project-id,omitempty"`
	StorageProfile    WarehouseStorageProfile    `json:"storage-profile"`
	StorageCredential WarehouseStorageCredential `json:"storage-credential"`
}

// Warehouse is the subset of the create/list response we use.
type Warehouse struct {
	ID            string `json:"id"`
	WarehouseID   string `json:"warehouse-id"`
	Name          string `json:"name"`
	ProjectID     string `json:"project-id"`
	Status        string `json:"status"`
}

// listWarehousesResponse wraps the list endpoint response.
type listWarehousesResponse struct {
	Warehouses []Warehouse `json:"warehouses"`
}

// EnsureWarehouse creates the warehouse if it doesn't exist, otherwise returns
// the existing one. Match is by warehouse-name within the default project.
//
// Idempotent under concurrent callers: if two callers both observe an empty
// list and both POST, the second POST will 409 and we re-list to return the
// winner. Callers that need stronger ordering should hold a per-org lock
// outside this method.
func (c *LakekeeperClient) EnsureWarehouse(ctx context.Context, req CreateWarehouseRequest) (*Warehouse, error) {
	if existing, err := c.findWarehouseByName(ctx, req.WarehouseName); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}
	var out Warehouse
	err := c.do(ctx, http.MethodPost, "/management/v1/warehouse", req, &out)
	if err == nil {
		return &out, nil
	}
	// 409 → another caller (or a previous attempt) won the race. Re-list and
	// return the existing warehouse rather than surface the conflict.
	var apiErr *APIError
	if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict {
		existing, lookupErr := c.findWarehouseByName(ctx, req.WarehouseName)
		if lookupErr != nil {
			return nil, fmt.Errorf("create warehouse %q hit 409, lookup failed: %w", req.WarehouseName, lookupErr)
		}
		if existing != nil {
			return existing, nil
		}
		// 409 but nothing matches by name — pass the original error through.
	}
	return nil, fmt.Errorf("create warehouse %q: %w", req.WarehouseName, err)
}

// findWarehouseByName scans the first page of the warehouse list for a name
// match. Lakekeeper paginates the list endpoint; we assume one warehouse per
// per-org Lakekeeper instance, so a single page is enough today. If we ever
// host multiple warehouses per instance, revisit to honor `next-page-token`.
func (c *LakekeeperClient) findWarehouseByName(ctx context.Context, name string) (*Warehouse, error) {
	var resp listWarehousesResponse
	if err := c.do(ctx, http.MethodGet, "/management/v1/warehouse", nil, &resp); err != nil {
		return nil, fmt.Errorf("list warehouses: %w", err)
	}
	for i := range resp.Warehouses {
		if resp.Warehouses[i].Name == name {
			return &resp.Warehouses[i], nil
		}
	}
	return nil, nil
}

// APIError is returned for non-2xx responses. Status holds the HTTP code;
// Body holds the raw response body (often a JSON error envelope from
// Lakekeeper that we don't bother unmarshalling).
type APIError struct {
	Status int
	Method string
	Path   string
	Body   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("lakekeeper %s %s: HTTP %d: %s", e.Method, e.Path, e.Status, e.Body)
}

func (c *LakekeeperClient) do(ctx context.Context, method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal %s %s body: %w", method, path, err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearer)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &APIError{Status: resp.StatusCode, Method: method, Path: path, Body: string(respBody)}
	}
	if out == nil || len(respBody) == 0 {
		return nil
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("decode %s %s response: %w", method, path, err)
	}
	return nil
}
