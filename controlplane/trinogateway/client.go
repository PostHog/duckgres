package trinogateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"
)

const (
	basePath = "/gateway/v1/pools"

	// One admin request budget, per the agreed retry policy. Operation progress
	// deadlines are the caller's and are deliberately much longer: a slow drain
	// is not a slow request.
	defaultRequestTimeout = 10 * time.Second
	maxResponseBytes      = 1 << 20
	minAdminTokenLength   = 32
)

// Config configures the client. The credential is the Gateway's EXISTING admin
// token; this protocol introduces no new secret and no client certificate.
type Config struct {
	BaseURL       string
	AdminToken    string
	TLSServerName string
	// AllowPlaintext permits an http:// origin. Only tests set it: the token
	// would otherwise cross the network in the clear.
	AllowPlaintext bool
	RequestTimeout time.Duration
	HTTPClient     *http.Client
}

// Client speaks the pooled member lifecycle protocol.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
	timeout time.Duration
}

// NewClient validates the origin and credential up front, so a misconfigured
// deployment fails at startup rather than at the first drain.
func NewClient(config Config) (*Client, error) {
	origin, err := url.Parse(strings.TrimSpace(config.BaseURL))
	if err != nil || origin.Hostname() == "" || origin.User != nil ||
		origin.RawQuery != "" || origin.Fragment != "" ||
		(origin.Path != "" && origin.Path != "/") {
		return nil, errors.New("gateway client requires a credential-free origin without a path")
	}
	if origin.Scheme != "https" && !(config.AllowPlaintext && origin.Scheme == "http") {
		return nil, errors.New("gateway client requires an HTTPS origin")
	}
	token := strings.TrimSpace(config.AdminToken)
	if len(token) < minAdminTokenLength || strings.IndexFunc(token, unicode.IsControl) != -1 {
		return nil, errors.New("gateway client requires the existing admin token")
	}

	client := config.HTTPClient
	if client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		// A registry-owned internal endpoint: no proxy, pinned server name.
		transport.Proxy = nil
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: config.TLSServerName}
		transport.MaxIdleConnsPerHost = 4
		transport.ResponseHeaderTimeout = 5 * time.Second
		client = &http.Client{
			Transport: transport,
			// Redirects are refused: following one would replay an admin
			// mutation against an origin nobody validated.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("gateway client does not follow redirects")
			},
		}
	}
	timeout := config.RequestTimeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	return &Client{
		baseURL: strings.TrimSuffix(origin.String(), "/"),
		token:   token,
		http:    client,
		timeout: timeout,
	}, nil
}

// GetPool reads the pool's authoritative counts and generations.
func (c *Client) GetPool(ctx context.Context, poolID string) (PoolState, error) {
	var pool PoolState
	err := c.do(ctx, http.MethodGet, c.poolPath(poolID), nil, &pool)
	return pool, err
}

// ConfigurePool applies the desired pool specification.
func (c *Client) ConfigurePool(ctx context.Context, poolID string, request ConfigurePoolRequest) (PoolState, error) {
	var pool PoolState
	err := c.do(ctx, http.MethodPut, c.poolPath(poolID), request, &pool)
	return pool, err
}

// ListMembers reads the authoritative member list. The Gateway returns a bare
// JSON array, not an envelope.
func (c *Client) ListMembers(ctx context.Context, poolID string) ([]Member, error) {
	var members []Member
	if err := c.do(ctx, http.MethodGet, c.poolPath(poolID)+"/members", nil, &members); err != nil {
		return nil, err
	}
	return members, nil
}

// GetMember reads one member back. This is the read-back path after a lost
// response on a member mutation.
func (c *Client) GetMember(ctx context.Context, poolID, instanceID string) (Member, error) {
	var member Member
	err := c.do(ctx, http.MethodGet, c.memberPath(poolID, instanceID), nil, &member)
	return member, err
}

// GetObligations reads what still pins a member. Drain completion is decided
// here and nowhere else.
func (c *Client) GetObligations(ctx context.Context, poolID, instanceID string) (Obligations, error) {
	var obligations Obligations
	err := c.do(ctx, http.MethodGet, c.memberPath(poolID, instanceID)+"/obligations", nil, &obligations)
	return obligations, err
}

// RegisterMember creates a PREPARING member against an EXISTING Gateway backend
// registration. It is not eligible for tenant work.
func (c *Client) RegisterMember(ctx context.Context, poolID string, request RegisterMemberRequest) (Member, error) {
	return c.member(ctx, http.MethodPost, c.poolPath(poolID)+"/members", request)
}

// AdmitMember is the single certified-activation call: it carries the
// duckgres-performed validation receipt and the generation CAS. The Gateway
// enforces certificate freshness, the budgets and any open publication barrier,
// and independently verifies the live process identity.
func (c *Client) AdmitMember(ctx context.Context, poolID, instanceID string, request AdmitMemberRequest) (Member, error) {
	return c.member(ctx, http.MethodPost, c.memberPath(poolID, instanceID)+"/admit", request)
}

// DrainMember starts a planned drain. The Gateway refuses it when the serving
// floor would break; that refusal is authoritative and is never overridden.
func (c *Client) DrainMember(ctx context.Context, poolID, instanceID string, request MemberStepRequest) (Member, error) {
	return c.member(ctx, http.MethodPost, c.memberPath(poolID, instanceID)+"/drain", request)
}

// SealMember moves a drained member to SEALED. The Gateway checks that no
// obligations remain; duckgres never decides drain completion from a timer.
func (c *Client) SealMember(ctx context.Context, poolID, instanceID string, request MemberStepRequest) (Member, error) {
	return c.member(ctx, http.MethodPost, c.memberPath(poolID, instanceID)+"/seal", request)
}

// SuspectMember excludes a member from new admissions. It authorizes nothing
// destructive.
func (c *Client) SuspectMember(ctx context.Context, poolID, instanceID string, request SuspectMemberRequest) (Member, error) {
	return c.member(ctx, http.MethodPost, c.memberPath(poolID, instanceID)+"/suspect", request)
}

// LostMember records that a member's process terminated, with evidence.
func (c *Client) LostMember(ctx context.Context, poolID, instanceID string, request LostMemberRequest) (Member, error) {
	return c.member(ctx, http.MethodPost, c.memberPath(poolID, instanceID)+"/lost", request)
}

// RetireMember claims retirement for the exact incarnation. This is the
// irreversible step, and its receipt is what authorizes deleting Kubernetes
// objects. Nothing else does.
func (c *Client) RetireMember(ctx context.Context, poolID, instanceID string, request MemberStepRequest) (Member, error) {
	return c.member(ctx, http.MethodPost, c.memberPath(poolID, instanceID)+"/retire", request)
}

// MemberRetired reports that the resources are gone. The Gateway records the
// assertion; it never infers deletion for itself.
func (c *Client) MemberRetired(ctx context.Context, poolID, instanceID string, request MemberStepRequest) (Member, error) {
	return c.member(ctx, http.MethodPost, c.memberPath(poolID, instanceID)+"/retired", request)
}

// GetFailureReceipt reads a failed member's preserved obligations.
func (c *Client) GetFailureReceipt(ctx context.Context, poolID, instanceID string) (FailureReceipt, error) {
	var receipt FailureReceipt
	err := c.do(ctx, http.MethodGet, c.memberPath(poolID, instanceID)+"/failure-receipt", nil, &receipt)
	return receipt, err
}

// GetOperation reads recorded step outcomes. This is the ONLY correct response
// to a lost reply: the operator resolves what actually happened under the same
// operation id instead of minting a new one.
func (c *Client) GetOperation(ctx context.Context, poolID, operationID string) (OperationHistory, error) {
	var history OperationHistory
	err := c.do(ctx, http.MethodGet, c.poolPath(poolID)+"/operations/"+url.PathEscape(operationID), nil, &history)
	return history, err
}

// OpenPublication opens the tenant publication barrier.
func (c *Client) OpenPublication(ctx context.Context, poolID string, request OpenPublicationRequest) (Publication, error) {
	var publication Publication
	err := c.do(ctx, http.MethodPost, c.poolPath(poolID)+"/publications", request, &publication)
	return publication, err
}

// RecordPublicationReceipt records one member's applied revision.
func (c *Client) RecordPublicationReceipt(ctx context.Context, poolID, publicationID string, request PublicationReceiptRequest) (Publication, error) {
	var publication Publication
	err := c.do(ctx, http.MethodPost, c.publicationPath(poolID, publicationID)+"/receipts", request, &publication)
	return publication, err
}

// CommitPublication opens the tenant admission gate. After it commits the
// Gateway's receipt is authoritative even if duckgres has not checkpointed yet;
// recovery completes the checkpoint and never retracts an opened gate.
func (c *Client) CommitPublication(ctx context.Context, poolID, publicationID string, request CommitPublicationRequest) (Publication, error) {
	var publication Publication
	err := c.do(ctx, http.MethodPost, c.publicationPath(poolID, publicationID)+"/commit", request, &publication)
	return publication, err
}

// AbandonPublication gives up a barrier that never committed.
func (c *Client) AbandonPublication(ctx context.Context, poolID, publicationID string, step Step) (Publication, error) {
	var publication Publication
	err := c.do(ctx, http.MethodPost, c.publicationPath(poolID, publicationID)+"/abandon", step, &publication)
	return publication, err
}

// GetPublication reads a barrier back, which is how a lost commit response is
// resolved.
func (c *Client) GetPublication(ctx context.Context, poolID, publicationID string) (Publication, error) {
	var publication Publication
	err := c.do(ctx, http.MethodGet, c.publicationPath(poolID, publicationID), nil, &publication)
	return publication, err
}

// GetTenant reads a tenant's admission gate.
func (c *Client) GetTenant(ctx context.Context, poolID, tenant string) (TenantAdmission, error) {
	var admission TenantAdmission
	err := c.do(ctx, http.MethodGet, c.tenantPath(poolID, tenant), nil, &admission)
	return admission, err
}

// RevokeTenant closes a tenant's admission gate.
func (c *Client) RevokeTenant(ctx context.Context, poolID, tenant string, request RevokeTenantRequest) (TenantAdmission, error) {
	var admission TenantAdmission
	err := c.do(ctx, http.MethodDelete, c.tenantPath(poolID, tenant), request, &admission)
	return admission, err
}

func (c *Client) member(ctx context.Context, method, path string, body any) (Member, error) {
	var member Member
	err := c.do(ctx, method, path, body, &member)
	return member, err
}

func (c *Client) poolPath(poolID string) string {
	return basePath + "/" + url.PathEscape(poolID)
}

func (c *Client) memberPath(poolID, instanceID string) string {
	return c.poolPath(poolID) + "/members/" + url.PathEscape(instanceID)
}

func (c *Client) publicationPath(poolID, publicationID string) string {
	return c.poolPath(poolID) + "/publications/" + url.PathEscape(publicationID)
}

func (c *Client) tenantPath(poolID, tenant string) string {
	return c.poolPath(poolID) + "/tenants/" + url.PathEscape(tenant)
}

func (c *Client) do(ctx context.Context, method, path string, body, target any) error {
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode gateway request: %w", err)
		}
	}
	return c.doRaw(ctx, method, path, encoded, "application/json", target)
}

// doRaw sends an already-encoded body. The legacy backend-delete endpoint takes
// a bare string rather than JSON, which is why the encoding is a parameter.
func (c *Client) doRaw(ctx context.Context, method, path string, body []byte, contentType string, target any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	var payload io.Reader
	if body != nil {
		payload = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, payload)
	if err != nil {
		return fmt.Errorf("build gateway request: %w", err)
	}
	// The Gateway accepts either form of the existing admin credential; sending
	// both keeps the client working across its own auth refactors.
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("X-Gateway-Transaction-Admin-Token", c.token)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", contentType)
	}

	response, err := c.http.Do(request)
	if err != nil {
		// No verdict: the mutation may or may not have been applied. The caller
		// must resolve this through GetOperation rather than retrying blind.
		return fmt.Errorf("gateway request failed: %w", err)
	}
	defer func() { _ = response.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(raw) > maxResponseBytes {
		return errors.New("gateway response exceeds the size limit")
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		return newGatewayError(response.StatusCode, response.Header.Get("X-Trino-Gateway-Error"), strings.TrimSpace(string(raw)))
	}
	if target == nil {
		return nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode gateway response: %w", err)
	}
	return nil
}
