package provisioning

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"gorm.io/gorm"
)

// TTL bounds for issued service credentials. The requester may ask for
// anything in [minCredentialTTL, maxCredentialTTL]; out-of-range values are
// clamped, not rejected, so a caller never has to guess the server-side policy
// to get a usable credential.
const (
	minCredentialTTL = time.Minute
	maxCredentialTTL = time.Hour
	// defaultCredentialTTL matches the RDS-IAM precedent: long enough that a
	// job mints a handful of credentials over its run, short enough that a
	// leaked one is a 15-minute liability.
	defaultCredentialTTL = 15 * time.Minute
)

// DefaultManagedIngressSuffix is the fallback managed tenant ingress DNS
// suffix used to build connect.host in the service-credential response
// (<org-id><suffix>) when the CP wasn't wired with an explicit ingress suffix.
// It matches the only managed production ingress today (*.dw.us.postwh.com,
// same as DUCKGRES_MANAGED_HOSTNAME_SUFFIXES). Production wiring
// (controlplane/multitenant.go) passes the CP's first configured
// ManagedHostnameSuffixes entry — the exact TLS server_name value the pgwire
// handshake pins — so this constant is only a safety net for unwired callers,
// never the source of truth for a configured CP.
const DefaultManagedIngressSuffix = ".dw.us.postwh.com"

// serviceCredentialRequest is the mint body
// (POST /api/v1/orgs/:id/service-credentials). There is deliberately NO
// team_id: service credentials are root-shaped org credentials, not
// project-scoped logins.
type serviceCredentialRequest struct {
	// Principal is audit attribution ("dagster:events-backfill") — required,
	// and it doubles as the reuse key: one live grant per (org, principal).
	Principal  string `json:"principal"`
	TTLSeconds int    `json:"ttl_seconds"`
	// ForceRotate bypasses the reuse path: when a live grant already exists
	// for (org, principal) the CP rotates its secret no matter how fresh it
	// is and returns the new plaintext. A caller MUST set this on its first
	// fetch of a run — it has nothing cached, and the reuse path deliberately
	// returns no plaintext for a still-valid grant (so concurrent long-lived
	// runs can't smash each other's credentials mid-flight). Omit/false means
	// "reuse the live grant if one exists, and only tell me its identity and
	// expiry".
	ForceRotate bool `json:"force_rotate"`
}

// serviceCredentialRefreshRequest is the refresh body
// (POST /api/v1/orgs/:id/service-credentials/refresh). Refresh ALWAYS rotates
// the named grant's secret — it is how a caller that already holds a
// credential_id extends its window (or recovers after an expiry) without
// minting a second identity for the same principal.
type serviceCredentialRefreshRequest struct {
	CredentialID string `json:"credential_id"`
	TTLSeconds   int    `json:"ttl_seconds"`
}

// connectDetails is the always-present `connect` block of the mint/refresh
// response: it tells the caller WHERE to use the credential from the same
// authoritative CP response that issued it, so nothing downstream re-derives
// its own idea of the warehouse endpoint (out-of-band endpoint knowledge — a
// Django `DuckgresServer` row — is exactly the drift this field exists to
// kill).
//
// Host is the org's canonical ingress hostname — orgID + the managed ingress
// suffix the CP is configured with — i.e. the very value the pgwire TLS
// server_name pins (the wildcard cert is *<suffix> and the SNI router resolves
// the single-label prefix as the org; see controlplane/sni_kubernetes.go). It
// is a single LOGICAL name returned verbatim for every caller: whether that
// name resolves over the public ingress or a caller-network-specific path
// (e.g. an AWS PrivateLink endpoint for dagster workers) is the caller
// network's business — NEVER an IP, NEVER resolved per caller network.
// Database/SslMode pin the pgwire handshake shape the CP enforces (managed
// warehouses accept only the ducklake catalog database, and TLS is required on
// the pgwire handshake).
type connectDetails struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	SslMode  string `json:"sslmode"`
}

// serviceCredentialResponse is the mint/refresh response. CredentialSecret is
// present ONLY when the CP bound a fresh secret (a fresh mint, a
// force_rotate, or any refresh); CredentialID/ExpiresAt/Connect are always
// present so the caller can always take its connection target from this same
// response.
type serviceCredentialResponse struct {
	CredentialID string `json:"credential_id"`
	// CredentialSecret is omitted (not empty-stringed) when the mint reused a
	// live grant: a caller that already holds the secret keeps using it;
	// echoing "" would risk clients treating "" as the secret itself.
	CredentialSecret string    `json:"credential_secret,omitempty"`
	ExpiresAt        time.Time `json:"expires_at"`
	// Connect is unconditional (unlike CredentialSecret): identical shape on
	// reuse and rotate, so the client can always take its connection target
	// from this same response instead of holding its own out-of-band copy.
	Connect connectDetails `json:"connect"`
}

// TenantStore is the subset of the config store the service-credential
// handlers need. Satisfied by the live gorm store in production; faked in
// tests.
type TenantStore interface {
	OrgExists(orgID string) (bool, error)
	MintServiceCredential(orgID, principal string, ttl time.Duration, forceRotate bool) (*configstore.ServiceCredentialIssue, error)
	RefreshServiceCredential(orgID, credentialID string, ttl time.Duration) (*configstore.ServiceCredentialIssue, error)
	ReloadSnapshot() error
}

// clampCredentialTTL applies the mint's policy: default when unset, clamped
// (never rejected) into [minCredentialTTL, maxCredentialTTL] when set.
func clampCredentialTTL(ttlSeconds int) time.Duration {
	ttl := defaultCredentialTTL
	if ttlSeconds > 0 {
		ttl = time.Duration(ttlSeconds) * time.Second
	}
	if ttl < minCredentialTTL {
		ttl = minCredentialTTL
	}
	if ttl > maxCredentialTTL {
		ttl = maxCredentialTTL
	}
	return ttl
}

// afterCredentialWrite makes a landed grant write authable without waiting a
// poll interval (default 30s): reload THIS replica's snapshot immediately,
// then fan the same reload out to PEER replicas (best-effort — a slow/down
// peer converges within one poll interval) because the client's pgwire
// connection can land on any CP behind the load balancer.
func (h *handler) afterCredentialWrite(c *gin.Context, tenantStore TenantStore) error {
	if err := tenantStore.ReloadSnapshot(); err != nil {
		return errors.New("credential written but snapshot reload failed: " + err.Error())
	}
	if h.peerFanout != nil {
		h.peerFanout.PostPeers(c.Request.Context(), "/api/v1/internal/reload-snapshot")
	}
	return nil
}

func (h *handler) credentialResponse(orgID string, issued *configstore.ServiceCredentialIssue) serviceCredentialResponse {
	return serviceCredentialResponse{
		CredentialID:     issued.CredentialID,
		CredentialSecret: issued.Plaintext,
		ExpiresAt:        issued.ExpiresAt,
		Connect: connectDetails{
			// The org's canonical ingress hostname — orgID + the CP's
			// configured managed-ingress suffix — i.e. exactly the value the
			// pgwire TLS server_name pins (never an IP, never resolved per
			// caller network; see the connectDetails doc).
			Host:     orgID + h.managedIngressSuffix(),
			Port:     5432,
			Database: "ducklake",
			SslMode:  "require",
		},
	}
}

// issueServiceCredential handles POST /api/v1/orgs/:id/service-credentials.
// It sits next to the other provisioning routes (not the admin package)
// because the caller is PostHog's backend (internal-secret), the same trust
// class as the other provisioning routes, not a human operator.
func (h *handler) issueServiceCredential(c *gin.Context, tenantStore TenantStore) {
	orgID := c.Param("id")

	var req serviceCredentialRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Principal == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "principal is required (e.g. \"dagster:events-backfill\")"})
		return
	}
	ttl := clampCredentialTTL(req.TTLSeconds)

	// Confirm the org exists before doing any credential work: a minted
	// credential authenticates against the org, so minting into a ghost org
	// would hand out an unresolvable identity — confusing to debug from the
	// caller side. 404: the path parameter names the org.
	exists, err := tenantStore.OrgExists(orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}

	issued, err := tenantStore.MintServiceCredential(orgID, req.Principal, ttl, req.ForceRotate)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := h.afterCredentialWrite(c, tenantStore); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// principal is required precisely so the mint is attributable — log it
	// (the admin audit log records operator actions; this path is
	// machine-driven and its audit record is the CP's structured log plus the
	// grant row itself).
	slog.Info("service credential minted.",
		"org", orgID,
		"credential_id", issued.CredentialID,
		"principal", issued.Principal,
		"rotated", issued.Rotated,
		"expires_at", issued.ExpiresAt.UTC().Format(time.RFC3339),
	)

	c.JSON(http.StatusOK, h.credentialResponse(orgID, issued))
}

// refreshServiceCredential handles
// POST /api/v1/orgs/:id/service-credentials/refresh. Refresh ALWAYS rotates
// the named grant's secret and returns the new plaintext — unlike mint, there
// is no reuse branch: the caller named a specific credential, so "change
// nothing" would be a lie either way.
func (h *handler) refreshServiceCredential(c *gin.Context, tenantStore TenantStore) {
	orgID := c.Param("id")

	var req serviceCredentialRefreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.CredentialID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "credential_id is required"})
		return
	}
	ttl := clampCredentialTTL(req.TTLSeconds)

	exists, err := tenantStore.OrgExists(orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}

	issued, err := tenantStore.RefreshServiceCredential(orgID, req.CredentialID, ttl)
	if err != nil {
		switch {
		case errors.Is(err, configstore.ErrServiceCredentialNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		case errors.Is(err, configstore.ErrServiceCredentialRevoked):
			// 410 Gone: the credential existed and is terminally dead —
			// distinguishable from a 404 (never existed) and from a rotation
			// race.
			c.JSON(http.StatusGone, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}
	if err := h.afterCredentialWrite(c, tenantStore); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	slog.Info("service credential refreshed.",
		"org", orgID,
		"credential_id", issued.CredentialID,
		"principal", issued.Principal,
		"expires_at", issued.ExpiresAt.UTC().Format(time.RFC3339),
	)

	c.JSON(http.StatusOK, h.credentialResponse(orgID, issued))
}

// managedIngressSuffix returns the DNS suffix joined onto the org ID to build
// connect.host. The wired value (the CP's configured managed hostname suffix)
// wins; when unwired (unit tests that build a handler directly) it falls back
// to DefaultManagedIngressSuffix.
func (h *handler) managedIngressSuffix() string {
	if h.ingressSuffix != "" {
		return h.ingressSuffix
	}
	return DefaultManagedIngressSuffix
}
