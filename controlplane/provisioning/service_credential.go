package provisioning

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
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

type serviceCredentialRequest struct {
	TeamID     int64  `json:"team_id"`
	Principal  string `json:"principal"`
	TTLSeconds int    `json:"ttl_seconds"`
	// ForceRotate bypasses the reuse path: the CP rotates the project_user
	// hash no matter how fresh the current grant is, and returns the new
	// plaintext. A caller MUST set this on its first fetch of a run — it has
	// nothing cached, and the reuse path deliberately returns no plaintext for
	// a still-valid grant (so concurrent long-lived runs can't smash each
	// other's credentials mid-flight). Omit/false means "reuse the live grant
	// if it still has runway, and only tell me its expiry".
	ForceRotate bool `json:"force_rotate"`
}

// connectDetails is the always-present `connect` block of the mint response:
// it tells the caller WHERE to use the credential from the same authoritative
// CP response that issued it, so nothing downstream re-derives its own idea of
// the warehouse endpoint (out-of-band endpoint knowledge — a Django
// `DuckgresServer` row — is exactly the drift this field exists to kill).
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

type serviceCredentialResponse struct {
	Username string `json:"username"`
	// Password is omitted (not empty-stringed) when the CP reused a live
	// grant: a caller that already holds the credential keeps using it;
	// echoing "" would risk clients treating "" as the credential itself.
	Password  string    `json:"password,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
	// Connect is unconditional (unlike Password): identical shape on reuse
	// and rotate, so the client can always take its connection target from
	// this same response instead of holding its own out-of-band copy.
	Connect connectDetails `json:"connect"`
}

// TenantStore is the subset of the config store the service-credential
// handler needs. Satisfied by the live gorm store in production; faked in
// tests.
type TenantStore interface {
	ListOrgTeams(orgID string) ([]configstore.OrgTeam, error)
	IssueProjectUserServiceCredential(orgID string, teamID int64, principal string, ttl time.Duration, forceRotate bool) (*configstore.ServiceCredentialIssue, error)
	ReloadSnapshot() error
}

// registerServiceCredentialAPI adds the credential-mint route to the same
// router group as the rest of the provisioning API (it's mounted by
// RegisterAPI). Keeping it here — not in the admin package — because the
// caller is PostHog's backend (internal-secret), the same trust class as the
// other provisioning routes, not a human operator.
func (h *handler) issueServiceCredential(c *gin.Context, tenantStore TenantStore) {
	orgID := c.Param("id")

	var req serviceCredentialRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.TeamID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "team_id must be a positive integer"})
		return
	}
	// The route carries :team_id too; require the two to agree so a copy-paste
	// bug in a caller surfaces as a 400 here instead of a credential minted
	// against the WRONG team's namespaces.
	if pathTeam, err := strconv.ParseInt(c.Param("team_id"), 10, 64); err != nil || pathTeam != req.TeamID {
		c.JSON(http.StatusBadRequest, gin.H{"error": "path team_id must match body team_id"})
		return
	}
	if req.Principal == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "principal is required (e.g. \"dagster:events-backfill\")"})
		return
	}
	ttl := defaultCredentialTTL
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
	}
	if ttl < minCredentialTTL {
		ttl = minCredentialTTL
	}
	if ttl > maxCredentialTTL {
		ttl = maxCredentialTTL
	}

	// Confirm the team exists and is enabled before doing any credential work:
	// the minted login binds exactly that team's namespaces, so minting against
	// a missing/disabled team would hand out a credential that resolves to a
	// fail-closed empty scope — confusing to debug from the caller side. 409
	// (not 404) to match the project-login admin endpoint's
	// ErrProjectTeamUnavailable mapping: the org may exist while the team
	// row is gone, which is a caller-visible state conflict, not "no org".
	teams, err := tenantStore.ListOrgTeams(orgID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	teamEnabled := false
	for _, t := range teams {
		if t.TeamID == req.TeamID && t.Enabled {
			teamEnabled = true
			break
		}
	}
	if !teamEnabled {
		c.JSON(http.StatusConflict, gin.H{"error": configstore.ErrProjectTeamUnavailable.Error()})
		return
	}

	issued, err := tenantStore.IssueProjectUserServiceCredential(orgID, req.TeamID, req.Principal, ttl, req.ForceRotate)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// The write landed in the shared config-store DB; make THIS replica's
	// snapshot see the new hash immediately rather than waiting one poll
	// interval (default 30s) — otherwise the freshly issued credential would
	// routinely fail its first few auth attempts on this CP.
	if err := tenantStore.ReloadSnapshot(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "credential issued but snapshot reload failed: " + err.Error()})
		return
	}
	// Fan the same reload out to PEER replicas: the client's pgwire
	// connection can land on any CP behind the load balancer, so a credential
	// minted on this replica must auth on whichever replica serves the
	// connect. Best-effort (PostPeers already drops a slow/down peer without
	// error) — a failed peer converges within one poll interval.
	if h.peerFanout != nil {
		h.peerFanout.PostPeers(c.Request.Context(), "/api/v1/internal/reload-snapshot")
	}
	// principal is required precisely so the rotation is attributable — log it
	// (the admin audit log records the equivalent project-login rotations from
	// operators; this path is machine-driven and its audit record is the CP's
	// structured log).
	slog.Info("service credential issued.",
		"org", orgID, "team_id", req.TeamID,
		"principal", req.Principal,
		"rotated", issued.Rotated,
		"expires_at", issued.ExpiresAt.UTC().Format(time.RFC3339),
	)

	resp := serviceCredentialResponse{
		Username:  issued.Username,
		Password:  issued.Plaintext,
		ExpiresAt: issued.ExpiresAt,
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
	c.JSON(http.StatusOK, resp)
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
