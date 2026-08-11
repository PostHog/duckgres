package provisioning

import (
	"errors"
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
	// rotationSafetyWindow is subtracted from the remaining TTL when deciding
	// whether the CURRENT credential is still safe to reuse: a job fetching a
	// credential needs enough runway to finish its work before expiry, not
	// just "not yet expired".
	rotationSafetyWindow = time.Minute
)

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

type serviceCredentialResponse struct {
	Username string `json:"username"`
	// Password is omitted (not empty-stringed) when the CP reused a live
	// grant: a caller that already holds the credential keeps using it;
	// echoing "" would risk clients treating "" as the credential itself.
	Password  string    `json:"password,omitempty"`
	ExpiresAt time.Time `json:"expires_at"`
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
	// routinely fail its first few auth attempts on this CP. Peer fan-out is
	// the same fire-and-forget pattern the admin project-login endpoint uses.
	if err := tenantStore.ReloadSnapshot(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "credential issued but snapshot reload failed: " + err.Error()})
		return
	}

	resp := serviceCredentialResponse{
		Username:  issued.Username,
		Password:  issued.Plaintext,
		ExpiresAt: issued.ExpiresAt,
	}
	c.JSON(http.StatusOK, resp)
}
