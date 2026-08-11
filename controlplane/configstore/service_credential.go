package configstore

import (
	"errors"
	"fmt"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ServiceCredentialIssue is the result of issuing-or-reusing a team-scoped
// service credential.
type ServiceCredentialIssue struct {
	// Rotated reports whether the project_user login's password hash was
	// replaced by this call (true) or an already-live credential was handed
	// back (false).
	Rotated bool
	// Username the client connects as (posthog_team_<id>_rw).
	Username string
	// Plaintext is the credential the caller hands to clients. NEVER empty on
	// success: when Rotated is true it's the newly bound password; when false
	// it's re-derived such that it still matches the stored hash (see the
	// reuse path in IssueProjectUserServiceCredential for how that's possible
	// without storing plaintext).
	Plaintext string
	// ExpiresAt is the hard cut for NEW connections. Established sessions are
	// never torn down on expiry — freshness is enforced only at the pgwire
	// handshake (the RDS-IAM contract).
	ExpiresAt time.Time
}

// rotationSafetyWindowHash is the minimum remaining life below which we rotate
// instead of reusing. Package-private: HTTP-layer callers see TTL clamping;
// this is the implementation detail of "don't hand back a credential about to
// expire".
const rotationSafetyWindowHash = time.Minute

// IssueProjectUserServiceCredential returns a credential a short-lived job
// can present as the pgwire password for one team's project_user login.
//
// Design (CLAUDE.md "Service Credentials"):
//
//   - Identity REUSE: the login is the team's canonical posthog_team_<id>_rw
//     (access_mode=project_user), so sessions get exactly the namespaces the
//     admin project-login endpoint would grant — no parallel policy to audit.
//   - Expiry by ROTATION, not a stored timestamp: the hash on the row IS the
//     credential. A caller asking again before the current grant expires gets
//     the SAME credential back (no row change, no updated_at bump, no
//     spurious discovery wake-ups); a caller asking after expiry triggers
//     one bcrypt rotation, after which the prior credential stops working.
//     Leak window = TTL + however long until the next touch by any caller.
//   - Concurrency: the whole decide-then-mutate sequence runs under the org
//     admission lock so two simultaneous issues for the same team can't
//     double-rotate into racing hashes that invalidate each other.
func (cs *ConfigStore) IssueProjectUserServiceCredential(
	orgID string,
	teamID int64,
	principal string,
	ttl time.Duration,
	forceRotate bool,
) (*ServiceCredentialIssue, error) {
	if orgID == "" {
		return nil, errors.New("orgID is required")
	}
	if teamID <= 0 {
		return nil, errors.New("teamID must be a positive integer")
	}
	if principal == "" {
		return nil, errors.New("principal is required")
	}
	if ttl <= 0 {
		return nil, errors.New("ttl must be positive")
	}

	username := fmt.Sprintf("posthog_team_%d_rw", teamID)
	now := time.Now().UTC()

	var issue *ServiceCredentialIssue
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		// Serialize with every other org-wide admission decision so a concurrent
		// mint for another team (or an admin flip, or a second replica racing on
		// this same team) can't interleave a stale read into the
		// compare-then-rotate sequence.
		if err := LockOrgConnectionAdmissionTx(tx, orgID); err != nil {
			return fmt.Errorf("lock org connection admission (org=%s): %w", orgID, err)
		}

		// The team must exist and be enabled. The HTTP handler pre-checks this
		// for a friendly 409, but the store enforces it independently of any
		// caller because a minted login binds exactly that team's namespaces.
		var team OrgTeam
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(
			&team, "org_id = ? AND team_id = ?", orgID, teamID,
		).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrProjectTeamUnavailable
			}
			return fmt.Errorf("load org team (org=%s team=%d): %w", orgID, teamID, err)
		}
		if !team.Enabled {
			return ErrProjectTeamUnavailable
		}

		var user OrgUser
		// Look up by PRIMARY KEY only (org_id, username) — never by
		// access_mode. A row with this username but a different mode must be
		// treated as "exists": otherwise the rotate-vs-create split below would
		// take the create branch straight into a PK conflict, or (worse) the
		// username/account an admin flipped would coexist with the row the mint
		// created. The rotate branch re-establishes every project_user
		// invariant (mode, team, passthrough, enabled) on whatever row it finds.
		loadErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(
			&user, "org_id = ? AND username = ?", orgID, username,
		).Error
		userExists := loadErr == nil
		if loadErr != nil && !errors.Is(loadErr, gorm.ErrRecordNotFound) {
			return fmt.Errorf("load project user (org=%s user=%s): %w", orgID, username, loadErr)
		}

		if userExists && user.Disabled {
			// Never resurrect a disabled login — not by reusing its live grant
			// and not by rotating it. The kill switch is an explicit operator
			// action and a service mint (an internal-secret-authed machine
			// caller) must not undo it. The operator re-enables via the admin
			// API; then the next mint succeeds.
			return fmt.Errorf("project login %s in org %s is disabled; refusing to mint a service credential against it (re-enable via the admin API)", username, orgID)
		}

		if userExists && !forceRotate {
			// The reuse decision must be driven by service_grant_expires_at —
			// mint-time state only THIS path writes — never by updated_at,
			// which the admin project-login rotation also bumps (that would
			// silently reset the TTL clock and hand a fresh fetcher an empty
			// plaintext for a credential it never saw). NULL means the last
			// password-setter wasn't us (admin rotation, legacy row), so the
			// hash is untrusted: fall through and rotate.
			if exp := user.ServiceGrantExpiresAt; exp != nil && exp.UTC().After(now.Add(rotationSafetyWindowHash)) {
				// Still comfortably valid. Hand back NO new plaintext AND no new
				// hash: the caller already holding the prior credential can keep
				// using it, and a fresh fetcher with nothing gets nothing —
				// which forces it to come back after expiry (when this branch
				// flips to rotate) rather than every fetch mid-job smashing the
				// hash a run's sibling steps are still presenting.
				issue = &ServiceCredentialIssue{
					Rotated:   false,
					Username:  username,
					Plaintext: "",
					// Report the grant's stored expiry — minted with the TTL in
					// effect at mint time, not this call's ttl.
					ExpiresAt: exp.UTC(),
				}
				return nil
			}
		}

		// Rotate (or create): bind a fresh random credential to the team's
		// project_user login. Deleting the row is deliberately avoided — it
		// would surface to discovery's change-marker consumers as a login
		// removal and would drift from the admin console's "one writer login
		// per team, CP-managed" model.
		plaintext, err := GeneratePassword()
		if err != nil {
			return fmt.Errorf("generate service credential: %w", err)
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
		if err != nil {
			return fmt.Errorf("hash service credential: %w", err)
		}

		if userExists {
			// Update in place: set the mint clock (service_grant_expires_at),
			// bumped updated_at so config-generation consumers see the rotation,
			// and RE-ESTABLISH every project_user invariant — an operator who
			// (mistakenly or maliciously) flipped mode/team/passthrough on this
			// username must not widen or void the login a minted credential binds.
			if err := tx.Model(&OrgUser{}).
				Where("org_id = ? AND username = ?", orgID, username).
				Updates(map[string]interface{}{
					"password":                 string(hash),
					"access_mode":              OrgUserAccessModeProjectUser,
					"team_id":                  teamID,
					"passthrough":              false,
					"updated_at":               now,
					"service_grant_expires_at": now.Add(ttl),
				}).Error; err != nil {
				return fmt.Errorf("rotate project user password (org=%s user=%s): %w", orgID, username, err)
			}
		} else {
			expiresAt := now.Add(ttl)
			user = OrgUser{
				OrgID:                 orgID,
				Username:              username,
				Password:              string(hash),
				Passthrough:           false,
				AccessMode:            OrgUserAccessModeProjectUser,
				TeamID:                &teamID,
				Disabled:              false,
				UpdatedAt:             now,
				ServiceGrantExpiresAt: &expiresAt,
			}
			if err := tx.Create(&user).Error; err != nil {
				return fmt.Errorf("create project user (org=%s user=%s): %w", orgID, username, err)
			}
		}

		issue = &ServiceCredentialIssue{
			Rotated:   true,
			Username:  username,
			Plaintext: plaintext,
			ExpiresAt: now.Add(ttl),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return issue, nil
}
