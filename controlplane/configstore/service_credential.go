package configstore

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ServiceCredentialPrefix marks a pgwire username as a minted service
// credential (credential_id) rather than a duckgres_org_users login. The
// config snapshot resolves such usernames against duckgres_service_grants
// rows ONLY — service credentials are never project-scoped and never touch
// the org users table.
const ServiceCredentialPrefix = "svc_"

// ErrServiceCredentialNotFound is returned by RefreshServiceCredential and
// RevokeServiceGrant when no grant row exists for (orgID, credentialID).
// HTTP handlers map it to 404.
var ErrServiceCredentialNotFound = errors.New("service credential not found")

// ErrServiceCredentialRevoked is returned by RefreshServiceCredential when the
// grant row exists but was revoked. Revocation is terminal; refresh must never
// resurrect it. HTTP handlers map it to 410.
var ErrServiceCredentialRevoked = errors.New("service credential revoked")

// ServiceCredentialIssue is the result of minting a service credential or
// refreshing one by credential ID.
type ServiceCredentialIssue struct {
	// CredentialID is the server-generated identity the client connects as
	// (svc_<24 random hex>).
	CredentialID string
	// Principal echoes the audit attribution the grant carries.
	Principal string
	// Plaintext is the freshly bound secret returned once by mint or refresh.
	Plaintext string
	// ExpiresAt is the hard cut for NEW connections. Established sessions are
	// never torn down on expiry — freshness is enforced only at the pgwire
	// handshake (the RDS-IAM contract).
	ExpiresAt time.Time
}

// GenerateCredentialID returns a server-side random credential identity —
// "svc_" + 24 hex chars (96 bits). Never caller-supplied.
func GenerateCredentialID() (string, error) {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate credential id: %w", err)
	}
	return ServiceCredentialPrefix + hex.EncodeToString(b), nil
}

// MintServiceCredential creates a new service credential for orgID.
//
// Design (CLAUDE.md "Service Credentials"):
//
//   - Per-credential rows: every minted credential is its own
//     duckgres_service_grants row keyed (org_id, credential_id). NOTHING is
//     written to duckgres_org_users — there is no shared login row for an
//     operator action to clobber mid-run.
//   - Every call creates a fresh identity and secret. Principal is required
//     audit attribution, not an identity or reuse key. Two jobs may therefore
//     use the same principal without sharing or invalidating credentials.
//   - Plaintext is returned exactly once, on this call, and never stored.
//     Subsequent management names this credential ID explicitly through
//     RefreshServiceCredential or RevokeServiceGrant.
func (cs *ConfigStore) MintServiceCredential(
	orgID string,
	principal string,
	ttl time.Duration,
) (*ServiceCredentialIssue, error) {
	if orgID == "" {
		return nil, errors.New("orgID is required")
	}
	if principal == "" {
		return nil, errors.New("principal is required")
	}
	if ttl <= 0 {
		return nil, errors.New("ttl must be positive")
	}

	plaintext, err := GeneratePassword()
	if err != nil {
		return nil, fmt.Errorf("generate service credential secret: %w", err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash service credential secret: %w", err)
	}
	credentialID, err := GenerateCredentialID()
	if err != nil {
		return nil, err
	}
	var expiresAt time.Time

	err = cs.db.Transaction(func(tx *gorm.DB) error {
		// Serialize against org lifecycle/config mutations. Always-create no
		// longer needs principal-based serialization, but without this lock a
		// concurrent org deletion could commit between the existence check and
		// insert (the grants table intentionally has no foreign key).
		if err := LockOrgConnectionAdmissionTx(tx, orgID); err != nil {
			return fmt.Errorf("lock org connection admission (org=%s): %w", orgID, err)
		}
		// The org must exist. The HTTP handler pre-checks for a friendly 404,
		// but the store enforces independently of any caller — a minted
		// credential authenticates against the org, so minting into a ghost
		// org would hand out an unresolvable identity.
		var org Org
		if err := tx.First(&org, "name = ?", orgID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return gorm.ErrRecordNotFound
			}
			return fmt.Errorf("load org (org=%s): %w", orgID, err)
		}

		// Start the TTL only after admission/lifecycle lock contention. Secret
		// generation and bcrypt deliberately happen before the transaction so
		// expensive hashing never lengthens this critical section.
		now := time.Now().UTC()
		expiresAt = now.Add(ttl)
		grant := ServiceGrant{
			OrgID:         orgID,
			CredentialID:  credentialID,
			Principal:     principal,
			PasswordHash:  string(hash),
			MintedAt:      now,
			LastRotatedAt: now,
			ExpiresAt:     expiresAt,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := tx.Create(&grant).Error; err != nil {
			return fmt.Errorf("create service grant (org=%s principal=%s): %w", orgID, principal, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &ServiceCredentialIssue{
		CredentialID: credentialID,
		Principal:    principal,
		Plaintext:    plaintext,
		ExpiresAt:    expiresAt,
	}, nil
}

// RefreshServiceCredential ALWAYS rotates an existing grant's secret and
// re-arms its expiry from now+ttl, returning the new plaintext. It never
// creates a grant (unknown credential_id → ErrServiceCredentialNotFound) and
// never resurrects one (revoked → ErrServiceCredentialRevoked). An
// already-expired (but not revoked) grant MAY be refreshed: expiry only
// refuses NEW pgwire handshakes, and a refresh is how a caller that missed
// the window gets back to a working secret without a full mint.
//
// The rotation does NOT tear down established sessions — the mint plane is
// separate from connection scheduling.
func (cs *ConfigStore) RefreshServiceCredential(
	orgID string,
	credentialID string,
	ttl time.Duration,
) (*ServiceCredentialIssue, error) {
	if orgID == "" {
		return nil, errors.New("orgID is required")
	}
	if credentialID == "" {
		return nil, errors.New("credentialID is required")
	}
	if ttl <= 0 {
		return nil, errors.New("ttl must be positive")
	}

	// Hash before taking the org lifecycle/admission lock. This may do a
	// little wasted work for an unknown/revoked ID, but keeps bcrypt out of a
	// lock shared with scheduling and org mutations.
	plaintext, err := GeneratePassword()
	if err != nil {
		return nil, fmt.Errorf("generate service credential secret: %w", err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(plaintext), bcrypt.DefaultCost)
	if err != nil {
		return nil, fmt.Errorf("hash service credential secret: %w", err)
	}

	var issue *ServiceCredentialIssue
	err = cs.db.Transaction(func(tx *gorm.DB) error {
		if err := LockOrgConnectionAdmissionTx(tx, orgID); err != nil {
			return fmt.Errorf("lock org connection admission (org=%s): %w", orgID, err)
		}

		var grant ServiceGrant
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(
			&grant, "org_id = ? AND credential_id = ?", orgID, credentialID,
		).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrServiceCredentialNotFound
			}
			return fmt.Errorf("load service grant (org=%s credential=%s): %w", orgID, credentialID, err)
		}
		if grant.RevokedAt != nil {
			return ErrServiceCredentialRevoked
		}

		// Admission lock waits must not consume the newly issued TTL.
		now := time.Now().UTC()
		expiresAt := now.Add(ttl)
		result := tx.Model(&ServiceGrant{}).
			Where("org_id = ? AND credential_id = ?", orgID, credentialID).
			Updates(map[string]any{
				"password_hash":   string(hash),
				"last_rotated_at": now,
				"expires_at":      expiresAt,
				"updated_at":      now,
			})
		if result.Error != nil {
			return fmt.Errorf("rotate service grant (org=%s credential=%s): %w", orgID, credentialID, result.Error)
		}
		issue = &ServiceCredentialIssue{
			CredentialID: credentialID,
			Principal:    grant.Principal,
			Plaintext:    plaintext,
			ExpiresAt:    expiresAt,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return issue, nil
}

// ListServiceGrants returns every grant row for the org — all statuses
// (live, expired, revoked) — newest-activity first, for the admin UI. The
// org-existence probe returns gorm.ErrRecordNotFound for a ghost org (the
// admin handler maps it to 404). Rows carry the bcrypt hash but never
// plaintext; callers must not serialize PasswordHash (the field is json:"-").
func (cs *ConfigStore) ListServiceGrants(orgID string) ([]ServiceGrant, error) {
	var count int64
	if err := cs.db.Model(&Org{}).Where("name = ?", orgID).Count(&count).Error; err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, gorm.ErrRecordNotFound
	}
	var grants []ServiceGrant
	if err := cs.db.Where("org_id = ?", orgID).
		Order("last_rotated_at DESC, credential_id").
		Find(&grants).Error; err != nil {
		return nil, err
	}
	return grants, nil
}

// RevokeServiceGrant terminally revokes one grant: sets revoked_at and BLANKS
// the bcrypt hash so a leaked credential can never authenticate again, even if
// the row is later misread. The row is kept for provenance — investigation can
// still see who minted it and when. Returns gorm.ErrRecordNotFound for a ghost
// org and ErrServiceCredentialNotFound for an unknown credential_id; revoking
// an already-revoked grant succeeds (idempotent).
func (cs *ConfigStore) RevokeServiceGrant(orgID, credentialID string) error {
	if orgID == "" {
		return errors.New("orgID is required")
	}
	if credentialID == "" {
		return errors.New("credentialID is required")
	}
	now := time.Now().UTC()
	return cs.db.Transaction(func(tx *gorm.DB) error {
		if err := LockOrgConnectionAdmissionTx(tx, orgID); err != nil {
			return fmt.Errorf("lock org connection admission (org=%s): %w", orgID, err)
		}
		var org Org
		if err := tx.First(&org, "name = ?", orgID).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return gorm.ErrRecordNotFound
			}
			return fmt.Errorf("load org (org=%s): %w", orgID, err)
		}
		var grant ServiceGrant
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(
			&grant, "org_id = ? AND credential_id = ?", orgID, credentialID,
		).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrServiceCredentialNotFound
			}
			return fmt.Errorf("load service grant (org=%s credential=%s): %w", orgID, credentialID, err)
		}
		if grant.RevokedAt != nil {
			return nil // already revoked — idempotent
		}
		result := tx.Model(&ServiceGrant{}).
			Where("org_id = ? AND credential_id = ?", orgID, credentialID).
			Updates(map[string]any{
				"revoked_at":    now,
				"password_hash": "",
				"updated_at":    now,
			})
		if result.Error != nil {
			return fmt.Errorf("revoke service grant (org=%s credential=%s): %w", orgID, credentialID, result.Error)
		}
		return nil
	})
}
