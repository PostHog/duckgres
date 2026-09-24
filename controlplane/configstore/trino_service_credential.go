package configstore

import (
	"container/list"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

var ErrTrinoServiceCredentialDenied = errors.New("invalid service credential")

var trinoServiceCredentialUsername = regexp.MustCompile(`^([a-z0-9](?:[a-z0-9-]*[a-z0-9])?)\.(svc_[0-9a-f]{24})$`)

type TrinoServiceCredentialIdentity struct {
	User      string    `json:"user"`
	Groups    []string  `json:"groups"`
	ExpiresAt time.Time `json:"expires_at"`
}

type trinoServiceCredentialRecord struct {
	PasswordHash string
	ExpiresAt    time.Time
	RevokedAt    *time.Time
	Tier         string
	Enabled      bool
}

// Only expensive bcrypt work is cached. Every request still reads the live
// grant, expiry, revocation, tenant enablement, and tier from Postgres.
type servicePasswordCache struct {
	key     []byte
	mu      sync.Mutex
	entries map[[sha256.Size]byte]*list.Element
	order   list.List
}

type servicePasswordCacheEntry struct {
	key       [sha256.Size]byte
	expiresAt time.Time
}

func newServicePasswordCache() *servicePasswordCache {
	return &servicePasswordCache{
		key:     []byte(rand.Text()),
		entries: make(map[[sha256.Size]byte]*list.Element),
	}
}

var trinoServicePasswords = newServicePasswordCache()

func (c *servicePasswordCache) matches(hash, password string) bool {
	// An ephemeral key prevents cache entries becoming reusable password digests.
	mac := hmac.New(sha256.New, c.key)
	_, _ = mac.Write([]byte(hash + "\x00" + password))
	var key [sha256.Size]byte
	copy(key[:], mac.Sum(nil))
	c.mu.Lock()
	if entry, found := c.entries[key]; found {
		if entry.Value.(servicePasswordCacheEntry).expiresAt.After(time.Now()) {
			c.order.MoveToFront(entry)
			c.mu.Unlock()
			return true
		}
		delete(c.entries, key)
		c.order.Remove(entry)
	}
	c.mu.Unlock()
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, found := c.entries[key]; found {
		c.order.Remove(entry)
	}
	c.entries[key] = c.order.PushFront(servicePasswordCacheEntry{key, time.Now().Add(5 * time.Minute)})
	if c.order.Len() > 4096 {
		oldest := c.order.Back()
		delete(c.entries, oldest.Value.(servicePasswordCacheEntry).key)
		c.order.Remove(oldest)
	}
	return true
}

func trinoServiceIdentity(username string, row trinoServiceCredentialRecord, password string) (*TrinoServiceCredentialIdentity, error) {
	if !row.Enabled || row.RevokedAt != nil || !row.ExpiresAt.After(time.Now()) || row.PasswordHash == "" {
		return nil, ErrTrinoServiceCredentialDenied
	}
	if !trinoServicePasswords.matches(row.PasswordHash, password) || !row.ExpiresAt.After(time.Now()) {
		return nil, ErrTrinoServiceCredentialDenied
	}
	tier := row.Tier
	if tier != "scale" && tier != "growth" {
		tier = "free"
	}
	databaseName, _, _ := strings.Cut(username, ".")
	return &TrinoServiceCredentialIdentity{
		User:      username,
		Groups:    []string{TrinoCatalogName(databaseName), "tier_" + tier},
		ExpiresAt: row.ExpiresAt,
	}, nil
}

// ValidateTrinoServiceCredential authenticates a tenant-qualified service grant.
// Unlike pgwire, Trino authenticates each HTTP request, including query polling.
func (cs *ConfigStore) ValidateTrinoServiceCredential(ctx context.Context, username, password string) (*TrinoServiceCredentialIdentity, error) {
	parts := trinoServiceCredentialUsername.FindStringSubmatch(username)
	if parts == nil || len(parts[1]) > 63 || len(password) == 0 || len(password) > 72 {
		return nil, ErrTrinoServiceCredentialDenied
	}
	var row trinoServiceCredentialRecord
	result := cs.db.WithContext(ctx).Raw(`
		SELECT g.password_hash, g.expires_at, g.revoked_at, t.tier, t.enabled
		FROM duckgres_orgs AS o
		JOIN duckgres_service_grants AS g ON g.org_id = o.name
		JOIN duckgres_managed_warehouse_trino AS t ON t.org_id = o.name
		WHERE o.database_name = ? AND g.credential_id = ?`, parts[1], parts[2]).Scan(&row)
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected != 1 {
		return nil, ErrTrinoServiceCredentialDenied
	}
	return trinoServiceIdentity(username, row, password)
}
