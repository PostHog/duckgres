//go:build kubernetes

package admin

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// AdminAuditEntry is one append-only record of an admin-UI action. Every
// mutation (config-store write, user/secret change) and every impersonation
// statement writes a row. SQL text is stored already-redacted by the caller —
// never store raw secret DDL here.
type AdminAuditEntry struct {
	ID         uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	Timestamp  time.Time `gorm:"index" json:"ts"`
	Actor      string    `gorm:"index" json:"actor"`  // SSO email or "internal-secret"
	Role       string    `json:"role"`                // viewer/admin
	Source     string    `json:"source"`              // sso / internal-secret
	Action     string    `gorm:"index" json:"action"` // e.g. "config.update", "impersonate.query"
	Method     string    `json:"method"`
	Path       string    `json:"path"`
	Org        string    `gorm:"index" json:"org"`
	TargetUser string    `json:"target_user"`
	SQLRedacted string   `json:"sql_redacted"`
	Status     int       `json:"status"`
	RemoteAddr string    `json:"remote_addr"`
}

// TableName pins the audit table name in the config-store database.
func (AdminAuditEntry) TableName() string { return "duckgres_admin_audit" }

// AuditStore persists admin audit entries.
type AuditStore struct {
	db *gorm.DB
}

// NewAuditStore returns an AuditStore over the config-store DB and ensures the
// table exists (AutoMigrate, matching the runtime-table bootstrap pattern — the
// audit log is operational state, not goose-migrated tenant config).
func NewAuditStore(db *gorm.DB) (*AuditStore, error) {
	if err := db.AutoMigrate(&AdminAuditEntry{}); err != nil {
		return nil, err
	}
	return &AuditStore{db: db}, nil
}

// Record appends an entry. A failure to persist is returned to the caller; for
// security-sensitive actions (impersonation) the handler treats a failed audit
// write as a failed request.
func (a *AuditStore) Record(e *AdminAuditEntry) error {
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now().UTC()
	}
	return a.db.Create(e).Error
}

// List returns recent entries, newest first, optionally filtered by org/actor.
func (a *AuditStore) List(org, actor string, limit int) ([]AdminAuditEntry, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	q := a.db.Model(&AdminAuditEntry{}).Order("id DESC").Limit(limit)
	if org != "" {
		q = q.Where("org = ?", org)
	}
	if actor != "" {
		q = q.Where("actor = ?", actor)
	}
	var out []AdminAuditEntry
	if err := q.Find(&out).Error; err != nil {
		return nil, err
	}
	return out, nil
}

// AuditMiddleware records every mutating request (POST/PUT/PATCH/DELETE) after
// it completes. It deliberately does NOT capture request bodies (they may carry
// credentials); the impersonation handler records its own richer entry with
// redacted SQL. A best-effort write — a logging failure must not fail an
// already-applied config mutation.
func AuditMiddleware(store *AuditStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		switch c.Request.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		default:
			return
		}
		// Impersonation records itself with full detail; skip the generic row.
		if c.GetBool(ctxAuditHandledKey) {
			return
		}
		id := IdentityFromContext(c)
		entry := &AdminAuditEntry{
			Action:     "config." + actionVerb(c.Request.Method),
			Method:     c.Request.Method,
			Path:       c.FullPath(),
			Org:        c.Param("id"),
			Status:     c.Writer.Status(),
			RemoteAddr: c.ClientIP(),
		}
		if id != nil {
			entry.Actor, entry.Role, entry.Source = id.Email, string(id.Role), id.Source
		}
		_ = store.Record(entry)
	}
}

const ctxAuditHandledKey = "duckgres_audit_handled"

func actionVerb(method string) string {
	switch method {
	case http.MethodPost:
		return "create"
	case http.MethodPut, http.MethodPatch:
		return "update"
	case http.MethodDelete:
		return "delete"
	default:
		return "other"
	}
}

// registerAuditAPI wires GET /audit (admin-only via RoleGate adminOnlyGET list).
func registerAuditAPI(r *gin.RouterGroup, store *AuditStore) {
	r.GET("/audit", func(c *gin.Context) {
		limit, _ := strconv.Atoi(c.Query("limit"))
		entries, err := store.List(c.Query("org"), c.Query("actor"), limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"entries": entries})
	})
}
