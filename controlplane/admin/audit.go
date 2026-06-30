//go:build kubernetes

package admin

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// AdminAuditEntry is one append-only record of an admin-UI action. Every
// mutation (config-store write, user/secret change) and every impersonation
// statement writes a row. SQL text is stored already-redacted by the caller —
// never store raw secret DDL here.
type AdminAuditEntry struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	Timestamp   time.Time `gorm:"index" json:"ts"`
	Actor       string    `gorm:"index" json:"actor"`  // SSO email or "internal-secret"
	Role        string    `json:"role"`                // viewer/admin
	Source      string    `json:"source"`              // sso / internal-secret
	Action      string    `gorm:"index" json:"action"` // e.g. "config.update", "impersonate.query"
	Method      string    `json:"method"`
	Path        string    `json:"path"`
	Org         string    `gorm:"index" json:"org"`
	TargetUser  string    `json:"target_user"`
	SQLRedacted string    `json:"sql_redacted"`
	Status      int       `json:"status"`
	RemoteAddr  string    `json:"remote_addr"`
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

// Record appends an entry. A failure to persist is returned to the caller. The
// impersonation handler logs a failed audit write loudly (slog.Error) but does
// not discard an already-executed query — it cannot be un-run; other mutations
// are audited best-effort by AuditMiddleware.
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
		// Operators routes key the target on :email; org user routes on :username.
		targetUser := c.Param("username")
		if targetUser == "" {
			targetUser = c.Param("email")
		}
		id := IdentityFromContext(c)
		entry := &AdminAuditEntry{
			Action: auditActionFor(c.Request.Method, c.Request.URL.Path),
			Method: c.Request.Method,
			// Resolved path (not the route template) so "who changed which
			// user/secret" is answerable from the log.
			Path:       c.Request.URL.Path,
			Org:        c.Param("id"),
			TargetUser: targetUser,
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

// auditActionFor derives a resource-specific audit Action ("<resource>.<verb>")
// from the request method and path. It tolerates both the resolved path
// (c.Request.URL.Path, e.g. "/api/v1/orgs/acme/users/bob") and the route
// template (c.FullPath()), and an optional "/api/v1" version prefix. Unknown
// shapes fall back to the generic "config.<verb>".
func auditActionFor(method, path string) string {
	verb := actionVerb(method)
	segs := strings.FieldsFunc(path, func(r rune) bool { return r == '/' })
	// Drop a leading API version prefix so segs[0] is the resource.
	if len(segs) >= 2 && segs[0] == "api" && segs[1] == "v1" {
		segs = segs[2:]
	}
	if len(segs) == 0 {
		return "config." + verb
	}
	switch segs[0] {
	case "operators":
		return "operators." + verb
	case "orgs":
		// Sub-resources under an org get their own action; the org row itself
		// (and anything else) is generic config.
		for _, s := range segs[1:] {
			switch s {
			case "warehouse":
				return "warehouse." + verb
			case "users":
				return "user." + verb
			}
		}
	}
	return "config." + verb
}

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

// registerAuditAPI wires GET /audit, admin-only via a per-route RequireAdmin
// gate that travels with the route (a rename cannot silently expose it).
func registerAuditAPI(r *gin.RouterGroup, store *AuditStore) {
	r.GET("/audit", RequireAdmin(), func(c *gin.Context) {
		limit, _ := strconv.Atoi(c.Query("limit"))
		entries, err := store.List(c.Query("org"), c.Query("actor"), limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"entries": entries})
	})
}
