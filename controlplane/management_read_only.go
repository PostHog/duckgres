//go:build kubernetes

package controlplane

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// envManagementReadOnly overrides the management API's read-only state.
// Unset (the default) follows the hand-over marker; "true" forces read-only
// with or without a marker; "false" keeps the API writable despite a marker,
// a break-glass for an operator who must repair a row through duckgres while
// hogtower is down. Unparsable values are ignored (marker decides). Read once
// at startup, like the other env-only knobs.
const envManagementReadOnly = "DUCKGRES_MANAGEMENT_READ_ONLY"

// managementReadOnlyTTL bounds how long a cached hand-over answer is reused.
// Only management writes consult the gate, and those are rare (Django,
// operators), so the marker costs at most one primary-key read per replica
// every TTL. The switch reaches every replica within one TTL, no restart.
const managementReadOnlyTTL = 10 * time.Second

// managementReadOnlyLookupTimeout bounds the marker read on the request path.
const managementReadOnlyLookupTimeout = 2 * time.Second

// defaultManagementOwner names the owner in the 409 when the env override
// forces read-only without a recorded hand-over.
const defaultManagementOwner = "hogtower"

// managementOperationalWrites are the non-GET routes on the authenticated
// /api/v1 group that stay open after the hand-over. They act on pgwire
// runtime state or on data duckgres still owns, not on the management model
// (warehouses, teams, users, credentials, Trino placement, operators) that
// hogtower now owns. Everything else that is not GET/HEAD/OPTIONS is refused,
// so a write route added later is read-only by default until someone decides
// otherwise here.
var managementOperationalWrites = map[string]string{
	// Peer fan-out: replicas reload their config snapshot. hogtower writes
	// the store directly, so reloading is exactly what must keep working.
	"POST /api/v1/internal/reload-snapshot": "snapshot reload",
	// Compute billing stays with duckgres, which meters pgwire usage.
	"POST /api/v1/billing/ack": "billing pull acknowledgement",
	// Live pgwire/Trino sessions and queries.
	"POST /api/v1/orgs/:id/impersonate/query":     "operator query",
	"POST /api/v1/sessions/:pid/cancel":           "session cancel",
	"POST /api/v1/sessions/by-worker/:wid/cancel": "session cancel",
	"POST /api/v1/orgs/:id/users/:username/kill":  "session kill",
	"POST /api/v1/trino/queries/:id/kill":         "query kill",
	// Persistent secrets belong to pgwire (CREATE PERSISTENT SECRET).
	"DELETE /api/v1/orgs/:id/users/:username/secrets/:name": "pgwire persistent secret",
	// Metadata-store reshards are still run by duckgres; hogtower observes
	// the resharding state but never starts one.
	"POST /api/v1/orgs/:id/reshard":      "reshard",
	"POST /api/v1/reshards/:opid/cancel": "reshard cancel",
}

// managementLocalScopeWrites are user-state routes whose peer fan-out leg
// (?scope=local) only reloads the snapshot and kills local sessions; the DB
// write happens on the primary call, which the gate refuses.
var managementLocalScopeWrites = map[string]struct{}{
	"POST /api/v1/orgs/:id/users/:username/disable": {},
	"POST /api/v1/orgs/:id/users/:username/enable":  {},
}

// managementWriteGate refuses management writes on the HTTP API once another
// control plane owns provisioning. Only the HTTP surface becomes read-only:
// the config store itself stays writable, because hogtower mirrors its state
// into duckgres' tables directly.
type managementWriteGate struct {
	override *bool
	lookup   func(context.Context) (string, error)
	ttl      time.Duration
	now      func() time.Time

	mu        sync.Mutex
	owner     string
	checkedAt time.Time
}

// newManagementWriteGate builds the gate over the config store. startupOwner
// is what applyControlHandover read, so the first writes after startup need
// no extra query.
func newManagementWriteGate(db *gorm.DB, startupOwner string) *managementWriteGate {
	g := &managementWriteGate{
		override: managementReadOnlyOverride(),
		lookup: func(ctx context.Context) (string, error) {
			return readControlHandoverOwner(db.WithContext(ctx))
		},
		ttl:   managementReadOnlyTTL,
		now:   time.Now,
		owner: startupOwner,
	}
	g.checkedAt = g.now()
	switch {
	case g.override != nil && *g.override:
		slog.Warn("Management API forced read-only by " + envManagementReadOnly + ".")
	case g.override != nil:
		slog.Warn("Management API forced writable by "+envManagementReadOnly+"; a recorded hand-over is ignored for HTTP writes.", "owner", startupOwner)
	case startupOwner != "":
		slog.Warn("Management API is read-only: provisioning is handed over.", "owner", startupOwner)
	}
	return g
}

func managementReadOnlyOverride() *bool {
	raw := strings.TrimSpace(os.Getenv(envManagementReadOnly))
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("Ignoring unparsable "+envManagementReadOnly+"; the hand-over marker decides.", "value", raw)
		return nil
	}
	return &v
}

// readOnlyOwner returns the owner that makes the API read-only, or "" when
// writes are allowed. The marker is re-read at most once per TTL; a failed
// read keeps the last answer (and waits a full TTL before retrying) rather
// than flipping on a transient store error.
func (g *managementWriteGate) readOnlyOwner() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.override != nil && !*g.override {
		return ""
	}
	if now := g.now(); now.Sub(g.checkedAt) >= g.ttl {
		g.checkedAt = now
		ctx, cancel := context.WithTimeout(context.Background(), managementReadOnlyLookupTimeout)
		owner, err := g.lookup(ctx)
		cancel()
		switch {
		case err != nil:
			slog.Warn("Management API hand-over check failed; keeping the previous state.", "read_only", g.owner != "", "error", err)
		case owner != g.owner:
			slog.Warn("Management API hand-over changed.", "owner", owner, "previous_owner", g.owner, "read_only", owner != "")
			g.owner = owner
		}
	}
	if g.owner == "" && g.override != nil && *g.override {
		return defaultManagementOwner
	}
	return g.owner
}

// managementWriteAllowed reports whether a request stays open after the
// hand-over regardless of the gate's state.
func managementWriteAllowed(c *gin.Context) bool {
	switch c.Request.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	}
	key := c.Request.Method + " " + c.FullPath()
	if _, ok := managementOperationalWrites[key]; ok {
		return true
	}
	if _, ok := managementLocalScopeWrites[key]; ok && c.Query("scope") == "local" {
		return true
	}
	return false
}

// Middleware returns the gin handler for the authenticated /api/v1 group.
func (g *managementWriteGate) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if managementWriteAllowed(c) {
			c.Next()
			return
		}
		if owner := g.readOnlyOwner(); owner != "" {
			c.AbortWithStatusJSON(http.StatusConflict, gin.H{
				"error":      "managed by " + owner + ": this warehouse control plane is read-only",
				"managed_by": owner,
			})
			return
		}
		c.Next()
	}
}
