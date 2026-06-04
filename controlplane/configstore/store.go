package configstore

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"k8s.io/apimachinery/pkg/api/resource"
)

// OrgUserKey is the composite key for org-scoped user lookups.
type OrgUserKey struct {
	OrgID    string
	Username string
}

var ErrWorkerOwnerEpochMismatch = errors.New("worker owner epoch mismatch")

// ErrWorkerRecordUpsertFenceMiss indicates an UpsertWorkerRecord conflict was
// rejected by the monotonic owner/terminal-state fence and did not persist.
var ErrWorkerRecordUpsertFenceMiss = errors.New("worker record upsert fence miss")

// Snapshot holds a point-in-time copy of all config data for fast lookups.
type Snapshot struct {
	Orgs                  map[string]*OrgConfig
	DatabaseOrg           map[string]string     // database name -> org ID
	HostnameAliasOrg      map[string]string     // hostname alias -> org ID (sparse — only orgs with non-empty alias)
	OrgUserPassword       map[OrgUserKey]string // (orgID, username) -> bcrypt hash
	OrgUserPassthrough    map[OrgUserKey]bool   // (orgID, username) -> passthrough flag
	OrgUserDefaultCatalog map[OrgUserKey]string // (orgID, username) -> default session catalog
	Global                GlobalConfig
	DuckLake              DuckLakeConfig
	RateLimit             RateLimitConfig
	QueryLog              QueryLogConfig
}

// Selectable catalog names. The startup `database` param now names the catalog
// a session defaults to rather than identifying the org — these are the only
// non-empty values a client may request.
const (
	catalogDuckLake = "ducklake"
	catalogIceberg  = "iceberg"
)

// PostgresConnectionResolution is the result of resolving and authenticating a
// Postgres startup packet against one immutable config snapshot.
//
// Identity (OrgID) comes solely from the managed hostname (SNI) plus the
// username/password; the startup `database` param is treated as catalog
// selection, not identity.
type PostgresConnectionResolution struct {
	// OrgID is the organization the connection belongs to, resolved from the
	// managed hostname (SNI). Empty unless SNIResolved.
	OrgID string
	// SNIOrgID mirrors OrgID; kept distinct for log/observability parity.
	SNIOrgID string
	// SNIAliasUsed reports whether the hostname matched via hostname_alias.
	SNIAliasUsed bool
	// SNIResolved is true when the managed hostname resolved to a known org.
	SNIResolved bool
	// EffectiveCatalog is the catalog the session should default to, selected by
	// the startup `database` param: "" (use the per-user/attached default),
	// "ducklake", or "iceberg".
	EffectiveCatalog string
	// CatalogValid is false when the requested `database` is not a selectable
	// catalog name (anything other than "", "ducklake", "iceberg").
	CatalogValid bool
	// Valid is true when (OrgID, username, password) authenticated.
	Valid bool
	// Passthrough / DefaultCatalog are the per-user flags for the resolved user.
	Passthrough    bool
	DefaultCatalog string
}

// ConfigStore manages configuration stored in a PostgreSQL database.
type ConfigStore struct {
	db            *gorm.DB
	runtimeSchema string
	mu            sync.RWMutex
	snapshot      *Snapshot
	pollInterval  time.Duration
	onChange      []func(old, new *Snapshot)
}

// NewConfigStore connects to the PostgreSQL config store, runs migrations,
// ensures singleton rows exist, and loads the initial snapshot.
func NewConfigStore(connStr string, pollInterval time.Duration) (*ConfigStore, error) {
	if pollInterval <= 0 {
		pollInterval = 30 * time.Second
	}

	db, err := gorm.Open(postgres.Open(connStr), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("connect to config store: %w", err)
	}

	// Migrate OrgUser PK from (username) to (org_id, username) if needed.
	// GORM AutoMigrate cannot alter primary keys, so we do it manually.
	if err := migrateOrgUserPK(db); err != nil {
		return nil, fmt.Errorf("migrate org user PK: %w", err)
	}

	// Auto-migrate all models
	if err := db.AutoMigrate(
		&Org{},
		&ManagedWarehouse{},
		&ManagedWarehouseTrino{},
		&TrinoClusterBootstrap{},
		&OrgUser{},
		&GlobalConfig{},
		&DuckLakeConfig{},
		&RateLimitConfig{},
		&QueryLogConfig{},
		&SchemaMigration{},
	); err != nil {
		return nil, fmt.Errorf("auto-migrate config store: %w", err)
	}

	// One-shot data migrations (idempotent — tracked in duckgres_schema_migrations).
	if err := migrateDeltaCatalogDefaultEnabled(db); err != nil {
		return nil, fmt.Errorf("migrate delta catalog default: %w", err)
	}
	if err := migrateIcebergBackendBackfill(db); err != nil {
		return nil, fmt.Errorf("migrate iceberg backend backfill: %w", err)
	}

	runtimeSchema, err := resolveRuntimeSchema(db)
	if err != nil {
		return nil, fmt.Errorf("resolve runtime schema: %w", err)
	}
	if err := ensureRuntimeSchema(db, runtimeSchema); err != nil {
		return nil, fmt.Errorf("ensure runtime schema: %w", err)
	}
	if err := autoMigrateRuntimeTables(db, runtimeSchema); err != nil {
		return nil, fmt.Errorf("auto-migrate runtime schema: %w", err)
	}

	// Ensure singleton rows exist with defaults
	db.FirstOrCreate(&GlobalConfig{}, GlobalConfig{ID: 1})
	db.FirstOrCreate(&DuckLakeConfig{}, DuckLakeConfig{ID: 1})
	db.FirstOrCreate(&RateLimitConfig{}, RateLimitConfig{ID: 1})
	db.FirstOrCreate(&QueryLogConfig{}, QueryLogConfig{ID: 1})

	cs := &ConfigStore{
		db:            db,
		runtimeSchema: runtimeSchema,
		pollInterval:  pollInterval,
	}

	// Load initial snapshot
	snap, err := cs.load()
	if err != nil {
		return nil, fmt.Errorf("load initial config: %w", err)
	}
	cs.snapshot = snap

	slog.Info("Config store connected.", "orgs", len(snap.Orgs), "users", len(snap.OrgUserPassword))
	return cs, nil
}

// Start begins the polling goroutine that periodically reloads config.
func (cs *ConfigStore) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(cs.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				newSnap, err := cs.load()
				if err != nil {
					slog.Warn("Config store poll failed.", "error", err)
					continue
				}

				cs.mu.Lock()
				oldSnap := cs.snapshot
				cs.snapshot = newSnap
				callbacks := make([]func(old, new *Snapshot), len(cs.onChange))
				copy(callbacks, cs.onChange)
				cs.mu.Unlock()

				// Fire callbacks outside the lock
				for _, fn := range callbacks {
					fn(oldSnap, newSnap)
				}
			}
		}
	}()
}

// load fetches all config from the database and builds a Snapshot.
func (cs *ConfigStore) load() (*Snapshot, error) {
	var orgs []Org
	if err := cs.db.Preload("Users").Preload("Warehouse").Find(&orgs).Error; err != nil {
		return nil, fmt.Errorf("load orgs: %w", err)
	}

	var global GlobalConfig
	cs.db.First(&global, 1)

	var duckLake DuckLakeConfig
	cs.db.First(&duckLake, 1)

	var rateLimit RateLimitConfig
	cs.db.First(&rateLimit, 1)

	var queryLog QueryLogConfig
	cs.db.First(&queryLog, 1)

	snap := &Snapshot{
		Orgs:                  make(map[string]*OrgConfig),
		DatabaseOrg:           make(map[string]string),
		HostnameAliasOrg:      make(map[string]string),
		OrgUserPassword:       make(map[OrgUserKey]string),
		OrgUserPassthrough:    make(map[OrgUserKey]bool),
		OrgUserDefaultCatalog: make(map[OrgUserKey]string),
		Global:                global,
		DuckLake:              duckLake,
		RateLimit:             rateLimit,
		QueryLog:              queryLog,
	}

	for _, o := range orgs {
		alias := ""
		if o.HostnameAlias != nil {
			alias = *o.HostnameAlias
		}
		oc := &OrgConfig{
			Name:                o.Name,
			DatabaseName:        o.DatabaseName,
			HostnameAlias:       alias,
			MaxWorkers:          o.MaxWorkers,
			MaxConnections:      o.MaxConnections,
			MemoryBudget:        o.MemoryBudget,
			IdleTimeoutS:        o.IdleTimeoutS,
			WorkerCPURequest:    o.WorkerCPURequest,
			WorkerMemoryRequest: o.WorkerMemoryRequest,
			Users:               make(map[string]string),
			Warehouse:           copyManagedWarehouseConfig(o.Warehouse),
		}
		if o.DatabaseName != "" {
			snap.DatabaseOrg[o.DatabaseName] = o.Name
		}
		if alias != "" {
			snap.HostnameAliasOrg[alias] = o.Name
		}
		for _, u := range o.Users {
			oc.Users[u.Username] = u.Password
			key := OrgUserKey{OrgID: o.Name, Username: u.Username}
			snap.OrgUserPassword[key] = u.Password
			if u.Passthrough {
				snap.OrgUserPassthrough[key] = true
			}
			if u.DefaultCatalog != "" {
				snap.OrgUserDefaultCatalog[key] = u.DefaultCatalog
			}
		}
		snap.Orgs[o.Name] = oc
	}

	return snap, nil
}

// Snapshot returns the current config snapshot.
func (cs *ConfigStore) Snapshot() *Snapshot {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.snapshot
}

// ResolveDatabase maps a database name to an org ID. Returns "" if not found.
func (cs *ConfigStore) ResolveDatabase(database string) string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return ""
	}
	return cs.snapshot.DatabaseOrg[database]
}

// DatabaseNameForSNIPrefix translates an SNI hostname prefix (the single label
// before a managed suffix, e.g. "entirely-chief-wildcat") into the canonical
// database_name for the org it routes to. If the prefix matches a registered
// hostname_alias, returns that org's database_name. Otherwise returns the
// prefix as-is so legacy tenants (no alias configured, prefix == database_name)
// keep working.
//
// Returns "" only when the input is empty.
func (cs *ConfigStore) DatabaseNameForSNIPrefix(prefix string) string {
	if prefix == "" {
		return ""
	}
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return prefix
	}
	if orgID, ok := cs.snapshot.HostnameAliasOrg[prefix]; ok {
		if oc, ok := cs.snapshot.Orgs[orgID]; ok && oc.DatabaseName != "" {
			return oc.DatabaseName
		}
	}
	return prefix
}

// ResolveSNIPrefix maps a managed-hostname prefix to the org/database it names.
// It accepts all supported public hostname labels:
//   - hostname_alias, when configured
//   - database_name, for legacy tenants whose hostname label is the database
//   - org name, for DNS-safe org IDs that differ from database_name
//
// Alias precedence matches DatabaseNameForSNIPrefix: if a label collides with
// another org's database_name or name, the explicit hostname_alias wins.
func (cs *ConfigStore) ResolveSNIPrefix(prefix string) (orgID, databaseName string) {
	if prefix == "" {
		return "", ""
	}
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return "", ""
	}
	orgID, databaseName, _ = resolveSNIPrefixFromSnapshot(cs.snapshot, prefix)
	return orgID, databaseName
}

func resolveSNIPrefixFromSnapshot(snap *Snapshot, prefix string) (orgID, databaseName string, aliasUsed bool) {
	if snap == nil || prefix == "" {
		return "", "", false
	}
	if orgID, ok := snap.HostnameAliasOrg[prefix]; ok {
		if oc, ok := snap.Orgs[orgID]; ok {
			return orgID, oc.DatabaseName, true
		}
		return "", "", false
	}
	if orgID, ok := snap.DatabaseOrg[prefix]; ok {
		return orgID, prefix, false
	}
	if isDNSLabel(prefix) {
		if oc, ok := snap.Orgs[prefix]; ok {
			return oc.Name, oc.DatabaseName, false
		}
	}
	return "", "", false
}

func isDNSLabel(label string) bool {
	if len(label) == 0 || len(label) > 63 {
		return false
	}
	for i, r := range label {
		isAlnum := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		isHyphen := r == '-'
		if !isAlnum && !isHyphen {
			return false
		}
		if isHyphen && (i == 0 || i == len(label)-1) {
			return false
		}
	}
	return true
}

func (cs *ConfigStore) ResolvePostgresConnection(startupDatabase, sniPrefix string, useManagedSNI bool, username, password string) PostgresConnectionResolution {
	result := PostgresConnectionResolution{}

	// The startup `database` param is now pure catalog selection, not identity.
	// Valid values: "" (use the per-user/attached default), "ducklake", or
	// "iceberg". Anything else fails closed — there is no logical-name masking,
	// so an arbitrary name no longer routes anywhere.
	switch strings.ToLower(strings.TrimSpace(startupDatabase)) {
	case "":
		result.CatalogValid = true
	case catalogDuckLake:
		result.EffectiveCatalog = catalogDuckLake
		result.CatalogValid = true
	case catalogIceberg:
		result.EffectiveCatalog = catalogIceberg
		result.CatalogValid = true
	}

	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return result
	}

	// Identity comes from the managed hostname (SNI) only. Without a managed,
	// resolvable hostname there is no org to authenticate against — the database
	// name is no longer consulted for routing.
	if !useManagedSNI {
		return result
	}
	orgID, _, aliasUsed := resolveSNIPrefixFromSnapshot(cs.snapshot, sniPrefix)
	if orgID == "" {
		return result
	}
	result.SNIResolved = true
	result.SNIAliasUsed = aliasUsed
	result.SNIOrgID = orgID
	result.OrgID = orgID

	// Authenticate the user within the resolved org.
	key := OrgUserKey{OrgID: orgID, Username: username}
	storedHash, ok := cs.snapshot.OrgUserPassword[key]
	if !ok {
		// Timing-leak guard: still spend bcrypt time on unknown users.
		_ = bcrypt.CompareHashAndPassword([]byte("$2a$10$000000000000000000000000000000000000000000000000000000"), []byte(password))
		return result
	}
	if bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password)) != nil {
		return result
	}
	result.Valid = true
	result.Passthrough = cs.snapshot.OrgUserPassthrough[key]
	result.DefaultCatalog = cs.snapshot.OrgUserDefaultCatalog[key]
	return result
}

// OrgWarehouseStatus reports the current warehouse provisioning state for an
// org from the in-memory snapshot. Used by the connection handler to emit a
// meaningful error when a client connects while the warehouse is still being
// stood up (instead of the misleading "no org configured for user" fallback).
//
// Returns:
//   - ("", false) when the org does not exist
//   - ("", true)  when the org exists but has no warehouse row (legacy tenants)
//   - (<state>, true) when a warehouse row exists; <state> is one of
//     pending / provisioning / ready / failed / deleting / deleted.
func (cs *ConfigStore) OrgWarehouseStatus(orgID string) (string, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return "", false
	}
	oc, ok := cs.snapshot.Orgs[orgID]
	if !ok {
		return "", false
	}
	if oc.Warehouse == nil {
		return "", true
	}
	return string(oc.Warehouse.State), true
}

// ValidateOrgUser checks username/password scoped to a specific org.
func (cs *ConfigStore) ValidateOrgUser(orgID, username, password string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return false
	}
	storedHash, ok := cs.snapshot.OrgUserPassword[OrgUserKey{OrgID: orgID, Username: username}]
	if !ok {
		// Spend time on a dummy bcrypt compare to avoid timing leaks on username enumeration.
		_ = bcrypt.CompareHashAndPassword([]byte("$2a$10$000000000000000000000000000000000000000000000000000000"), []byte(password))
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password)) == nil
}

// IsOrgUserPassthrough reports whether the given (org, user) is configured to
// bypass the PostgreSQL compatibility layer. Returns false for unknown users —
// callers must validate credentials separately before trusting this.
//
// Prefer ValidateOrgUserAndGetPassthrough when both auth and passthrough are
// needed at the same point; this method is exposed for introspection (e.g.
// admin dashboards) where credentials aren't being checked.
func (cs *ConfigStore) IsOrgUserPassthrough(orgID, username string) bool {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		return false
	}
	return cs.snapshot.OrgUserPassthrough[OrgUserKey{OrgID: orgID, Username: username}]
}

// ValidateOrgUserAndGetPassthrough validates credentials AND reads the
// passthrough flag against the same snapshot, eliminating the swap window
// that two separate ValidateOrgUser + IsOrgUserPassthrough calls would
// expose. valid=false always returns passthrough=false — never leak the
// flag for failed auth or unknown users.
func (cs *ConfigStore) ValidateOrgUserAndGetPassthrough(orgID, username, password string) (valid, passthrough bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.snapshot == nil {
		// Match ValidateOrgUser's timing-leak guard: still spend bcrypt time
		// on failed auth so unknown-user paths look the same as wrong-password.
		_ = bcrypt.CompareHashAndPassword([]byte("$2a$10$000000000000000000000000000000000000000000000000000000"), []byte(password))
		return false, false
	}
	key := OrgUserKey{OrgID: orgID, Username: username}
	storedHash, ok := cs.snapshot.OrgUserPassword[key]
	if !ok {
		_ = bcrypt.CompareHashAndPassword([]byte("$2a$10$000000000000000000000000000000000000000000000000000000"), []byte(password))
		return false, false
	}
	if bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(password)) != nil {
		return false, false
	}
	return true, cs.snapshot.OrgUserPassthrough[key]
}

// HashPassword hashes a plaintext password using bcrypt.
func HashPassword(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hash), nil
}

// GeneratePassword returns a cryptographically random 32-byte URL-safe password.
func GeneratePassword() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate password: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// CreateOrgUser creates a new user for the given org.
func (cs *ConfigStore) CreateOrgUser(orgID, username, passwordHash string) error {
	user := OrgUser{
		OrgID:    orgID,
		Username: username,
		Password: passwordHash,
	}
	return cs.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "org_id"}, {Name: "username"}},
		DoUpdates: clause.AssignmentColumns([]string{"password", "updated_at"}),
	}).Create(&user).Error
}

// UpdateOrgUserPassword updates the password hash for an existing user.
func (cs *ConfigStore) UpdateOrgUserPassword(orgID, username, passwordHash string) error {
	result := cs.db.Model(&OrgUser{}).
		Where("org_id = ? AND username = ?", orgID, username).
		Update("password", passwordHash)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("user %q not found in org %q", username, orgID)
	}
	return nil
}

// OnChange registers a callback that fires when the config snapshot changes.
func (cs *ConfigStore) OnChange(fn func(old, new *Snapshot)) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.onChange = append(cs.onChange, fn)
}

// ListWarehousesByStates returns all warehouses with a state matching one of the given values.
// This is a direct DB query, not snapshot-based, for use by the provisioning controller.
func (cs *ConfigStore) ListWarehousesByStates(states []ManagedWarehouseProvisioningState) ([]ManagedWarehouse, error) {
	var warehouses []ManagedWarehouse
	if err := cs.db.Where("state IN ?", states).Find(&warehouses).Error; err != nil {
		return nil, fmt.Errorf("list warehouses by states: %w", err)
	}
	return warehouses, nil
}

// UpdateWarehouseState performs a compare-and-swap update on a warehouse row.
// Only updates if the current state matches expectedState, preventing races.
// ErrWarehouseStateMismatch is returned (wrapped) by UpdateWarehouseState
// when the CAS guard fails — the row is missing or no longer in the
// expected state. Callers that want to treat that as benign (because
// another writer has already advanced the state machine) can detect via
// errors.Is(err, configstore.ErrWarehouseStateMismatch).
var ErrWarehouseStateMismatch = errors.New("warehouse not in expected state")

// ErrWarehouseNotFound is returned by row-targeted updates when the orgID
// has no row in duckgres_managed_warehouses.
var ErrWarehouseNotFound = errors.New("warehouse not found")

// UpdateIcebergConfig writes the supplied column updates to the org's
// warehouse row without CAS'ing on the top-level state. Used by the
// Lakekeeper provisioner — Iceberg sub-state runs in parallel with the
// main warehouse state machine, so persisting the Lakekeeper endpoint
// after a top-level state transition shouldn't silently no-op.
//
// Caller-side discipline: the updates map should only contain
// iceberg_* columns. Untyped to keep the controller's WarehouseStore
// interface independent of the column list.
func (cs *ConfigStore) UpdateIcebergConfig(orgID string, updates map[string]interface{}) error {
	result := cs.db.Model(&ManagedWarehouse{}).
		Where("org_id = ?", orgID).
		Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("update iceberg config: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("warehouse %q: %w", orgID, ErrWarehouseNotFound)
	}
	return nil
}

func (cs *ConfigStore) UpdateWarehouseState(orgID string, expectedState ManagedWarehouseProvisioningState, updates map[string]interface{}) error {
	result := cs.db.Model(&ManagedWarehouse{}).
		Where("org_id = ? AND state = ?", orgID, expectedState).
		Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("update warehouse state: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("warehouse %q expected state %q: %w", orgID, expectedState, ErrWarehouseStateMismatch)
	}
	return nil
}

// TrinoSettings carries the per-org Trino options EnableTrino persists. New
// fields can be added without changing call sites — the zero value matches
// the existing default (no tier, enabled).
type TrinoSettings struct {
	// Tier is the resource-group tier label. Empty == default tier.
	Tier string
}

// EnableTrino marks the org as Trino-enabled and stores the per-org Trino
// settings. Idempotent: re-enabling updates Tier without flipping Enabled
// back through a disabled state. Safe to call as part of the
// `POST /orgs/:id/provision` path or the standalone
// `POST /orgs/:id/trino` endpoint.
func (cs *ConfigStore) EnableTrino(orgID string, settings TrinoSettings) error {
	if orgID == "" {
		return errors.New("EnableTrino: orgID is required")
	}
	row := ManagedWarehouseTrino{
		OrgID:   orgID,
		Enabled: true,
		Tier:    settings.Tier,
		State:   ManagedWarehouseStatePending,
	}
	// On conflict update Enabled+Tier+UpdatedAt only. Do NOT touch
	// State / StatusMessage / ReadyAt / FailedAt — those are owned by
	// the reconcile loop. A re-enable on an already-Ready row stays
	// Ready; the reconcile loop's next tick will refresh status.
	if err := cs.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "org_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"enabled", "tier", "updated_at"}),
	}).Create(&row).Error; err != nil {
		return fmt.Errorf("enable trino for %q: %w", orgID, err)
	}
	return nil
}

// TrinoStateUpdate is the small set of columns the reconcile loop
// writes through UpdateTrinoState. The State + StatusMessage are
// always set; the timestamp pointers are optional ("nil" == don't
// touch). Callers that want to clear an existing timestamp can pass a
// pointer to a zero time.Time, but in practice the next state
// transition just overwrites.
type TrinoStateUpdate struct {
	State         ManagedWarehouseProvisioningState
	StatusMessage string
	ReadyAt       *time.Time
	FailedAt      *time.Time
}

// UpdateTrinoState writes the reconcile loop's per-tick outcome onto
// an org's Trino row. Predicates on enabled=true: if DisableTrino
// raced ahead during the reconcile tick, the row is no longer enabled
// and any state write is a stale leftover that would mis-represent
// the org's operational status. Returning nil on RowsAffected==0
// keeps that race silent (the next reconcile tick won't see the
// disabled org and won't try again).
//
// No CAS on state — the provisioning controller runs single-threaded
// per pod, so the only race is between reconcile and DisableTrino.
//
// StatusMessage is truncated to the column width (1024 chars) so a
// long joined-error message can't trip a Postgres "value too long"
// error and silently fail the state record.
func (cs *ConfigStore) UpdateTrinoState(orgID string, upd TrinoStateUpdate) error {
	if orgID == "" {
		return errors.New("UpdateTrinoState: orgID is required")
	}
	msg := upd.StatusMessage
	if len(msg) > 1024 {
		// Leave a trailing marker so anyone reading the row knows it
		// was clipped; the full error went to the slog stream.
		msg = msg[:1021] + "..."
	}
	updates := map[string]interface{}{
		"state":          upd.State,
		"status_message": msg,
		"updated_at":     time.Now().UTC(),
	}
	// Pointer fields participate only when explicitly set. nil means
	// "leave the column alone"; a non-nil zero time.Time clears it.
	if upd.ReadyAt != nil {
		updates["ready_at"] = upd.ReadyAt
	}
	if upd.FailedAt != nil {
		// Distinguish "clear" (zero value pointer) from "set to a
		// specific timestamp" by passing nil into the SQL UPDATE when
		// the pointer points at a zero time.
		if upd.FailedAt.IsZero() {
			updates["failed_at"] = nil
		} else {
			updates["failed_at"] = upd.FailedAt
		}
	}
	result := cs.db.Model(&ManagedWarehouseTrino{}).
		Where("org_id = ? AND enabled = ?", orgID, true).
		Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("update trino state for %q: %w", orgID, result.Error)
	}
	return nil
}

// DisableTrino marks the org as no longer Trino-enabled. The row is
// kept (rather than deleted) so the provisioner can observe the
// transition and clean up the catalog + password file entry on its
// next reconcile tick. Returns nil if no row exists — disabling
// something that was never enabled is a no-op, not an error.
//
// State is reset to Pending alongside the enabled flip so operators
// see the lifecycle restart: the previous Ready/Failed status no
// longer reflects current reality (the catalog is being torn down).
// status_message + failed_at are cleared since they belong to the
// previous enabled lifecycle. The reconcile loop will not advance
// state for disabled orgs (UpdateTrinoState predicates on enabled),
// so state stays at Pending until either:
//   - EnableTrino re-activates the org, OR
//   - the row is deleted (e.g. via the FK CASCADE when the Org row goes).
func (cs *ConfigStore) DisableTrino(orgID string) error {
	if orgID == "" {
		return errors.New("DisableTrino: orgID is required")
	}
	result := cs.db.Model(&ManagedWarehouseTrino{}).
		Where("org_id = ?", orgID).
		Updates(map[string]interface{}{
			"enabled":        false,
			"state":          ManagedWarehouseStatePending,
			"status_message": "",
			"failed_at":      nil,
			"updated_at":     time.Now().UTC(),
		})
	if result.Error != nil {
		return fmt.Errorf("disable trino for %q: %w", orgID, result.Error)
	}
	return nil
}

// ListTrinoEnabledOrgs returns every org with ManagedWarehouseTrino.Enabled
// = true joined against its `root` OrgUser row. The provisioner needs the
// bcrypt hash to project the Trino password file, so this is a single join
// rather than two round-trips.
//
// Orgs that are Trino-enabled but have no `root` OrgUser are skipped — that
// shape can't legitimately happen via the provisioning API (CreateOrgUser
// runs in the same handler that toggles Enabled), and silently skipping is
// safer than projecting a half-built password file.
func (cs *ConfigStore) ListTrinoEnabledOrgs() ([]TrinoEnabledOrg, error) {
	var out []TrinoEnabledOrg
	// Inner join with duckgres_org_users on (org_id, username='root') so a
	// missing OrgUser row drops the org from the result. Then left join with
	// duckgres_orgs to pick up database_name.
	err := cs.db.Table("duckgres_managed_warehouse_trino AS t").
		Select(`t.org_id AS org_id,
		         COALESCE(o.database_name, '') AS database_name,
		         t.tier AS tier,
		         u.password AS root_password_hash,
		         t.state AS state`).
		Joins(`INNER JOIN duckgres_org_users AS u
		        ON u.org_id = t.org_id AND u.username = 'root'`).
		Joins(`LEFT JOIN duckgres_orgs AS o ON o.name = t.org_id`).
		Where("t.enabled = ?", true).
		Order("t.org_id ASC").
		Scan(&out).Error
	if err != nil {
		return nil, fmt.Errorf("list trino-enabled orgs: %w", err)
	}
	return out, nil
}

// GetManagedWarehouseIceberg reads the embedded Iceberg config for an
// org. Returns (nil, nil) when the org has no warehouse row so callers
// can distinguish "never provisioned" from a DB error. Used by the
// Trino provisioner to read LakekeeperEndpoint + LakekeeperWarehouse
// when building catalog properties — Trino-enabled orgs without an
// Iceberg row are skipped at the call site (Iceberg isn't ready yet).
func (cs *ConfigStore) GetManagedWarehouseIceberg(orgID string) (*ManagedWarehouseIceberg, error) {
	var warehouse ManagedWarehouse
	err := cs.db.First(&warehouse, "org_id = ?", orgID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get iceberg config for %q: %w", orgID, err)
	}
	ic := warehouse.Iceberg
	return &ic, nil
}

// GetManagedWarehouseTrino reads the Trino row for an org. Returns
// (nil, nil) when no row exists so callers can distinguish "never
// configured" from a DB error.
func (cs *ConfigStore) GetManagedWarehouseTrino(orgID string) (*ManagedWarehouseTrino, error) {
	var row ManagedWarehouseTrino
	err := cs.db.First(&row, "org_id = ?", orgID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get trino row for %q: %w", orgID, err)
	}
	return &row, nil
}

// migrateDeltaCatalogDefaultEnabled is a one-shot backfill that flips existing
// rows where delta_catalog_enabled was stored as false (the old default) to
// true. New rows get true automatically via the gorm:"default:true" column
// default. Tracked in duckgres_schema_migrations so it runs exactly once;
// admins can disable per-warehouse via the admin API after the backfill
// without it being re-flipped on subsequent restarts.
const deltaCatalogDefaultMigrationName = "2026_05_delta_catalog_default_enabled"

func migrateDeltaCatalogDefaultEnabled(db *gorm.DB) error {
	var existing SchemaMigration
	err := db.Where("name = ?", deltaCatalogDefaultMigrationName).First(&existing).Error
	if err == nil {
		return nil // already applied
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("check schema migration: %w", err)
	}

	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(
			`UPDATE duckgres_ducklake_config SET delta_catalog_enabled = TRUE WHERE delta_catalog_enabled IS DISTINCT FROM TRUE`,
		).Error; err != nil {
			return fmt.Errorf("backfill ducklake config: %w", err)
		}
		if err := tx.Exec(
			`UPDATE duckgres_managed_warehouses SET s3_delta_catalog_enabled = TRUE WHERE s3_delta_catalog_enabled IS DISTINCT FROM TRUE`,
		).Error; err != nil {
			return fmt.Errorf("backfill managed warehouses: %w", err)
		}
		if err := tx.Create(&SchemaMigration{
			Name:      deltaCatalogDefaultMigrationName,
			AppliedAt: time.Now().UTC(),
		}).Error; err != nil {
			return fmt.Errorf("record schema migration: %w", err)
		}
		slog.Info("Backfilled delta_catalog_enabled=true on existing config rows.")
		return nil
	})
}

// migrateIcebergBackendBackfill stamps iceberg_backend = 's3_tables' on any
// existing row whose iceberg_table_bucket_arn is populated but whose
// iceberg_backend is empty. Without this, ResolvedBackend() on those rows
// would return "lakekeeper" (the empty-default semantics) and the worker
// activator + controller reconcile loop would treat them as Lakekeeper
// orgs — silently breaking S3 Tables ATTACH AND firing redundant
// Lakekeeper provisioning attempts.
//
// New rows inserted after the GORM `default:'lakekeeper'` migration get
// the default at insert time, so this migration only matters for rows
// that predate the column. One-shot, tracked in duckgres_schema_migrations.
const icebergBackendBackfillMigrationName = "2026_05_iceberg_backend_backfill"

func migrateIcebergBackendBackfill(db *gorm.DB) error {
	var existing SchemaMigration
	err := db.Where("name = ?", icebergBackendBackfillMigrationName).First(&existing).Error
	if err == nil {
		return nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("check schema migration: %w", err)
	}
	return db.Transaction(func(tx *gorm.DB) error {
		// Backfill rows that have a populated S3 Tables ARN but no
		// explicit backend — these are pre-feature rows whose semantics
		// would change without this stamp.
		res := tx.Exec(
			`UPDATE duckgres_managed_warehouses
			   SET iceberg_backend = 's3_tables'
			 WHERE COALESCE(iceberg_table_bucket_arn, '') <> ''
			   AND COALESCE(iceberg_backend, '') = ''`,
		)
		if res.Error != nil {
			return fmt.Errorf("backfill iceberg_backend: %w", res.Error)
		}
		if err := tx.Create(&SchemaMigration{
			Name:      icebergBackendBackfillMigrationName,
			AppliedAt: time.Now().UTC(),
		}).Error; err != nil {
			return fmt.Errorf("record schema migration: %w", err)
		}
		slog.Info("Backfilled iceberg_backend=s3_tables on pre-Lakekeeper rows.",
			"rows", res.RowsAffected)
		return nil
	})
}

func migrateOrgUserPK(db *gorm.DB) error {
	// Check if the PK already has 2 columns (idempotent)
	var count int64
	db.Raw(`
		SELECT COUNT(*) FROM information_schema.key_column_usage
		WHERE table_name = 'duckgres_org_users'
		AND constraint_name = 'duckgres_org_users_pkey'
	`).Scan(&count)
	if count >= 2 {
		return nil // Already migrated
	}
	if count == 0 {
		return nil // Table doesn't exist yet, AutoMigrate will create it
	}
	// Migrate: drop old single-column PK, add composite PK
	return db.Exec(`
		ALTER TABLE duckgres_org_users DROP CONSTRAINT duckgres_org_users_pkey;
		ALTER TABLE duckgres_org_users ADD PRIMARY KEY (org_id, username);
	`).Error
}

func resolveRuntimeSchema(db *gorm.DB) (string, error) {
	var currentSchema string
	if err := db.Raw("SELECT current_schema()").Scan(&currentSchema).Error; err != nil {
		return "", err
	}
	if currentSchema == "" || currentSchema == "public" {
		return "cp_runtime", nil
	}
	return currentSchema + "_runtime", nil
}

func ensureRuntimeSchema(db *gorm.DB, runtimeSchema string) error {
	return db.Exec(`CREATE SCHEMA IF NOT EXISTS "` + quoteIdentifier(runtimeSchema) + `"`).Error
}

func autoMigrateRuntimeTables(db *gorm.DB, runtimeSchema string) error {
	for _, spec := range []struct {
		table string
		model any
	}{
		{table: runtimeSchema + ".cp_instances", model: &ControlPlaneInstance{}},
		{table: runtimeSchema + ".worker_records", model: &WorkerRecord{}},
		{table: runtimeSchema + ".flight_session_records", model: &FlightSessionRecord{}},
		{table: runtimeSchema + ".org_connection_queue", model: &OrgConnectionQueueEntry{}},
		{table: runtimeSchema + ".org_connection_leases", model: &OrgConnectionLease{}},
		{table: runtimeSchema + ".warm_capacity_miss_buckets", model: &WarmCapacityMissBucket{}},
	} {
		if err := db.Table(spec.table).AutoMigrate(spec.model); err != nil {
			return err
		}
	}
	return nil
}

func quoteIdentifier(v string) string {
	return strings.ReplaceAll(v, `"`, `""`)
}

// DB exposes the GORM database for direct CRUD operations (used by admin API).
func (cs *ConfigStore) DB() *gorm.DB {
	return cs.db
}

// RuntimeSchema returns the dedicated runtime coordination schema name.
func (cs *ConfigStore) RuntimeSchema() string {
	return cs.runtimeSchema
}

func (cs *ConfigStore) runtimeTable(base string) string {
	return cs.runtimeSchema + "." + base
}

// RecordWarmCapacityMiss increments the shared bucket for a foreground warm
// capacity miss. The insert/upsert is atomic so concurrent control-plane pods
// can all contribute to the same scope/reason/bucket row without coordination.
func (cs *ConfigStore) RecordWarmCapacityMiss(scope string, reason WorkerClaimMissReason, now time.Time) error {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		return fmt.Errorf("record warm capacity miss: scope is required")
	}
	if reason == WorkerClaimMissReasonNone {
		return fmt.Errorf("record warm capacity miss: reason is required")
	}
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()

	bucket := WarmCapacityMissBucket{
		Scope:       scope,
		Reason:      reason,
		BucketStart: now.Truncate(WarmCapacityMissBucketSize),
		Count:       1,
		UpdatedAt:   now,
	}
	if err := cs.db.Table(cs.runtimeTable(bucket.TableName())).Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "scope"},
			{Name: "reason"},
			{Name: "bucket_start"},
		},
		DoUpdates: clause.Assignments(map[string]any{
			"count":      gorm.Expr(`"warm_capacity_miss_buckets"."count" + EXCLUDED."count"`),
			"updated_at": now,
		}),
	}).Create(&bucket).Error; err != nil {
		return fmt.Errorf("record warm capacity miss: %w", err)
	}
	return nil
}

// ListWarmCapacityMissesSince returns aggregated warm-capacity miss counts by
// scope and reason for buckets at or after the bucket containing since. Passing
// reasons narrows the aggregation to those miss reasons.
func (cs *ConfigStore) ListWarmCapacityMissesSince(since time.Time, reasons ...WorkerClaimMissReason) ([]WarmCapacityMissAggregate, error) {
	reasonFilters := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		if reason == WorkerClaimMissReasonNone {
			continue
		}
		reasonFilters = append(reasonFilters, string(reason))
	}

	sinceBucket := since.UTC().Truncate(WarmCapacityMissBucketSize)
	query := cs.db.Table(cs.runtimeTable((&WarmCapacityMissBucket{}).TableName())).
		Select("scope, reason, COALESCE(SUM(count), 0)::bigint AS count").
		Where("bucket_start >= ?", sinceBucket).
		Group("scope, reason").
		Order("scope ASC, reason ASC")
	if len(reasonFilters) > 0 {
		query = query.Where("reason IN ?", reasonFilters)
	}

	var out []WarmCapacityMissAggregate
	if err := query.Scan(&out).Error; err != nil {
		return nil, fmt.Errorf("list warm capacity misses: %w", err)
	}
	return out, nil
}

// PruneWarmCapacityMissBuckets removes buckets older than the caller-provided
// cutoff and returns the number of deleted rows.
func (cs *ConfigStore) PruneWarmCapacityMissBuckets(before time.Time) (int64, error) {
	result := cs.db.Table(cs.runtimeTable((&WarmCapacityMissBucket{}).TableName())).
		Where("bucket_start < ?", before.UTC()).
		Delete(&WarmCapacityMissBucket{})
	if result.Error != nil {
		return 0, fmt.Errorf("prune warm capacity miss buckets: %w", result.Error)
	}
	return result.RowsAffected, nil
}

// ListWorkerLifecycleStats returns grouped cluster-wide active worker lifecycle
// state by image and tenant binding for Prometheus observability.
func (cs *ConfigStore) ListWorkerLifecycleStats() ([]WorkerLifecycleStats, error) {
	const bindingExpr = "CASE WHEN NULLIF(org_id, '') IS NULL THEN 'neutral' ELSE 'org_bound' END"
	var out []WorkerLifecycleStats
	err := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Select("image, state, "+bindingExpr+" AS binding, COUNT(*)::bigint AS count").
		Where("image <> ''").
		Where("state IN ?", []WorkerState{
			WorkerStateSpawning,
			WorkerStateIdle,
			WorkerStateReserved,
			WorkerStateActivating,
			WorkerStateHot,
			WorkerStateHotIdle,
			WorkerStateDraining,
		}).
		Group("image, state, " + bindingExpr).
		Order("image ASC, state ASC, binding ASC").
		Scan(&out).Error
	if err != nil {
		return nil, fmt.Errorf("list worker lifecycle stats: %w", err)
	}
	return out, nil
}

// UpsertControlPlaneInstance inserts or updates a runtime control-plane instance row.
func (cs *ConfigStore) UpsertControlPlaneInstance(instance *ControlPlaneInstance) error {
	if err := cs.db.Table(cs.runtimeTable(instance.TableName())).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{"pod_name", "pod_uid", "boot_id", "state", "started_at", "last_heartbeat_at", "draining_at", "expired_at", "updated_at"}),
	}).Create(instance).Error; err != nil {
		return fmt.Errorf("upsert control plane instance: %w", err)
	}
	return nil
}

// GetControlPlaneInstance returns a runtime control-plane instance row by id.
func (cs *ConfigStore) GetControlPlaneInstance(id string) (*ControlPlaneInstance, error) {
	var instance ControlPlaneInstance
	if err := cs.db.Table(cs.runtimeTable(instance.TableName())).First(&instance, "id = ?", id).Error; err != nil {
		return nil, fmt.Errorf("get control plane instance: %w", err)
	}
	return &instance, nil
}

// ListLiveControlPlaneInstanceIDs returns the IDs of control-plane instances
// that are not yet expired — i.e. either currently active OR draining (still
// alive, waiting on in-flight queries to finish before SIGTERM completes).
// Used by the K8s pool's startup orphan sweep to distinguish "owned by a CP
// that is still serving traffic" from "owned by a dead CP".
//
// Including draining CPs is critical: a draining CP's worker pods are still
// running queries that haven't finished yet, and treating them as orphans
// would kill those queries mid-flight.
func (cs *ConfigStore) ListLiveControlPlaneInstanceIDs() ([]string, error) {
	var ids []string
	if err := cs.db.Table(cs.runtimeTable((&ControlPlaneInstance{}).TableName())).
		Where("state <> ?", ControlPlaneInstanceStateExpired).
		Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("list live control plane instance ids: %w", err)
	}
	return ids, nil
}

// ExpireControlPlaneInstances marks stale control-plane instance rows as expired.
func (cs *ConfigStore) ExpireControlPlaneInstances(cutoff time.Time) (int64, error) {
	now := time.Now()
	result := cs.db.Table(cs.runtimeTable((&ControlPlaneInstance{}).TableName())).
		Where("state <> ? AND last_heartbeat_at < ?", ControlPlaneInstanceStateExpired, cutoff).
		Updates(map[string]any{
			"state":      ControlPlaneInstanceStateExpired,
			"expired_at": now,
			"updated_at": now,
		})
	if result.Error != nil {
		return 0, fmt.Errorf("expire control plane instances: %w", result.Error)
	}
	return result.RowsAffected, nil
}

// ExpireDrainingControlPlaneInstances marks draining control-plane rows expired
// once their draining_at timestamp exceeds the configured handover timeout.
func (cs *ConfigStore) ExpireDrainingControlPlaneInstances(before time.Time) (int64, error) {
	now := time.Now()
	result := cs.db.Table(cs.runtimeTable((&ControlPlaneInstance{}).TableName())).
		Where("state = ? AND draining_at IS NOT NULL AND draining_at <= ?", ControlPlaneInstanceStateDraining, before).
		Updates(map[string]any{
			"state":      ControlPlaneInstanceStateExpired,
			"expired_at": now,
			"updated_at": now,
		})
	if result.Error != nil {
		return 0, fmt.Errorf("expire draining control plane instances: %w", result.Error)
	}
	return result.RowsAffected, nil
}

// UpsertWorkerRecord inserts or updates a runtime worker row.
func (cs *ConfigStore) UpsertWorkerRecord(record *WorkerRecord) error {
	protectedStates := []WorkerState{WorkerStateDraining, WorkerStateRetired, WorkerStateLost}
	result := cs.db.Table(cs.runtimeTable(record.TableName())).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "worker_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"pod_name", "image", "state", "org_id", "owner_cp_instance_id", "owner_epoch", "activation_started_at", "last_heartbeat_at", "retire_reason", "s3_credentials_expires_at", "updated_at"}),
		Where: clause.Where{Exprs: []clause.Expression{
			clause.Expr{SQL: `"worker_records"."state" NOT IN ?`, Vars: []any{protectedStates}},
			clause.Expr{SQL: `(excluded."owner_epoch" > "worker_records"."owner_epoch" OR (excluded."owner_epoch" = "worker_records"."owner_epoch" AND excluded."owner_cp_instance_id" = "worker_records"."owner_cp_instance_id"))`},
		}},
	}).Create(record)
	if result.Error != nil {
		return fmt.Errorf("upsert worker record: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("%w: worker_id=%d owner=%q epoch=%d state=%q", ErrWorkerRecordUpsertFenceMiss, record.WorkerID, record.OwnerCPInstanceID, record.OwnerEpoch, record.State)
	}
	return nil
}

// ListWorkerRecordsByStatesBefore returns worker rows in any of the given
// states whose updated_at is at or before the given cutoff. The age filter is
// what makes this safe to use against in-flight spawns: callers pass a cutoff
// well in the past (e.g. now - 30s) so a row that another CP is currently
// touching will not appear in the result.
func (cs *ConfigStore) ListWorkerRecordsByStatesBefore(states []WorkerState, updatedBefore time.Time) ([]WorkerRecord, error) {
	if len(states) == 0 {
		return nil, nil
	}
	var workers []WorkerRecord
	if err := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("state IN ?", states).
		Where("updated_at <= ?", updatedBefore).
		Order("worker_id ASC").
		Find(&workers).Error; err != nil {
		return nil, fmt.Errorf("list worker records by state before: %w", err)
	}
	return workers, nil
}

// GetWorkerRecord returns a runtime worker row by worker id. Returns
// (nil, nil) when no row matches — "not found" is a normal state for
// callers like cleanupOrphanedWorkerPods that need to distinguish between
// a known terminal row and no row at all. Any other DB error is wrapped
// and returned so callers can log and retry on the next tick.
func (cs *ConfigStore) GetWorkerRecord(workerID int) (*WorkerRecord, error) {
	var record WorkerRecord
	err := cs.db.Table(cs.runtimeTable(record.TableName())).First(&record, "worker_id = ?", workerID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get worker record: %w", err)
	}
	return &record, nil
}

// ClaimIdleWorker atomically claims one idle worker row for a control-plane instance.
// The selected row is locked with SKIP LOCKED and transitioned to reserved while
// incrementing owner_epoch. When maxOrgWorkers is set, org claims are serialized
// under the same advisory lock used for spawn-slot allocation. maxGlobalWorkers
// is only used to classify an unfulfilled claim after no suitable idle worker
// exists; an existing idle worker remains claimable even when the global pool is
// at capacity.
func (cs *ConfigStore) ClaimIdleWorker(ownerCPInstanceID, orgID, image string, profileCPU, profileMemory string, profileColocate bool, maxOrgWorkers, maxGlobalWorkers int, maxColocatedCPU int, maxColocatedMemBytes uint64) (*WorkerRecord, WorkerClaimMissReason, error) {
	var claimed *WorkerRecord
	missReason := WorkerClaimMissReasonNone
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		if orgID != "" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:org:"+orgID)).Error; err != nil {
				return err
			}
		}
		// The worker-count cap bounds only exclusive workers — each pins a
		// dedicated node. Colocated workers bin-pack and are intentionally
		// unbounded: a colocated request never counts toward the cap and is
		// never refused because the exclusive budget is full.
		if maxOrgWorkers > 0 && orgID != "" && !profileColocate {
			count, err := cs.countActiveWorkers(tx, "org_id = ? AND COALESCE(profile_colocate, false) = false", orgID)
			if err != nil {
				return err
			}
			if count >= int64(maxOrgWorkers) {
				missReason = WorkerClaimMissReasonOrgCap
				return nil
			}
		}

		var current WorkerRecord
		query := tx.Table(cs.runtimeTable(current.TableName())).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("state = ?", WorkerStateIdle)

		if image != "" {
			query = query.Where("image = ?", image)
		}
		// Profile is a match dimension orthogonal to image: a request only claims
		// an idle worker of the same shape. The default request ("","",false)
		// matches default/legacy/warm rows; a colocated request only matches
		// colocated rows (and vice versa). Always filtered, including the default.
		query = query.Where("COALESCE(profile_cpu, '') = ? AND COALESCE(profile_memory, '') = ? AND COALESCE(profile_colocate, false) = ?", profileCPU, profileMemory, profileColocate)

		err := query.Order("worker_id ASC").Take(&current).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				// Exclusive-only global cap, bypassed for colocated requests —
				// see the org-cap note above.
				if maxGlobalWorkers > 0 && !profileColocate {
					count, err := cs.countActiveWorkers(tx, "COALESCE(profile_colocate, false) = false")
					if err != nil {
						return err
					}
					if count >= int64(maxGlobalWorkers) {
						missReason = WorkerClaimMissReasonGlobalCap
						return nil
					}
				}
				missReason = WorkerClaimMissReasonNoIdle
				return nil
			}
			return err
		}

		// Authoritative per-org colocated resource quota (cross-CP): summed under
		// the org advisory lock held above, so it can't be raced by another CP.
		// Only colocated claims count; reusing OrgCap surfaces as retryable
		// backpressure. The in-process OrgReservedPool check is the fast pre-filter.
		if profileColocate && orgID != "" && (maxColocatedCPU > 0 || maxColocatedMemBytes > 0) {
			curCPU, curMem, sErr := cs.sumOrgColocatedResources(tx, orgID)
			if sErr != nil {
				return sErr
			}
			reqCPU := parseColocatedCPUCores(current.ProfileCPU)
			reqMem := parseColocatedMemBytes(current.ProfileMemory)
			if (maxColocatedCPU > 0 && curCPU+reqCPU > maxColocatedCPU) ||
				(maxColocatedMemBytes > 0 && curMem+reqMem > maxColocatedMemBytes) {
				missReason = WorkerClaimMissReasonOrgCap
				return nil
			}
		}

		now := time.Now()
		if err := tx.Table(cs.runtimeTable(current.TableName())).
			Where("worker_id = ?", current.WorkerID).
			Updates(map[string]any{
				"state":                WorkerStateReserved,
				"org_id":               orgID,
				"owner_cp_instance_id": ownerCPInstanceID,
				"owner_epoch":          gorm.Expr("owner_epoch + 1"),
				"updated_at":           now,
			}).Error; err != nil {
			return err
		}

		if err := tx.Table(cs.runtimeTable(current.TableName())).
			First(&current, "worker_id = ?", current.WorkerID).Error; err != nil {
			return err
		}
		claimed = &current
		return nil
	})
	if err != nil {
		return nil, WorkerClaimMissReasonNone, fmt.Errorf("claim idle worker: %w", err)
	}
	return claimed, missReason, nil
}

// ClaimHotIdleWorker atomically claims one hot-idle worker row that was
// previously activated for the given org. The selected row is locked with
// SKIP LOCKED and transitioned to reserved while incrementing owner_epoch.
// When maxOrgWorkers is set, the org cap is checked under the same advisory
// lock as neutral idle claims, excluding hot-idle rows from the count so a
// cached worker can be reclaimed as the org's only active slot.
func (cs *ConfigStore) ClaimHotIdleWorker(ownerCPInstanceID, orgID string, profileCPU, profileMemory string, profileColocate bool, maxOrgWorkers int) (*WorkerRecord, WorkerClaimMissReason, error) {
	var claimed *WorkerRecord
	missReason := WorkerClaimMissReasonNone
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		if orgID != "" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:org:"+orgID)).Error; err != nil {
				return err
			}
		}
		// Exclusive-only count cap, bypassed for colocated requests — colocated
		// workers bin-pack and are intentionally unbounded.
		if maxOrgWorkers > 0 && orgID != "" && !profileColocate {
			count, err := cs.countActiveWorkers(tx, "org_id = ? AND state <> ? AND COALESCE(profile_colocate, false) = false", orgID, WorkerStateHotIdle)
			if err != nil {
				return err
			}
			if count >= int64(maxOrgWorkers) {
				missReason = WorkerClaimMissReasonOrgCap
				return nil
			}
		}

		var current WorkerRecord
		err := tx.Table(cs.runtimeTable(current.TableName())).
			Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("state = ? AND org_id = ?", WorkerStateHotIdle, orgID).
			// Only reclaim a hot-idle worker of the requested shape, so a
			// differently-shaped request (e.g. an 8/48 colocated backfill) doesn't
			// claim-and-retire this org's default-shape hot-idle workers. COALESCE
			// keeps legacy NULL-profile rows in the default bucket.
			Where("COALESCE(profile_cpu, '') = ? AND COALESCE(profile_memory, '') = ? AND COALESCE(profile_colocate, false) = ?", profileCPU, profileMemory, profileColocate).
			Order("worker_id ASC").
			Take(&current).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				missReason = WorkerClaimMissReasonNoIdle
				return nil
			}
			return err
		}

		now := time.Now()
		if err := tx.Table(cs.runtimeTable(current.TableName())).
			Where("worker_id = ?", current.WorkerID).
			Updates(map[string]any{
				"state":                WorkerStateReserved,
				"owner_cp_instance_id": ownerCPInstanceID,
				"owner_epoch":          gorm.Expr("owner_epoch + 1"),
				"updated_at":           now,
			}).Error; err != nil {
			return err
		}

		if err := tx.Table(cs.runtimeTable(current.TableName())).
			First(&current, "worker_id = ?", current.WorkerID).Error; err != nil {
			return err
		}
		claimed = &current
		return nil
	})
	if err != nil {
		return nil, WorkerClaimMissReasonNone, fmt.Errorf("claim hot-idle worker: %w", err)
	}
	return claimed, missReason, nil
}

// ListExpiredHotIdleWorkers returns hot-idle workers whose updated_at timestamp
// is at or before the given cutoff time.
func (cs *ConfigStore) ListExpiredHotIdleWorkers(before time.Time) ([]WorkerRecord, error) {
	var workers []WorkerRecord
	err := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("state = ? AND updated_at <= ?", WorkerStateHotIdle, before).
		Find(&workers).Error
	if err != nil {
		return nil, fmt.Errorf("list expired hot-idle workers: %w", err)
	}
	return workers, nil
}

// MarkWorkerTerminalIfCurrent moves an observed active worker row to a terminal
// state only if the durable row still matches that observation. It is the shared
// CAS for list-then-cleanup paths where a worker id alone is not enough to prove
// the listed worker was not reclaimed by another CP.
func (cs *ConfigStore) MarkWorkerTerminalIfCurrent(record *WorkerRecord, targetState WorkerState, reason string) (bool, error) {
	if record == nil {
		return false, nil
	}
	if targetState != WorkerStateRetired && targetState != WorkerStateLost {
		return false, fmt.Errorf("worker %d unsupported terminal state %q", record.WorkerID, targetState)
	}

	eligibleStates := workerTerminalEligibleStates(targetState)
	query := cs.db.Table(cs.runtimeTable(record.TableName())).
		Where("worker_id = ? AND state = ? AND owner_epoch = ?", record.WorkerID, record.State, record.OwnerEpoch).
		Where("(owner_cp_instance_id = ? OR (? = '' AND owner_cp_instance_id IS NULL))", record.OwnerCPInstanceID, record.OwnerCPInstanceID).
		Where("state IN ?", eligibleStates)
	if !record.UpdatedAt.IsZero() {
		query = query.Where("updated_at <= ?", record.UpdatedAt)
	}

	result := query.Updates(map[string]any{
		"state":         targetState,
		"retire_reason": reason,
		"updated_at":    time.Now(),
	})
	if result.Error != nil {
		return false, fmt.Errorf("mark worker %d terminal: %w", record.WorkerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// RetireHotIdleWorker atomically transitions the observed hot-idle row to
// retired. Returns false if the row no longer matches the listed snapshot.
func (cs *ConfigStore) RetireHotIdleWorker(record *WorkerRecord) (bool, error) {
	if record == nil || record.State != WorkerStateHotIdle {
		return false, nil
	}
	return cs.MarkWorkerTerminalIfCurrent(record, WorkerStateRetired, "hot_idle_ttl_expired")
}

// RetireOrphanWorker is the orphan-cleanup counterpart to
// RetireIdleOrHotIdleWorker. Whereas the latter only handles `idle` /
// `hot_idle`, this method transitions a worker to `retired` from any
// active state (spawning, idle, reserved, activating, hot, hot_idle,
// draining). That breadth is safe in the orphan path because by the time
// orphan cleanup picks up the row, no live CP could still be acting on
// it (the owner CP is expired or absent) — so we can short-circuit the
// state-machine guards and just terminate the row.
//
// Returns true if a row transitioned, false if the observed row was already
// terminal (`retired` / `lost`) or has changed since it was listed.
func (cs *ConfigStore) RetireOrphanWorker(record *WorkerRecord, reason string) (bool, error) {
	if record == nil {
		return false, nil
	}

	workerTable := cs.runtimeTable(record.TableName())
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())
	query := cs.db.Table(workerTable).
		Where("worker_id = ? AND state = ? AND owner_epoch = ?", record.WorkerID, record.State, record.OwnerEpoch).
		Where("(owner_cp_instance_id = ? OR (? = '' AND owner_cp_instance_id IS NULL))", record.OwnerCPInstanceID, record.OwnerCPInstanceID).
		Where("state IN ?", workerTerminalEligibleStates(WorkerStateRetired))
	if !record.UpdatedAt.IsZero() {
		query = query.Where("updated_at <= ?", record.UpdatedAt)
	}
	if record.OwnerCPInstanceID != "" {
		query = query.Where(
			"NOT EXISTS (SELECT 1 FROM "+cpTable+" AS cp WHERE cp.id = ? AND cp.state <> ?)",
			record.OwnerCPInstanceID,
			ControlPlaneInstanceStateExpired,
		)
	}

	result := query.Updates(map[string]any{
		"state":         WorkerStateRetired,
		"retire_reason": reason,
		"updated_at":    time.Now(),
	})
	if result.Error != nil {
		return false, fmt.Errorf("retire orphan worker %d: %w", record.WorkerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// ListWorkersDueForCredentialRefresh returns workers owned by the given CP
// whose S3 credentials are about to expire (or have already expired) and
// therefore need a refresh. The cutoff defines "soon": typically the
// scheduler passes (now + half the STS session duration), so a worker is
// picked up well before its session token actually goes invalid.
//
// NULL s3_credentials_expires_at is treated as "due immediately". This
// covers two cases: warm-pool rows that haven't been activated yet (these
// have no creds, so the predicate is irrelevant — they're filtered out by
// the state set anyway since neutral idle workers shouldn't carry creds),
// and pre-migration rows that existed before this column was introduced
// (these get refreshed eagerly so we converge to the new state).
//
// Only already-activated states are considered: retired/lost/draining rows
// don't need creds, and reserved/activating rows must not be refreshed because
// their first ActivateTenant RPC may still be in flight. Refreshing them would
// bump owner_epoch underneath the activation path and make the original
// owner_epoch look stale to the worker. We include `idle` deliberately — an
// idle worker that belongs to an org (post-activation) still has live creds in
// DuckDB and will need them on the next session.
func (cs *ConfigStore) ListWorkersDueForCredentialRefresh(ownerCPInstanceID string, cutoff time.Time) ([]WorkerRecord, error) {
	var workers []WorkerRecord
	credEligibleStates := []WorkerState{
		WorkerStateIdle,
		WorkerStateHot,
		WorkerStateHotIdle,
	}
	err := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("owner_cp_instance_id = ?", ownerCPInstanceID).
		Where("state IN ?", credEligibleStates).
		// Org-bound rows only: a neutral warm row (org_id='') hasn't been
		// activated, so it has no STS-brokered creds yet.
		Where("org_id <> ''").
		Where("s3_credentials_expires_at IS NULL OR s3_credentials_expires_at <= ?", cutoff).
		Order("s3_credentials_expires_at ASC NULLS FIRST, worker_id ASC").
		Find(&workers).Error
	if err != nil {
		return nil, fmt.Errorf("list workers due for credential refresh: %w", err)
	}
	return workers, nil
}

// BumpWorkerEpoch atomically increments owner_epoch on a worker we
// already own, returning the new epoch. Used by the credential-refresh
// scheduler before re-sending ActivateTenant with rotated STS creds: the
// worker's reuseExistingActivation guard requires payload.OwnerEpoch >
// current, so the scheduler bumps here, applies the new epoch on the
// in-memory ManagedWorker, then dispatches the activation. If another CP
// has already taken over the row (different owner_cp_instance_id or
// already-bumped epoch), this returns ErrWorkerOwnerEpochMismatch and the
// caller drops the in-flight refresh.
func (cs *ConfigStore) BumpWorkerEpoch(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64) (int64, error) {
	var newEpoch int64
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
			Where("worker_id = ? AND owner_cp_instance_id = ? AND owner_epoch = ?",
				workerID, ownerCPInstanceID, expectedOwnerEpoch).
			Updates(map[string]any{
				"owner_epoch": gorm.Expr("owner_epoch + 1"),
				"updated_at":  time.Now(),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return ErrWorkerOwnerEpochMismatch
		}
		var current WorkerRecord
		if err := tx.Table(cs.runtimeTable(current.TableName())).
			Where("worker_id = ?", workerID).
			Take(&current).Error; err != nil {
			return err
		}
		newEpoch = current.OwnerEpoch
		return nil
	})
	if err != nil {
		return 0, err
	}
	return newEpoch, nil
}

// MarkCredentialsRefreshed conditionally stamps a new S3 credential
// expiration onto a worker row. The update only takes effect when the
// caller is still the owner (same owner_cp_instance_id and owner_epoch);
// any drift means another CP took over the worker and the caller's just-
// minted creds are stale — we discard them rather than trample. Returns
// true when the row was updated.
func (cs *ConfigStore) MarkCredentialsRefreshed(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64, expiresAt time.Time) (bool, error) {
	result := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("worker_id = ? AND owner_cp_instance_id = ? AND owner_epoch = ?", workerID, ownerCPInstanceID, expectedOwnerEpoch).
		Updates(map[string]any{
			"s3_credentials_expires_at": expiresAt,
			"updated_at":                time.Now(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("mark credentials refreshed for worker %d: %w", workerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// RetireIdleOrHotIdleWorker atomically transitions a worker from idle or hot_idle
// to retired. Returns true if the transition happened, false if the worker was
// in some other state (e.g. claimed/activating/hot).
func (cs *ConfigStore) RetireIdleOrHotIdleWorker(record *WorkerRecord, reason string) (bool, error) {
	if record == nil {
		return false, nil
	}
	if record.State != WorkerStateIdle && record.State != WorkerStateHotIdle {
		return false, nil
	}
	return cs.MarkWorkerTerminalIfCurrent(record, WorkerStateRetired, reason)
}

// MarkWorkerLostIfCurrentLease atomically marks a worker lost only when the
// caller still owns the same lease observed by its local health checker. A
// false return means another CP or lifecycle path has already moved the row.
func (cs *ConfigStore) MarkWorkerLostIfCurrentLease(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64, reason string) (bool, error) {
	lostEligibleStates := []WorkerState{
		WorkerStateSpawning,
		WorkerStateIdle,
		WorkerStateReserved,
		WorkerStateActivating,
		WorkerStateHot,
		WorkerStateHotIdle,
	}
	result := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("worker_id = ? AND owner_cp_instance_id = ? AND owner_epoch = ? AND state IN ?",
			workerID, ownerCPInstanceID, expectedOwnerEpoch, lostEligibleStates).
		Updates(map[string]any{
			"state":         WorkerStateLost,
			"retire_reason": reason,
			"updated_at":    time.Now(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("mark worker %d lost: %w", workerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// MarkWorkerDraining atomically transitions a worker into the draining state
// if and only if it is still owned by the caller and not already terminal. It
// returns true when the transition happened.
//
// Used by ShutdownAll to fence a worker before issuing its K8s pod delete: no
// other CP can claim the worker once it's draining (ClaimIdleWorker and
// ClaimHotIdleWorker filter on state=idle and state=hot_idle respectively),
// so the pod-delete/DB-retire chain can proceed without a claim race. If the
// CP then crashes before the final retired transition, ListOrphanedWorkers
// includes draining rows whose owner CP has expired, so orphan cleanup
// retires the worker and deletes the pod.
//
// The ownerCPInstanceID guard prevents a stale CP from moving a worker that
// has already been taken over by a successor.
func (cs *ConfigStore) MarkWorkerDraining(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64) (bool, error) {
	drainableStates := []WorkerState{
		WorkerStateSpawning,
		WorkerStateIdle,
		WorkerStateReserved,
		WorkerStateActivating,
		WorkerStateHot,
		WorkerStateHotIdle,
	}
	result := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("worker_id = ? AND owner_cp_instance_id = ? AND owner_epoch = ? AND state IN ?", workerID, ownerCPInstanceID, expectedOwnerEpoch, drainableStates).
		Updates(map[string]any{
			"state":      WorkerStateDraining,
			"updated_at": time.Now(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("mark worker %d draining: %w", workerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// RetireDrainingWorker atomically transitions a draining worker to retired.
// Returns true if the transition happened, false if the worker was no longer
// in draining (e.g. already retired by an orphan sweep after a CP restart).
func (cs *ConfigStore) RetireDrainingWorker(workerID int, ownerCPInstanceID string, expectedOwnerEpoch int64, reason string) (bool, error) {
	result := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("worker_id = ? AND owner_cp_instance_id = ? AND owner_epoch = ? AND state = ?", workerID, ownerCPInstanceID, expectedOwnerEpoch, WorkerStateDraining).
		Updates(map[string]any{
			"state":         WorkerStateRetired,
			"retire_reason": reason,
			"updated_at":    time.Now(),
		})
	if result.Error != nil {
		return false, fmt.Errorf("retire draining worker %d: %w", workerID, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// TakeOverWorker transfers durable worker ownership to a new control-plane
// instance when the caller still has the expected prior owner_epoch.
func (cs *ConfigStore) TakeOverWorker(workerID int, ownerCPInstanceID, orgID string, expectedOwnerEpoch int64) (*WorkerRecord, error) {
	var claimed *WorkerRecord
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		var current WorkerRecord
		err := tx.Table(cs.runtimeTable(current.TableName())).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("worker_id = ?", workerID).
			Take(&current).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil
			}
			return err
		}
		if current.OwnerEpoch != expectedOwnerEpoch {
			return ErrWorkerOwnerEpochMismatch
		}
		if current.State != WorkerStateHot {
			return nil
		}
		now := time.Now()
		result := tx.Table(cs.runtimeTable(current.TableName())).
			Where("worker_id = ?", current.WorkerID).
			Updates(map[string]any{
				"state":                WorkerStateReserved,
				"org_id":               orgID,
				"owner_cp_instance_id": ownerCPInstanceID,
				"owner_epoch":          gorm.Expr("owner_epoch + 1"),
				"updated_at":           now,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		if err := tx.Table(cs.runtimeTable(current.TableName())).
			First(&current, "worker_id = ?", current.WorkerID).Error; err != nil {
			return err
		}
		claimed = &current
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("take over worker: %w", err)
	}
	return claimed, nil
}

// CreateSpawningWorkerSlot creates a durable spawning worker row under advisory-lock
// protected org/global capacity checks. A nil result means capacity blocked the spawn.
func (cs *ConfigStore) CreateSpawningWorkerSlot(ownerCPInstanceID, orgID, image string, ownerEpoch int64, podNamePrefix string, maxOrgWorkers, maxGlobalWorkers int) (*WorkerRecord, error) {
	if strings.TrimSpace(podNamePrefix) == "" {
		return nil, fmt.Errorf("pod name prefix is required")
	}

	var created *WorkerRecord
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		if orgID != "" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:org:"+orgID)).Error; err != nil {
				return err
			}
		}
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:global-worker-capacity")).Error; err != nil {
			return err
		}

		// Exclusive-only count caps: colocated workers bin-pack and are unbounded.
		if maxOrgWorkers > 0 && orgID != "" {
			count, err := cs.countActiveWorkers(tx, "org_id = ? AND COALESCE(profile_colocate, false) = false", orgID)
			if err != nil {
				return err
			}
			if count >= int64(maxOrgWorkers) {
				return nil
			}
		}

		if maxGlobalWorkers > 0 {
			count, err := cs.countActiveWorkers(tx, "COALESCE(profile_colocate, false) = false")
			if err != nil {
				return err
			}
			if count >= int64(maxGlobalWorkers) {
				return nil
			}
		}

		var workerID int64
		if err := tx.Raw("SELECT COALESCE(MAX(worker_id), 0) + 1 FROM " + cs.runtimeTable((&WorkerRecord{}).TableName())).Scan(&workerID).Error; err != nil {
			return err
		}
		now := time.Now()
		record := &WorkerRecord{
			WorkerID:          int(workerID),
			PodName:           fmt.Sprintf("%s-%d", podNamePrefix, workerID),
			Image:             image,
			State:             WorkerStateSpawning,
			OrgID:             orgID,
			OwnerCPInstanceID: ownerCPInstanceID,
			OwnerEpoch:        ownerEpoch,
			LastHeartbeatAt:   now,
		}
		if err := tx.Table(cs.runtimeTable(record.TableName())).Create(record).Error; err != nil {
			return err
		}
		created = record
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create spawning worker slot: %w", err)
	}
	return created, nil
}

// CreateNeutralWarmWorkerSlotForImage creates a durable spawning worker row
// for the shared neutral warm pool, but the per-image target is enforced
// against workers using the same image only — letting the per-image warm
// floor coexist with the cluster-default warm pool without one starving the
// other. Same advisory-lock + global-cap protections as the image-blind
// sibling. A nil result means the per-image target is already met or the
// global worker cap blocked the spawn.
func (cs *ConfigStore) CreateNeutralWarmWorkerSlotForImage(ownerCPInstanceID, podNamePrefix, image string, perImageTarget, maxGlobalWorkers int) (*WorkerRecord, error) {
	if strings.TrimSpace(podNamePrefix) == "" {
		return nil, fmt.Errorf("pod name prefix is required")
	}
	if strings.TrimSpace(image) == "" {
		return nil, fmt.Errorf("image is required for per-image warm slot")
	}

	var created *WorkerRecord
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:shared-warm-target")).Error; err != nil {
			return err
		}
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:global-worker-capacity")).Error; err != nil {
			return err
		}

		if perImageTarget > 0 {
			count, err := cs.countNeutralWarmWorkersForImage(tx, image)
			if err != nil {
				return err
			}
			if count >= int64(perImageTarget) {
				return nil
			}
		}

		// Exclusive-only global cap: colocated workers are unbounded and must
		// not block an exclusive warm spawn.
		if maxGlobalWorkers > 0 {
			count, err := cs.countActiveWorkers(tx, "COALESCE(profile_colocate, false) = false")
			if err != nil {
				return err
			}
			if count >= int64(maxGlobalWorkers) {
				return nil
			}
		}

		var workerID int64
		if err := tx.Raw("SELECT COALESCE(MAX(worker_id), 0) + 1 FROM " + cs.runtimeTable((&WorkerRecord{}).TableName())).Scan(&workerID).Error; err != nil {
			return err
		}
		now := time.Now()
		record := &WorkerRecord{
			WorkerID:          int(workerID),
			PodName:           fmt.Sprintf("%s-%d", podNamePrefix, workerID),
			Image:             image,
			State:             WorkerStateSpawning,
			OrgID:             "",
			OwnerCPInstanceID: ownerCPInstanceID,
			OwnerEpoch:        0,
			LastHeartbeatAt:   now,
		}
		if err := tx.Table(cs.runtimeTable(record.TableName())).Create(record).Error; err != nil {
			return err
		}
		created = record
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create neutral warm worker slot for image: %w", err)
	}
	return created, nil
}

// CreateNeutralWarmWorkerSlot creates a durable spawning worker row for the shared
// neutral warm pool under advisory-lock protected cluster-wide warm-target and
// global capacity checks. A nil result means capacity already satisfies the target
// or the global worker cap blocked the spawn.
func (cs *ConfigStore) CreateNeutralWarmWorkerSlot(ownerCPInstanceID, podNamePrefix, image string, profileCPU, profileMemory string, profileColocate bool, targetWarmWorkers, maxGlobalWorkers int) (*WorkerRecord, error) {
	if strings.TrimSpace(podNamePrefix) == "" {
		return nil, fmt.Errorf("pod name prefix is required")
	}

	var created *WorkerRecord
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:shared-warm-target")).Error; err != nil {
			return err
		}
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:global-worker-capacity")).Error; err != nil {
			return err
		}

		if targetWarmWorkers > 0 {
			count, err := cs.countNeutralWarmWorkers(tx, profileCPU, profileMemory, profileColocate)
			if err != nil {
				return err
			}
			if count >= int64(targetWarmWorkers) {
				return nil
			}
		}

		// Exclusive-only global cap, bypassed for colocated warm shapes —
		// colocated workers bin-pack and are intentionally unbounded.
		if maxGlobalWorkers > 0 && !profileColocate {
			count, err := cs.countActiveWorkers(tx, "COALESCE(profile_colocate, false) = false")
			if err != nil {
				return err
			}
			if count >= int64(maxGlobalWorkers) {
				return nil
			}
		}

		var workerID int64
		if err := tx.Raw("SELECT COALESCE(MAX(worker_id), 0) + 1 FROM " + cs.runtimeTable((&WorkerRecord{}).TableName())).Scan(&workerID).Error; err != nil {
			return err
		}
		now := time.Now()
		record := &WorkerRecord{
			WorkerID:          int(workerID),
			PodName:           fmt.Sprintf("%s-%d", podNamePrefix, workerID),
			Image:             image,
			ProfileCPU:        profileCPU,
			ProfileMemory:     profileMemory,
			ProfileColocate:   profileColocate,
			State:             WorkerStateSpawning,
			OrgID:             "",
			OwnerCPInstanceID: ownerCPInstanceID,
			OwnerEpoch:        0,
			LastHeartbeatAt:   now,
		}
		if err := tx.Table(cs.runtimeTable(record.TableName())).Create(record).Error; err != nil {
			return err
		}
		created = record
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create neutral warm worker slot: %w", err)
	}
	return created, nil
}

// ListOrphanedWorkers returns workers in active states that no live CP
// is responsible for any longer. Three independent failure modes are
// covered, joined by OR:
//
//  1. The owning CP row exists and has been marked expired at least
//     `before` ago. This is the canonical case — a CP died, the
//     liveness janitor flipped its row to expired, the orphan grace
//     elapsed, and now the worker is fair game.
//  2. owner_cp_instance_id is empty / NULL and the worker hasn't
//     heartbeat since `before`. Observed in production: rows whose
//     owner string was lost end up invisible to (1)'s INNER JOIN and
//     accumulate forever, blocking warm-pool replenishment because
//     countNeutralWarmWorkers still counts them. The stale-heartbeat
//     guard avoids racing the spawn path's create-then-stamp window.
//  3. owner_cp_instance_id is set but no matching cp_instances row
//     exists at all (hard-deleted somehow), and again the heartbeat
//     is stale. Same shape as (2), different cause.
//
// Retired/lost rows are deliberately excluded — their pods are already
// gone (or are reconciled by K8sWorkerPool.cleanupOrphanedWorkerPods).
// Re-listing terminal rows here would loop the janitor on K8s 404s.
//
// The join switched from INNER to LEFT in Apr 2026 to handle (2)/(3);
// the original implementation only handled (1).
//
// Apr 2026 also added an exclusion for workers with reclaimable Flight
// sessions: a row with at least one flight_session_records entry in
// active or reconnecting state is spared from orphan retirement so a
// customer reconnecting by session token can still pick up their query
// (see TakeOverWorker). Once the session record itself becomes terminal
// (expired/closed via ExpireFlightSessionRecords), the worker is
// retired normally on the next sweep.
//
// See TestListOrphanedWorkers* in tests/configstore for the regression
// fixtures.
func (cs *ConfigStore) ListOrphanedWorkers(before time.Time) ([]WorkerRecord, error) {
	var workers []WorkerRecord
	cleanupStates := []WorkerState{
		WorkerStateSpawning,
		WorkerStateIdle,
		WorkerStateReserved,
		WorkerStateActivating,
		WorkerStateHot,
		WorkerStateHotIdle,
		WorkerStateDraining,
	}
	reclaimableSessionStates := []FlightSessionState{
		FlightSessionStateActive,
		FlightSessionStateReconnecting,
	}
	workerTable := cs.runtimeTable((&WorkerRecord{}).TableName())
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())
	flightTable := cs.runtimeTable((&FlightSessionRecord{}).TableName())
	err := cs.db.Table(workerTable+" AS w").
		Select("w.*").
		Joins("LEFT JOIN "+cpTable+" AS cp ON cp.id = w.owner_cp_instance_id").
		Where("w.state IN ?", cleanupStates).
		Where(
			// (1) owner CP exists and is expired long enough ago
			"(cp.state = ? AND cp.expired_at IS NOT NULL AND cp.expired_at <= ?) "+
				// (2) owner string is empty/NULL and heartbeat is stale
				"OR (NULLIF(w.owner_cp_instance_id, '') IS NULL AND w.last_heartbeat_at <= ?) "+
				// (3) owner string is set but no matching CP row, heartbeat stale
				"OR (cp.id IS NULL AND NULLIF(w.owner_cp_instance_id, '') IS NOT NULL AND w.last_heartbeat_at <= ?)",
			ControlPlaneInstanceStateExpired, before,
			before,
			before,
		).
		// Spare workers with at least one reclaimable Flight session: a
		// retire here would kill the customer's mid-flight query at the
		// moment they reconnect by session token. Bounded by
		// ExpireFlightSessionRecords — once the session record is moved
		// to a terminal state, the worker is no longer protected.
		Where("NOT EXISTS (SELECT 1 FROM "+flightTable+" AS f "+
			"WHERE f.worker_id = w.worker_id AND f.state IN ?)",
			reclaimableSessionStates).
		Order("w.worker_id ASC").
		Find(&workers).Error
	if err != nil {
		return nil, fmt.Errorf("list orphaned workers: %w", err)
	}
	return workers, nil
}

// ListStuckWorkers returns workers stuck in spawning, reserved, or activating
// beyond their respective cutoffs.
func (cs *ConfigStore) ListStuckWorkers(spawningBefore, activatingBefore time.Time) ([]WorkerRecord, error) {
	var workers []WorkerRecord
	workerTable := cs.runtimeTable((&WorkerRecord{}).TableName())
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())
	err := cs.db.Table(workerTable+" AS w").
		Select("w.*").
		Joins("LEFT JOIN "+cpTable+" AS cp ON cp.id = w.owner_cp_instance_id").
		Where("(w.state = ? AND w.updated_at <= ?) OR (w.state IN ? AND w.updated_at <= ?)",
			WorkerStateSpawning,
			spawningBefore,
			[]WorkerState{WorkerStateReserved, WorkerStateActivating},
			activatingBefore,
		).
		Where("cp.id IS NULL OR cp.state <> ?", ControlPlaneInstanceStateExpired).
		Find(&workers).Error
	if err != nil {
		return nil, fmt.Errorf("list stuck workers: %w", err)
	}
	return workers, nil
}

// ExpireFlightSessionRecords marks reconnectable Flight sessions expired when
// their reconnect deadline has passed.
func (cs *ConfigStore) ExpireFlightSessionRecords(before time.Time) (int64, error) {
	result := cs.db.Table(cs.runtimeTable((&FlightSessionRecord{}).TableName())).
		Where("state NOT IN ?", []FlightSessionState{FlightSessionStateExpired, FlightSessionStateClosed}).
		Where("expires_at <= ?", before).
		Updates(map[string]any{
			"state":      FlightSessionStateExpired,
			"updated_at": time.Now(),
		})
	if result.Error != nil {
		return 0, fmt.Errorf("expire flight session records: %w", result.Error)
	}
	return result.RowsAffected, nil
}

func (cs *ConfigStore) countActiveWorkers(tx *gorm.DB, where ...any) (int64, error) {
	var count int64
	query := tx.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).Where("state IN ?", workerActiveStates())
	if len(where) > 0 {
		if clauseStr, ok := where[0].(string); ok {
			query = query.Where(clauseStr, where[1:]...)
		}
	}
	if err := query.Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func workerActiveStates() []WorkerState {
	return []WorkerState{
		WorkerStateSpawning,
		WorkerStateIdle,
		WorkerStateReserved,
		WorkerStateActivating,
		WorkerStateHot,
		WorkerStateHotIdle,
		WorkerStateDraining,
	}
}

// workerLostEligibleStates lists the worker states from which a worker may be
// moved to `lost`. `draining` is intentionally excluded: a draining row is
// already mid-shutdown under an owning CP's CAS chain, and the right terminal
// for it is `retired` via RetireDrainingWorker (or the orphan sweep if its CP
// died). Calling MarkWorkerTerminalIfCurrent with target=lost on a draining
// snapshot is therefore a silent no-op by design.
func workerLostEligibleStates() []WorkerState {
	return []WorkerState{
		WorkerStateSpawning,
		WorkerStateIdle,
		WorkerStateReserved,
		WorkerStateActivating,
		WorkerStateHot,
		WorkerStateHotIdle,
	}
}

func workerTerminalEligibleStates(targetState WorkerState) []WorkerState {
	if targetState == WorkerStateLost {
		return workerLostEligibleStates()
	}
	return workerActiveStates()
}

// countNeutralWarmWorkers counts neutral (unassigned) warm workers of a single
// profile shape. The warm pool is maintained per shape, so the default
// ("","",false) and colocated targets are counted independently.
// sumOrgColocatedResources sums the CPU (whole cores) and memory (bytes) of an
// org's live colocated workers. Used inside ClaimIdleWorker's advisory-locked txn
// to enforce the per-org colocated budget authoritatively across CP replicas.
func (cs *ConfigStore) sumOrgColocatedResources(tx *gorm.DB, orgID string) (cpuCores int, memBytes uint64, err error) {
	var rows []WorkerRecord
	if err := tx.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Select("profile_cpu", "profile_memory").
		Where("org_id = ? AND COALESCE(profile_colocate, false) = true AND state <> ?", orgID, WorkerStateRetired).
		Find(&rows).Error; err != nil {
		return 0, 0, err
	}
	for _, r := range rows {
		cpuCores += parseColocatedCPUCores(r.ProfileCPU)
		memBytes += parseColocatedMemBytes(r.ProfileMemory)
	}
	return cpuCores, memBytes, nil
}

// parseColocatedCPUCores parses a CPU quantity to whole cores (e.g. "4" -> 4).
func parseColocatedCPUCores(s string) int {
	if s == "" {
		return 0
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	return int(q.Value())
}

// parseColocatedMemBytes parses a memory quantity to bytes (e.g. "16Gi").
func parseColocatedMemBytes(s string) uint64 {
	if s == "" {
		return 0
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	if v := q.Value(); v > 0 {
		return uint64(v)
	}
	return 0
}

func (cs *ConfigStore) countNeutralWarmWorkers(tx *gorm.DB, profileCPU, profileMemory string, profileColocate bool) (int64, error) {
	var count int64
	if err := tx.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("org_id = ''").
		Where("COALESCE(profile_cpu, '') = ? AND COALESCE(profile_memory, '') = ? AND COALESCE(profile_colocate, false) = ?", profileCPU, profileMemory, profileColocate).
		Where("state IN ?", []WorkerState{WorkerStateIdle, WorkerStateSpawning}).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func (cs *ConfigStore) countNeutralWarmWorkersForImage(tx *gorm.DB, image string) (int64, error) {
	var count int64
	if err := tx.Table(cs.runtimeTable((&WorkerRecord{}).TableName())).
		Where("org_id = ''").
		Where("image = ?", image).
		// Only the DEFAULT (exclusive) shape counts toward the per-image warm
		// target — colocated workers share p.workerImage but live in their own
		// warm pool, so without this filter they'd cross-count and starve the
		// exclusive pool. COALESCE keeps legacy NULL-profile rows in the default
		// bucket. Keep in sync with countNeutralWarmWorkers / the default
		// MatchKey ("","",false).
		Where("COALESCE(profile_cpu, '') = '' AND COALESCE(profile_memory, '') = '' AND COALESCE(profile_colocate, false) = false").
		Where("state IN ?", []WorkerState{WorkerStateIdle, WorkerStateSpawning}).
		Count(&count).Error; err != nil {
		return 0, err
	}
	return count, nil
}

func advisoryLockKey(s string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

// UpsertFlightSessionRecord inserts or updates a durable Flight reconnect row.
func (cs *ConfigStore) UpsertFlightSessionRecord(record *FlightSessionRecord) error {
	if err := cs.db.Table(cs.runtimeTable(record.TableName())).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "session_token"}},
		DoUpdates: clause.AssignmentColumns([]string{"username", "org_id", "worker_id", "owner_epoch", "cp_instance_id", "state", "expires_at", "last_seen_at", "updated_at"}),
	}).Create(record).Error; err != nil {
		return fmt.Errorf("upsert flight session record: %w", err)
	}
	return nil
}

// GetFlightSessionRecord returns a durable Flight reconnect row by session token.
func (cs *ConfigStore) GetFlightSessionRecord(sessionToken string) (*FlightSessionRecord, error) {
	var record FlightSessionRecord
	err := cs.db.Table(cs.runtimeTable(record.TableName())).First(&record, "session_token = ?", sessionToken).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("get flight session record: %w", err)
	}
	return &record, nil
}

func (cs *ConfigStore) TouchFlightSessionRecord(sessionToken string, lastSeenAt time.Time) error {
	result := cs.db.Table(cs.runtimeTable((&FlightSessionRecord{}).TableName())).
		Where("session_token = ?", sessionToken).
		Updates(map[string]any{
			"last_seen_at": lastSeenAt,
			"updated_at":   time.Now(),
		})
	if result.Error != nil {
		return fmt.Errorf("touch flight session record: %w", result.Error)
	}
	return nil
}

func (cs *ConfigStore) CloseFlightSessionRecord(sessionToken string, closedAt time.Time) error {
	result := cs.db.Table(cs.runtimeTable((&FlightSessionRecord{}).TableName())).
		Where("session_token = ?", sessionToken).
		Updates(map[string]any{
			"state":        FlightSessionStateClosed,
			"last_seen_at": closedAt,
			"updated_at":   time.Now(),
		})
	if result.Error != nil {
		return fmt.Errorf("close flight session record: %w", result.Error)
	}
	return nil
}

// Reload forces an immediate config reload from the database.
func (cs *ConfigStore) Reload() error {
	newSnap, err := cs.load()
	if err != nil {
		return err
	}

	cs.mu.Lock()
	oldSnap := cs.snapshot
	cs.snapshot = newSnap
	callbacks := make([]func(old, new *Snapshot), len(cs.onChange))
	copy(callbacks, cs.onChange)
	cs.mu.Unlock()

	for _, fn := range callbacks {
		fn(oldSnap, newSnap)
	}
	return nil
}
