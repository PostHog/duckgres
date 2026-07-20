//go:build kubernetes

package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"k8s.io/apimachinery/pkg/api/resource"
)

var errWarehousePayloadNotAllowed = errors.New("warehouse payload must be updated via /orgs/:id/warehouse")

// The teams association is read-only through the org endpoints: the billing
// team is managed via the default_team_id field, and there is no direct
// team-list mutation surface yet.
var errOrgTeamsPayloadNotAllowed = errors.New("teams cannot be set directly; set the billing team via default_team_id")

var errWarehouseStillExists = errors.New("managed warehouse still exists for org")

// maxWarehousePutBodyBytes caps the admin PUT body. Warehouse payloads are
// under 10 KB in practice; 1 MiB leaves room for future fields while keeping
// the handler from loading unbounded input into memory.
const maxWarehousePutBodyBytes = 1 << 20

// WorkerStatus represents a worker's current status for the API.
type WorkerStatus struct {
	ID             int    `json:"id"`
	Org            string `json:"org"`
	ActiveSessions int    `json:"active_sessions"`
	Status         string `json:"status"`
	CPU            string `json:"cpu"`
	Memory         string `json:"memory"`
	TTLSeconds     int    `json:"ttl_seconds"`
}

// SessionStatus represents an active session for the API.
type SessionStatus struct {
	PID      int32  `json:"pid"`
	WorkerID int    `json:"worker_id"`
	Org      string `json:"org"`
	User     string `json:"user"`
	Protocol string `json:"protocol"`
}

// ClusterStatus aggregates cluster state for the dashboard.
type ClusterStatus struct {
	TotalOrgs     int         `json:"total_orgs"`
	TotalWorkers  int         `json:"total_workers"`
	TotalSessions int         `json:"total_sessions"`
	Orgs          []OrgStatus `json:"orgs"`
}

// OrgStatus is a per-org summary.
type OrgStatus struct {
	Name           string `json:"name"`
	Workers        int    `json:"workers"`
	ActiveSessions int    `json:"active_sessions"`
	MaxWorkers     int    `json:"max_workers"`
}

// OrgStackInfo provides info about an org's live state.
// Implemented by the controlplane.OrgRouter via adapter.
type OrgStackInfo interface {
	// AllOrgStats returns per-org worker and session counts.
	AllOrgStats() []OrgStatus
	// AllWorkerStatuses returns all workers across orgs.
	AllWorkerStatuses() []WorkerStatus
	// AllSessionStatuses returns all active sessions across orgs.
	AllSessionStatuses() []SessionStatus
}

// RegisterAPI registers all admin REST endpoints on the given router group.
// fetcher (may be nil) aggregates per-CP live state (sessions/workers) across
// replicas so the dashboard shows cluster-wide numbers instead of one CP's slice.
func RegisterAPI(r *gin.RouterGroup, store *configstore.ConfigStore, info OrgStackInfo, fetcher PeerFetcher) {
	registerAPIWithStore(r, newGormAPIStore(store), info, fetcher)
	// Generic read-only models explorer (sidebar + table + detail UI). Reads
	// the concrete store directly because it needs the runtime schema name and
	// raw DB for tables the typed apiStore interface doesn't surface.
	registerModelsAPI(r, store)
	// Admin-only Operators management (the admin-console access list). Each
	// route self-gates with RequireAdmin; mutations are audited via the group.
	registerOperatorsAPI(r, store)
}

func registerAPIWithStore(r *gin.RouterGroup, store apiStore, info OrgStackInfo, fetcher PeerFetcher) {
	h := &apiHandler{store: store, info: info, fetcher: fetcher}

	// Orgs CRUD
	r.GET("/orgs", h.listOrgs)
	r.POST("/orgs", h.createOrg)
	r.GET("/orgs/:id", h.getOrg)
	r.PUT("/orgs/:id", h.updateOrg)
	r.DELETE("/orgs/:id", h.deleteOrg)
	r.GET("/orgs/:id/warehouse", h.getManagedWarehouse)
	r.PUT("/orgs/:id/warehouse", h.putManagedWarehouse)
	// Focused endpoint for pinning a tenant to a specific worker image and
	// DuckLake spec version — the operator workflow we want to be able to
	// run without ever touching the config-store DB directly.
	r.PATCH("/orgs/:id/warehouse/pinning", h.patchTenantPinning)

	// Org teams (duckgres_org_teams). The per-org list/upsert/delete routes
	// (GET/POST /orgs/:id/teams, DELETE /orgs/:id/teams/:team_id) are
	// registered by the provisioning API on this same group — gin refuses
	// duplicate routes, so the admin surface adds only what provisioning
	// doesn't have: the cross-org list, a CREATE-ONLY POST (org_id in the
	// body, mirroring POST /users), and the update. The PUT is the operator
	// break-glass: it can edit EVERY team setting, including schema_name and
	// the legacy table-name overrides — schema immutability is a rule for the
	// user-facing product flows, not for operators repairing rows here.
	r.GET("/teams", h.listAllOrgTeams)
	r.POST("/teams", h.createOrgTeam)
	r.PUT("/orgs/:id/teams/:team_id", h.updateOrgTeam)
	r.PUT("/orgs/:id/teams/:team_id/project-reader", h.upsertProjectReader)

	// Users CRUD
	r.GET("/users", h.listUsers)
	r.POST("/users", h.createUser)
	r.GET("/orgs/:id/users/:username", h.getUser)
	r.PUT("/orgs/:id/users/:username", h.updateUser)
	r.DELETE("/orgs/:id/users/:username", h.deleteUser)
	// Peer-only fan-out target: see reloadSnapshot below.
	r.POST("/internal/reload-snapshot", h.reloadSnapshot)

	// Workers (read-only)
	r.GET("/workers", h.listWorkers)

	// Sessions (read-only)
	r.GET("/sessions", h.listSessions)

	// Overview
	r.GET("/status", h.getClusterStatus)
}

type apiStore interface {
	ListOrgs() ([]configstore.Org, error)
	CreateOrg(org *configstore.Org) error
	GetOrg(name string) (*configstore.Org, error)
	// UpdateOrg persists the merged org row. reattributeUsageTeam is non-nil
	// when the update changes the org's billing team (the wire field
	// default_team_id): the store must then repoint the org's billing team row
	// (duckgres_org_teams) AND re-attribute the org's buffered (unacked)
	// billing buckets to that new team id in the SAME transaction as the
	// org-row update, so a committed team change never leaves usage stranded
	// under the old team.
	UpdateOrg(name string, updates configstore.Org, reattributeUsageTeam *int64) (*configstore.Org, bool, error)
	DeleteOrg(name string) (bool, error)

	// Org teams. CreateOrgTeam is create-only (the admin surface never
	// overwrites an existing row); it returns errOrgTeamExists /
	// configstore.ErrOrgTeamSchemaConflict for the two conflict shapes,
	// gorm.ErrRecordNotFound for an unknown org. UpdateOrgTeam applies the
	// presence-aware orgTeamUpdate (every column is editable — operator
	// break-glass) and can repoint the billing team (usage buckets
	// re-attributed in the same transaction); it returns the row as it was
	// BEFORE the update alongside the stored result so the handler can audit
	// old → new per field. A schema_name change that collides with another
	// team in the org returns configstore.ErrOrgTeamSchemaConflict.
	// Per-org list and delete are served by the provisioning API's routes on
	// the same router group (identical rules — configstore.DeleteOrgTeamTx).
	ListAllOrgTeams() ([]configstore.OrgTeam, error)
	CreateOrgTeam(orgID string, team *configstore.OrgTeam) error
	UpdateOrgTeam(orgID string, teamID int64, upd orgTeamUpdate) (prev, stored *configstore.OrgTeam, err error)

	ListUsers() ([]configstore.OrgUser, error)
	CreateUser(user *configstore.OrgUser) error
	GetUser(orgID, username string) (*configstore.OrgUser, error)
	UpdateUser(orgID, username, passwordHash string, passthrough *bool, maxVCPUs *int) (*configstore.OrgUser, bool, error)
	DeleteUser(orgID, username string) (bool, error)
	UpsertProjectReader(orgID string, teamID int64, username, password string) (*configstore.OrgUser, error)

	GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error)
	UpsertManagedWarehouse(orgID string, warehouse *configstore.ManagedWarehouse) (*configstore.ManagedWarehouse, bool, error)
	// MutateManagedWarehouse loads the existing warehouse (or a zero value if
	// none), calls mutate to apply changes, and persists the result — all
	// inside a single transaction with a row-level lock on the warehouse row.
	// Closes the read-modify-write race that plain Get+Upsert is exposed to
	// when concurrent PUTs target the same org. Returns (nil, false, nil) if
	// the org doesn't exist.
	MutateManagedWarehouse(orgID string, mutate func(*configstore.ManagedWarehouse) error) (*configstore.ManagedWarehouse, bool, error)

	// ReloadSnapshot forces this replica's in-memory config snapshot to reload
	// from the config-store DB immediately, bypassing the poll interval. Used
	// by createUser/updateUser/deleteUser so a write is authable on this
	// replica the instant the request returns — see notifyPeersOfChange.
	ReloadSnapshot() error
}

type gormAPIStore struct {
	store *configstore.ConfigStore
}

func newGormAPIStore(store *configstore.ConfigStore) apiStore {
	return &gormAPIStore{store: store}
}

func (s *gormAPIStore) db() *gorm.DB {
	return s.store.DB()
}

func (s *gormAPIStore) ListOrgs() ([]configstore.Org, error) {
	var orgs []configstore.Org
	if err := s.db().Preload("Users").Preload("Warehouse").Preload("Teams").Find(&orgs).Error; err != nil {
		return nil, err
	}
	for i := range orgs {
		orgs[i].DefaultTeamID = orgs[i].BillingTeamID()
	}
	return orgs, nil
}

func (s *gormAPIStore) CreateOrg(org *configstore.Org) error {
	org.Warehouse = nil
	// One transaction: org row + its billing team row (duckgres_org_teams).
	// The handler already required a positive DefaultTeamID, so a created org
	// is always born with its billing team.
	return s.db().Transaction(func(tx *gorm.DB) error {
		if err := tx.Omit("Warehouse", "Teams").Create(org).Error; err != nil {
			return err
		}
		if org.DefaultTeamID == nil {
			return nil
		}
		return configstore.SetOrgBillingTeamTx(tx, org.Name, *org.DefaultTeamID)
	})
}

func (s *gormAPIStore) GetOrg(name string) (*configstore.Org, error) {
	var org configstore.Org
	if err := s.db().Preload("Users").Preload("Warehouse").Preload("Teams").First(&org, "name = ?", name).Error; err != nil {
		return nil, err
	}
	org.DefaultTeamID = org.BillingTeamID()
	return &org, nil
}

func (s *gormAPIStore) UpdateOrg(name string, updates configstore.Org, reattributeUsageTeam *int64) (*configstore.Org, bool, error) {
	fields := map[string]interface{}{
		"max_workers": updates.MaxWorkers,
		"max_vcpus":   updates.MaxVCPUs,
		// Org default worker profile: written unconditionally so an explicit
		// empty string CLEARS the default (the handler's presence-merge keeps
		// omitted fields at their stored values before this runs).
		"default_worker_cpu":          updates.DefaultWorkerCPU,
		"default_worker_memory":       updates.DefaultWorkerMemory,
		"default_worker_ttl":          updates.DefaultWorkerTTL,
		"default_worker_min_hot_idle": updates.DefaultWorkerMinHotIdle,
	}
	// HostnameAlias is *string: nil = preserve, "" = clear (NULL), "x" = set.
	// NULL releases the unique-index slot so other orgs can take that alias.
	if updates.HostnameAlias != nil {
		if *updates.HostnameAlias == "" {
			fields["hostname_alias"] = nil
		} else {
			fields["hostname_alias"] = *updates.HostnameAlias
		}
	}
	// The billing team (wire field default_team_id) is not an org column: a
	// change repoints the org's duckgres_org_teams billing row inside the same
	// transaction below. The handler rejects 0/null/negative before this runs
	// and only passes reattributeUsageTeam when the value actually changes.
	found := false
	err := s.db().Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&configstore.Org{}).Where("name = ?", name).Updates(fields)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
		}
		found = true
		if reattributeUsageTeam != nil {
			// Read the pre-update team id purely for the log line — the
			// handler already decided the billing team changes.
			oldTeam, err := configstore.OrgBillingTeamIDTx(tx, name)
			if err != nil {
				return err
			}
			if err := configstore.SetOrgBillingTeamTx(tx, name, *reattributeUsageTeam); err != nil {
				return err
			}
			// Same transaction as the billing-team repoint: a committed team
			// change always carries its buffered buckets along. In-flight
			// metering under the old team can still land a small residual row
			// right after this (config-snapshot poll lag, ~30s) — tolerated,
			// see configstore.ReattributeUsageTeamTx.
			moved, err := configstore.ReattributeUsageTeamTx(tx, name, *reattributeUsageTeam)
			if err != nil {
				return err
			}
			slog.Info("Re-attributed org usage buckets to new billing team.",
				"org", name, "old_team", oldTeam, "new_team", *reattributeUsageTeam, "rows", moved)
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}
	org, err := s.GetOrg(name)
	if err != nil {
		return nil, true, err
	}
	return org, true, nil
}

func (s *gormAPIStore) DeleteOrg(name string) (bool, error) {
	returnRows := int64(0)
	err := s.db().Transaction(func(tx *gorm.DB) error {
		// Deleting an org while a managed warehouse row is in a non-terminal
		// state would leak the Duckling CR + AWS infra behind it, so those
		// still block deletion. But deprovisioning does NOT remove the
		// warehouse row — the provisioner tears the infra down and leaves the
		// row in the terminal "deleted" state (controller.go reconcileDeleting).
		// A "deleted" row means the infra is gone, so cascade it away here and
		// let the org (and its unique database_name) be released. Without this,
		// a fully deprovisioned org could never be deleted and its
		// database_name would be squatted forever.
		var liveWarehouses int64
		if err := tx.Model(&configstore.ManagedWarehouse{}).
			Where("org_id = ? AND state <> ?", name, configstore.ManagedWarehouseStateDeleted).
			Count(&liveWarehouses).Error; err != nil {
			return err
		}
		if liveWarehouses > 0 {
			return errWarehouseStillExists
		}
		if err := tx.Where("org_id = ?", name).Delete(&configstore.ManagedWarehouse{}).Error; err != nil {
			return err
		}
		if err := tx.Where("org_id = ?", name).Delete(&configstore.OrgUser{}).Error; err != nil {
			return err
		}
		result := tx.Where("name = ?", name).Delete(&configstore.Org{})
		if result.Error != nil {
			return result.Error
		}
		returnRows = result.RowsAffected
		return nil
	})
	if err != nil {
		return false, err
	}
	return returnRows > 0, nil
}

// errOrgTeamExists distinguishes the (org, team) primary-key conflict from
// the schema-name conflict on the admin create endpoint — the admin surface
// never overwrites an existing row (that's the internal provisioning
// grandfather path), so an existing row is a 409, not an upsert.
var errOrgTeamExists = errors.New("team already exists in this org")

// orgTeamUpdate carries the admin-editable fields of one org team — every
// column, this is the operator break-glass surface. Pointer fields are
// presence-aware (nil = preserve). The *Set booleans distinguish an explicit
// JSON null (clear to unset/NULL) from an absent key for the nullable columns.
type orgTeamUpdate struct {
	// SchemaName: nil = preserve. A non-nil value is the operator override —
	// validated by the handler, refused with ErrOrgTeamSchemaConflict when
	// another team in the org already uses it. Changing it does NOT move any
	// warehouse data; tables under the old schema are not renamed.
	SchemaName  *string
	Enabled     *bool
	BackfillSet bool
	Backfill    *bool
	// Legacy explicit table names for grandfathered teams (NULL = derive from
	// schema_name). XSet + nil value clears the column back to NULL.
	EventsTableNameSet       bool
	EventsTableName          *string
	PersonsTableNameSet      bool
	PersonsTableName         *string
	SchemaDataImportsNameSet bool
	SchemaDataImportsName    *string
	// MakeBilling repoints the org's billing team to this row (the buffered
	// usage buckets follow atomically). There is no "unset billing" — every
	// org must keep a billing team; repoint by marking another team.
	MakeBilling bool
}

func (s *gormAPIStore) ListAllOrgTeams() ([]configstore.OrgTeam, error) {
	var teams []configstore.OrgTeam
	if err := s.db().Order("org_id, team_id").Find(&teams).Error; err != nil {
		return nil, err
	}
	return teams, nil
}

func (s *gormAPIStore) CreateOrgTeam(orgID string, team *configstore.OrgTeam) error {
	return s.db().Transaction(func(tx *gorm.DB) error {
		var orgCount int64
		if err := tx.Model(&configstore.Org{}).Where("name = ?", orgID).Count(&orgCount).Error; err != nil {
			return err
		}
		if orgCount == 0 {
			return gorm.ErrRecordNotFound
		}
		var dup int64
		if err := tx.Model(&configstore.OrgTeam{}).
			Where("org_id = ? AND team_id = ?", orgID, team.TeamID).Count(&dup).Error; err != nil {
			return err
		}
		if dup > 0 {
			return errOrgTeamExists
		}
		var schemaClash int64
		if err := tx.Model(&configstore.OrgTeam{}).
			Where("org_id = ? AND schema_name = ?", orgID, team.SchemaName).Count(&schemaClash).Error; err != nil {
			return err
		}
		if schemaClash > 0 {
			return configstore.ErrOrgTeamSchemaConflict
		}
		team.OrgID = orgID
		// Capture intent BEFORE Create: gorm's RETURNING write-back stamps
		// the DB row (enabled defaulted TRUE) back onto the struct, so the
		// post-Create value lies about what the caller asked for.
		wantDisabled := !team.Enabled
		if err := tx.Create(team).Error; err != nil {
			return err
		}
		// Enabled carries gorm's `default:true` tag: a zero-valued (false)
		// field is omitted from the INSERT and the DB default TRUE wins —
		// the same pitfall fixed in configstore.UpsertOrgTeamTx, on this
		// surface reachable via POST /teams {"enabled":false}. Force the
		// column explicitly. Pinned by TestAdminCreateOrgTeamDisabledPostgres.
		if wantDisabled {
			if err := tx.Model(&configstore.OrgTeam{}).
				Where("org_id = ? AND team_id = ?", orgID, team.TeamID).
				Update("enabled", false).Error; err != nil {
				return err
			}
			team.Enabled = false // undo the RETURNING write-back for the caller
		}
		return nil
	})
}

func (s *gormAPIStore) UpdateOrgTeam(orgID string, teamID int64, upd orgTeamUpdate) (*configstore.OrgTeam, *configstore.OrgTeam, error) {
	var prev, stored configstore.OrgTeam
	err := s.db().Transaction(func(tx *gorm.DB) error {
		var team configstore.OrgTeam
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&team, "org_id = ? AND team_id = ?", orgID, teamID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return configstore.ErrOrgTeamNotFound
		}
		if err != nil {
			return err
		}
		prev = team

		fields := map[string]interface{}{"updated_at": gorm.Expr("now()")}
		if upd.SchemaName != nil && *upd.SchemaName != team.SchemaName {
			// Pre-check the per-org schema uniqueness for a clean error; the
			// unique (org_id, schema_name) index still backs it — a concurrent
			// writer that slips past the check fails with a 23505 (same
			// contract as configstore.UpsertOrgTeamTx).
			var clash int64
			if err := tx.Model(&configstore.OrgTeam{}).
				Where("org_id = ? AND schema_name = ? AND team_id <> ?", orgID, *upd.SchemaName, teamID).
				Count(&clash).Error; err != nil {
				return err
			}
			if clash > 0 {
				return configstore.ErrOrgTeamSchemaConflict
			}
			fields["schema_name"] = *upd.SchemaName
		}
		if upd.Enabled != nil {
			fields["enabled"] = *upd.Enabled
		}
		if upd.BackfillSet {
			fields["backfill_enabled"] = upd.Backfill
		}
		if upd.EventsTableNameSet {
			fields["events_table_name"] = upd.EventsTableName
		}
		if upd.PersonsTableNameSet {
			fields["persons_table_name"] = upd.PersonsTableName
		}
		if upd.SchemaDataImportsNameSet {
			fields["schema_data_imports_name"] = upd.SchemaDataImportsName
		}
		if len(fields) > 1 {
			if err := tx.Model(&configstore.OrgTeam{}).
				Where("org_id = ? AND team_id = ?", orgID, teamID).
				Updates(fields).Error; err != nil {
				return err
			}
		}

		if upd.MakeBilling && (team.IsBillingTeam == nil || !*team.IsBillingTeam) {
			// Repointing billing carries the org's buffered usage buckets
			// along in the SAME transaction — identical to the
			// default_team_id repoint on the org endpoints.
			oldTeam, err := configstore.OrgBillingTeamIDTx(tx, orgID)
			if err != nil {
				return err
			}
			if err := configstore.SetOrgBillingTeamTx(tx, orgID, teamID); err != nil {
				return err
			}
			moved, err := configstore.ReattributeUsageTeamTx(tx, orgID, teamID)
			if err != nil {
				return err
			}
			slog.Info("Re-attributed org usage buckets to new billing team.",
				"org", orgID, "old_team", oldTeam, "new_team", teamID, "rows", moved)
		}

		return tx.First(&stored, "org_id = ? AND team_id = ?", orgID, teamID).Error
	})
	if err != nil {
		return nil, nil, err
	}
	return &prev, &stored, nil
}

func (s *gormAPIStore) ListUsers() ([]configstore.OrgUser, error) {
	var users []configstore.OrgUser
	if err := s.db().Find(&users).Error; err != nil {
		return nil, err
	}
	return users, nil
}

func (s *gormAPIStore) CreateUser(user *configstore.OrgUser) error {
	return s.db().Create(user).Error
}

func (s *gormAPIStore) GetUser(orgID, username string) (*configstore.OrgUser, error) {
	var user configstore.OrgUser
	if err := s.db().First(&user, "org_id = ? AND username = ?", orgID, username).Error; err != nil {
		return nil, err
	}
	return &user, nil
}

func (s *gormAPIStore) UpdateUser(orgID, username, passwordHash string, passthrough *bool, maxVCPUs *int) (*configstore.OrgUser, bool, error) {
	updates := map[string]interface{}{}
	if passwordHash != "" {
		updates["password"] = passwordHash
	}
	if passthrough != nil {
		updates["passthrough"] = *passthrough
	}
	if maxVCPUs != nil {
		updates["max_vcpus"] = *maxVCPUs
	}
	if len(updates) == 0 {
		// Nothing to change — return the current row so callers can still
		// distinguish "user not found" from "no-op update".
		user, err := s.GetUser(orgID, username)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, false, nil
			}
			return nil, false, err
		}
		return user, true, nil
	}
	result := s.db().Model(&configstore.OrgUser{}).Where("org_id = ? AND username = ?", orgID, username).Updates(updates)
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, false, nil
	}
	user, err := s.GetUser(orgID, username)
	if err != nil {
		return nil, true, err
	}
	return user, true, nil
}

func (s *gormAPIStore) DeleteUser(orgID, username string) (bool, error) {
	result := s.db().Where("org_id = ? AND username = ?", orgID, username).Delete(&configstore.OrgUser{})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (s *gormAPIStore) UpsertProjectReader(orgID string, teamID int64, username, password string) (*configstore.OrgUser, error) {
	var stored configstore.OrgUser
	err := s.db().Transaction(func(tx *gorm.DB) error {
		var team configstore.OrgTeam
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(
			&team, "org_id = ? AND team_id = ? AND enabled IS TRUE", orgID, teamID,
		).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return configstore.ErrProjectReaderTeamUnavailable
			}
			return err
		}
		passwordHash := ""
		var existing configstore.OrgUser
		existingResult := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(
			&existing, "org_id = ? AND username = ?", orgID, username,
		)
		if existingResult.Error == nil && bcrypt.CompareHashAndPassword([]byte(existing.Password), []byte(password)) == nil {
			passwordHash = existing.Password
		} else if existingResult.Error != nil && !errors.Is(existingResult.Error, gorm.ErrRecordNotFound) {
			return existingResult.Error
		} else {
			var err error
			passwordHash, err = configstore.HashPassword(password)
			if err != nil {
				return err
			}
		}
		boundTeamID := teamID
		user := configstore.OrgUser{
			OrgID:       orgID,
			Username:    username,
			Password:    passwordHash,
			Passthrough: false,
			AccessMode:  configstore.OrgUserAccessModeProjectReader,
			TeamID:      &boundTeamID,
			Disabled:    false,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "org_id"}, {Name: "username"}},
			DoUpdates: clause.Assignments(map[string]interface{}{
				"password":    passwordHash,
				"passthrough": false,
				"access_mode": configstore.OrgUserAccessModeProjectReader,
				"team_id":     teamID,
				"disabled":    false,
				"updated_at":  time.Now().UTC(),
			}),
		}).Create(&user).Error; err != nil {
			return err
		}
		return tx.First(&stored, "org_id = ? AND username = ?", orgID, username).Error
	})
	if err != nil {
		return nil, err
	}
	return &stored, nil
}

func (s *gormAPIStore) ReloadSnapshot() error {
	return s.store.ReloadSnapshot()
}

func (s *gormAPIStore) GetManagedWarehouse(orgID string) (*configstore.ManagedWarehouse, error) {
	var warehouse configstore.ManagedWarehouse
	if err := s.db().First(&warehouse, "org_id = ?", orgID).Error; err != nil {
		return nil, err
	}
	return &warehouse, nil
}

func (s *gormAPIStore) UpsertManagedWarehouse(orgID string, warehouse *configstore.ManagedWarehouse) (*configstore.ManagedWarehouse, bool, error) {
	var count int64
	if err := s.db().Model(&configstore.Org{}).Where("name = ?", orgID).Count(&count).Error; err != nil {
		return nil, false, err
	}
	if count == 0 {
		return nil, false, nil
	}

	warehouse.OrgID = orgID
	warehouse.UpdatedAt = time.Now().UTC()
	if err := s.db().Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "org_id"}},
		DoUpdates: clause.AssignmentColumns(managedWarehouseUpsertColumns()),
	}).Create(warehouse).Error; err != nil {
		return nil, true, err
	}
	stored, err := s.GetManagedWarehouse(orgID)
	if err != nil {
		return nil, true, err
	}
	return stored, true, nil
}

func (s *gormAPIStore) MutateManagedWarehouse(orgID string, mutate func(*configstore.ManagedWarehouse) error) (*configstore.ManagedWarehouse, bool, error) {
	var (
		stored    *configstore.ManagedWarehouse
		orgExists bool
	)
	err := s.db().Transaction(func(tx *gorm.DB) error {
		var count int64
		if err := tx.Model(&configstore.Org{}).Where("name = ?", orgID).Count(&count).Error; err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		orgExists = true

		// SELECT ... FOR UPDATE: blocks concurrent mutators on the same row
		// until this transaction commits. A missing row is not an error —
		// PUT on a brand-new warehouse lands in the same path.
		var warehouse configstore.ManagedWarehouse
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&warehouse, "org_id = ?", orgID).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		if err := mutate(&warehouse); err != nil {
			return err
		}

		warehouse.OrgID = orgID
		warehouse.UpdatedAt = time.Now().UTC()
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "org_id"}},
			DoUpdates: clause.AssignmentColumns(managedWarehouseUpsertColumns()),
		}).Create(&warehouse).Error; err != nil {
			return err
		}

		var reloaded configstore.ManagedWarehouse
		if err := tx.First(&reloaded, "org_id = ?", orgID).Error; err != nil {
			return err
		}
		stored = &reloaded
		return nil
	})
	if err != nil {
		return nil, orgExists, err
	}
	return stored, orgExists, nil
}

func managedWarehouseUpsertColumns() []string {
	return []string{
		"image",
		// GORM's default naming strategy splits CamelCase on every
		// uppercase-after-lowercase boundary, so `DuckLakeVersion` lands as
		// `duck_lake_version` in Postgres — NOT `ducklake_version`. The JSON
		// tag is `ducklake_version` but that only controls API I/O, not the
		// DB column name. Mismatching this against the actual column makes
		// the ON CONFLICT … DO UPDATE clause throw 42703.
		"duck_lake_version",
		"duckling_name",
		"warehouse_database_endpoint",
		"warehouse_database_port",
		"metadata_store_kind",
		"metadata_store_endpoint",
		"metadata_store_port",
		"metadata_store_database_name",
		"metadata_store_username",
		"pgbouncer_enabled",
		"s3_provider",
		"s3_region",
		"s3_bucket",
		"s3_path_prefix",
		"s3_endpoint",
		"s3_use_ssl",
		"s3_url_style",
		"s3_delta_catalog_enabled",
		"s3_delta_catalog_path",
		"worker_identity_namespace",
		"worker_identity_iam_role_arn",
		"warehouse_database_credentials_namespace",
		"warehouse_database_credentials_name",
		"warehouse_database_credentials_key",
		"metadata_store_credentials_namespace",
		"metadata_store_credentials_name",
		"metadata_store_credentials_key",
		"s3_credentials_namespace",
		"s3_credentials_name",
		"s3_credentials_key",
		"runtime_config_namespace",
		"runtime_config_name",
		"runtime_config_key",
		"state",
		"status_message",
		"metadata_store_state",
		"s3_state",
		"identity_state",
		"secrets_state",
		"ready_at",
		"failed_at",
		"updated_at",
	}
}

type apiHandler struct {
	store   apiStore
	info    OrgStackInfo
	fetcher PeerFetcher // nil = no cross-CP aggregation (single-CP or tests)
}

// managedWarehouseRequest is the whitelist of fields a caller may set on the
// PUT endpoint. It's only used for strict decode (DisallowUnknownFields) — the
// actual merge is performed by json.Unmarshal directly onto a ManagedWarehouse
// (see putManagedWarehouse). For that to work, every `json:` tag here must
// match the corresponding `json:` tag on configstore.ManagedWarehouse. If you
// add a field here without a matching tag on ManagedWarehouse, strict decode
// will accept it and the merge will silently drop it.
type managedWarehouseRequest struct {
	Image                        string                                        `json:"image"`
	DuckLakeVersion              string                                        `json:"ducklake_version"`
	DucklingName                 string                                        `json:"duckling_name"`
	WarehouseDatabase            configstore.ManagedWarehouseDatabase          `json:"warehouse_database"`
	MetadataStore                configstore.ManagedWarehouseMetadataStore     `json:"metadata_store"`
	PgBouncer                    configstore.ManagedWarehousePgBouncer         `json:"pgbouncer"`
	S3                           configstore.ManagedWarehouseS3                `json:"s3"`
	WorkerIdentity               configstore.ManagedWarehouseWorkerIdentity    `json:"worker_identity"`
	WarehouseDatabaseCredentials configstore.SecretRef                         `json:"warehouse_database_credentials"`
	MetadataStoreCredentials     configstore.SecretRef                         `json:"metadata_store_credentials"`
	S3Credentials                configstore.SecretRef                         `json:"s3_credentials"`
	RuntimeConfig                configstore.SecretRef                         `json:"runtime_config"`
	State                        configstore.ManagedWarehouseProvisioningState `json:"state"`
	StatusMessage                string                                        `json:"status_message"`
	MetadataStoreState           configstore.ManagedWarehouseProvisioningState `json:"metadata_store_state"`
	S3State                      configstore.ManagedWarehouseProvisioningState `json:"s3_state"`
	IdentityState                configstore.ManagedWarehouseProvisioningState `json:"identity_state"`
	SecretsState                 configstore.ManagedWarehouseProvisioningState `json:"secrets_state"`
	ReadyAt                      *time.Time                                    `json:"ready_at"`
	FailedAt                     *time.Time                                    `json:"failed_at"`
}

// decodeStrictWarehouseRequest validates a PUT body by decoding it into
// managedWarehouseRequest with DisallowUnknownFields. This whitelists which
// top-level fields a caller may set; the actual merge is performed separately
// by unmarshaling the same body onto an existing ManagedWarehouse (see
// putManagedWarehouse) so missing keys — at any nesting level — preserve
// whatever the stored row already holds.
func decodeStrictWarehouseRequest(body []byte, dst *managedWarehouseRequest) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

// --- Orgs ---

func (h *apiHandler) listOrgs(c *gin.Context) {
	orgs, err := h.store.ListOrgs()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, orgs)
}

func (h *apiHandler) createOrg(c *gin.Context) {
	var org configstore.Org
	if err := c.ShouldBindJSON(&org); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validateOrgMutationPayload(&org); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if org.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}
	// Every org must be born with its billing PostHog team — it is the
	// billing bucket key, stored as the org's duckgres_org_teams row with
	// is_billing_team = TRUE (same invariant as the provisioning API's
	// ErrDefaultTeamIDRequired). The wire field keeps its historical
	// default_team_id name. Positivity of a present value is enforced by
	// validateOrgMutationPayload above.
	if org.DefaultTeamID == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "default_team_id is required (the org's billing PostHog team id)"})
		return
	}
	// Normalize empty hostname_alias to NULL on insert so the unique index
	// doesn't reject a second org with an explicit empty string. Centralizing
	// the rule at the handler layer keeps any future store impl from having
	// to repeat it.
	if org.HostnameAlias != nil && *org.HostnameAlias == "" {
		org.HostnameAlias = nil
	}
	// POST /orgs has no :id param, so the audit org column is blank — record the
	// created org's name here instead.
	setAuditDetail(c, "created org "+org.Name)
	if err := h.store.CreateOrg(&org); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, org)
}

func (h *apiHandler) getOrg(c *gin.Context) {
	name := c.Param("id")
	org, err := h.store.GetOrg(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, org)
}

func (h *apiHandler) updateOrg(c *gin.Context) {
	name := c.Param("id")
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var updates configstore.Org
	if err := json.Unmarshal(body, &updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := validateOrgMutationPayload(&updates); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	existing, err := h.store.GetOrg(name)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	merged := *existing
	if _, ok := fields["max_workers"]; ok {
		merged.MaxWorkers = updates.MaxWorkers
	}
	if _, ok := fields["max_vcpus"]; ok {
		merged.MaxVCPUs = updates.MaxVCPUs
	}
	// Org default worker profile: present-in-payload wins, including an
	// explicit "" which clears the default.
	if _, ok := fields["default_worker_cpu"]; ok {
		merged.DefaultWorkerCPU = updates.DefaultWorkerCPU
	}
	if _, ok := fields["default_worker_memory"]; ok {
		merged.DefaultWorkerMemory = updates.DefaultWorkerMemory
	}
	if _, ok := fields["default_worker_ttl"]; ok {
		merged.DefaultWorkerTTL = updates.DefaultWorkerTTL
	}
	if _, ok := fields["default_worker_min_hot_idle"]; ok {
		merged.DefaultWorkerMinHotIdle = updates.DefaultWorkerMinHotIdle
	}
	if _, ok := fields["hostname_alias"]; ok {
		merged.HostnameAlias = updates.HostnameAlias
	}
	// Set when the PUT actually changes default_team_id (the org's billing
	// team): the store then repoints the org's duckgres_org_teams billing row
	// and re-attributes the buffered (unacked) billing buckets to the new
	// team in the same transaction as the org update, so the next billing pull
	// reports them under the new team (docs/design/billing-pull-api.md,
	// "Changing an org's default team").
	var reattributeUsageTeam *int64
	if _, ok := fields["default_team_id"]; ok {
		// Key present: a positive number sets. Clearing is rejected — every
		// org must keep a billing team (the billing bucket key). An explicit
		// JSON null unmarshals to a nil pointer here; 0 and negatives are
		// already rejected by validateOrgMutationPayload above.
		if updates.DefaultTeamID == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "default_team_id cannot be cleared (every org must keep a billing team); pass a valid team id"})
			return
		}
		merged.DefaultTeamID = updates.DefaultTeamID
		if existing.DefaultTeamID == nil || *existing.DefaultTeamID != *updates.DefaultTeamID {
			reattributeUsageTeam = updates.DefaultTeamID
		}
	}

	// Audit detail: which fields changed and their old → new values, so the
	// console shows "max_workers 4 → 10" instead of a bare "org.update". These
	// are all non-sensitive config columns (no credentials among them).
	var changes []string
	addChange := func(key string, old, next any) {
		if _, ok := fields[key]; ok && old != next {
			changes = append(changes, fmt.Sprintf("%s %v → %v", key, old, next))
		}
	}
	addChange("max_workers", existing.MaxWorkers, merged.MaxWorkers)
	addChange("max_vcpus", existing.MaxVCPUs, merged.MaxVCPUs)
	addChange("default_worker_cpu", orgStr(existing.DefaultWorkerCPU), orgStr(merged.DefaultWorkerCPU))
	addChange("default_worker_memory", orgStr(existing.DefaultWorkerMemory), orgStr(merged.DefaultWorkerMemory))
	addChange("default_worker_ttl", orgStr(existing.DefaultWorkerTTL), orgStr(merged.DefaultWorkerTTL))
	addChange("default_worker_min_hot_idle", existing.DefaultWorkerMinHotIdle, merged.DefaultWorkerMinHotIdle)
	addChange("hostname_alias", orgStrPtr(existing.HostnameAlias), orgStrPtr(merged.HostnameAlias))
	addChange("default_team_id", orgInt64Ptr(existing.DefaultTeamID), orgInt64Ptr(merged.DefaultTeamID))
	if len(changes) > 0 {
		setAuditDetail(c, strings.Join(changes, ", "))
	}

	org, ok, err := h.store.UpdateOrg(name, merged, reattributeUsageTeam)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, org)
}

func (h *apiHandler) deleteOrg(c *gin.Context) {
	name := c.Param("id")
	ok, err := h.store.DeleteOrg(name)
	if err != nil {
		if errors.Is(err, errWarehouseStillExists) {
			c.JSON(http.StatusConflict, gin.H{"error": "warehouse still exists — deprovision it and wait for teardown to complete before deleting the org"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": name})
}

// --- Org teams ---

func (h *apiHandler) listAllOrgTeams(c *gin.Context) {
	teams, err := h.store.ListAllOrgTeams()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"teams": teams})
}

// createOrgTeamRequest is the admin create body (POST /teams — the org rides
// in the body like POST /users, because the /orgs/:id/teams POST route
// belongs to the provisioning grandfather upsert). schema_name set here is
// immutable through user-facing flows; operators can later change it via the
// break-glass PUT, and the internal provisioning grandfather path may
// overwrite it.
type createOrgTeamRequest struct {
	OrgID           string `json:"org_id"`
	TeamID          int64  `json:"team_id"`
	SchemaName      string `json:"schema_name"`
	Enabled         *bool  `json:"enabled,omitempty"`
	BackfillEnabled *bool  `json:"backfill_enabled,omitempty"`
}

func (h *apiHandler) createOrgTeam(c *gin.Context) {
	var req createOrgTeamRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	orgID := req.OrgID
	if orgID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "org_id is required"})
		return
	}
	if req.TeamID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "team_id is required (a positive PostHog team id)"})
		return
	}
	if err := configstore.ValidateOrgTeamSchemaName(req.SchemaName); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	team := configstore.OrgTeam{
		OrgID:           orgID,
		TeamID:          req.TeamID,
		SchemaName:      req.SchemaName,
		Enabled:         req.Enabled == nil || *req.Enabled,
		BackfillEnabled: req.BackfillEnabled,
	}
	// POST /teams has no :id param, so the audit org column is blank — record
	// the target org here (mirrors POST /users).
	setAuditDetail(c, fmt.Sprintf("created team %d (schema %s) in org %s", req.TeamID, req.SchemaName, orgID))
	if err := h.store.CreateOrgTeam(orgID, &team); err != nil {
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		case errors.Is(err, errOrgTeamExists), errors.Is(err, configstore.ErrOrgTeamSchemaConflict):
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}
	c.JSON(http.StatusCreated, team)
}

// updateOrgTeamRequest is the admin PUT body — the operator break-glass that
// can edit every team setting. All fields are presence-aware (absent =
// preserve). schema_name may be CHANGED here (immutability is a user-facing
// flow rule, not an operator one) but never cleared — it stays NOT NULL. The
// nullable columns (backfill_enabled and the three legacy table-name
// overrides) treat an explicit JSON null — and, for the table names, "" — as
// "clear back to NULL". Billing can only be pointed AT a team
// (is_billing_team: true), never cleared.
type updateOrgTeamRequest struct {
	SchemaName            *string `json:"schema_name,omitempty"`
	Enabled               *bool   `json:"enabled,omitempty"`
	BackfillEnabled       *bool   `json:"backfill_enabled,omitempty"`
	IsBillingTeam         *bool   `json:"is_billing_team,omitempty"`
	EventsTableName       *string `json:"events_table_name,omitempty"`
	PersonsTableName      *string `json:"persons_table_name,omitempty"`
	SchemaDataImportsName *string `json:"schema_data_imports_name,omitempty"`
}

// legacyTableNameUpdate folds one presence-aware legacy table-name field of
// the PUT body into (set, value): absent = (false, nil); explicit null or ""
// = (true, nil) — clear back to NULL / derive from schema_name; anything else
// = (true, &value).
func legacyTableNameUpdate(present bool, v *string) (bool, *string) {
	if !present {
		return false, nil
	}
	if v == nil || *v == "" {
		return true, nil
	}
	return true, v
}

func (h *apiHandler) updateOrgTeam(c *gin.Context) {
	orgID := c.Param("id")
	teamID, perr := strconv.ParseInt(c.Param("team_id"), 10, 64)
	if perr != nil || teamID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "team_id must be a positive integer"})
		return
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var req updateOrgTeamRequest
	if err := json.Unmarshal(body, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if _, ok := fields["schema_name"]; ok {
		// The operator override: changing the schema is allowed HERE (and only
		// here — user-facing flows keep it immutable), but it must stay a
		// valid, non-null identifier. It renames NOTHING in the warehouse.
		if req.SchemaName == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "schema_name cannot be cleared; pass a valid schema name"})
			return
		}
		if err := configstore.ValidateOrgTeamSchemaName(*req.SchemaName); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	if _, ok := fields["is_billing_team"]; ok && (req.IsBillingTeam == nil || !*req.IsBillingTeam) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "is_billing_team can only be set to true (repoint billing by marking another team); every org must keep a billing team"})
		return
	}
	for name, v := range map[string]*string{
		"events_table_name":        req.EventsTableName,
		"persons_table_name":       req.PersonsTableName,
		"schema_data_imports_name": req.SchemaDataImportsName,
	} {
		if v == nil {
			continue
		}
		// Shared bare-identifier contract (see configstore.ValidateOrgTeamTableName):
		// overrides are never schema-qualified; a dot stored here would be
		// silently ambiguous to every discovery consumer. "" passes (clear).
		if err := configstore.ValidateOrgTeamTableName(name, *v); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	_, backfillSet := fields["backfill_enabled"]
	_, eventsPresent := fields["events_table_name"]
	_, personsPresent := fields["persons_table_name"]
	_, importsPresent := fields["schema_data_imports_name"]
	upd := orgTeamUpdate{
		SchemaName:  req.SchemaName,
		Enabled:     req.Enabled,
		BackfillSet: backfillSet,
		Backfill:    req.BackfillEnabled,
		MakeBilling: req.IsBillingTeam != nil && *req.IsBillingTeam,
	}
	upd.EventsTableNameSet, upd.EventsTableName = legacyTableNameUpdate(eventsPresent, req.EventsTableName)
	upd.PersonsTableNameSet, upd.PersonsTableName = legacyTableNameUpdate(personsPresent, req.PersonsTableName)
	upd.SchemaDataImportsNameSet, upd.SchemaDataImportsName = legacyTableNameUpdate(importsPresent, req.SchemaDataImportsName)

	prev, team, err := h.store.UpdateOrgTeam(orgID, teamID, upd)
	if err != nil {
		switch {
		case errors.Is(err, configstore.ErrOrgTeamNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": "org team not found"})
		case errors.Is(err, configstore.ErrOrgTeamSchemaConflict):
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	// Audit detail: which fields actually changed and their old → new values
	// (same idiom as the org update). Computed AFTER the store call so the
	// "old" side is the row the update really replaced; AuditMiddleware reads
	// the detail once the handler returns.
	var changes []string
	addChange := func(key, old, next string) {
		if old != next {
			changes = append(changes, fmt.Sprintf("%s %s → %s", key, old, next))
		}
	}
	addChange("schema_name", prev.SchemaName, team.SchemaName)
	addChange("enabled", fmt.Sprintf("%v", prev.Enabled), fmt.Sprintf("%v", team.Enabled))
	addChange("backfill_enabled", orgBoolPtr(prev.BackfillEnabled), orgBoolPtr(team.BackfillEnabled))
	addChange("is_billing_team", orgBoolPtr(prev.IsBillingTeam), orgBoolPtr(team.IsBillingTeam))
	addChange("events_table_name", orgStrPtr(prev.EventsTableName), orgStrPtr(team.EventsTableName))
	addChange("persons_table_name", orgStrPtr(prev.PersonsTableName), orgStrPtr(team.PersonsTableName))
	addChange("schema_data_imports_name", orgStrPtr(prev.SchemaDataImportsName), orgStrPtr(team.SchemaDataImportsName))
	if len(changes) > 0 {
		setAuditDetail(c, fmt.Sprintf("team %d: %s", teamID, strings.Join(changes, ", ")))
	}

	c.JSON(http.StatusOK, team)
}

// topLevelJSONKeys returns the sorted top-level object keys of a JSON body, or
// nil if it isn't a JSON object. Used to audit WHICH warehouse sections a PUT
// touched without recording their (possibly secret) values.
func topLevelJSONKeys(body []byte) []string {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// orgStr renders an org config string value for audit detail, showing "" as a
// readable "(unset)" so a cleared field is unambiguous.
func orgStr(s string) string {
	if s == "" {
		return "(unset)"
	}
	return s
}

// orgStrPtr renders an optional org config string (nil == unset) for audit
// detail.
func orgStrPtr(s *string) string {
	if s == nil {
		return "(unset)"
	}
	return orgStr(*s)
}

// orgBoolPtr renders an optional tri-state boolean (nil == unset) for audit
// detail.
func orgBoolPtr(b *bool) string {
	if b == nil {
		return "(unset)"
	}
	return fmt.Sprintf("%v", *b)
}

// orgInt64Ptr renders an optional org config integer (nil or 0 == unset) for
// audit detail. Every org keeps a billing team nowadays, so "(unset)" only
// ever renders for legacy display of orgs predating that.
func orgInt64Ptr(v *int64) string {
	if v == nil || *v == 0 {
		return "(unset)"
	}
	return fmt.Sprintf("%d", *v)
}

func validateOrgMutationPayload(org *configstore.Org) error {
	if org == nil {
		return nil
	}
	if org.Warehouse != nil {
		return errWarehousePayloadNotAllowed
	}
	if len(org.Teams) > 0 {
		return errOrgTeamsPayloadNotAllowed
	}
	if org.HostnameAlias != nil {
		if err := validateHostnameAlias(*org.HostnameAlias); err != nil {
			return err
		}
	}
	if err := validateOrgDefaultWorkerProfile(org); err != nil {
		return err
	}
	if org.DefaultWorkerMinHotIdle < 0 {
		return fmt.Errorf("default_worker_min_hot_idle: value %d must be >= 0", org.DefaultWorkerMinHotIdle)
	}
	if org.MaxVCPUs < 0 {
		return fmt.Errorf("max_vcpus: value %d must be >= 0", org.MaxVCPUs)
	}
	// Shared by create and update: nil stays allowed here (create requires
	// the field itself; update treats an absent field as "preserve"), but a
	// present value must be a positive PostHog team id — a billing team row
	// is never stored with team 0, so there is no clear sentinel.
	if org.DefaultTeamID != nil && *org.DefaultTeamID <= 0 {
		return fmt.Errorf("default_team_id: value %d must be a positive PostHog team id (it cannot be cleared — every org must keep a billing team)", *org.DefaultTeamID)
	}
	return nil
}

// validateOrgDefaultWorkerProfile rejects garbage default-worker-profile
// values at the API boundary so they can never enter the config store (the
// control plane tolerates bad rows by ignoring them, but a 400 here surfaces
// the typo to the operator instead of a silently-ignored default). Empty
// strings are allowed: they mean "unset" on create / "clear" on update.
func validateOrgDefaultWorkerProfile(org *configstore.Org) error {
	for _, f := range []struct{ name, raw string }{
		{"default_worker_cpu", org.DefaultWorkerCPU},
		{"default_worker_memory", org.DefaultWorkerMemory},
	} {
		if f.raw == "" {
			continue
		}
		q, err := resource.ParseQuantity(f.raw)
		if err != nil {
			return fmt.Errorf("%s: invalid quantity %q", f.name, f.raw)
		}
		if q.Sign() <= 0 {
			return fmt.Errorf("%s: quantity %q must be positive", f.name, f.raw)
		}
	}
	if raw := org.DefaultWorkerTTL; raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("default_worker_ttl: invalid duration %q (use a Go duration like \"30m\")", raw)
		}
		if d < 0 {
			return fmt.Errorf("default_worker_ttl: duration %q must be >= 0", raw)
		}
	}
	return nil
}

// validateHostnameAlias enforces that an alias is a valid single DNS label
// (RFC 1035): alphanumeric + hyphens, no leading/trailing hyphen, 1–63
// characters. The empty string is allowed (means "clear" on update / "no
// alias" on create — see handler-level normalization). Aliases that violate
// this would silently fail SNI matching (`sni_kubernetes.go:23` rejects
// multi-label prefixes), so the validation lives at admission time to surface
// the typo as a 400 instead of a mysteriously unreachable tenant.
func validateHostnameAlias(alias string) error {
	if alias == "" {
		return nil
	}
	if len(alias) > 63 {
		return errors.New("hostname_alias must be at most 63 characters (DNS label limit)")
	}
	for i, r := range alias {
		isAlnum := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		isHyphen := r == '-'
		if !isAlnum && !isHyphen {
			return fmt.Errorf("hostname_alias contains invalid character %q (allowed: A-Z, a-z, 0-9, hyphen)", r)
		}
		if isHyphen && (i == 0 || i == len(alias)-1) {
			return errors.New("hostname_alias must not start or end with a hyphen")
		}
	}
	return nil
}

func (h *apiHandler) getManagedWarehouse(c *gin.Context) {
	warehouse, err := h.store.GetManagedWarehouse(c.Param("id"))
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "managed warehouse not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, warehouse)
}

// warehouseBadRequestError marks an error from the mutate closure as caused by
// bad caller input rather than a store-level failure. The handler maps it to
// 400, not 500.
type warehouseBadRequestError struct{ err error }

func (e warehouseBadRequestError) Error() string { return e.err.Error() }
func (e warehouseBadRequestError) Unwrap() error { return e.err }

func (h *apiHandler) putManagedWarehouse(c *gin.Context) {
	orgID := c.Param("id")

	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxWarehousePutBodyBytes))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Strict decode rejects unknown top-level fields and malformed JSON. We
	// don't use the decoded value directly; it just gates which keys the body
	// is allowed to carry.
	var req managedWarehouseRequest
	if err := decodeStrictWarehouseRequest(body, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Audit detail: the top-level warehouse sections touched by this PUT (field
	// NAMES only — the values can carry credentials/secret refs, so we never put
	// them in the audit log). Gives "changed: metadata_store, s3" instead of a
	// bare "warehouse.update".
	if changed := topLevelJSONKeys(body); len(changed) > 0 {
		setAuditDetail(c, "changed: "+strings.Join(changed, ", "))
	}

	// MutateManagedWarehouse locks the row inside a transaction, runs the
	// closure, and commits — closing the race where two concurrent PUTs would
	// otherwise Get + modify different snapshots and silently clobber each
	// other. The closure does the merge and validation on the locked row.
	stored, ok, err := h.store.MutateManagedWarehouse(orgID, func(w *configstore.ManagedWarehouse) error {
		// json.Unmarshal only overwrites fields whose keys appear in the body
		// — top-level AND nested. Callers can PATCH one inner field (e.g.
		// `{"metadata_store":{"database_name":"x"}}`) without wiping siblings.
		if err := json.Unmarshal(body, w); err != nil {
			return warehouseBadRequestError{err}
		}
		if w.DucklingName == "" {
			return warehouseBadRequestError{errors.New("duckling_name cannot be empty")}
		}
		cfgView := &configstore.ManagedWarehouseConfig{
			OrgID:                        orgID,
			WarehouseDatabase:            w.WarehouseDatabase,
			MetadataStore:                w.MetadataStore,
			S3:                           w.S3,
			WorkerIdentity:               w.WorkerIdentity,
			WarehouseDatabaseCredentials: w.WarehouseDatabaseCredentials,
			MetadataStoreCredentials:     w.MetadataStoreCredentials,
			S3Credentials:                w.S3Credentials,
			RuntimeConfig:                w.RuntimeConfig,
		}
		if err := configstore.ValidateManagedWarehouseSecretRefs(orgID, "", cfgView); err != nil {
			return warehouseBadRequestError{err}
		}
		return nil
	})
	if err != nil {
		var badReq warehouseBadRequestError
		if errors.As(err, &badReq) {
			c.JSON(http.StatusBadRequest, gin.H{"error": badReq.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, stored)
}

// tenantPinningRequest carries the two columns that determine which worker
// image and DuckLake spec version a tenant pins to. Both fields are
// optional — omitting one preserves the stored value, mirroring the partial
// patch semantics of putManagedWarehouse.
//
// We use *string instead of string so the JSON decoder can distinguish
// "field absent" (preserve) from "field present and empty" (clear). Clearing
// the image falls back to the global default; clearing ducklake_version
// likewise falls back to the global default. We don't allow both to be nil
// after the patch — that's a no-op which is almost certainly a caller bug.
type tenantPinningRequest struct {
	Image           *string `json:"image,omitempty"`
	DuckLakeVersion *string `json:"ducklake_version,omitempty"`
}

const maxPinningPatchBodyBytes = 4 << 10 // 4 KiB; the body is two strings

// patchTenantPinning sets the image and/or ducklake_version columns on a
// tenant's managed_warehouses row without touching the rest of the
// warehouse config. This replaces the operational shape of running
// `UPDATE duckgres_managed_warehouses SET image=..., ducklake_version=...`
// against the config store directly — which is fine for one-offs but
// creates audit + concurrency risks at any scale.
//
// Returns 200 with the updated warehouse on success, 400 for malformed
// requests (unknown fields, both fields absent, invalid version format),
// 404 if the org has no managed warehouse row, and 500 on store errors.
func (h *apiHandler) patchTenantPinning(c *gin.Context) {
	orgID := c.Param("id")

	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxPinningPatchBodyBytes))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var req tenantPinningRequest
	if err := dec.Decode(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Image == nil && req.DuckLakeVersion == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least one of image or ducklake_version must be set"})
		return
	}
	if req.DuckLakeVersion != nil && *req.DuckLakeVersion != "" {
		if !isValidDuckLakeSpecVersion(*req.DuckLakeVersion) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "ducklake_version must be a major.minor string like \"0.4\" or \"1.0\""})
			return
		}
	}

	// Audit detail: image / ducklake_version are safe (non-secret) pins.
	var pins []string
	if req.Image != nil {
		pins = append(pins, "image="+orgStr(*req.Image))
	}
	if req.DuckLakeVersion != nil {
		pins = append(pins, "ducklake_version="+orgStr(*req.DuckLakeVersion))
	}
	setAuditDetail(c, strings.Join(pins, ", "))

	stored, ok, err := h.store.MutateManagedWarehouse(orgID, func(w *configstore.ManagedWarehouse) error {
		if req.Image != nil {
			w.Image = *req.Image
		}
		if req.DuckLakeVersion != nil {
			w.DuckLakeVersion = *req.DuckLakeVersion
		}
		return nil
	})
	if err != nil {
		var badReq warehouseBadRequestError
		if errors.As(err, &badReq) {
			c.JSON(http.StatusBadRequest, gin.H{"error": badReq.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "org not found"})
		return
	}
	c.JSON(http.StatusOK, stored)
}

// isValidDuckLakeSpecVersion accepts the same major.minor format that
// server/ducklake.versionLessThan parses (e.g., "0.4", "1.0", "0.10"). We
// don't import server/ducklake here to avoid a cross-cutting dependency
// from the admin API onto a server subpackage; the check is just a shape
// gate to catch typos before the value gets persisted.
func isValidDuckLakeSpecVersion(v string) bool {
	dot := -1
	for i := 0; i < len(v); i++ {
		if v[i] == '.' {
			if dot >= 0 {
				return false // multiple dots
			}
			dot = i
			continue
		}
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	return dot > 0 && dot < len(v)-1
}

// --- Users ---

func (h *apiHandler) listUsers(c *gin.Context) {
	users, err := h.store.ListUsers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, users)
}

func (h *apiHandler) createUser(c *gin.Context) {
	// Use a raw struct because OrgUser.Password has json:"-"
	var raw struct {
		Username    string `json:"username"`
		Password    string `json:"password"`
		OrgID       string `json:"org_id"`
		Passthrough bool   `json:"passthrough"`
		MaxVCPUs    int    `json:"max_vcpus"`
	}
	if err := c.ShouldBindJSON(&raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if raw.Username == "" || raw.OrgID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "username and org_id are required"})
		return
	}
	if raw.Password == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "password is required"})
		return
	}
	if raw.MaxVCPUs < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "max_vcpus must be >= 0"})
		return
	}
	hash, err := configstore.HashPassword(raw.Password)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
		return
	}
	user := configstore.OrgUser{
		Username:    raw.Username,
		Password:    hash,
		OrgID:       raw.OrgID,
		Passthrough: raw.Passthrough,
		MaxVCPUs:    raw.MaxVCPUs,
	}
	// The audit row's org/target columns come from URL params, but this route is
	// the top-level POST /users with no params — record who was created here.
	setAuditDetail(c, "created user "+raw.Username+" in org "+raw.OrgID)
	if err := h.store.CreateUser(&user); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if err := h.notifyPeersOfChange(c); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, user)
}

func (h *apiHandler) getUser(c *gin.Context) {
	orgID := c.Param("id")
	username := c.Param("username")
	user, err := h.store.GetUser(orgID, username)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	c.JSON(http.StatusOK, user)
}

func (h *apiHandler) updateUser(c *gin.Context) {
	orgID := c.Param("id")
	username := c.Param("username")
	// Passthrough is *bool so omitting it preserves the stored value; sending
	// `false` explicitly clears the flag.
	var raw struct {
		Password    string `json:"password"`
		Passthrough *bool  `json:"passthrough,omitempty"`
		MaxVCPUs    *int   `json:"max_vcpus,omitempty"`
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	maxVCPUs := raw.MaxVCPUs
	if rawMaxVCPUs, ok := fields["max_vcpus"]; ok && bytes.Equal(bytes.TrimSpace(rawMaxVCPUs), []byte("null")) {
		zero := 0
		maxVCPUs = &zero
	}
	passwordHash := ""
	if raw.Password != "" {
		hash, err := configstore.HashPassword(raw.Password)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to hash password"})
			return
		}
		passwordHash = hash
	}
	if maxVCPUs != nil && *maxVCPUs < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "max_vcpus must be >= 0"})
		return
	}
	// Audit detail: which fields the update touched. The password is NEVER
	// logged — only that it was reset.
	var changes []string
	if _, ok := fields["password"]; ok && raw.Password != "" {
		changes = append(changes, "password reset")
	}
	if raw.Passthrough != nil {
		changes = append(changes, fmt.Sprintf("passthrough=%v", *raw.Passthrough))
	}
	if maxVCPUs != nil {
		changes = append(changes, fmt.Sprintf("max_vcpus=%d", *maxVCPUs))
	}
	if len(changes) > 0 {
		setAuditDetail(c, strings.Join(changes, ", "))
	}

	user, ok, err := h.store.UpdateUser(orgID, username, passwordHash, raw.Passthrough, maxVCPUs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	if err := h.notifyPeersOfChange(c); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, user)
}

func (h *apiHandler) deleteUser(c *gin.Context) {
	orgID := c.Param("id")
	username := c.Param("username")
	ok, err := h.store.DeleteUser(orgID, username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "user not found"})
		return
	}
	if err := h.notifyPeersOfChange(c); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": username})
}

// upsertProjectReader creates or rotates the single read-only credential for
// one PostHog project. The plaintext password is returned only in this
// response; the config store persists only its bcrypt hash.
func (h *apiHandler) upsertProjectReader(c *gin.Context) {
	orgID := c.Param("id")
	teamID, err := strconv.ParseInt(c.Param("team_id"), 10, 64)
	if err != nil || teamID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "team_id must be a positive integer"})
		return
	}
	var request struct {
		Password string `json:"password"`
	}
	if c.Request.ContentLength > 0 {
		decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	password := request.Password
	if password == "" {
		password, err = configstore.GeneratePassword()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate password"})
			return
		}
	} else if len(password) < 32 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "password must be at least 32 characters"})
		return
	}
	username := fmt.Sprintf("posthog_team_%d", teamID)
	user, err := h.store.UpsertProjectReader(orgID, teamID, username, password)
	if err != nil {
		if errors.Is(err, configstore.ErrProjectReaderTeamUnavailable) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	setAuditDetail(c, fmt.Sprintf("created or rotated project reader for team %d", teamID))
	if err := h.notifyPeersOfChange(c); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"username": user.Username,
		"password": password,
	})
}

// notifyPeersOfChange reloads THIS replica's config snapshot immediately
// (bypassing the poll interval, default 30s) and fans the same reload out to
// every other CP replica, mirroring the disable/enable reload pattern in
// live.go. Unlike disable/enable, the write has already landed in the shared
// config-store DB by the time this runs, so a peer only needs to reload — it
// must never re-run the create/update/delete (that would 409 or double-apply
// against a row that's already there). Peer fan-out is best-effort: PostPeers
// already drops a slow/down peer without error, so only a failure to reload
// THIS replica is surfaced to the caller.
func (h *apiHandler) notifyPeersOfChange(c *gin.Context) error {
	if err := h.store.ReloadSnapshot(); err != nil {
		return err
	}
	if h.fetcher != nil {
		h.fetcher.PostPeers(c.Request.Context(), "/api/v1/internal/reload-snapshot")
	}
	return nil
}

// reloadSnapshot is a peer-fan-out target only (see notifyPeersOfChange): it
// forces this replica's config snapshot to reload immediately. There is
// nothing to re-execute here — the write already landed in the shared
// config-store DB — so unlike the per-user kill-switch actions this handler
// has no scope=local branch to guard: it never calls PostPeers itself, so it
// cannot recurse regardless of who calls it.
func (h *apiHandler) reloadSnapshot(c *gin.Context) {
	if err := h.store.ReloadSnapshot(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"reloaded": true})
}

// --- Workers ---

func (h *apiHandler) listWorkers(c *gin.Context) {
	workers := []WorkerStatus{}
	if h.info != nil {
		workers = h.info.AllWorkerStatuses()
	}
	// A worker is owned by exactly one CP (disjoint union); dedup makes it idempotent.
	if !localScope(c) && h.fetcher != nil {
		bodies, _ := h.fetcher.FetchPeers(c.Request.Context(), "/api/v1/workers")
		mergePeer(&workers, bodies, func(e []WorkerStatus) []WorkerStatus { return e })
		workers = dedupeBy(workers, func(w WorkerStatus) int { return w.ID })
	}
	c.JSON(http.StatusOK, workers)
}

// --- Sessions ---

func (h *apiHandler) listSessions(c *gin.Context) {
	sessions := []SessionStatus{}
	if h.info != nil {
		sessions = h.info.AllSessionStatuses()
	}
	// A session lives on exactly one CP (disjoint union); dedup makes it idempotent.
	if !localScope(c) && h.fetcher != nil {
		bodies, _ := h.fetcher.FetchPeers(c.Request.Context(), "/api/v1/sessions")
		mergePeer(&sessions, bodies, func(e []SessionStatus) []SessionStatus { return e })
		sessions = dedupeBy(sessions, func(s SessionStatus) int { return s.WorkerID })
	}
	c.JSON(http.StatusOK, sessions)
}

// --- Status ---

func (h *apiHandler) getClusterStatus(c *gin.Context) {
	orgStats := []OrgStatus{}
	if h.info != nil {
		orgStats = h.info.AllOrgStats()
	}
	// Per-org active-session counts are per-CP; merge every CP's slice so the
	// Overview cards reflect the whole cluster instead of one replica's view.
	if !localScope(c) && h.fetcher != nil {
		bodies, _ := h.fetcher.FetchPeers(c.Request.Context(), "/api/v1/status")
		orgStats = mergeOrgStats(orgStats, bodies)
	}

	totalWorkers := 0
	totalSessions := 0
	for _, os := range orgStats {
		totalWorkers += os.Workers
		totalSessions += os.ActiveSessions
	}

	c.JSON(http.StatusOK, ClusterStatus{
		TotalOrgs:     len(orgStats),
		TotalWorkers:  totalWorkers,
		TotalSessions: totalSessions,
		Orgs:          orgStats,
	})
}
