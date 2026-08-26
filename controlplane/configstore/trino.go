package configstore

import (
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Per-org Trino opt-in reads/writes. The cluster-level bootstrap sentinel
// lives next door in trino_cluster_secrets.go; the projection logic that
// consumes all of this is controlplane/provisioner/trino_provisioner.go.

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
//
// The cell is deliberately NOT set here. The enable surfaces are HTTP
// handlers with no knowledge of the Trino fleet; the reconciling
// provisioner claims unassigned rows into its own cell (AssignTrinoCell).
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
	// State / StatusMessage / ReadyAt / FailedAt / TrinoCellID — those are
	// owned by the reconcile loop. A re-enable on an already-Ready row stays
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

// AssignTrinoCell claims an UNASSIGNED Trino-enabled org into a cell. The
// WHERE clause is the whole point: it only ever writes a row whose
// trino_cell_id is empty/NULL, so a second cell's provisioner can never
// steal an org that this cell already owns (which would leave two
// coordinators projecting the same tenant's credentials and catalog).
// Moving an org between cells is deliberately NOT expressible here — it
// needs a drain of the source cell's catalog first and is out of scope.
//
// RowsAffected==0 is not an error: it means the row was claimed by someone
// else (or disabled) between the list and the write. The next tick re-reads
// and skips the org because its cell no longer matches.
func (cs *ConfigStore) AssignTrinoCell(orgID, cellID string) error {
	if orgID == "" {
		return errors.New("AssignTrinoCell: orgID is required")
	}
	if cellID == "" {
		return errors.New("AssignTrinoCell: cellID is required")
	}
	result := cs.db.Model(&ManagedWarehouseTrino{}).
		Where("org_id = ? AND enabled = ? AND (trino_cell_id IS NULL OR trino_cell_id = ?)", orgID, true, "").
		Updates(map[string]interface{}{
			"trino_cell_id": cellID,
			"updated_at":    time.Now().UTC(),
		})
	if result.Error != nil {
		return fmt.Errorf("assign trino cell for %q: %w", orgID, result.Error)
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
// previous enabled lifecycle. trino_cell_id is deliberately PRESERVED:
// the cell that owns the org is the one that still has to tear its
// catalog and Secret keys down, and it finds the org by not-in-wanted-set
// rather than by row. The next EnableTrino lands back on the same cell.
// The reconcile loop will not advance state for disabled orgs
// (UpdateTrinoState predicates on enabled), so state stays at Pending
// until either:
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
//
// Orgs with no duckgres_orgs row, or a blank database_name, are skipped for
// the same reason: database_name is the org's Trino principal (see
// TrinoEnabledOrg.TrinoPrincipal), so without it there is no username to put
// in password.db and no stem to build a catalog name from. Skipping leaves
// the org Pending rather than projecting a catalog called `org_`.
//
// Every cell's rows are returned, unassigned ones included; the caller
// filters by cell (see TrinoEnabledOrg.CellID for why the filter is not in
// the SQL).
func (cs *ConfigStore) ListTrinoEnabledOrgs() ([]TrinoEnabledOrg, error) {
	var out []TrinoEnabledOrg
	// Inner join with duckgres_org_users on (org_id, username='root') so a
	// missing OrgUser row drops the org from the result. Inner join with
	// duckgres_orgs for database_name, which is the org's Trino principal —
	// a missing or blank one drops the org for the same reason.
	err := cs.db.Table("duckgres_managed_warehouse_trino AS t").
		Select(`t.org_id AS org_id,
		         o.database_name AS database_name,
		         t.tier AS tier,
		         COALESCE(t.trino_cell_id, '') AS cell_id,
		         u.password AS root_password_hash,
		         t.state AS state`).
		Joins(`INNER JOIN duckgres_org_users AS u
		        ON u.org_id = t.org_id AND u.username = 'root'`).
		Joins(`INNER JOIN duckgres_orgs AS o ON o.name = t.org_id`).
		Where("t.enabled = ?", true).
		Where("o.database_name <> ''").
		Order("t.org_id ASC").
		Scan(&out).Error
	if err != nil {
		return nil, fmt.Errorf("list trino-enabled orgs: %w", err)
	}
	return out, nil
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

// GetManagedWarehouseForTrino reads the org's warehouse row for the Trino
// catalog builder. Returns (nil, nil) when the org has no warehouse row, so
// the caller can distinguish "opted into Trino before the warehouse was
// provisioned" (wait, and try again next tick) from a real DB error (fail
// the org). Deliberately distinct from GetManagedWarehouse, which surfaces
// gorm.ErrRecordNotFound verbatim to callers that want it.
func (cs *ConfigStore) GetManagedWarehouseForTrino(orgID string) (*ManagedWarehouse, error) {
	var warehouse ManagedWarehouse
	err := cs.db.First(&warehouse, "org_id = ?", orgID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("get warehouse for %q: %w", orgID, err)
	}
	return &warehouse, nil
}
