package configstore

import (
	"errors"
	"fmt"
	"regexp"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrOrgTeamNotFound is returned by DeleteOrgTeamTx when the (org, team) row
// does not exist. HTTP handlers map it to 404.
var ErrOrgTeamNotFound = errors.New("org team not found")

// ErrLastOrgTeam is returned by DeleteOrgTeamTx when the row is the org's LAST
// team. An org must always keep at least one team (the billing team is the
// billing bucket key); the only way to remove the last team is deleting the
// org itself. HTTP handlers map it to 409.
var ErrLastOrgTeam = errors.New("cannot delete the org's last team; deleting the org is the only way to remove it")

// ErrOrgTeamSchemaConflict is returned by UpsertOrgTeamTx when the requested
// schema_name is already used by a DIFFERENT team in the same org (two teams
// must never share a schema — their warehouse tables would interleave).
// HTTP handlers map it to 409.
var ErrOrgTeamSchemaConflict = errors.New("schema_name is already used by another team in this org")

// orgTeamSchemaNamePattern constrains team schema names to safe lowercase SQL
// identifiers, mirroring the DNS-label strictness of provisioning org IDs:
// these names are interpolated into warehouse DDL and object paths, so no
// quoting-dependent characters are allowed.
var orgTeamSchemaNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// maxOrgTeamSchemaNameLength caps schema names at the Postgres identifier
// limit (NAMEDATALEN-1). The derived "<schema_name>_data_imports" schema must
// also fit, but grandfathered pre-existing names take priority over reserving
// suffix headroom — the derived-name path only applies to new conventional
// names ("team_<id>"), which are far below the cap.
const maxOrgTeamSchemaNameLength = 63

// ValidateOrgTeamSchemaName rejects team schema names that are not safe
// lowercase identifiers ([a-z0-9_], not starting with a digit, ≤63 chars).
func ValidateOrgTeamSchemaName(name string) error {
	if name == "" {
		return errors.New("schema_name is required")
	}
	if len(name) > maxOrgTeamSchemaNameLength {
		return fmt.Errorf("schema_name must be at most %d characters", maxOrgTeamSchemaNameLength)
	}
	if !orgTeamSchemaNamePattern.MatchString(name) {
		return errors.New("schema_name must be a lowercase identifier: [a-z0-9_], not starting with a digit")
	}
	return nil
}

// ValidateOrgTeamTableName rejects legacy table/schema override names that
// are not safe bare identifiers. The override contract (pinned at the
// discovery derivation site, provisioning/discovery.go resolveTeamTables):
// events/persons overrides are BARE TABLE NAMES within the team's schema,
// and the data-imports override is a bare SCHEMA name — never
// schema-qualified. A dot in a stored name would be silently ambiguous to
// every consumer of the resolved locations, so reject at write time on
// every surface.
func ValidateOrgTeamTableName(field, name string) error {
	if name == "" {
		return nil // empty = clear back to derive-from-schema
	}
	if len(name) > maxOrgTeamSchemaNameLength {
		return fmt.Errorf("%s must be at most %d characters", field, maxOrgTeamSchemaNameLength)
	}
	if !orgTeamSchemaNamePattern.MatchString(name) {
		return fmt.Errorf("%s must be a bare lowercase identifier: [a-z0-9_], not starting with a digit, no schema qualification", field)
	}
	return nil
}

// validateOrgTeamTableNames applies ValidateOrgTeamTableName to every legacy
// override present in the upsert.
func validateOrgTeamTableNames(up OrgTeamUpsert) error {
	for _, f := range []struct {
		field string
		value *string
	}{
		{"events_table_name", up.EventsTableName},
		{"persons_table_name", up.PersonsTableName},
		{"schema_data_imports_name", up.SchemaDataImportsName},
	} {
		if f.value == nil {
			continue
		}
		if err := ValidateOrgTeamTableName(f.field, *f.value); err != nil {
			return err
		}
	}
	return nil
}

// OrgBillingTeamIDTx returns the org's current billing team id (the
// duckgres_org_teams row with is_billing_team = TRUE) inside the caller's
// transaction, or 0 when the org has none.
func OrgBillingTeamIDTx(tx *gorm.DB, orgID string) (int64, error) {
	var ids []int64
	if err := tx.Model(&OrgTeam{}).
		Where("org_id = ? AND is_billing_team IS TRUE", orgID).
		Pluck("team_id", &ids).Error; err != nil {
		return 0, fmt.Errorf("read billing team (org=%s): %w", orgID, err)
	}
	if len(ids) == 0 {
		return 0, nil
	}
	return ids[0], nil
}

// SetOrgBillingTeamTx points the org's billing team at teamID inside the
// caller's transaction. Any other row currently marked billing is demoted
// first — the partial unique index (at most one billing row per org, migration
// 000024) is checked immediately, not deferred, so the order matters. When the
// org doesn't have teamID yet the row is inserted with the conventional
// "team_<id>" schema and enabled = TRUE; an existing row keeps its
// schema/enabled/backfill state and only gains the billing mark.
//
// Callers that change an EXISTING org's billing team must re-attribute its
// buffered usage buckets in the same transaction (ReattributeUsageTeamTx) so
// the billing pull never strands usage under the stale team.
func SetOrgBillingTeamTx(tx *gorm.DB, orgID string, teamID int64) error {
	if err := tx.Exec(`
		UPDATE duckgres_org_teams
		SET is_billing_team = NULL, updated_at = now()
		WHERE org_id = ? AND team_id <> ? AND is_billing_team IS TRUE`,
		orgID, teamID).Error; err != nil {
		return fmt.Errorf("demote billing team (org=%s): %w", orgID, err)
	}
	if err := tx.Exec(`
		INSERT INTO duckgres_org_teams (org_id, team_id, schema_name, enabled, is_billing_team, created_at, updated_at)
		VALUES (?, ?, ?, TRUE, TRUE, now(), now())
		ON CONFLICT (org_id, team_id)
		DO UPDATE SET is_billing_team = TRUE, updated_at = now()`,
		orgID, teamID, fmt.Sprintf("team_%d", teamID)).Error; err != nil {
		return fmt.Errorf("set billing team (org=%s team=%d): %w", orgID, teamID, err)
	}
	return nil
}

// OrgTeamUpsert is the provisioning-API input for creating or overwriting one
// duckgres_org_teams row. Pointer fields are presence-aware: nil preserves the
// stored value on an existing row (and takes the documented default on
// insert); a non-nil pointer sets the field, with the empty string clearing a
// legacy table-name override back to NULL ("derive from schema_name").
type OrgTeamUpsert struct {
	TeamID     int64
	SchemaName string
	// Enabled: nil = TRUE on insert / preserve on update.
	Enabled *bool
	// BackfillEnabled: nil = NULL on insert / preserve on update.
	BackfillEnabled *bool
	// Legacy explicit table names for grandfathered pre-existing teams
	// (NULL = derive from schema_name). nil = preserve; "" = clear to NULL.
	EventsTableName       *string
	PersonsTableName      *string
	SchemaDataImportsName *string
}

// UpsertOrgTeamTx creates or overwrites the (org, team) row inside the
// caller's transaction and returns the stored row.
//
// This is deliberately a full upsert, NOT an immutable-create: it is the
// PostHog-side grandfather path. The migration seeds every existing team with
// the conventional "team_<id>" schema placeholder, and the PostHog backfill
// then replaces it (and sets the legacy explicit table names) through this
// call — so schema_name immutability CANNOT be enforced here. It IS enforced
// on the user-facing surfaces (the admin API update rejects schema changes).
//
// Returns gorm.ErrRecordNotFound when the org doesn't exist and
// ErrOrgTeamSchemaConflict when schema_name is already used by a different
// team in the same org (backed by the unique (org_id, schema_name) index,
// migration 000025 — a concurrent insert that slips past the pre-check still
// fails with a 23505 on that index).
func UpsertOrgTeamTx(tx *gorm.DB, orgID string, up OrgTeamUpsert) (*OrgTeam, error) {
	if err := validateOrgTeamTableNames(up); err != nil {
		return nil, err
	}
	if err := ValidateOrgTeamSchemaName(up.SchemaName); err != nil {
		return nil, err
	}

	var orgCount int64
	if err := tx.Model(&Org{}).Where("name = ?", orgID).Count(&orgCount).Error; err != nil {
		return nil, fmt.Errorf("check org (org=%s): %w", orgID, err)
	}
	if orgCount == 0 {
		return nil, gorm.ErrRecordNotFound
	}

	var schemaClash int64
	if err := tx.Model(&OrgTeam{}).
		Where("org_id = ? AND schema_name = ? AND team_id <> ?", orgID, up.SchemaName, up.TeamID).
		Count(&schemaClash).Error; err != nil {
		return nil, fmt.Errorf("check schema conflict (org=%s schema=%s): %w", orgID, up.SchemaName, err)
	}
	if schemaClash > 0 {
		return nil, ErrOrgTeamSchemaConflict
	}

	// Lock the existing row (if any) so the presence-aware merge below can't
	// interleave with a concurrent upsert of the same team.
	var existing OrgTeam
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		First(&existing, "org_id = ? AND team_id = ?", orgID, up.TeamID).Error
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		row := OrgTeam{
			OrgID:           orgID,
			TeamID:          up.TeamID,
			SchemaName:      up.SchemaName,
			Enabled:         up.Enabled == nil || *up.Enabled,
			BackfillEnabled: up.BackfillEnabled,
		}
		applyOrgTeamTableNames(&row, up)
		if err := tx.Create(&row).Error; err != nil {
			return nil, fmt.Errorf("create org team (org=%s team=%d): %w", orgID, up.TeamID, err)
		}
		// Enabled carries gorm's `default:true` tag, and gorm omits
		// zero-valued default-tagged fields from the INSERT — so a create
		// with enabled=false silently stores (and serves, and ingests)
		// TRUE, with row.Enabled lying to the caller. Pinned by
		// TestCreateOrgTeamDisabledPostgres; force the column explicitly.
		if up.Enabled != nil && !*up.Enabled {
			if err := tx.Model(&OrgTeam{}).
				Where("org_id = ? AND team_id = ?", orgID, up.TeamID).
				Update("enabled", false).Error; err != nil {
				return nil, fmt.Errorf("persist enabled=false (org=%s team=%d): %w", orgID, up.TeamID, err)
			}
			row.Enabled = false
		}
		return &row, nil
	case err != nil:
		return nil, fmt.Errorf("read org team (org=%s team=%d): %w", orgID, up.TeamID, err)
	}

	existing.SchemaName = up.SchemaName
	if up.Enabled != nil {
		existing.Enabled = *up.Enabled
	}
	if up.BackfillEnabled != nil {
		existing.BackfillEnabled = up.BackfillEnabled
	}
	applyOrgTeamTableNames(&existing, up)
	// Save with explicit column selection: gorm's Save would skip NULLing the
	// pointer fields via zero-value pruning on composite-PK updates.
	if err := tx.Model(&OrgTeam{}).
		Where("org_id = ? AND team_id = ?", orgID, up.TeamID).
		Select("schema_name", "enabled", "backfill_enabled",
			"events_table_name", "persons_table_name", "schema_data_imports_name", "updated_at").
		Updates(map[string]interface{}{
			"schema_name":              existing.SchemaName,
			"enabled":                  existing.Enabled,
			"backfill_enabled":         existing.BackfillEnabled,
			"events_table_name":        existing.EventsTableName,
			"persons_table_name":       existing.PersonsTableName,
			"schema_data_imports_name": existing.SchemaDataImportsName,
			"updated_at":               gorm.Expr("now()"),
		}).Error; err != nil {
		return nil, fmt.Errorf("update org team (org=%s team=%d): %w", orgID, up.TeamID, err)
	}
	var stored OrgTeam
	if err := tx.First(&stored, "org_id = ? AND team_id = ?", orgID, up.TeamID).Error; err != nil {
		return nil, fmt.Errorf("reload org team (org=%s team=%d): %w", orgID, up.TeamID, err)
	}
	return &stored, nil
}

// applyOrgTeamTableNames folds the presence-aware legacy table-name fields of
// an upsert into row: nil preserves, "" clears to NULL, a value sets.
func applyOrgTeamTableNames(row *OrgTeam, up OrgTeamUpsert) {
	for _, f := range []struct {
		src *string
		dst **string
	}{
		{up.EventsTableName, &row.EventsTableName},
		{up.PersonsTableName, &row.PersonsTableName},
		{up.SchemaDataImportsName, &row.SchemaDataImportsName},
	} {
		if f.src == nil {
			continue
		}
		if *f.src == "" {
			*f.dst = nil
			continue
		}
		v := *f.src
		*f.dst = &v
	}
}

// OrgTeamDeleteResult reports what DeleteOrgTeamTx did beyond the delete
// itself, so callers can surface the billing handover.
type OrgTeamDeleteResult struct {
	// WasBilling is true when the deleted row carried the billing mark.
	WasBilling bool
	// NewBillingTeamID is the remaining team that automatically became the
	// billing team (the one with the OLDEST created_at). 0 when the deleted
	// row was not the billing team.
	NewBillingTeamID int64
	// UsageRowsMoved counts the buffered usage-bucket rows re-attributed to
	// the new billing team.
	UsageRowsMoved int64
}

// DeleteOrgTeamTx deletes the (org, team) CONFIG row inside the caller's
// transaction. It never touches warehouse data — the team's schema and tables
// stay untouched; only the mapping goes away.
//
// Rules, atomic with the delete:
//   - The org's LAST team cannot be deleted (ErrLastOrgTeam) — deleting the
//     org is the only way to remove it. Every org must keep a billing team.
//   - Deleting the billing team automatically promotes the remaining team
//     with the OLDEST created_at (ties broken by team_id) and re-attributes
//     the org's buffered usage buckets to it (ReattributeUsageTeamTx), the
//     same handover a billing repoint does.
//
// All of the org's team rows are locked up front so two concurrent deletes
// can't each see the other's row as "remaining" and empty the org.
func DeleteOrgTeamTx(tx *gorm.DB, orgID string, teamID int64) (*OrgTeamDeleteResult, error) {
	var teams []OrgTeam
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("org_id = ?", orgID).
		Order("created_at, team_id").
		Find(&teams).Error; err != nil {
		return nil, fmt.Errorf("lock org teams (org=%s): %w", orgID, err)
	}

	var target *OrgTeam
	var successor *OrgTeam
	for i := range teams {
		if teams[i].TeamID == teamID {
			target = &teams[i]
		} else if successor == nil {
			// First non-target in (created_at, team_id) order = the oldest
			// remaining team.
			successor = &teams[i]
		}
	}
	if target == nil {
		return nil, ErrOrgTeamNotFound
	}
	if successor == nil {
		return nil, ErrLastOrgTeam
	}

	if err := tx.Where("org_id = ? AND team_id = ?", orgID, teamID).
		Delete(&OrgTeam{}).Error; err != nil {
		return nil, fmt.Errorf("delete org team (org=%s team=%d): %w", orgID, teamID, err)
	}
	// A DELETE leaves no updated_at bump behind, so a non-billing deletion
	// would be invisible to change-marker consumers (discovery's
	// config_generation is MAX(updated_at) over the three config tables) —
	// the exact removal signal a poller must not skip. Touch the parent
	// org row in the same transaction so the marker advances.
	if err := tx.Model(&Org{}).Where("name = ?", orgID).
		Update("updated_at", gorm.Expr("now()")).Error; err != nil {
		return nil, fmt.Errorf("touch org row after team delete (org=%s): %w", orgID, err)
	}

	res := &OrgTeamDeleteResult{
		WasBilling: target.IsBillingTeam != nil && *target.IsBillingTeam,
	}
	if !res.WasBilling {
		return res, nil
	}

	// The billing team is gone: the oldest remaining team takes over, and the
	// org's buffered usage buckets follow it in the same transaction — exactly
	// the handover a billing repoint performs.
	if err := SetOrgBillingTeamTx(tx, orgID, successor.TeamID); err != nil {
		return nil, err
	}
	moved, err := ReattributeUsageTeamTx(tx, orgID, successor.TeamID)
	if err != nil {
		return nil, err
	}
	res.NewBillingTeamID = successor.TeamID
	res.UsageRowsMoved = moved
	return res, nil
}
