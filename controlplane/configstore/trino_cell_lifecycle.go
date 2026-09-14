package configstore

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"
	"unicode"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrTrinoCellConflict = errors.New("trino cell lifecycle conflict")
var trinoCellHashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

type TrinoCellLease struct {
	CellID         string
	Owner          string
	ReconcileEpoch int64
	AdmissionEpoch int64
	IntentSequence int64
}

type TrinoCatalogIntent struct {
	Sequence int64  `json:"sequence"`
	ID       string `json:"id"`
	Backend  string `json:"backend"`
	Action   string `json:"action"`
	Catalog  string `json:"catalog"`
}

type TrinoCellCertificate struct {
	TargetBackend string `json:"targetBackend"`
	NodeID        string `json:"nodeId"`
	CoordinatorID string `json:"coordinatorId"`
	RosterHash    string `json:"rosterHash"`
	AdmittedCount int    `json:"admittedCount"`
}

type TrinoCellFreeze struct {
	OperationID    string                `json:"operationId"`
	PlanHash       string                `json:"planHash"`
	TargetBackend  string                `json:"targetBackend"`
	AdmissionEpoch int64                 `json:"admissionEpoch"`
	Certificate    *TrinoCellCertificate `json:"certificate,omitempty"`
	Stable         bool                  `json:"stable"`
}

type trinoCellLifecycle struct {
	CellID                 string `gorm:"primaryKey"`
	ReconcileOwner         string
	ReconcileEpoch         int64
	IntentSequence         int64
	Intent                 string `gorm:"type:jsonb"`
	AdmissionEpoch         int64
	FreezeOperationID      string
	FreezePlanHash         string
	FreezeTarget           string
	FreezeStable           bool
	Certificate            string `gorm:"type:jsonb"`
	ReleasedOperationID    string
	ReleasedAdmissionEpoch int64
	UpdatedAt              time.Time
}

func (trinoCellLifecycle) TableName() string { return "duckgres_trino_cell_lifecycle" }

func validTrinoCellValue(s string) bool {
	return s != "" && len(s) <= 255 && strings.IndexFunc(s, unicode.IsControl) == -1
}

func (cs *ConfigStore) withTrinoCell(ctx context.Context, cell string, fn func(*gorm.DB, *trinoCellLifecycle) error) error {
	if !strings.HasPrefix(cell, "registered:") || !validTrinoCellValue(cell) {
		return ErrTrinoCellConflict
	}
	return cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		seed := trinoCellLifecycle{CellID: cell, Intent: "{}", Certificate: "{}", UpdatedAt: time.Now().UTC()}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&seed).Error; err != nil {
			return err
		}
		var row trinoCellLifecycle
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&row, "cell_id = ?", cell).Error; err != nil {
			return err
		}
		return fn(tx, &row)
	})
}

func saveTrinoCell(tx *gorm.DB, row *trinoCellLifecycle) error {
	row.UpdatedAt = time.Now().UTC()
	return tx.Save(row).Error
}

func ownsTrinoCell(row *trinoCellLifecycle, lease TrinoCellLease) bool {
	return row.CellID == lease.CellID && row.ReconcileOwner != "" && row.ReconcileOwner == lease.Owner && row.ReconcileEpoch == lease.ReconcileEpoch
}

// BeginTrinoCellReconcile must precede the tenant and Gateway snapshots.
// A held owner never expires, including after a controller process disappears.
func (cs *ConfigStore) BeginTrinoCellReconcile(ctx context.Context, cell, owner string) (*TrinoCellLease, bool, error) {
	if !validTrinoCellValue(owner) {
		return nil, false, ErrTrinoCellConflict
	}
	var lease *TrinoCellLease
	err := cs.withTrinoCell(ctx, cell, func(tx *gorm.DB, row *trinoCellLifecycle) error {
		if row.ReconcileOwner != "" {
			return nil
		}
		row.ReconcileOwner = owner
		row.ReconcileEpoch++
		if err := saveTrinoCell(tx, row); err != nil {
			return err
		}
		lease = &TrinoCellLease{CellID: cell, Owner: owner, ReconcileEpoch: row.ReconcileEpoch, AdmissionEpoch: row.AdmissionEpoch, IntentSequence: row.IntentSequence}
		return nil
	})
	return lease, lease != nil && err == nil, err
}

// SetTrinoCellIntent authorizes one submission only after its durable commit.
// A duplicate call is a conflict even when its payload is identical.
func (cs *ConfigStore) SetTrinoCellIntent(ctx context.Context, lease TrinoCellLease, intent TrinoCatalogIntent) error {
	if !validTrinoCellValue(intent.ID) || !validTrinoCellValue(intent.Backend) || !validTrinoCellValue(intent.Catalog) || (intent.Action != "create" && intent.Action != "drop") {
		return ErrTrinoCellConflict
	}
	encoded, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	return cs.withTrinoCell(ctx, lease.CellID, func(tx *gorm.DB, row *trinoCellLifecycle) error {
		if !ownsTrinoCell(row, lease) || row.Intent != "{}" || row.FreezeOperationID != "" || row.AdmissionEpoch != lease.AdmissionEpoch || intent.Sequence <= 0 || intent.Sequence != row.IntentSequence+1 {
			return ErrTrinoCellConflict
		}
		row.Intent = string(encoded)
		row.IntentSequence = intent.Sequence
		return saveTrinoCell(tx, row)
	})
}

// ClearTrinoCellIntent requires a confirmed terminal response for this intent.
func (cs *ConfigStore) ClearTrinoCellIntent(ctx context.Context, lease TrinoCellLease, intentID string) error {
	return cs.withTrinoCell(ctx, lease.CellID, func(tx *gorm.DB, row *trinoCellLifecycle) error {
		var intent TrinoCatalogIntent
		if !ownsTrinoCell(row, lease) || json.Unmarshal([]byte(row.Intent), &intent) != nil || intent.ID == "" || intent.ID != intentID {
			return ErrTrinoCellConflict
		}
		row.Intent = "{}"
		return saveTrinoCell(tx, row)
	})
}

func (cs *ConfigStore) FinishTrinoCellReconcile(ctx context.Context, lease TrinoCellLease) error {
	return cs.withTrinoCell(ctx, lease.CellID, func(tx *gorm.DB, row *trinoCellLifecycle) error {
		if !ownsTrinoCell(row, lease) || row.Intent != "{}" {
			return ErrTrinoCellConflict
		}
		row.ReconcileOwner = ""
		if row.FreezeOperationID != "" {
			row.FreezeStable = true
		}
		return saveTrinoCell(tx, row)
	})
}

func freezeFromRow(row *trinoCellLifecycle) (*TrinoCellFreeze, error) {
	if row.FreezeOperationID == "" {
		return nil, nil
	}
	freeze := &TrinoCellFreeze{OperationID: row.FreezeOperationID, PlanHash: row.FreezePlanHash, TargetBackend: row.FreezeTarget, AdmissionEpoch: row.AdmissionEpoch, Stable: row.FreezeStable}
	if row.Certificate != "{}" {
		if err := json.Unmarshal([]byte(row.Certificate), &freeze.Certificate); err != nil {
			return nil, err
		}
	}
	return freeze, nil
}

func (cs *ConfigStore) FreezeTrinoCellAdmissions(ctx context.Context, cell, operation, planHash, target string, expectedEpoch int64) (*TrinoCellFreeze, error) {
	if !validTrinoCellValue(operation) || !validTrinoCellValue(target) || !trinoCellHashPattern.MatchString(planHash) {
		return nil, ErrTrinoCellConflict
	}
	var result *TrinoCellFreeze
	err := cs.withTrinoCell(ctx, cell, func(tx *gorm.DB, row *trinoCellLifecycle) error {
		if row.ReleasedOperationID == operation {
			return ErrTrinoCellConflict
		}
		if row.FreezeOperationID != "" {
			if row.FreezeOperationID != operation || row.FreezePlanHash != planHash || row.FreezeTarget != target {
				return ErrTrinoCellConflict
			}
		} else {
			if row.AdmissionEpoch != expectedEpoch {
				return ErrTrinoCellConflict
			}
			row.AdmissionEpoch++
			row.FreezeOperationID, row.FreezePlanHash, row.FreezeTarget = operation, planHash, target
			row.FreezeStable = row.ReconcileOwner == ""
			row.Certificate = "{}"
			if err := saveTrinoCell(tx, row); err != nil {
				return err
			}
		}
		var err error
		result, err = freezeFromRow(row)
		return err
	})
	return result, err
}

func (cs *ConfigStore) GetTrinoCellFreeze(ctx context.Context, cell string) (*TrinoCellFreeze, error) {
	var row trinoCellLifecycle
	err := cs.db.WithContext(ctx).First(&row, "cell_id = ?", cell).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return freezeFromRow(&row)
}

type TrinoCellLifecycleStatus struct {
	CellID                 string
	AdmissionEpoch         int64
	ReconcileOwner         string
	ReconcileEpoch         int64
	IntentSequence         int64
	Intent                 *TrinoCatalogIntent
	Freeze                 *TrinoCellFreeze
	ReleasedOperationID    string
	ReleasedAdmissionEpoch int64
}

// GetTrinoCellLifecycle returns the current epoch even when no freeze is active.
// Reading an uninitialized managed cell does not create a lifecycle row.
func (cs *ConfigStore) GetTrinoCellLifecycle(ctx context.Context, cell string) (*TrinoCellLifecycleStatus, error) {
	if !strings.HasPrefix(cell, "registered:") || !validTrinoCellValue(cell) {
		return nil, ErrTrinoCellConflict
	}
	var row trinoCellLifecycle
	err := cs.db.WithContext(ctx).First(&row, "cell_id = ?", cell).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return &TrinoCellLifecycleStatus{CellID: cell}, nil
	}
	if err != nil {
		return nil, err
	}
	freeze, err := freezeFromRow(&row)
	if err != nil {
		return nil, err
	}
	result := &TrinoCellLifecycleStatus{CellID: cell, AdmissionEpoch: row.AdmissionEpoch, ReconcileOwner: row.ReconcileOwner, ReconcileEpoch: row.ReconcileEpoch, IntentSequence: row.IntentSequence, Freeze: freeze, ReleasedOperationID: row.ReleasedOperationID, ReleasedAdmissionEpoch: row.ReleasedAdmissionEpoch}
	if row.Intent != "{}" {
		if err := json.Unmarshal([]byte(row.Intent), &result.Intent); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (cs *ConfigStore) CertifyTrinoCellTarget(ctx context.Context, lease TrinoCellLease, operation string, certificate TrinoCellCertificate) error {
	if !validTrinoCellValue(certificate.TargetBackend) || !validTrinoCellValue(certificate.NodeID) || !validTrinoCellValue(certificate.CoordinatorID) || !trinoCellHashPattern.MatchString(certificate.RosterHash) || certificate.AdmittedCount < 0 || certificate.AdmittedCount > 100000 {
		return ErrTrinoCellConflict
	}
	encoded, err := json.Marshal(certificate)
	if err != nil {
		return err
	}
	return cs.withTrinoCell(ctx, lease.CellID, func(tx *gorm.DB, row *trinoCellLifecycle) error {
		if !ownsTrinoCell(row, lease) || row.Intent != "{}" || !row.FreezeStable || row.FreezeOperationID != operation || row.FreezeTarget != certificate.TargetBackend || row.AdmissionEpoch != lease.AdmissionEpoch {
			return ErrTrinoCellConflict
		}
		if row.Certificate != "{}" {
			var existing TrinoCellCertificate
			if json.Unmarshal([]byte(row.Certificate), &existing) != nil || existing != certificate {
				return ErrTrinoCellConflict
			}
			return nil
		}
		row.Certificate = string(encoded)
		return saveTrinoCell(tx, row)
	})
}

// ReleaseTrinoCellAdmissions follows verification of the exact Gateway target route.
// The caller must validate the active operation and certified coordinator process.
func (cs *ConfigStore) ReleaseTrinoCellAdmissions(ctx context.Context, cell, operation string, epoch int64) error {
	return cs.withTrinoCell(ctx, cell, func(tx *gorm.DB, row *trinoCellLifecycle) error {
		if row.FreezeOperationID == "" && row.ReleasedOperationID == operation && row.ReleasedAdmissionEpoch == epoch {
			return nil
		}
		if operation == "" || row.FreezeOperationID != operation || row.AdmissionEpoch != epoch || row.Certificate == "{}" {
			return ErrTrinoCellConflict
		}
		row.ReleasedOperationID, row.ReleasedAdmissionEpoch = operation, epoch
		row.FreezeOperationID, row.FreezePlanHash, row.FreezeTarget = "", "", ""
		row.FreezeStable = false
		row.Certificate = "{}"
		row.AdmissionEpoch++
		return saveTrinoCell(tx, row)
	})
}

// UpdateManagedTrinoState fences new admission in the same database transaction.
// A backend health observation cannot implicitly grant new admission after cutover.
func (cs *ConfigStore) UpdateManagedTrinoState(ctx context.Context, lease TrinoCellLease, org string, update TrinoStateUpdate) (bool, error) {
	updated := false
	err := cs.withTrinoCell(ctx, lease.CellID, func(tx *gorm.DB, row *trinoCellLifecycle) error {
		if !ownsTrinoCell(row, lease) {
			return ErrTrinoCellConflict
		}
		var tenant ManagedWarehouseTrino
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&tenant, "org_id = ? AND enabled = ? AND trino_cell_id = ?", org, true, lease.CellID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if update.State == ManagedWarehouseStateReady && tenant.State != ManagedWarehouseStateReady && (row.FreezeOperationID != "" || row.AdmissionEpoch != lease.AdmissionEpoch) {
			return nil
		}
		temporary := &ConfigStore{db: tx}
		if err := temporary.UpdateTrinoState(org, update); err != nil {
			return err
		}
		updated = true
		return nil
	})
	return updated, err
}
