package configstore

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrInvalidOperatorRole is returned when an operator role is not one of the
// recognized values.
var ErrInvalidOperatorRole = errors.New("operator role must be \"admin\" or \"viewer\"")

// operatorRoleAdmin / operatorRoleViewer are the only valid operator roles.
const (
	operatorRoleAdmin  = "admin"
	operatorRoleViewer = "viewer"
)

func validOperatorRole(role string) bool {
	return role == operatorRoleAdmin || role == operatorRoleViewer
}

// operatorsTable returns the schema-qualified operators table name. Operators
// live in the CP runtime schema (operational state), so reads/writes must be
// schema-qualified the same way the other runtime-table accessors are.
func (cs *ConfigStore) operatorsTable() string {
	return cs.runtimeTable((Operator{}).TableName())
}

// OperatorRole returns the role for an operator email, case-insensitively.
// Returns "" (and no error) when the email has no operator row — callers treat
// "no row" as "not an operator" (which resolves to viewer at the auth layer).
func (cs *ConfigStore) OperatorRole(email string) (string, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return "", nil
	}
	var op Operator
	err := cs.db.Table(cs.operatorsTable()).Where("email = ?", email).Take(&op).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", fmt.Errorf("get operator role: %w", err)
	}
	return op.Role, nil
}

// ListOperators returns all operators ordered by email.
func (cs *ConfigStore) ListOperators() ([]Operator, error) {
	var ops []Operator
	if err := cs.db.Table(cs.operatorsTable()).Order("email ASC").Find(&ops).Error; err != nil {
		return nil, fmt.Errorf("list operators: %w", err)
	}
	return ops, nil
}

// UpsertOperator creates or updates an operator. The email is lowercased; role
// must be "admin" or "viewer". On conflict (email PK) the role + added_by +
// updated_at are updated; created_at is set on insert only.
func (cs *ConfigStore) UpsertOperator(email, role, addedBy string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return errors.New("operator email is required")
	}
	if !validOperatorRole(role) {
		return ErrInvalidOperatorRole
	}
	now := time.Now()
	op := Operator{
		Email:     email,
		Role:      role,
		AddedBy:   addedBy,
		CreatedAt: now,
		UpdatedAt: now,
	}
	err := cs.db.Table(cs.operatorsTable()).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "email"}},
		DoUpdates: clause.AssignmentColumns([]string{"role", "added_by", "updated_at"}),
	}).Create(&op).Error
	if err != nil {
		return fmt.Errorf("upsert operator: %w", err)
	}
	return nil
}

// DeleteOperator removes an operator by email (case-insensitive), reporting
// whether a row was deleted. Deleting a missing operator is not an error.
func (cs *ConfigStore) DeleteOperator(email string) (bool, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	result := cs.db.Table(cs.operatorsTable()).Where("email = ?", email).Delete(&Operator{})
	if result.Error != nil {
		return false, fmt.Errorf("delete operator: %w", result.Error)
	}
	return result.RowsAffected > 0, nil
}

// CountAdmins returns the number of operators with the admin role. Used by the
// last-admin guard so the admin role can never be fully removed from the table.
func (cs *ConfigStore) CountAdmins() (int64, error) {
	var count int64
	if err := cs.db.Table(cs.operatorsTable()).Where("role = ?", operatorRoleAdmin).Count(&count).Error; err != nil {
		return 0, fmt.Errorf("count admins: %w", err)
	}
	return count, nil
}

// SeedOperator inserts an operator if (and only if) the email is not already
// present — an idempotent create-only bootstrap. An existing row is left
// untouched (ON CONFLICT DO NOTHING), so a re-run never overwrites a role an
// admin has since changed. The email is lowercased; role must be valid.
func (cs *ConfigStore) SeedOperator(email, role string) error {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return errors.New("operator email is required")
	}
	if !validOperatorRole(role) {
		return ErrInvalidOperatorRole
	}
	now := time.Now()
	op := Operator{
		Email:     email,
		Role:      role,
		AddedBy:   "bootstrap",
		CreatedAt: now,
		UpdatedAt: now,
	}
	err := cs.db.Table(cs.operatorsTable()).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "email"}},
		DoNothing: true,
	}).Create(&op).Error
	if err != nil {
		return fmt.Errorf("seed operator: %w", err)
	}
	return nil
}
