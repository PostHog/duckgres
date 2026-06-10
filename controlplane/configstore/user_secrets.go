package configstore

import (
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrTooManyUserSecrets is returned by UpsertOrgUserSecret when creating a
// new secret would push the user past the per-user cap.
var ErrTooManyUserSecrets = errors.New("too many persistent secrets for user")

// UpsertOrgUserSecret stores (or replaces) one sealed user secret. When
// maxPerUser > 0, creating a NEW secret name is rejected with
// ErrTooManyUserSecrets once the user already has maxPerUser secrets;
// replacing an existing name is always allowed.
func (cs *ConfigStore) UpsertOrgUserSecret(orgID, username, secretName string, ciphertext []byte, maxPerUser int) error {
	return cs.db.Transaction(func(tx *gorm.DB) error {
		if maxPerUser > 0 {
			var existing int64
			if err := tx.Model(&OrgUserSecret{}).
				Where("org_id = ? AND username = ? AND secret_name = ?", orgID, username, secretName).
				Count(&existing).Error; err != nil {
				return err
			}
			if existing == 0 {
				var total int64
				if err := tx.Model(&OrgUserSecret{}).
					Where("org_id = ? AND username = ?", orgID, username).
					Count(&total).Error; err != nil {
					return err
				}
				if total >= int64(maxPerUser) {
					return fmt.Errorf("%w (max %d)", ErrTooManyUserSecrets, maxPerUser)
				}
			}
		}
		secret := OrgUserSecret{
			OrgID:      orgID,
			Username:   username,
			SecretName: secretName,
			Ciphertext: ciphertext,
		}
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "org_id"}, {Name: "username"}, {Name: "secret_name"}},
			DoUpdates: clause.AssignmentColumns([]string{"ciphertext", "updated_at"}),
		}).Create(&secret).Error
	})
}

// DeleteOrgUserSecret removes one user secret, reporting whether it existed.
// Deleting a secret that does not exist is not an error (DROP ... IF EXISTS
// semantics live with the caller).
func (cs *ConfigStore) DeleteOrgUserSecret(orgID, username, secretName string) (bool, error) {
	result := cs.db.
		Where("org_id = ? AND username = ? AND secret_name = ?", orgID, username, secretName).
		Delete(&OrgUserSecret{})
	return result.RowsAffected > 0, result.Error
}

// ListOrgUserSecrets returns all sealed secrets for one user, ordered by
// creation time so replay order is deterministic.
func (cs *ConfigStore) ListOrgUserSecrets(orgID, username string) ([]OrgUserSecret, error) {
	var secrets []OrgUserSecret
	err := cs.db.
		Where("org_id = ? AND username = ?", orgID, username).
		Order("created_at ASC").
		Find(&secrets).Error
	return secrets, err
}
