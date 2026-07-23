package configstore

import "gorm.io/gorm"

// LockOrgConnectionAdmissionTx serializes admission decisions with mutations
// of the org and user limits those decisions read. Callers must acquire it as
// the first database operation in their transaction to keep lock ordering
// consistent with the admission scheduler.
func LockOrgConnectionAdmissionTx(tx *gorm.DB, orgID string) error {
	return lockOrgConnectionAdmission(tx, orgID)
}
