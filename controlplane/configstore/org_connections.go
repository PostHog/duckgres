package configstore

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const missingOwnerOrgConnectionLeaseGrace = 5 * time.Minute

// EnqueueOrgConnectionRequest inserts a pending cluster-wide connection
// admission request. FIFO ordering is scoped to org_id and ordered by
// enqueued_at, then request_id.
func (cs *ConfigStore) EnqueueOrgConnectionRequest(entry *OrgConnectionQueueEntry) error {
	if entry == nil {
		return fmt.Errorf("org connection queue entry is required")
	}
	if strings.TrimSpace(entry.RequestID) == "" {
		return fmt.Errorf("org connection request id is required")
	}
	if strings.TrimSpace(entry.OrgID) == "" {
		return fmt.Errorf("org connection org id is required")
	}
	if entry.EnqueuedAt.IsZero() {
		entry.EnqueuedAt = time.Now()
	}
	if entry.ExpiresAt.IsZero() {
		return fmt.Errorf("org connection request expiry is required")
	}
	ttl := entry.ExpiresAt.Sub(entry.EnqueuedAt)
	if ttl <= 0 {
		return fmt.Errorf("org connection request expiry must be after enqueue time")
	}

	entryCopy := *entry
	if err := cs.db.Transaction(func(tx *gorm.DB) error {
		now, err := cs.orgConnectionDatabaseNow(tx)
		if err != nil {
			return err
		}
		entryCopy.EnqueuedAt = now
		entryCopy.ExpiresAt = now.Add(ttl)
		return tx.Table(cs.runtimeTable(entryCopy.TableName())).Create(&entryCopy).Error
	}); err != nil {
		return fmt.Errorf("enqueue org connection request: %w", err)
	}
	return nil
}

// TryAcquireOrgConnectionLease attempts to grant one queued request under a
// cluster-wide per-org limit. A nil lease means the request is still waiting
// behind FIFO order or active capacity.
func (cs *ConfigStore) TryAcquireOrgConnectionLease(requestID string, maxConnections int, _ time.Time) (*OrgConnectionLease, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("org connection request id is required")
	}

	for {
		lease, retry, err := cs.tryAcquireOrgConnectionLeaseOnce(requestID, maxConnections)
		if retry {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("try acquire org connection lease: %w", err)
		}
		return lease, nil
	}
}

type orgConnectionRuntimeTables struct {
	queue string
	lease string
}

func (cs *ConfigStore) orgConnectionRuntimeTables() orgConnectionRuntimeTables {
	return orgConnectionRuntimeTables{
		queue: cs.runtimeTable((&OrgConnectionQueueEntry{}).TableName()),
		lease: cs.runtimeTable((&OrgConnectionLease{}).TableName()),
	}
}

func (cs *ConfigStore) tryAcquireOrgConnectionLeaseOnce(requestID string, maxConnections int) (*OrgConnectionLease, bool, error) {
	tables := cs.orgConnectionRuntimeTables()
	var lease *OrgConnectionLease
	retryWithFreshOrg := false

	err := cs.db.Transaction(func(tx *gorm.DB) error {
		orgID, found, err := cs.orgIDForConnectionRequest(tx, tables.queue, requestID)
		if err != nil || !found {
			return err
		}
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:org-connections:"+orgID)).Error; err != nil {
			return err
		}
		now, err := cs.orgConnectionDatabaseNow(tx)
		if err != nil {
			return err
		}
		if err := cs.cleanupOrgConnectionRowsLocked(tx, orgID, now); err != nil {
			return err
		}

		request, found, err := cs.lockOrgConnectionRequest(tx, tables.queue, requestID)
		if err != nil || !found {
			return err
		}
		if request.OrgID != orgID {
			retryWithFreshOrg = true
			return nil
		}

		existing, found, err := cs.existingOrgConnectionLease(tx, tables.lease, requestID)
		if err != nil || found {
			lease = existing
			return err
		}
		if !request.ExpiresAt.After(now) || request.GrantedAt != nil {
			return nil
		}
		atHead, err := cs.isOrgConnectionQueueHead(tx, tables.queue, request, now)
		if err != nil || !atHead {
			return err
		}
		if maxConnections > 0 {
			count, err := cs.countActiveOrgConnectionLeases(tx, orgID)
			if err != nil {
				return err
			}
			if count >= int64(maxConnections) {
				return nil
			}
		}

		created, err := cs.createOrgConnectionLease(tx, request, now)
		if err != nil {
			return err
		}
		lease = created
		return nil
	})
	return lease, retryWithFreshOrg, err
}

func (cs *ConfigStore) orgConnectionDatabaseNow(tx *gorm.DB) (time.Time, error) {
	var now time.Time
	if err := tx.Raw("SELECT clock_timestamp()").Scan(&now).Error; err != nil {
		return time.Time{}, err
	}
	return now, nil
}

func (cs *ConfigStore) orgIDForConnectionRequest(tx *gorm.DB, queueTable, requestID string) (string, bool, error) {
	var requestOrg struct {
		OrgID string
	}
	if err := tx.Table(queueTable).
		Select("org_id").
		Where("request_id = ?", requestID).
		Take(&requestOrg).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	return requestOrg.OrgID, true, nil
}

func (cs *ConfigStore) lockOrgConnectionRequest(tx *gorm.DB, queueTable, requestID string) (*OrgConnectionQueueEntry, bool, error) {
	var request OrgConnectionQueueEntry
	if err := tx.Table(queueTable).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("request_id = ?", requestID).
		Take(&request).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &request, true, nil
}

func (cs *ConfigStore) existingOrgConnectionLease(tx *gorm.DB, leaseTable, requestID string) (*OrgConnectionLease, bool, error) {
	var existing OrgConnectionLease
	if err := tx.Table(leaseTable).
		Where("request_id = ?", requestID).
		Take(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &existing, true, nil
}

func (cs *ConfigStore) isOrgConnectionQueueHead(tx *gorm.DB, queueTable string, request *OrgConnectionQueueEntry, now time.Time) (bool, error) {
	var head OrgConnectionQueueEntry
	if err := tx.Table(queueTable).
		Where("org_id = ? AND granted_at IS NULL AND expires_at > ?", request.OrgID, now).
		Order("enqueued_at ASC, request_id ASC").
		Limit(1).
		Take(&head).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return head.RequestID == request.RequestID, nil
}

func (cs *ConfigStore) createOrgConnectionLease(tx *gorm.DB, request *OrgConnectionQueueEntry, now time.Time) (*OrgConnectionLease, error) {
	granted := now
	created := &OrgConnectionLease{
		LeaseID:      request.RequestID,
		RequestID:    request.RequestID,
		OrgID:        request.OrgID,
		CPInstanceID: request.CPInstanceID,
		PID:          request.PID,
		Protocol:     request.Protocol,
		AcquiredAt:   now,
	}
	if err := tx.Table(cs.runtimeTable(created.TableName())).Create(created).Error; err != nil {
		return nil, err
	}
	if err := tx.Table(cs.runtimeTable(request.TableName())).
		Where("request_id = ?", request.RequestID).
		Updates(map[string]any{
			"granted_at": granted,
			"updated_at": now,
		}).Error; err != nil {
		return nil, err
	}
	return created, nil
}

// ReleaseOrgConnectionLease releases one active cluster-wide connection lease.
func (cs *ConfigStore) ReleaseOrgConnectionLease(leaseID string) error {
	if strings.TrimSpace(leaseID) == "" {
		return nil
	}
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Table(cs.runtimeTable((&OrgConnectionLease{}).TableName())).
			Where("lease_id = ?", leaseID).
			Delete(&OrgConnectionLease{}).Error; err != nil {
			return err
		}
		return tx.Table(cs.runtimeTable((&OrgConnectionQueueEntry{}).TableName())).
			Where("request_id = ?", leaseID).
			Delete(&OrgConnectionQueueEntry{}).Error
	})
	if err != nil {
		return fmt.Errorf("release org connection lease: %w", err)
	}
	return nil
}

// CancelOrgConnectionRequest removes a pending queue request. It does not
// delete an already-granted lease; callers must release leases explicitly.
func (cs *ConfigStore) CancelOrgConnectionRequest(requestID string, _ time.Time) error {
	if strings.TrimSpace(requestID) == "" {
		return nil
	}
	result := cs.db.Table(cs.runtimeTable((&OrgConnectionQueueEntry{}).TableName())).
		Where("request_id = ? AND granted_at IS NULL", requestID).
		Delete(&OrgConnectionQueueEntry{})
	if result.Error != nil {
		return fmt.Errorf("cancel org connection request: %w", result.Error)
	}
	return nil
}

// ActiveOrgConnectionLeaseCount returns the active cluster-wide lease count for
// an org, ignoring leases owned by expired control-plane instances.
func (cs *ConfigStore) ActiveOrgConnectionLeaseCount(orgID string) (int64, error) {
	var count int64
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		var err error
		count, err = cs.countActiveOrgConnectionLeases(tx, orgID)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("count active org connection leases: %w", err)
	}
	return count, nil
}

func (cs *ConfigStore) cleanupOrgConnectionRowsLocked(tx *gorm.DB, orgID string, now time.Time) error {
	queueTable := cs.runtimeTable((&OrgConnectionQueueEntry{}).TableName())
	leaseTable := cs.runtimeTable((&OrgConnectionLease{}).TableName())
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())

	if err := tx.Table(queueTable).
		Where("org_id = ? AND granted_at IS NULL AND expires_at <= ?", orgID, now).
		Delete(&OrgConnectionQueueEntry{}).Error; err != nil {
		return err
	}

	if err := tx.Exec("DELETE FROM "+leaseTable+" AS l USING "+cpTable+" AS cp "+
		"WHERE l.cp_instance_id = cp.id AND l.org_id = ? AND cp.state = ?",
		orgID, ControlPlaneInstanceStateExpired).Error; err != nil {
		return err
	}

	return tx.Exec(
		"DELETE FROM "+leaseTable+" AS l "+
			"WHERE l.org_id = ? AND l.acquired_at <= ? "+
			"AND NOT EXISTS (SELECT 1 FROM "+cpTable+" AS cp WHERE cp.id = l.cp_instance_id)",
		orgID, now.Add(-missingOwnerOrgConnectionLeaseGrace),
	).Error
}

func (cs *ConfigStore) countActiveOrgConnectionLeases(tx *gorm.DB, orgID string) (int64, error) {
	var count int64
	leaseTable := cs.runtimeTable((&OrgConnectionLease{}).TableName())
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())
	err := tx.Table(leaseTable+" AS l").
		Joins("LEFT JOIN "+cpTable+" AS cp ON cp.id = l.cp_instance_id").
		Where("l.org_id = ?", orgID).
		Where("cp.id IS NULL OR cp.state <> ?", ControlPlaneInstanceStateExpired).
		Count(&count).Error
	return count, err
}
