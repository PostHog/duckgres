package configstore

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	missingOwnerOrgConnectionLeaseGrace = 5 * time.Minute
	legacyOrgConnectionRequestedVCPUs   = 1
)

// EnqueueOrgConnectionRequest inserts a pending cluster-wide connection
// admission request. FIFO ordering is scoped to org_id and ordered by
// enqueued_at, then request_id. RequestedVCPUs is charged against active
// resource leases when the request is granted.
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
	if strings.TrimSpace(entry.Username) == "" {
		return fmt.Errorf("org connection username is required")
	}
	if entry.RequestedVCPUs <= 0 {
		return fmt.Errorf("org connection requested vcpus must be positive")
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

// TryAcquireOrgConnectionLease attempts to grant one queued request under
// cluster-wide per-org and per-user vCPU budgets. A nil lease means the request
// is still waiting behind FIFO order or active resource capacity.
func (cs *ConfigStore) TryAcquireOrgConnectionLease(requestID string, limits OrgResourceLimits, now time.Time) (*OrgConnectionLease, error) {
	return cs.TryAcquireOrgConnectionLeaseWithLimitLookup(requestID, func(string) OrgResourceLimits {
		return limits
	}, now)
}

// TryAcquireOrgConnectionLeaseWithLimitLookup attempts to grant one queued
// request using a username-scoped limit lookup. Earlier queued requests must be
// evaluated with their own user limits so one saturated user does not stall
// unrelated users in the same org.
func (cs *ConfigStore) TryAcquireOrgConnectionLeaseWithLimitLookup(requestID string, limitLookup func(string) OrgResourceLimits, _ time.Time) (*OrgConnectionLease, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("org connection request id is required")
	}
	if limitLookup == nil {
		limitLookup = func(string) OrgResourceLimits { return OrgResourceLimits{} }
	}

	start := time.Now()
	outcome := orgConnectionAdmissionOutcomeWaiting
	var stats orgConnectionAdmissionStats
	defer func() {
		observeOrgConnectionAdmission(time.Since(start), outcome, stats)
	}()

	for {
		lease, retry, attemptStats, attemptOutcome, err := cs.tryAcquireOrgConnectionLeaseOnce(requestID, limitLookup)
		stats = attemptStats
		outcome = attemptOutcome
		if retry {
			continue
		}
		if err != nil {
			outcome = orgConnectionAdmissionOutcomeError
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

func (cs *ConfigStore) tryAcquireOrgConnectionLeaseOnce(requestID string, limitLookup func(string) OrgResourceLimits) (*OrgConnectionLease, bool, orgConnectionAdmissionStats, string, error) {
	tables := cs.orgConnectionRuntimeTables()
	var lease *OrgConnectionLease
	retryWithFreshOrg := false
	var stats orgConnectionAdmissionStats
	outcome := orgConnectionAdmissionOutcomeMissing

	err := cs.db.Transaction(func(tx *gorm.DB) error {
		orgID, found, err := cs.orgIDForConnectionRequest(tx, tables.queue, requestID)
		if err != nil || !found {
			return err
		}
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:org-connections:"+orgID)).Error; err != nil {
			return err
		}
		// LOAD-BEARING reshard barrier: refuse to grant any lease while the
		// org's warehouse is resharding. The connect-time 57P03 gate alone is
		// unsound — a request that passed it can be granted a lease up to a
		// queue-timeout later. Checking here, under the same advisory lock
		// SetWarehouseResharding takes for the ready→resharding CAS, makes
		// "no new session after the flip commits" exact: any grant either
		// committed before the flip (visible to the drain as a lease) or runs
		// after it and sees state=resharding.
		resharding, err := cs.warehouseReshardingLocked(tx, orgID)
		if err != nil {
			return err
		}
		if resharding {
			outcome = orgConnectionAdmissionOutcomeResharding
			return nil
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
			outcome = orgConnectionAdmissionOutcomeRetry
			return nil
		}

		existing, found, err := cs.existingOrgConnectionLease(tx, tables.lease, requestID)
		if err != nil || found {
			lease = existing
			if found {
				outcome = orgConnectionAdmissionOutcomeAlreadyGranted
			}
			return err
		}
		if !request.ExpiresAt.After(now) || request.GrantedAt != nil {
			outcome = orgConnectionAdmissionOutcomeInactive
			return nil
		}

		next, selectionStats, selectionOutcome, err := cs.nextEligibleOrgConnectionRequestLocked(tx, tables, orgID, limitLookup, now)
		stats = selectionStats
		if err != nil {
			return err
		}
		if next == nil {
			outcome = selectionOutcome
			return nil
		}
		if next.RequestID != requestID {
			outcome = orgConnectionAdmissionOutcomeWaiting
			return nil
		}
		lease, err = cs.createOrgConnectionLease(tx, request, now)
		if err != nil {
			return err
		}
		outcome = orgConnectionAdmissionOutcomeGranted
		return nil
	})
	return lease, retryWithFreshOrg, stats, outcome, err
}

// warehouseReshardingLocked reports whether the org's managed warehouse is in
// the resharding state. Read inside the grant transaction (under the org's
// connection advisory lock) so it serializes against SetWarehouseResharding.
// Orgs without a warehouse row are not resharding.
func (cs *ConfigStore) warehouseReshardingLocked(tx *gorm.DB, orgID string) (bool, error) {
	var count int64
	if err := tx.Model(&ManagedWarehouse{}).
		Where("org_id = ? AND state = ?", orgID, ManagedWarehouseStateResharding).
		Count(&count).Error; err != nil {
		return false, fmt.Errorf("warehouse resharding check: %w", err)
	}
	return count > 0, nil
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

func (cs *ConfigStore) nextEligibleOrgConnectionRequestLocked(tx *gorm.DB, tables orgConnectionRuntimeTables, orgID string, limitLookup func(string) OrgResourceLimits, now time.Time) (*OrgConnectionQueueEntry, orgConnectionAdmissionStats, string, error) {
	heads, stats, err := cs.pendingOrgConnectionUserQueueHeads(tx, tables.queue, orgID, now)
	if err != nil {
		return nil, stats, orgConnectionAdmissionOutcomeError, err
	}
	if len(heads) == 0 {
		return nil, stats, orgConnectionAdmissionOutcomeWaiting, nil
	}

	orgUsed, userUsed, legacyUserUsed, err := cs.activeOrgConnectionLeaseVCPUUsage(tx, orgID)
	if err != nil {
		return nil, stats, orgConnectionAdmissionOutcomeError, err
	}

	for i := range heads {
		head := &heads[i]
		limits := limitLookup(head.Username)
		requested := int64(head.RequestedVCPUs)
		if limits.UserMaxVCPUs > 0 {
			used := legacyUserUsed + userUsed[head.Username]
			if used+requested > int64(limits.UserMaxVCPUs) {
				stats.userLimitSkips++
				continue
			}
		}
		if limits.OrgMaxVCPUs > 0 && orgUsed+requested > int64(limits.OrgMaxVCPUs) {
			return nil, stats, orgConnectionAdmissionOutcomeBlockedOrgVCPU, nil
		}

		return head, stats, orgConnectionAdmissionOutcomeGranted, nil
	}

	if stats.userLimitSkips > 0 {
		return nil, stats, orgConnectionAdmissionOutcomeBlockedUserVCPU, nil
	}
	return nil, stats, orgConnectionAdmissionOutcomeWaiting, nil
}

func (cs *ConfigStore) pendingOrgConnectionUserQueueHeads(tx *gorm.DB, queueTable, orgID string, now time.Time) ([]OrgConnectionQueueEntry, orgConnectionAdmissionStats, error) {
	var stats orgConnectionAdmissionStats
	if err := tx.Table(queueTable).
		Where("org_id = ? AND granted_at IS NULL AND expires_at > ?", orgID, now).
		Count(&stats.queueDepth).Error; err != nil {
		return nil, stats, err
	}

	var heads []OrgConnectionQueueEntry
	if err := tx.Raw(
		"SELECT * FROM ("+
			"SELECT DISTINCT ON (username) * FROM "+queueTable+" "+
			"WHERE org_id = ? AND granted_at IS NULL AND expires_at > ? "+
			"ORDER BY username ASC, enqueued_at ASC, request_id ASC"+
			") AS user_heads ORDER BY enqueued_at ASC, request_id ASC",
		orgID, now,
	).Scan(&heads).Error; err != nil {
		return nil, stats, err
	}
	stats.userQueues = len(heads)
	return heads, stats, nil
}

func (cs *ConfigStore) activeOrgConnectionLeaseVCPUUsage(tx *gorm.DB, orgID string) (int64, map[string]int64, int64, error) {
	orgUsed, err := cs.sumActiveOrgConnectionLeaseVCPUs(tx, orgID)
	if err != nil {
		return 0, nil, 0, err
	}

	type userUsageRow struct {
		Username string
		VCPUs    int64 `gorm:"column:vcpus"`
	}
	var rows []userUsageRow
	leaseTable := cs.runtimeTable((&OrgConnectionLease{}).TableName())
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())
	if err := tx.Table(leaseTable+" AS l").
		Select("COALESCE(l.username, '') AS username, COALESCE(SUM(CASE WHEN l.requested_vcpus > 0 THEN l.requested_vcpus ELSE ? END), 0) AS vcpus", legacyOrgConnectionRequestedVCPUs).
		Joins("LEFT JOIN "+cpTable+" AS cp ON cp.id = l.cp_instance_id").
		Where("l.org_id = ?", orgID).
		Where("cp.id IS NULL OR cp.state <> ?", ControlPlaneInstanceStateExpired).
		Group("COALESCE(l.username, '')").
		Scan(&rows).Error; err != nil {
		return 0, nil, 0, err
	}

	userUsed := make(map[string]int64, len(rows))
	var legacyUserUsed int64
	for _, row := range rows {
		if row.Username == "" {
			legacyUserUsed += row.VCPUs
			continue
		}
		userUsed[row.Username] += row.VCPUs
	}
	return orgUsed, userUsed, legacyUserUsed, nil
}

func (cs *ConfigStore) createOrgConnectionLease(tx *gorm.DB, request *OrgConnectionQueueEntry, now time.Time) (*OrgConnectionLease, error) {
	granted := now
	created := &OrgConnectionLease{
		LeaseID:        request.RequestID,
		RequestID:      request.RequestID,
		OrgID:          request.OrgID,
		Username:       request.Username,
		CPInstanceID:   request.CPInstanceID,
		PID:            request.PID,
		Protocol:       request.Protocol,
		RequestedVCPUs: request.RequestedVCPUs,
		AcquiredAt:     now,
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

// CancelOrgConnectionRequest removes a request whose owner gave up before
// Acquire returned. If acquisition committed but its response was lost, the
// owner still has no lease handle, so cancellation must also reclaim that
// unclaimed lease.
func (cs *ConfigStore) CancelOrgConnectionRequest(requestID string, _ time.Time) error {
	if strings.TrimSpace(requestID) == "" {
		return nil
	}
	tables := cs.orgConnectionRuntimeTables()
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		request, found, err := cs.lockOrgConnectionRequest(tx, tables.queue, requestID)
		if err != nil || !found {
			return err
		}
		if request.GrantedAt != nil {
			if err := tx.Table(tables.lease).
				Where("request_id = ?", requestID).
				Delete(&OrgConnectionLease{}).Error; err != nil {
				return err
			}
		}
		return tx.Table(tables.queue).
			Where("request_id = ?", requestID).
			Delete(&OrgConnectionQueueEntry{}).Error
	})
	if err != nil {
		return fmt.Errorf("cancel org connection request: %w", err)
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

func (cs *ConfigStore) sumActiveOrgConnectionLeaseVCPUs(tx *gorm.DB, orgID string) (int64, error) {
	return cs.sumActiveConnectionLeaseVCPUs(tx, "l.org_id = ?", orgID)
}

func (cs *ConfigStore) sumActiveConnectionLeaseVCPUs(tx *gorm.DB, where string, args ...any) (int64, error) {
	var total int64
	leaseTable := cs.runtimeTable((&OrgConnectionLease{}).TableName())
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())
	query := tx.Table(leaseTable+" AS l").
		Select("COALESCE(SUM(CASE WHEN l.requested_vcpus > 0 THEN l.requested_vcpus ELSE ? END), 0)", legacyOrgConnectionRequestedVCPUs).
		Joins("LEFT JOIN "+cpTable+" AS cp ON cp.id = l.cp_instance_id").
		Where(where, args...).
		Where("cp.id IS NULL OR cp.state <> ?", ControlPlaneInstanceStateExpired)
	if err := query.Scan(&total).Error; err != nil {
		return 0, err
	}
	return total, nil
}
