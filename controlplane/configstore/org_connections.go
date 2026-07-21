package configstore

import (
	"context"
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

// ErrOrgConnectionAdmissionRejected identifies a request that can never fit
// under its configured hard vCPU ceiling. Temporary saturation does not wrap
// this sentinel; those requests stay queued until capacity becomes available.
var ErrOrgConnectionAdmissionRejected = errors.New("org connection admission rejected")

type OrgConnectionAdmissionRejectionReason string

const (
	OrgConnectionAdmissionRejectedOrgVCPU  OrgConnectionAdmissionRejectionReason = "org_vcpu"
	OrgConnectionAdmissionRejectedUserVCPU OrgConnectionAdmissionRejectionReason = "user_vcpu"
)

// OrgConnectionAdmissionRejectedError carries the stable reason and values
// needed to return an actionable PostgreSQL configuration-limit error.
type OrgConnectionAdmissionRejectedError struct {
	Reason         OrgConnectionAdmissionRejectionReason
	RequestedVCPUs int
	MaximumVCPUs   int
}

func (e *OrgConnectionAdmissionRejectedError) Error() string {
	if e == nil {
		return ErrOrgConnectionAdmissionRejected.Error()
	}
	return fmt.Sprintf("%s: requested %d vCPUs exceeds %s maximum of %d vCPUs", ErrOrgConnectionAdmissionRejected, e.RequestedVCPUs, e.Reason, e.MaximumVCPUs)
}

func (e *OrgConnectionAdmissionRejectedError) Unwrap() error {
	return ErrOrgConnectionAdmissionRejected
}

// EnqueueOrgConnectionRequest inserts a pending cluster-wide connection
// admission request. FIFO ordering is scoped to org_id and ordered by
// enqueued_at, then request_id. RequestedVCPUs is charged against active
// resource leases when the request is granted.
func (cs *ConfigStore) EnqueueOrgConnectionRequest(entry *OrgConnectionQueueEntry) error {
	return cs.EnqueueOrgConnectionRequestContext(context.Background(), entry)
}

// EnqueueOrgConnectionRequestContext is the context-aware production path.
// In particular, control-plane drain can interrupt a request waiting for the
// per-org admission lock before it has entered the durable queue.
func (cs *ConfigStore) EnqueueOrgConnectionRequestContext(ctx context.Context, entry *OrgConnectionQueueEntry) error {
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
	entryCopy.GrantedAt = nil
	if err := cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockOrgConnectionAdmission(tx, entryCopy.OrgID); err != nil {
			return err
		}
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
// cluster-wide per-org and per-user vCPU budgets. It is retained for callers
// that do not yet pass their control-plane identity explicitly. New runtime
// callers should use ScheduleAndClaimOrgConnectionLease so owner validation is
// part of the claim transaction.
func (cs *ConfigStore) TryAcquireOrgConnectionLease(requestID string, limits OrgResourceLimits, now time.Time) (*OrgConnectionLease, error) {
	return cs.TryAcquireOrgConnectionLeaseWithLimitLookup(requestID, func(string) OrgResourceLimits {
		return limits
	}, now)
}

// TryAcquireOrgConnectionLeaseWithLimitLookup is the compatibility adapter for
// callers that supply their own limit snapshot. Production callers use
// ScheduleAndClaimOrgConnectionLease, which reads authoritative limits inside
// the serialized PostgreSQL transaction.
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
		lease, retry, attemptStats, attemptOutcome, err := cs.scheduleAndClaimOrgConnectionLeaseOnce(context.Background(), requestID, "", limitLookup)
		stats = attemptStats
		outcome = attemptOutcome
		if retry {
			continue
		}
		if err != nil {
			if !errors.Is(err, ErrOrgConnectionAdmissionRejected) {
				outcome = orgConnectionAdmissionOutcomeError
			}
			return nil, fmt.Errorf("try acquire org connection lease: %w", err)
		}
		return lease, nil
	}
}

// ScheduleAndClaimOrgConnectionLease runs one authoritative admission
// evaluation and can create only the caller's own lease. It never reserves or
// mutates another request.
func (cs *ConfigStore) ScheduleAndClaimOrgConnectionLease(requestID, cpInstanceID string) (*OrgConnectionLease, error) {
	return cs.ScheduleAndClaimOrgConnectionLeaseContext(context.Background(), requestID, cpInstanceID)
}

// ScheduleAndClaimOrgConnectionLeaseContext is the context-aware production
// path. PostgreSQL lock waits and queries are canceled when the client goes
// away or the owning control plane starts draining.
func (cs *ConfigStore) ScheduleAndClaimOrgConnectionLeaseContext(ctx context.Context, requestID, cpInstanceID string) (*OrgConnectionLease, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("org connection request id is required")
	}
	if strings.TrimSpace(cpInstanceID) == "" {
		return nil, fmt.Errorf("control-plane instance id is required")
	}

	start := time.Now()
	outcome := orgConnectionAdmissionOutcomeWaiting
	var stats orgConnectionAdmissionStats
	defer func() {
		observeOrgConnectionAdmission(time.Since(start), outcome, stats)
	}()

	for {
		if err := ctx.Err(); err != nil {
			outcome = orgConnectionAdmissionOutcomeError
			return nil, err
		}
		lease, retry, attemptStats, attemptOutcome, err := cs.scheduleAndClaimOrgConnectionLeaseOnce(ctx, requestID, cpInstanceID, nil)
		stats = attemptStats
		outcome = attemptOutcome
		if retry {
			continue
		}
		if err != nil {
			if !errors.Is(err, ErrOrgConnectionAdmissionRejected) {
				outcome = orgConnectionAdmissionOutcomeError
			}
			return nil, fmt.Errorf("schedule and claim org connection lease: %w", err)
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

func (cs *ConfigStore) scheduleAndClaimOrgConnectionLeaseOnce(ctx context.Context, requestID, cpInstanceID string, fallbackLimitLookup func(string) OrgResourceLimits) (*OrgConnectionLease, bool, orgConnectionAdmissionStats, string, error) {
	tables := cs.orgConnectionRuntimeTables()
	var lease *OrgConnectionLease
	var rejection *OrgConnectionAdmissionRejectedError
	retryWithFreshOrg := false
	var stats orgConnectionAdmissionStats
	outcome := orgConnectionAdmissionOutcomeMissing

	err := cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		orgID, found, err := cs.orgIDForConnectionRequest(tx, tables.queue, requestID)
		if err != nil || !found {
			return err
		}
		if err := lockOrgConnectionAdmission(tx, orgID); err != nil {
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
			outcome = orgConnectionAdmissionOutcomeRetry
			return nil
		}
		ownerID := cpInstanceID
		if ownerID == "" {
			ownerID = request.CPInstanceID
		}
		if request.CPInstanceID != ownerID {
			outcome = orgConnectionAdmissionOutcomeWaiting
			return nil
		}

		ownerActive, err := cs.lockActiveControlPlaneOwner(tx, ownerID)
		if err != nil {
			return err
		}
		if !ownerActive {
			outcome = orgConnectionAdmissionOutcomeInactive
			return nil
		}
		existing, found, err := cs.existingOrgConnectionLease(tx, tables.lease, requestID)
		if err != nil || found {
			if found && existing.CPInstanceID == ownerID {
				lease = existing
			}
			if found {
				outcome = orgConnectionAdmissionOutcomeAlreadyGranted
			}
			return err
		}
		if !request.ExpiresAt.After(now) || request.GrantedAt != nil {
			outcome = orgConnectionAdmissionOutcomeInactive
			return nil
		}

		// Cleanup deliberately runs before this barrier. Resharding prevents
		// grants, but must not pin expired queue rows and wedge drain.
		resharding, err := cs.warehouseReshardingLocked(tx, orgID)
		if err != nil {
			return err
		}
		if resharding {
			outcome = orgConnectionAdmissionOutcomeResharding
			return nil
		}

		limits, authoritative, err := cs.authoritativeOrgConnectionLimits(tx, orgID)
		if err != nil {
			return err
		}
		if !authoritative && fallbackLimitLookup == nil {
			// The explicit production API must never reinterpret a deleted or
			// otherwise missing org as "unlimited". Legacy callback fallback is
			// retained only for compatibility callers and isolated old fixtures.
			outcome = orgConnectionAdmissionOutcomeInactive
			return nil
		}

		limitLookup := fallbackLimitLookup
		var userAllowed func(string) bool
		if authoritative {
			limitLookup = limits.lookup
			userAllowed = limits.userAllowed
		}
		if limitLookup == nil {
			limitLookup = func(string) OrgResourceLimits { return OrgResourceLimits{} }
		}

		// Reject only requests that cannot fit even when the org and user are
		// otherwise idle. Capacity consumed by active leases is temporary and
		// remains ordinary queueing. The delete and rejection decision commit in
		// this transaction; returning the typed error from inside the callback
		// would roll the delete back.
		requestLimits := limitLookup(request.Username)
		switch {
		case requestLimits.OrgMaxVCPUs > 0 && request.RequestedVCPUs > requestLimits.OrgMaxVCPUs:
			rejection = &OrgConnectionAdmissionRejectedError{
				Reason:         OrgConnectionAdmissionRejectedOrgVCPU,
				RequestedVCPUs: request.RequestedVCPUs,
				MaximumVCPUs:   requestLimits.OrgMaxVCPUs,
			}
			outcome = orgConnectionAdmissionOutcomeRejectedOrgVCPU
		case requestLimits.UserMaxVCPUs > 0 && request.RequestedVCPUs > requestLimits.UserMaxVCPUs:
			rejection = &OrgConnectionAdmissionRejectedError{
				Reason:         OrgConnectionAdmissionRejectedUserVCPU,
				RequestedVCPUs: request.RequestedVCPUs,
				MaximumVCPUs:   requestLimits.UserMaxVCPUs,
			}
			outcome = orgConnectionAdmissionOutcomeRejectedUserVCPU
		}
		if rejection != nil {
			return tx.Table(tables.queue).
				Where("request_id = ? AND granted_at IS NULL", requestID).
				Delete(&OrgConnectionQueueEntry{}).Error
		}

		next, selectionStats, selectionOutcome, err := cs.nextEligibleOrgConnectionRequestLocked(tx, tables, orgID, limitLookup, userAllowed, now)
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
	if err == nil && rejection != nil {
		err = rejection
	}
	return lease, retryWithFreshOrg, stats, outcome, err
}

// lockActiveControlPlaneOwner closes the race between cleanup's active-owner
// snapshot and lease creation. The SHARE row lock conflicts with draining and
// expiry updates, so either the state transition commits first and admission
// observes a non-active owner, or the lease commits before the owner can leave
// active state.
func (cs *ConfigStore) lockActiveControlPlaneOwner(tx *gorm.DB, cpInstanceID string) (bool, error) {
	var owner struct {
		State ControlPlaneInstanceState
	}
	err := tx.Table(cs.runtimeTable((&ControlPlaneInstance{}).TableName())).
		Select("state").
		Clauses(clause.Locking{Strength: "SHARE"}).
		Where("id = ?", cpInstanceID).
		Take(&owner).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return owner.State == ControlPlaneInstanceStateActive, nil
}

func lockOrgConnectionAdmission(tx *gorm.DB, orgID string) error {
	return tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:org-connections:"+orgID)).Error
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

type authoritativeOrgConnectionUserLimit struct {
	maxVCPUs int64
	disabled bool
}

type authoritativeOrgConnectionLimitSet struct {
	orgMaxVCPUs int64
	users       map[string]authoritativeOrgConnectionUserLimit
}

func (l authoritativeOrgConnectionLimitSet) lookup(username string) OrgResourceLimits {
	user := l.users[username]
	return OrgResourceLimits{
		OrgMaxVCPUs:  int(l.orgMaxVCPUs),
		UserMaxVCPUs: int(user.maxVCPUs),
	}
}

func (l authoritativeOrgConnectionLimitSet) userAllowed(username string) bool {
	user, exists := l.users[username]
	return exists && !user.disabled
}

func (cs *ConfigStore) authoritativeOrgConnectionLimits(tx *gorm.DB, orgID string) (authoritativeOrgConnectionLimitSet, bool, error) {
	type orgLimitRow struct {
		MaxVCPUs *int64 `gorm:"column:max_vcpus"`
	}
	var orgRow orgLimitRow
	if err := tx.Model(&Org{}).
		Select("max_vcpus").
		Where("name = ?", orgID).
		Take(&orgRow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return authoritativeOrgConnectionLimitSet{}, false, nil
		}
		return authoritativeOrgConnectionLimitSet{}, false, err
	}

	limits := authoritativeOrgConnectionLimitSet{
		users: make(map[string]authoritativeOrgConnectionUserLimit),
	}
	if orgRow.MaxVCPUs != nil {
		limits.orgMaxVCPUs = *orgRow.MaxVCPUs
	}
	if limits.orgMaxVCPUs < 0 {
		return authoritativeOrgConnectionLimitSet{}, false, fmt.Errorf("org %q has invalid negative max_vcpus", orgID)
	}

	type userLimitRow struct {
		Username string `gorm:"column:username"`
		MaxVCPUs *int64 `gorm:"column:max_vcpus"`
		Disabled bool   `gorm:"column:disabled"`
	}
	var users []userLimitRow
	if err := tx.Model(&OrgUser{}).
		Select("username, max_vcpus, disabled").
		Where("org_id = ?", orgID).
		Scan(&users).Error; err != nil {
		return authoritativeOrgConnectionLimitSet{}, false, err
	}
	for _, row := range users {
		var maxVCPUs int64
		if row.MaxVCPUs != nil {
			maxVCPUs = *row.MaxVCPUs
		}
		if maxVCPUs < 0 {
			return authoritativeOrgConnectionLimitSet{}, false, fmt.Errorf("org %q user %q has invalid negative max_vcpus", orgID, row.Username)
		}
		limits.users[row.Username] = authoritativeOrgConnectionUserLimit{
			maxVCPUs: maxVCPUs,
			disabled: row.Disabled,
		}
	}
	return limits, true, nil
}

func (cs *ConfigStore) nextEligibleOrgConnectionRequestLocked(tx *gorm.DB, tables orgConnectionRuntimeTables, orgID string, limitLookup func(string) OrgResourceLimits, userAllowed func(string) bool, now time.Time) (*OrgConnectionQueueEntry, orgConnectionAdmissionStats, string, error) {
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
		if userAllowed != nil && !userAllowed(head.Username) {
			stats.ineligibleUserSkips++
			continue
		}
		limits := limitLookup(head.Username)
		requested := int64(head.RequestedVCPUs)
		// A permanently impossible foreign head must not block unrelated
		// requests. Only that request's owner deletes and receives the typed
		// rejection; other evaluators simply skip it.
		if limits.UserMaxVCPUs > 0 && requested > int64(limits.UserMaxVCPUs) {
			continue
		}
		if limits.OrgMaxVCPUs > 0 && requested > int64(limits.OrgMaxVCPUs) {
			continue
		}
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
	if stats.ineligibleUserSkips > 0 {
		return nil, stats, orgConnectionAdmissionOutcomeIneligibleUser, nil
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
	return cs.ReleaseOrgConnectionLeaseContext(context.Background(), leaseID)
}

func (cs *ConfigStore) ReleaseOrgConnectionLeaseContext(ctx context.Context, leaseID string) error {
	if strings.TrimSpace(leaseID) == "" {
		return nil
	}
	tables := cs.orgConnectionRuntimeTables()
	err := cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		resolvedOrgID, found, err := cs.orgIDForConnectionLeaseOrRequest(tx, tables, leaseID)
		if err != nil || !found {
			return err
		}
		if err := lockOrgConnectionAdmission(tx, resolvedOrgID); err != nil {
			return err
		}
		if err := tx.Table(tables.lease).
			Where("lease_id = ?", leaseID).
			Delete(&OrgConnectionLease{}).Error; err != nil {
			return err
		}
		return tx.Table(tables.queue).
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
func (cs *ConfigStore) CancelOrgConnectionRequest(requestID string, canceledAt time.Time) error {
	return cs.CancelOrgConnectionRequestContext(context.Background(), requestID, canceledAt)
}

func (cs *ConfigStore) CancelOrgConnectionRequestContext(ctx context.Context, requestID string, _ time.Time) error {
	if strings.TrimSpace(requestID) == "" {
		return nil
	}
	tables := cs.orgConnectionRuntimeTables()
	err := cs.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		resolvedOrgID, found, err := cs.orgIDForConnectionRequest(tx, tables.queue, requestID)
		if err != nil || !found {
			return err
		}
		if err := lockOrgConnectionAdmission(tx, resolvedOrgID); err != nil {
			return err
		}
		request, found, err := cs.lockOrgConnectionRequest(tx, tables.queue, requestID)
		if err != nil || !found {
			return err
		}
		if request.OrgID != resolvedOrgID {
			return nil
		}
		if err := tx.Table(tables.lease).
			Where("request_id = ?", requestID).
			Delete(&OrgConnectionLease{}).Error; err != nil {
			return err
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

func (cs *ConfigStore) orgIDForConnectionLeaseOrRequest(tx *gorm.DB, tables orgConnectionRuntimeTables, id string) (string, bool, error) {
	var row struct {
		OrgID string
	}
	if err := tx.Table(tables.lease).
		Select("org_id").
		Where("lease_id = ?", id).
		Take(&row).Error; err == nil {
		return row.OrgID, true, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", false, err
	}
	return cs.orgIDForConnectionRequest(tx, tables.queue, id)
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

	// A granted marker without its atomic lease is not active. Restore it to
	// pending so a still-live owner can retry instead of pinning a zombie row.
	if err := tx.Exec(
		"UPDATE "+queueTable+" AS q SET granted_at = NULL, updated_at = ? "+
			"WHERE q.org_id = ? AND q.granted_at IS NOT NULL "+
			"AND NOT EXISTS (SELECT 1 FROM "+leaseTable+" AS l WHERE l.request_id = q.request_id)",
		now, orgID,
	).Error; err != nil {
		return err
	}

	if err := tx.Table(queueTable).
		Where("org_id = ? AND granted_at IS NULL AND expires_at <= ?", orgID, now).
		Delete(&OrgConnectionQueueEntry{}).Error; err != nil {
		return err
	}
	if err := tx.Exec(
		"DELETE FROM "+queueTable+" AS q WHERE q.org_id = ? AND q.granted_at IS NULL "+
			"AND NOT EXISTS (SELECT 1 FROM "+cpTable+" AS cp WHERE cp.id = q.cp_instance_id AND cp.state = ?)",
		orgID,
		ControlPlaneInstanceStateActive,
	).Error; err != nil {
		return err
	}

	if err := tx.Exec("DELETE FROM "+leaseTable+" AS l USING "+cpTable+" AS cp "+
		"WHERE l.cp_instance_id = cp.id AND l.org_id = ? AND cp.state = ?",
		orgID, ControlPlaneInstanceStateExpired).Error; err != nil {
		return err
	}

	if err := tx.Exec(
		"DELETE FROM "+leaseTable+" AS l "+
			"WHERE l.org_id = ? AND l.acquired_at <= ? "+
			"AND NOT EXISTS (SELECT 1 FROM "+cpTable+" AS cp WHERE cp.id = l.cp_instance_id)",
		orgID, now.Add(-missingOwnerOrgConnectionLeaseGrace),
	).Error; err != nil {
		return err
	}
	return tx.Exec(
		"DELETE FROM "+queueTable+" AS q WHERE q.org_id = ? AND q.granted_at IS NOT NULL "+
			"AND NOT EXISTS (SELECT 1 FROM "+leaseTable+" AS l WHERE l.request_id = q.request_id)",
		orgID,
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
