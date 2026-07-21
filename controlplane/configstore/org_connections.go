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
	defaultOrgConnectionOfferTTL        = 5 * time.Second
	maxOrgConnectionOffersPerDispatch   = 64
)

// ErrAdmissionOfferProtocolActivationBlocked means the irreversible durable
// offer protocol cannot be enabled while an incompatible control plane or
// runtime row remains. Callers may surface this as an operator-actionable
// conflict and retry after the reported fleet/runtime state is reconciled.
var ErrAdmissionOfferProtocolActivationBlocked = errors.New("admission offer protocol activation blocked")

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
	if entryCopy.State == "" {
		entryCopy.State = OrgConnectionRequestStatePending
	}
	if entryCopy.State != OrgConnectionRequestStatePending {
		return fmt.Errorf("new org connection request must be pending")
	}
	entryCopy.OfferedAt = nil
	entryCopy.OfferExpiresAt = nil
	entryCopy.GrantedAt = nil
	if err := cs.db.Transaction(func(tx *gorm.DB) error {
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

// TryAcquireOrgConnectionLeaseWithLimitLookup is the rolling-upgrade adapter
// for the pre-offer API. Once the offer protocol is explicitly activated, the
// callback is ignored and limits are read directly from PostgreSQL. Before
// activation, the callback is only a fallback for test or orphan rows whose org
// no longer exists in the authoritative config tables.
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
		lease, retry, attemptStats, attemptOutcome, err := cs.scheduleAndClaimOrgConnectionLeaseOnce(requestID, "", limitLookup)
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

// ScheduleAndClaimOrgConnectionLease runs one authoritative scheduling pass
// for the request's org and then claims only this control plane's own durable
// offer. A scheduling pass may reserve capacity for other owners, but it never
// creates their active leases.
func (cs *ConfigStore) ScheduleAndClaimOrgConnectionLease(requestID, cpInstanceID string) (*OrgConnectionLease, error) {
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
		lease, retry, attemptStats, attemptOutcome, err := cs.scheduleAndClaimOrgConnectionLeaseOnce(requestID, cpInstanceID, nil)
		stats = attemptStats
		outcome = attemptOutcome
		if retry {
			continue
		}
		if err != nil {
			outcome = orgConnectionAdmissionOutcomeError
			return nil, fmt.Errorf("schedule and claim org connection lease: %w", err)
		}
		return lease, nil
	}
}

// ActivateOrgConnectionAdmissionOffers explicitly crosses the irreversible
// cluster-wide rollout boundary. It serializes with pre-activation queue and
// lease inserts through the protocol singleton, then fences CP registration
// while rechecking that no incompatible process or runtime owner remains.
func (cs *ConfigStore) ActivateOrgConnectionAdmissionOffers() error {
	tables := cs.orgConnectionRuntimeTables()
	protocolTable := cs.runtimeTable((&OrgConnectionAdmissionProtocol{}).TableName())
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())

	err := cs.db.Transaction(func(tx *gorm.DB) error {
		var protocol OrgConnectionAdmissionProtocol
		if err := tx.Table(protocolTable).
			Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", 1).
			Take(&protocol).Error; err != nil {
			return err
		}
		if protocol.OffersEnabled {
			return nil
		}

		// SHARE conflicts with the ROW EXCLUSIVE lock taken by CP INSERT/UPDATE.
		// Together with the CP trigger, this closes registration and heartbeat
		// races across the capability check and protocol update.
		if err := tx.Exec("LOCK TABLE " + cpTable + " IN SHARE MODE").Error; err != nil {
			return err
		}

		var unsupportedLiveCPs int64
		if err := tx.Table(cpTable).
			Where("state <> ? AND supports_admission_offers = ?", ControlPlaneInstanceStateExpired, false).
			Count(&unsupportedLiveCPs).Error; err != nil {
			return err
		}

		countIncompatibleOwners := func(table string) (int64, error) {
			var count int64
			err := tx.Raw(
				"SELECT COUNT(*) FROM " + table + " AS runtime_row " +
					"LEFT JOIN " + cpTable + " AS cp ON cp.id = runtime_row.cp_instance_id " +
					"WHERE cp.id IS NULL OR NOT cp.supports_admission_offers",
			).Scan(&count).Error
			return count, err
		}
		incompatibleQueueRows, err := countIncompatibleOwners(tables.queue)
		if err != nil {
			return err
		}
		incompatibleLeaseRows, err := countIncompatibleOwners(tables.lease)
		if err != nil {
			return err
		}
		if unsupportedLiveCPs != 0 || incompatibleQueueRows != 0 || incompatibleLeaseRows != 0 {
			return fmt.Errorf(
				"%w: live unsupported control planes=%d, queue rows with unsupported or missing owners=%d, lease rows with unsupported or missing owners=%d",
				ErrAdmissionOfferProtocolActivationBlocked,
				unsupportedLiveCPs,
				incompatibleQueueRows,
				incompatibleLeaseRows,
			)
		}

		now, err := cs.orgConnectionDatabaseNow(tx)
		if err != nil {
			return err
		}
		result := tx.Table(protocolTable).
			Where("id = ? AND offers_enabled = ?", 1, false).
			Updates(map[string]any{
				"offers_enabled": true,
				"enabled_at":     now,
				"updated_at":     now,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("offer protocol activation updated %d rows, want 1", result.RowsAffected)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("activate org connection admission offers: %w", err)
	}
	return nil
}

// DispatchOrgConnectionAdmissions creates a bounded FIFO batch of durable
// offers for one org. The per-org advisory transaction lock makes this a
// single-writer scheduling decision even when every control-plane replica is
// eligible to invoke it.
func (cs *ConfigStore) DispatchOrgConnectionAdmissions(orgID string) (int, error) {
	if strings.TrimSpace(orgID) == "" {
		return 0, fmt.Errorf("org connection org id is required")
	}

	tables := cs.orgConnectionRuntimeTables()
	offered := 0
	err := cs.db.Transaction(func(tx *gorm.DB) error {
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

		ready, err := cs.orgConnectionOffersEnabled(tx)
		if err != nil {
			return err
		}
		if !ready {
			return cs.revokeOrgConnectionOffersLocked(tx, tables.queue, orgID, now)
		}
		resharding, err := cs.warehouseReshardingLocked(tx, orgID)
		if err != nil || resharding {
			return err
		}

		limits, found, err := cs.authoritativeOrgConnectionLimits(tx, orgID)
		if err != nil || !found {
			return err
		}
		offered, _, _, err = cs.dispatchOrgConnectionAdmissionsLocked(tx, tables, orgID, limits, now)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("dispatch org connection admissions: %w", err)
	}
	return offered, nil
}

// ClaimOrgConnectionOffer atomically converts an unexpired offer into an
// active lease. A foreign control-plane identity is a clean miss: it cannot
// claim, cancel, or otherwise mutate the owner's reservation.
func (cs *ConfigStore) ClaimOrgConnectionOffer(requestID, cpInstanceID string) (*OrgConnectionLease, error) {
	if strings.TrimSpace(requestID) == "" {
		return nil, fmt.Errorf("org connection request id is required")
	}
	if strings.TrimSpace(cpInstanceID) == "" {
		return nil, fmt.Errorf("control-plane instance id is required")
	}

	for {
		lease, retry, err := cs.claimOrgConnectionOfferOnce(requestID, cpInstanceID)
		if retry {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("claim org connection offer: %w", err)
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

func (cs *ConfigStore) scheduleAndClaimOrgConnectionLeaseOnce(requestID, cpInstanceID string, fallbackLimitLookup func(string) OrgResourceLimits) (*OrgConnectionLease, bool, orgConnectionAdmissionStats, string, error) {
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
		if !request.ExpiresAt.After(now) || request.State == OrgConnectionRequestStateActive || request.GrantedAt != nil {
			outcome = orgConnectionAdmissionOutcomeInactive
			return nil
		}

		// Cleanup deliberately runs before this barrier. Resharding prevents
		// offers and claims, but must not pin expired queue rows and wedge drain.
		resharding, err := cs.warehouseReshardingLocked(tx, orgID)
		if err != nil {
			return err
		}
		if resharding {
			outcome = orgConnectionAdmissionOutcomeResharding
			return nil
		}

		ready, err := cs.orgConnectionOffersEnabled(tx)
		if err != nil {
			return err
		}
		if !ready {
			if err := cs.revokeOrgConnectionOffersLocked(tx, tables.queue, orgID, now); err != nil {
				return err
			}
			request, found, err = cs.lockOrgConnectionRequest(tx, tables.queue, requestID)
			if err != nil || !found {
				return err
			}
		}
		limits, authoritative, err := cs.authoritativeOrgConnectionLimits(tx, orgID)
		if err != nil {
			return err
		}
		if ready && authoritative {
			_, selectionStats, selectionOutcome, err := cs.dispatchOrgConnectionAdmissionsLocked(tx, tables, orgID, limits, now)
			stats = selectionStats
			if err != nil {
				return err
			}
			request, found, err = cs.lockOrgConnectionRequest(tx, tables.queue, requestID)
			if err != nil || !found {
				return err
			}
			lease, err = cs.claimOrgConnectionOfferLocked(tx, tables, request, ownerID, now)
			if err != nil {
				return err
			}
			if lease != nil {
				outcome = orgConnectionAdmissionOutcomeGranted
			} else if selectionOutcome == orgConnectionAdmissionOutcomeGranted {
				outcome = orgConnectionAdmissionOutcomeGrantedOther
			} else {
				outcome = selectionOutcome
			}
			return nil
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
	return lease, retryWithFreshOrg, stats, outcome, err
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

func (cs *ConfigStore) orgConnectionOffersEnabled(tx *gorm.DB) (bool, error) {
	protocolTable := cs.runtimeTable((&OrgConnectionAdmissionProtocol{}).TableName())
	var protocol OrgConnectionAdmissionProtocol
	if err := tx.Table(protocolTable).
		Where("id = ?", 1).
		Take(&protocol).Error; err != nil {
		return false, err
	}
	return protocol.OffersEnabled, nil
}

func (cs *ConfigStore) revokeOrgConnectionOffersLocked(tx *gorm.DB, queueTable, orgID string, now time.Time) error {
	return tx.Table(queueTable).
		Where("org_id = ? AND state = ? AND granted_at IS NULL", orgID, OrgConnectionRequestStateOffered).
		Updates(map[string]any{
			"state":            OrgConnectionRequestStatePending,
			"offered_at":       nil,
			"offer_expires_at": nil,
			"updated_at":       now,
		}).Error
}

func (cs *ConfigStore) dispatchOrgConnectionAdmissionsLocked(tx *gorm.DB, tables orgConnectionRuntimeTables, orgID string, limits authoritativeOrgConnectionLimitSet, now time.Time) (int, orgConnectionAdmissionStats, string, error) {
	orgUsed, userUsed, legacyUserUsed, err := cs.activeOrgConnectionLeaseVCPUUsage(tx, orgID)
	if err != nil {
		return 0, orgConnectionAdmissionStats{}, orgConnectionAdmissionOutcomeError, err
	}
	offeredOrg, offeredUsers, err := cs.reconcileOrgConnectionOffersLocked(tx, tables.queue, orgID, limits, now, orgUsed, userUsed, legacyUserUsed)
	if err != nil {
		return 0, orgConnectionAdmissionStats{}, orgConnectionAdmissionOutcomeError, err
	}
	orgUsed += offeredOrg
	for username, used := range offeredUsers {
		userUsed[username] += used
	}

	var pending []OrgConnectionQueueEntry
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())
	if err := tx.Table(tables.queue+" AS q").
		Select("q.*").
		Joins("JOIN "+cpTable+" AS cp ON cp.id = q.cp_instance_id AND cp.state = ?", ControlPlaneInstanceStateActive).
		Where("q.org_id = ? AND q.state = ? AND q.granted_at IS NULL AND q.expires_at > ?", orgID, OrgConnectionRequestStatePending, now).
		Order("q.enqueued_at ASC, q.request_id ASC").
		Find(&pending).Error; err != nil {
		return 0, orgConnectionAdmissionStats{}, orgConnectionAdmissionOutcomeError, err
	}

	stats := orgConnectionAdmissionStats{queueDepth: int64(len(pending))}
	usersSeen := make(map[string]struct{})
	for i := range pending {
		usersSeen[pending[i].Username] = struct{}{}
	}
	stats.userQueues = len(usersSeen)

	offered := 0
	outcome := orgConnectionAdmissionOutcomeWaiting
	blockedUsers := make(map[string]struct{})
	for i := range pending {
		if offered >= maxOrgConnectionOffersPerDispatch {
			break
		}
		request := &pending[i]
		if _, blocked := blockedUsers[request.Username]; blocked {
			continue
		}
		userLimit, exists := limits.users[request.Username]
		if !exists || userLimit.disabled {
			blockedUsers[request.Username] = struct{}{}
			stats.ineligibleUserSkips++
			if outcome == orgConnectionAdmissionOutcomeWaiting {
				outcome = orgConnectionAdmissionOutcomeIneligibleUser
			}
			continue
		}

		requested := int64(request.RequestedVCPUs)
		if userLimit.maxVCPUs > 0 && legacyUserUsed+userUsed[request.Username]+requested > userLimit.maxVCPUs {
			blockedUsers[request.Username] = struct{}{}
			stats.userLimitSkips++
			outcome = orgConnectionAdmissionOutcomeBlockedUserVCPU
			continue
		}
		if limits.orgMaxVCPUs > 0 && orgUsed+requested > limits.orgMaxVCPUs {
			outcome = orgConnectionAdmissionOutcomeBlockedOrgVCPU
			break
		}

		expiresAt := now.Add(defaultOrgConnectionOfferTTL)
		if request.ExpiresAt.Before(expiresAt) {
			expiresAt = request.ExpiresAt
		}
		if !expiresAt.After(now) {
			continue
		}
		result := tx.Table(tables.queue).
			Where("request_id = ? AND state = ? AND granted_at IS NULL", request.RequestID, OrgConnectionRequestStatePending).
			Updates(map[string]any{
				"state":            OrgConnectionRequestStateOffered,
				"offered_at":       now,
				"offer_expires_at": expiresAt,
				"updated_at":       now,
			})
		if result.Error != nil {
			return offered, stats, orgConnectionAdmissionOutcomeError, result.Error
		}
		if result.RowsAffected == 0 {
			continue
		}
		offered++
		orgUsed += requested
		userUsed[request.Username] += requested
		outcome = orgConnectionAdmissionOutcomeGranted
	}
	return offered, stats, outcome, nil
}

func (cs *ConfigStore) reconcileOrgConnectionOffersLocked(tx *gorm.DB, queueTable, orgID string, limits authoritativeOrgConnectionLimitSet, now time.Time, activeOrg int64, activeUsers map[string]int64, legacyUserUsed int64) (int64, map[string]int64, error) {
	var offers []OrgConnectionQueueEntry
	if err := tx.Table(queueTable).
		Where("org_id = ? AND state = ? AND granted_at IS NULL AND expires_at > ?", orgID, OrgConnectionRequestStateOffered, now).
		Order("enqueued_at ASC, request_id ASC").
		Find(&offers).Error; err != nil {
		return 0, nil, err
	}

	var offeredOrg int64
	offeredUsers := make(map[string]int64)
	blockedUsers := make(map[string]struct{})
	orgBlocked := false
	for i := range offers {
		offer := &offers[i]
		requested := int64(offer.RequestedVCPUs)
		userLimit, exists := limits.users[offer.Username]
		_, userBlocked := blockedUsers[offer.Username]
		keep := !orgBlocked && !userBlocked && exists && !userLimit.disabled
		if keep && userLimit.maxVCPUs > 0 && legacyUserUsed+activeUsers[offer.Username]+offeredUsers[offer.Username]+requested > userLimit.maxVCPUs {
			keep = false
			blockedUsers[offer.Username] = struct{}{}
		}
		if keep && limits.orgMaxVCPUs > 0 && activeOrg+offeredOrg+requested > limits.orgMaxVCPUs {
			keep = false
			orgBlocked = true
		}
		if keep {
			offeredOrg += requested
			offeredUsers[offer.Username] += requested
			continue
		}
		if err := tx.Table(queueTable).
			Where("request_id = ? AND state = ?", offer.RequestID, OrgConnectionRequestStateOffered).
			Updates(map[string]any{
				"state":            OrgConnectionRequestStatePending,
				"offered_at":       nil,
				"offer_expires_at": nil,
				"updated_at":       now,
			}).Error; err != nil {
			return 0, nil, err
		}
	}
	return offeredOrg, offeredUsers, nil
}

func (cs *ConfigStore) claimOrgConnectionOfferOnce(requestID, cpInstanceID string) (*OrgConnectionLease, bool, error) {
	tables := cs.orgConnectionRuntimeTables()
	var lease *OrgConnectionLease
	retryWithFreshOrg := false
	err := cs.db.Transaction(func(tx *gorm.DB) error {
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
			return nil
		}
		if request.CPInstanceID != cpInstanceID {
			return nil
		}
		existing, found, err := cs.existingOrgConnectionLease(tx, tables.lease, requestID)
		if err != nil {
			return err
		}
		if found {
			if existing.CPInstanceID == cpInstanceID {
				lease = existing
			}
			return nil
		}
		ready, err := cs.orgConnectionOffersEnabled(tx)
		if err != nil {
			return err
		}
		if !ready {
			return cs.revokeOrgConnectionOffersLocked(tx, tables.queue, orgID, now)
		}
		resharding, err := cs.warehouseReshardingLocked(tx, orgID)
		if err != nil || resharding {
			return err
		}
		limits, authoritative, err := cs.authoritativeOrgConnectionLimits(tx, orgID)
		if err != nil {
			return err
		}
		if !authoritative {
			return nil
		}
		if _, _, err := cs.reconcileCurrentOrgConnectionOffersLocked(tx, tables, orgID, limits, now); err != nil {
			return err
		}
		request, found, err = cs.lockOrgConnectionRequest(tx, tables.queue, requestID)
		if err != nil || !found {
			return err
		}
		lease, err = cs.claimOrgConnectionOfferLocked(tx, tables, request, cpInstanceID, now)
		return err
	})
	return lease, retryWithFreshOrg, err
}

func (cs *ConfigStore) reconcileCurrentOrgConnectionOffersLocked(tx *gorm.DB, tables orgConnectionRuntimeTables, orgID string, limits authoritativeOrgConnectionLimitSet, now time.Time) (int64, map[string]int64, error) {
	activeOrg, activeUsers, legacyUserUsed, err := cs.activeOrgConnectionLeaseVCPUUsage(tx, orgID)
	if err != nil {
		return 0, nil, err
	}
	return cs.reconcileOrgConnectionOffersLocked(tx, tables.queue, orgID, limits, now, activeOrg, activeUsers, legacyUserUsed)
}

func (cs *ConfigStore) claimOrgConnectionOfferLocked(tx *gorm.DB, tables orgConnectionRuntimeTables, request *OrgConnectionQueueEntry, cpInstanceID string, now time.Time) (*OrgConnectionLease, error) {
	if request == nil || request.CPInstanceID != cpInstanceID {
		return nil, nil
	}
	if request.State != OrgConnectionRequestStateOffered || request.GrantedAt != nil || request.OfferExpiresAt == nil || !request.OfferExpiresAt.After(now) || !request.ExpiresAt.After(now) {
		return nil, nil
	}
	var activeOwner int64
	cpTable := cs.runtimeTable((&ControlPlaneInstance{}).TableName())
	if err := tx.Table(cpTable).
		Where("id = ? AND state = ? AND supports_admission_offers = ?", cpInstanceID, ControlPlaneInstanceStateActive, true).
		Count(&activeOwner).Error; err != nil {
		return nil, err
	}
	if activeOwner != 1 {
		return nil, nil
	}
	return cs.createOrgConnectionLease(tx, request, now)
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
		Where("org_id = ? AND state = ? AND granted_at IS NULL AND expires_at > ?", orgID, OrgConnectionRequestStatePending, now).
		Count(&stats.queueDepth).Error; err != nil {
		return nil, stats, err
	}

	var heads []OrgConnectionQueueEntry
	if err := tx.Raw(
		"SELECT * FROM ("+
			"SELECT DISTINCT ON (username) * FROM "+queueTable+" "+
			"WHERE org_id = ? AND state = ? AND granted_at IS NULL AND expires_at > ? "+
			"ORDER BY username ASC, enqueued_at ASC, request_id ASC"+
			") AS user_heads ORDER BY enqueued_at ASC, request_id ASC",
		orgID, OrgConnectionRequestStatePending, now,
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
			"state":            OrgConnectionRequestStateActive,
			"granted_at":       granted,
			"offer_expires_at": nil,
			"updated_at":       now,
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
	tables := cs.orgConnectionRuntimeTables()
	var orgID string
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		resolvedOrgID, found, err := cs.orgIDForConnectionLeaseOrRequest(tx, tables, leaseID)
		if err != nil || !found {
			return err
		}
		orgID = resolvedOrgID
		if err := lockOrgConnectionAdmission(tx, orgID); err != nil {
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
	// Refill immediately for low wake latency. Dispatch is best-effort here:
	// release is already durable, and owner polling is the recovery path.
	if orgID != "" {
		_, _ = cs.DispatchOrgConnectionAdmissions(orgID)
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
	var orgID string
	err := cs.db.Transaction(func(tx *gorm.DB) error {
		resolvedOrgID, found, err := cs.orgIDForConnectionRequest(tx, tables.queue, requestID)
		if err != nil || !found {
			return err
		}
		orgID = resolvedOrgID
		if err := lockOrgConnectionAdmission(tx, orgID); err != nil {
			return err
		}
		request, found, err := cs.lockOrgConnectionRequest(tx, tables.queue, requestID)
		if err != nil || !found {
			return err
		}
		if request.OrgID != orgID {
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
	// As with release, a failed refill must not undo a successful cancellation;
	// the next waiter poll retries scheduling from durable queue state.
	if orgID != "" {
		_, _ = cs.DispatchOrgConnectionAdmissions(orgID)
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

	// During the mixed-version window an old binary can commit the legacy
	// lease+granted_at pair without updating the new state column. Repair that
	// pair before any pending/offered cleanup so a real active lease can never
	// be expired or reset as a reservation.
	if err := tx.Exec(
		"UPDATE "+queueTable+" AS q SET state = ?, offer_expires_at = NULL, updated_at = ? "+
			"WHERE q.org_id = ? AND q.granted_at IS NOT NULL AND q.state <> ? "+
			"AND EXISTS (SELECT 1 FROM "+leaseTable+" AS l WHERE l.request_id = q.request_id)",
		OrgConnectionRequestStateActive, now, orgID, OrgConnectionRequestStateActive,
	).Error; err != nil {
		return err
	}
	// Conversely, a granted marker without its atomic lease is not active.
	// Restore it to pending so the owner can retry instead of pinning a zombie
	// row until an operator intervenes.
	if err := tx.Exec(
		"UPDATE "+queueTable+" AS q SET state = ?, granted_at = NULL, offered_at = NULL, offer_expires_at = NULL, updated_at = ? "+
			"WHERE q.org_id = ? AND q.granted_at IS NOT NULL "+
			"AND NOT EXISTS (SELECT 1 FROM "+leaseTable+" AS l WHERE l.request_id = q.request_id)",
		OrgConnectionRequestStatePending, now, orgID,
	).Error; err != nil {
		return err
	}

	if err := tx.Table(queueTable).
		Where("org_id = ? AND state IN ? AND granted_at IS NULL AND expires_at <= ?", orgID, []OrgConnectionRequestState{OrgConnectionRequestStatePending, OrgConnectionRequestStateOffered}, now).
		Delete(&OrgConnectionQueueEntry{}).Error; err != nil {
		return err
	}
	if err := tx.Exec(
		"DELETE FROM "+queueTable+" AS q WHERE q.org_id = ? AND q.state IN ? AND q.granted_at IS NULL "+
			"AND NOT EXISTS (SELECT 1 FROM "+cpTable+" AS cp WHERE cp.id = q.cp_instance_id AND cp.state = ?)",
		orgID,
		[]OrgConnectionRequestState{OrgConnectionRequestStatePending, OrgConnectionRequestStateOffered},
		ControlPlaneInstanceStateActive,
	).Error; err != nil {
		return err
	}
	if err := tx.Table(queueTable).
		Where("org_id = ? AND state = ? AND granted_at IS NULL AND (offer_expires_at IS NULL OR offer_expires_at <= ?)", orgID, OrgConnectionRequestStateOffered, now).
		Updates(map[string]any{
			"state":            OrgConnectionRequestStatePending,
			"offered_at":       nil,
			"offer_expires_at": nil,
			"updated_at":       now,
		}).Error; err != nil {
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
		"DELETE FROM "+queueTable+" AS q WHERE q.org_id = ? AND q.state = ? "+
			"AND NOT EXISTS (SELECT 1 FROM "+leaseTable+" AS l WHERE l.request_id = q.request_id)",
		orgID, OrgConnectionRequestStateActive,
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
