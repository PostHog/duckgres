//go:build linux || darwin

package configstore_test

import (
	"database/sql"
	"testing"
	"time"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
)

// TestAdmissionOfferProtocolSteadyStateDoesNotSerializeAcrossOrgs proves the
// enabled protocol row is a read-only fast path. Holding a row lock on the
// singleton must not stall otherwise-independent per-org dispatchers.
func TestAdmissionOfferProtocolSteadyStateDoesNotSerializeAcrossOrgs(t *testing.T) {
	storeA, storeB, _, _ := newSharedConfigStores(t)
	upsertActiveCP(t, storeA, "cp-a")

	if err := storeA.ActivateOrgConnectionAdmissionOffers(); err != nil {
		t.Fatalf("activate admission offer protocol: %v", err)
	}

	type requestSpec struct {
		store     *cpconfigstore.ConfigStore
		orgID     string
		username  string
		requestID string
	}
	requests := []requestSpec{
		{store: storeA, orgID: "org-protocol-fast-path-a", username: "alice", requestID: "request-protocol-fast-path-a"},
		{store: storeB, orgID: "org-protocol-fast-path-b", username: "bob", requestID: "request-protocol-fast-path-b"},
	}
	now := time.Now()
	for i, spec := range requests {
		seedAuthoritativeOrgConnectionLimits(t, storeA, spec.orgID, 1, map[string]int{spec.username: 0})
		if err := storeA.EnqueueOrgConnectionRequest(&cpconfigstore.OrgConnectionQueueEntry{
			RequestID: spec.requestID, OrgID: spec.orgID, Username: spec.username, CPInstanceID: "cp-a",
			PID: int32(1001 + i), Protocol: "postgres", RequestedVCPUs: 1,
			EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
		}); err != nil {
			t.Fatalf("enqueue %s: %v", spec.requestID, err)
		}
	}

	sqlDB, err := storeA.DB().DB()
	if err != nil {
		t.Fatalf("store sql db: %v", err)
	}
	protocolBlocker, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin protocol-row blocker: %v", err)
	}
	defer func() { _ = protocolBlocker.Rollback() }()
	if _, err := protocolBlocker.Exec(
		"SELECT id FROM " + storeA.RuntimeSchema() + ".org_connection_admission_protocol WHERE id = 1 FOR UPDATE",
	); err != nil {
		t.Fatalf("lock enabled protocol row: %v", err)
	}

	type dispatchResult struct {
		orgID   string
		offered int
		err     error
	}
	done := make(chan dispatchResult, len(requests))
	for _, spec := range requests {
		spec := spec
		go func() {
			offered, err := spec.store.DispatchOrgConnectionAdmissions(spec.orgID)
			done <- dispatchResult{orgID: spec.orgID, offered: offered, err: err}
		}()
	}

	results := make([]dispatchResult, 0, len(requests))
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for len(results) < len(requests) {
		select {
		case result := <-done:
			results = append(results, result)
		case <-timer.C:
			// Release the deliberate blocker before failing so the dispatcher
			// goroutines can finish cleanly before schema teardown.
			_ = protocolBlocker.Rollback()
			for len(results) < len(requests) {
				select {
				case result := <-done:
					results = append(results, result)
				case <-time.After(5 * time.Second):
					t.Fatalf("dispatchers remained stuck after releasing protocol-row lock; completed %d/%d", len(results), len(requests))
				}
			}
			t.Fatal("steady-state cross-org dispatch blocked on the enabled admission protocol row")
		}
	}

	for _, result := range results {
		if result.err != nil || result.offered != 1 {
			t.Fatalf("dispatch %s = %d, err = %v; want one offer", result.orgID, result.offered, result.err)
		}
	}
	if err := protocolBlocker.Rollback(); err != nil && err != sql.ErrTxDone {
		t.Fatalf("release protocol-row blocker: %v", err)
	}
	for _, spec := range requests {
		assertOrgConnectionOffer(t, storeA, spec.requestID)
	}
}

// TestCancelOrgConnectionRequestReclaimsAcquireThatCommitsFirst exercises the
// overlapping acquire-first half of the acquire/cancel race. The lease-table
// unique-key conflict stops acquisition after it has locked the queue row. This
// proves cancellation waits for the acquisition transaction and then observes
// its committed grant before reclaiming both rows.
func TestCancelOrgConnectionRequestReclaimsAcquireThatCommitsFirst(t *testing.T) {
	storeA, storeB, appA, appB := newSharedConfigStores(t)
	upsertActiveCP(t, storeA, "cp-a")

	now := time.Now()
	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-acquire-wins-cancel-race",
		OrgID:          "org-acquire-wins-cancel-race",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := storeA.EnqueueOrgConnectionRequest(request); err != nil {
		t.Fatalf("enqueue request: %v", err)
	}

	sqlDB, err := storeA.DB().DB()
	if err != nil {
		t.Fatalf("store sql db: %v", err)
	}
	leaseInsertBlocker, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin lease-insert blocker: %v", err)
	}
	defer func() { _ = leaseInsertBlocker.Rollback() }()

	var blockerPID int
	if err := leaseInsertBlocker.QueryRow("SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
		t.Fatalf("get lease-insert blocker pid: %v", err)
	}
	// The uncommitted duplicate is invisible to cleanup and the existing-lease
	// read, but makes the eventual unique insert wait after TryAcquire has
	// locked the queue row. Rolling it back lets the real insert win.
	if _, err := leaseInsertBlocker.Exec(
		"INSERT INTO "+storeA.RuntimeSchema()+".org_connection_leases "+
			"(lease_id, request_id, org_id, username, cp_instance_id, p_id, protocol, requested_vcpus, acquired_at, created_at, updated_at) "+
			"VALUES ($1, $1, $2, $3, $4, $5, $6, $7, $8, $8, $8)",
		request.RequestID,
		request.OrgID,
		request.Username,
		request.CPInstanceID,
		request.PID,
		request.Protocol,
		request.RequestedVCPUs,
		now,
	); err != nil {
		t.Fatalf("insert blocking lease: %v", err)
	}

	acquireDone := make(chan acquireResult, 1)
	go func() {
		lease, err := storeA.TryAcquireOrgConnectionLease(
			request.RequestID,
			cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1},
			now,
		)
		acquireDone <- acquireResult{lease: lease, err: err}
	}()

	// TryAcquire locks the queue row before inserting the lease. Waiting on the
	// duplicate therefore proves it already owns the queue-row lock.
	acquirePID := waitForBlockedApplicationPID(t, sqlDB, blockerPID, appA)

	cancelDone := make(chan error, 1)
	go func() {
		cancelDone <- storeB.CancelOrgConnectionRequest(request.RequestID, now)
	}()
	// Cancel's SELECT FOR UPDATE must now wait for the acquisition transaction,
	// not race ahead and turn this into the already-covered cancel-first case.
	waitForBlockedApplicationPID(t, sqlDB, acquirePID, appB)

	if err := leaseInsertBlocker.Rollback(); err != nil {
		t.Fatalf("release lease-insert blocker: %v", err)
	}

	result := waitForAcquireResult(t, acquireDone, "acquire-first transaction")
	if result.err != nil {
		t.Fatalf("acquire request: %v", result.err)
	}
	if result.lease == nil || result.lease.RequestID != request.RequestID {
		t.Fatalf("acquire result = %#v, want request's own lease", result.lease)
	}

	select {
	case err := <-cancelDone:
		if err != nil {
			t.Fatalf("cancel request after acquire commit: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancellation behind acquire")
	}

	assertOrgConnectionRequestAbsent(t, storeA, request.RequestID)
	assertOrgConnectionActiveVCPUs(t, storeA, request.OrgID, 0)
}

// TestOrgConnectionSchedulerOwnerClaimsOfferWithoutReevaluation pins the
// scheduler/owner handshake. A later request may run global scheduling work
// for an earlier request, but it must not receive the earlier request's lease.
// The earlier request then claims its durable offer without depending on a
// second replica-local limit evaluation.
func TestOrgConnectionSchedulerOwnerClaimsOfferWithoutReevaluation(t *testing.T) {
	storeA, storeB, _, _ := newSharedConfigStores(t)
	upsertActiveCP(t, storeA, "cp-a")
	upsertActiveCP(t, storeB, "cp-b")
	enableAdmissionOffers(t, storeA, "cp-a", "cp-b")

	const orgID = "org-offer-owner"
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 2, map[string]int{"alice": 0, "bob": 0})

	now := time.Now()
	alice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-offered-alice",
		OrgID:          orgID,
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 2,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-waiting-bob",
		OrgID:          orgID,
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	for _, request := range []*cpconfigstore.OrgConnectionQueueEntry{alice, bob} {
		if err := storeA.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}

	foreign, err := storeB.TryAcquireOrgConnectionLeaseWithLimitLookup(
		bob.RequestID,
		func(string) cpconfigstore.OrgResourceLimits {
			return cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 2}
		},
		now,
	)
	if err != nil {
		t.Fatalf("schedule from bob's poll: %v", err)
	}
	if foreign != nil {
		t.Fatalf("bob's poll returned a lease before its turn: %#v", foreign)
	}

	// Model another replica's stale local snapshot. Once Alice has an offer,
	// claiming it must not require all replicas to select Alice independently.
	owned, err := storeA.TryAcquireOrgConnectionLeaseWithLimitLookup(
		alice.RequestID,
		func(string) cpconfigstore.OrgResourceLimits {
			return cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}
		},
		now,
	)
	if err != nil {
		t.Fatalf("claim alice's offer: %v", err)
	}
	if owned == nil || owned.RequestID != alice.RequestID {
		t.Fatalf("alice claim = %#v, want Alice's own durable offer", owned)
	}
}

// TestOrgConnectionDrainStateClassifiesOffersAsQueued pins the durable state
// model consumed by reshard drain: pending requests and unclaimed offers are
// still waiting connections, while a claimed request's queue mirror must not
// be counted again beside its active lease.
func TestOrgConnectionDrainStateClassifiesOffersAsQueued(t *testing.T) {
	store := newIsolatedConfigStore(t)
	for _, cpID := range []string{"cp-a", "cp-b", "cp-c"} {
		upsertActiveCP(t, store, cpID)
	}
	enableAdmissionOffers(t, store, "cp-a", "cp-b", "cp-c")

	const orgID = "org-drain-offer-state"
	seedAuthoritativeOrgConnectionLimits(t, store, orgID, 2, map[string]int{"alice": 0, "bob": 0, "carol": 0})

	now := time.Now()
	active := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-active", OrgID: orgID, Username: "alice", CPInstanceID: "cp-a",
		PID: 1001, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	offered := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-offered", OrgID: orgID, Username: "bob", CPInstanceID: "cp-b",
		PID: 1002, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now.Add(time.Millisecond), ExpiresAt: now.Add(time.Minute),
	}
	for _, request := range []*cpconfigstore.OrgConnectionQueueEntry{active, offered} {
		if err := store.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}
	if count, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || count != 2 {
		t.Fatalf("dispatch initial offers = %d, err = %v; want 2", count, err)
	}
	lease, err := store.ClaimOrgConnectionOffer(active.RequestID, active.CPInstanceID)
	if err != nil || lease == nil {
		t.Fatalf("claim active request = %#v, err = %v", lease, err)
	}

	pending := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-pending", OrgID: orgID, Username: "carol", CPInstanceID: "cp-c",
		PID: 1003, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now.Add(2 * time.Millisecond), ExpiresAt: now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(pending); err != nil {
		t.Fatalf("enqueue pending request: %v", err)
	}

	drain, err := store.OrgConnectionDrainState(orgID)
	if err != nil {
		t.Fatalf("read org connection drain state: %v", err)
	}
	if drain.ActiveLeases != 1 || drain.QueuedConns != 2 || drain.Drained() {
		t.Fatalf("drain = %+v, want 1 active lease and 2 queued connections", drain)
	}
}

// TestOrgConnectionDrainStateSerializesWithOfferClaim pins the transition
// barrier directly. Once drain holds the org admission lock, an offer claim
// must wait until drain has classified the offer as queued and committed.
func TestOrgConnectionDrainStateSerializesWithOfferClaim(t *testing.T) {
	storeA, storeB, appA, appB := newSharedConfigStores(t)
	upsertActiveCP(t, storeA, "cp-a")
	enableAdmissionOffers(t, storeA, "cp-a")

	const orgID = "org-drain-claim-transition"
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 1, map[string]int{"alice": 0})

	now := time.Now()
	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-offer-claim-transition",
		OrgID:          orgID,
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := storeA.EnqueueOrgConnectionRequest(request); err != nil {
		t.Fatalf("enqueue request: %v", err)
	}
	if count, err := storeA.DispatchOrgConnectionAdmissions(orgID); err != nil || count != 1 {
		t.Fatalf("dispatch offer = %d, err = %v; want 1", count, err)
	}
	// This test deliberately blocks two database actors. Keep the fixture's
	// reservation alive well beyond those bounded waits so a slow CI host tests
	// serialization rather than the production five-second offer expiry.
	testExpiry := time.Now().Add(5 * time.Minute)
	result := storeA.DB().Table(storeA.RuntimeSchema()+".org_connection_queue").
		Where("request_id = ?", request.RequestID).
		Updates(map[string]any{"offer_expires_at": testExpiry, "expires_at": testExpiry})
	if result.Error != nil || result.RowsAffected != 1 {
		t.Fatalf("extend test offer expiry: rows=%d err=%v", result.RowsAffected, result.Error)
	}

	sqlDB, err := storeA.DB().DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	tableBlocker, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin lease table blocker: %v", err)
	}
	defer func() { _ = tableBlocker.Rollback() }()
	var blockerPID int
	if err := tableBlocker.QueryRow("SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
		t.Fatalf("read lease table blocker pid: %v", err)
	}
	if _, err := tableBlocker.Exec("LOCK TABLE " + storeA.RuntimeSchema() + ".org_connection_leases IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("lock lease table: %v", err)
	}

	type drainOutcome struct {
		status cpconfigstore.OrgConnectionDrainStatus
		err    error
	}
	drainResultCh := make(chan drainOutcome, 1)
	go func() {
		status, err := storeA.OrgConnectionDrainState(orgID)
		drainResultCh <- drainOutcome{status: status, err: err}
	}()
	drainPID := waitForBlockedApplicationPID(t, sqlDB, blockerPID, appA)

	type claimOutcome struct {
		lease *cpconfigstore.OrgConnectionLease
		err   error
	}
	claimResultCh := make(chan claimOutcome, 1)
	go func() {
		lease, err := storeB.ClaimOrgConnectionOffer(request.RequestID, request.CPInstanceID)
		claimResultCh <- claimOutcome{lease: lease, err: err}
	}()
	_ = waitForBlockedApplicationPID(t, sqlDB, drainPID, appB)

	if err := tableBlocker.Commit(); err != nil {
		t.Fatalf("release lease table blocker: %v", err)
	}

	var drainResult drainOutcome
	select {
	case drainResult = <-drainResultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for drain after releasing lease table blocker")
	}
	if drainResult.err != nil {
		t.Fatalf("read org connection drain state: %v", drainResult.err)
	}
	if drainResult.status.ActiveLeases != 0 || drainResult.status.QueuedConns != 1 || drainResult.status.Drained() {
		t.Fatalf("drain before serialized claim = %+v, want one queued offer", drainResult.status)
	}
	var claimResult claimOutcome
	select {
	case claimResult = <-claimResultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for claim after drain committed")
	}
	if claimResult.err != nil {
		t.Fatalf("claim offer after drain: %v", claimResult.err)
	}
	if claimResult.lease == nil || claimResult.lease.RequestID != request.RequestID {
		t.Fatalf("claim after drain = %#v, want request %q", claimResult.lease, request.RequestID)
	}

	finalDrain, err := storeA.OrgConnectionDrainState(orgID)
	if err != nil {
		t.Fatalf("read final org connection drain state: %v", err)
	}
	if finalDrain.ActiveLeases != 1 || finalDrain.QueuedConns != 0 || finalDrain.Drained() {
		t.Fatalf("drain after claim = %+v, want one active lease", finalDrain)
	}
}

// TestOrgConnectionAdmissionCleanupRunsWhileResharding protects drain
// liveness. Resharding forbids new grants, but it must not disable cleanup or
// an expired request/dead owner can keep OrgConnectionDrainState non-drained
// indefinitely.
func TestOrgConnectionAdmissionCleanupRunsWhileResharding(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *cpconfigstore.ConfigStore, *cpconfigstore.OrgConnectionQueueEntry)
	}{
		{
			name: "expired request",
			mutate: func(t *testing.T, store *cpconfigstore.ConfigStore, request *cpconfigstore.OrgConnectionQueueEntry) {
				t.Helper()
				if err := store.DB().Table(store.RuntimeSchema()+".org_connection_queue").
					Where("request_id = ?", request.RequestID).
					Update("expires_at", time.Now().Add(-time.Second)).Error; err != nil {
					t.Fatalf("expire request: %v", err)
				}
			},
		},
		{
			name: "expired owner",
			mutate: func(t *testing.T, store *cpconfigstore.ConfigStore, request *cpconfigstore.OrgConnectionQueueEntry) {
				t.Helper()
				if err := store.DB().Table(store.RuntimeSchema()+".cp_instances").
					Where("id = ?", request.CPInstanceID).
					Updates(map[string]any{
						"state":      cpconfigstore.ControlPlaneInstanceStateExpired,
						"expired_at": time.Now(),
					}).Error; err != nil {
					t.Fatalf("expire request owner: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newIsolatedConfigStore(t)
			const cpID = "cp-owner"
			upsertActiveCP(t, store, cpID)
			orgID := "org-reshard-cleanup-" + tt.name
			seedReadyWarehouse(t, store, orgID)

			now := time.Now()
			request := &cpconfigstore.OrgConnectionQueueEntry{
				RequestID:      "request-reshard-cleanup",
				OrgID:          orgID,
				Username:       "alice",
				CPInstanceID:   cpID,
				PID:            1001,
				Protocol:       "postgres",
				RequestedVCPUs: 1,
				EnqueuedAt:     now,
				ExpiresAt:      now.Add(time.Minute),
			}
			if err := store.EnqueueOrgConnectionRequest(request); err != nil {
				t.Fatalf("enqueue request: %v", err)
			}
			if err := store.SetWarehouseResharding(orgID); err != nil {
				t.Fatalf("set warehouse resharding: %v", err)
			}
			tt.mutate(t, store, request)

			lease, err := store.TryAcquireOrgConnectionLease(
				request.RequestID,
				cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1},
				now,
			)
			if err != nil {
				t.Fatalf("run admission cleanup during resharding: %v", err)
			}
			if lease != nil {
				t.Fatalf("request received lease during resharding: %#v", lease)
			}

			assertOrgConnectionRequestAbsent(t, store, request.RequestID)
			drain, err := store.OrgConnectionDrainState(orgID)
			if err != nil {
				t.Fatalf("read org connection drain state: %v", err)
			}
			if !drain.Drained() || drain.ActiveLeases != 0 || drain.QueuedConns != 0 {
				t.Fatalf("drain after cleanup = %+v, want drained", drain)
			}
		})
	}
}

func TestOrgConnectionDrainStateCleansExpiredOwnerWithoutWaiterPoll(t *testing.T) {
	store := newIsolatedConfigStore(t)
	const cpID = "cp-expired"
	upsertActiveCP(t, store, cpID)

	now := time.Now()
	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-expired-owner-drain",
		OrgID:          "org-expired-owner-drain",
		Username:       "alice",
		CPInstanceID:   cpID,
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(request); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := store.DB().Table(store.RuntimeSchema()+".cp_instances").
		Where("id = ?", cpID).
		Updates(map[string]any{
			"state":      cpconfigstore.ControlPlaneInstanceStateExpired,
			"expired_at": time.Now(),
		}).Error; err != nil {
		t.Fatalf("expire owner: %v", err)
	}

	// No TryAcquire call remains to run cleanup. The drain loop itself is the
	// recovery actor after the request owner disappears.
	drain, err := store.OrgConnectionDrainState(request.OrgID)
	if err != nil {
		t.Fatalf("drain state: %v", err)
	}
	if !drain.Drained() {
		t.Fatalf("drain after owner expiry = %+v, want drained", drain)
	}
	assertOrgConnectionRequestAbsent(t, store, request.RequestID)
}

func waitForBlockedApplicationPID(t *testing.T, db *sql.DB, blockerPID int, applicationName string) int {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var pid int
		err := db.QueryRow(`
			SELECT pid
			FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND datname = current_database()
			  AND $1 = ANY(pg_blocking_pids(pid))
			  AND application_name = $2
			ORDER BY pid
			LIMIT 1
		`, blockerPID, applicationName).Scan(&pid)
		if err == nil {
			return pid
		}
		if err != sql.ErrNoRows {
			t.Fatalf("poll blocked application %q: %v", applicationName, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for application %q to block behind pid %d", applicationName, blockerPID)
	return 0
}

func enableAdmissionOffers(t *testing.T, store *cpconfigstore.ConfigStore, cpInstanceIDs ...string) {
	t.Helper()

	result := store.DB().Table(store.RuntimeSchema()+".cp_instances").
		Where("id IN ?", cpInstanceIDs).
		Update("supports_admission_offers", true)
	if result.Error != nil {
		t.Fatalf("enable admission offers for %v: %v", cpInstanceIDs, result.Error)
	}
	if result.RowsAffected != int64(len(cpInstanceIDs)) {
		t.Fatalf("enabled admission offers for %d control planes, want %d", result.RowsAffected, len(cpInstanceIDs))
	}
	activateAdmissionOffers(t, store)
}
