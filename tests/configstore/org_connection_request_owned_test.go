//go:build linux || darwin

package configstore_test

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	controlplane "github.com/posthog/duckgres/controlplane"
	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
)

type observedAdmissionReclaimAttempt struct {
	number   uint64
	returned <-chan error
}

type observedAdmissionReclaimStore struct {
	delegate *cpconfigstore.ConfigStore
	next     atomic.Uint64
	entered  chan observedAdmissionReclaimAttempt
}

func newObservedAdmissionReclaimStore(delegate *cpconfigstore.ConfigStore) *observedAdmissionReclaimStore {
	return &observedAdmissionReclaimStore{
		delegate: delegate,
		entered:  make(chan observedAdmissionReclaimAttempt, 64),
	}
}

func (s *observedAdmissionReclaimStore) ReclaimOrgConnectionAdmissionContext(
	ctx context.Context,
	ref cpconfigstore.OrgConnectionAdmissionRef,
) error {
	returned := make(chan error, 1)
	s.entered <- observedAdmissionReclaimAttempt{
		number:   s.next.Add(1),
		returned: returned,
	}
	err := s.delegate.ReclaimOrgConnectionAdmissionContext(ctx, ref)
	returned <- err
	close(returned)
	return err
}

func waitForAdmissionReclaimAttempt(
	t *testing.T,
	store *observedAdmissionReclaimStore,
	wantNumber uint64,
) observedAdmissionReclaimAttempt {
	t.Helper()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case attempt := <-store.entered:
		if attempt.number != wantNumber {
			t.Fatalf("reclaim attempt number = %d, want %d", attempt.number, wantNumber)
		}
		return attempt
	case <-timer.C:
		t.Fatalf("timed out waiting for reclaim attempt %d to enter", wantNumber)
		return observedAdmissionReclaimAttempt{}
	}
}

func waitForAdmissionReclaimAttemptReturn(t *testing.T, attempt observedAdmissionReclaimAttempt) error {
	t.Helper()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err, ok := <-attempt.returned:
		if !ok {
			t.Fatalf("reclaim attempt %d closed without a result", attempt.number)
		}
		return err
	case <-timer.C:
		t.Fatalf("timed out waiting for reclaim attempt %d to return", attempt.number)
		return nil
	}
}

func waitForAdmissionReclaimAttemptToBlock(
	t *testing.T,
	db *sql.DB,
	holderPID int,
	applicationName string,
	attempt observedAdmissionReclaimAttempt,
) {
	t.Helper()

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(2 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case err := <-attempt.returned:
			t.Fatalf("reclaim attempt %d returned before its advisory lock wait was observed: %v", attempt.number, err)
		default:
		}

		var waiters int
		if err := db.QueryRow(`
			SELECT count(*)
			FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND wait_event = 'advisory'
			  AND datname = current_database()
			  AND $1 = ANY(pg_blocking_pids(pid))
			  AND application_name = $2
		`, holderPID, applicationName).Scan(&waiters); err != nil {
			t.Fatalf("poll reclaim advisory lock waiter: %v", err)
		}
		if waiters > 0 {
			select {
			case err := <-attempt.returned:
				t.Fatalf("reclaim attempt %d returned while confirming its advisory lock wait: %v", attempt.number, err)
			default:
				return
			}
		}

		select {
		case err := <-attempt.returned:
			t.Fatalf("reclaim attempt %d returned before its advisory lock wait was observed: %v", attempt.number, err)
		case <-poll.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for reclaim attempt %d to block on the advisory lock", attempt.number)
		}
	}
}

func TestAdmissionReclaimerEventuallyRestoresCapacityWhileOwnerActive(t *testing.T) {
	storeA, storeB, _, appB := newSharedConfigStores(t)
	const cpID = "cp-reclaimer-active"
	const orgID = "org-reclaimer-eventual"
	upsertActiveCP(t, storeA, cpID)
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 2, map[string]int{"alice": 0})

	now := time.Now()
	active := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-reclaimer-active", OrgID: orgID, Username: "alice", CPInstanceID: cpID,
		PID: 1001, Protocol: "postgres", RequestedVCPUs: 2,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	waiting := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-reclaimer-waiting", OrgID: orgID, Username: "alice", CPInstanceID: cpID,
		PID: 1002, Protocol: "postgres", RequestedVCPUs: 2,
		EnqueuedAt: now.Add(time.Millisecond), ExpiresAt: now.Add(time.Minute),
	}
	for _, request := range []*cpconfigstore.OrgConnectionQueueEntry{active, waiting} {
		if err := storeA.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}
	activeRef := cpconfigstore.OrgConnectionAdmissionRef{
		RequestID: active.RequestID, OrgID: active.OrgID, CPInstanceID: active.CPInstanceID,
	}
	waitingRef := cpconfigstore.OrgConnectionAdmissionRef{
		RequestID: waiting.RequestID, OrgID: waiting.OrgID, CPInstanceID: waiting.CPInstanceID,
	}
	lease, err := storeA.ScheduleAndClaimOrgConnectionLeaseForRef(activeRef)
	if err != nil || lease == nil {
		t.Fatalf("grant full-capacity request = %#v, err = %v", lease, err)
	}
	blocked, err := storeA.ScheduleAndClaimOrgConnectionLeaseForRef(waitingRef)
	if err != nil {
		t.Fatalf("evaluate request behind full capacity: %v", err)
	}
	if blocked != nil {
		t.Fatalf("request acquired despite full capacity: %#v", blocked)
	}
	assertOrgConnectionActiveVCPUs(t, storeA, orgID, 2)

	blocker := storeA.DB().Begin()
	if blocker.Error != nil {
		t.Fatalf("begin admission-lock blocker: %v", blocker.Error)
	}
	defer func() { _ = blocker.Rollback().Error }()
	var blockerPID int
	if err := blocker.Raw("SELECT pg_backend_pid()").Scan(&blockerPID).Error; err != nil {
		t.Fatalf("read blocker pid: %v", err)
	}
	if err := cpconfigstore.LockOrgConnectionAdmissionTx(blocker, orgID); err != nil {
		t.Fatalf("hold org admission lock: %v", err)
	}

	observedStore := newObservedAdmissionReclaimStore(storeB)
	reclaimer := controlplane.NewAdmissionReclaimer(observedStore, controlplane.AdmissionReclaimerConfig{
		MaxWorkers:     1,
		AttemptTimeout: 50 * time.Millisecond,
		RetryBackoff:   func(int) time.Duration { return 25 * time.Millisecond },
	})
	t.Cleanup(reclaimer.Shutdown)
	if err := reclaimer.Submit(activeRef, controlplane.AdmissionReclaimCauseLeaseRelease); err != nil {
		t.Fatalf("submit admission reclaim: %v", err)
	}

	// Prove that the first exact reclaim call reaches PostgreSQL and blocks on
	// the org advisory lock, then use the store wrapper's entry/return events to
	// synchronize retries without inferring attempt boundaries from backend PID
	// disappearance and reappearance.
	firstAttempt := waitForAdmissionReclaimAttempt(t, observedStore, 1)
	sqlDB := configStoreSQLDB(t, storeA)
	waitForAdmissionReclaimAttemptToBlock(t, sqlDB, blockerPID, appB, firstAttempt)
	if err := waitForAdmissionReclaimAttemptReturn(t, firstAttempt); err == nil {
		t.Fatal("first reclaim attempt returned nil while org admission lock remained held")
	}
	secondAttempt := waitForAdmissionReclaimAttempt(t, observedStore, 2)
	if err := waitForAdmissionReclaimAttemptReturn(t, secondAttempt); err == nil {
		t.Fatal("second reclaim attempt returned nil while org admission lock remained held")
	}
	thirdAttempt := waitForAdmissionReclaimAttempt(t, observedStore, 3)

	if err := blocker.Commit().Error; err != nil {
		t.Fatalf("release org admission lock: %v", err)
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	if err := reclaimer.DrainAndClose(drainCtx); err != nil {
		t.Fatalf("drain reclaimer after lock recovery: %v", err)
	}
	for attempt := thirdAttempt; ; attempt = waitForAdmissionReclaimAttempt(t, observedStore, attempt.number+1) {
		if err := waitForAdmissionReclaimAttemptReturn(t, attempt); err == nil {
			break
		}
	}

	assertOrgConnectionRequestAbsent(t, storeA, active.RequestID)
	assertOrgConnectionActiveVCPUs(t, storeA, orgID, 0)
	owner, err := storeA.GetControlPlaneInstance(cpID)
	if err != nil {
		t.Fatalf("read active control-plane owner: %v", err)
	}
	if owner.State != cpconfigstore.ControlPlaneInstanceStateActive {
		t.Fatalf("owner state after retry = %q, want active", owner.State)
	}

	replacementLease, err := storeA.ScheduleAndClaimOrgConnectionLeaseForRef(waitingRef)
	if err != nil || replacementLease == nil {
		t.Fatalf("admit waiting request after eventual reclaim = %#v, err = %v", replacementLease, err)
	}
}

func TestReclaimOrgConnectionAdmissionIsExactAtomicAndIdempotent(t *testing.T) {
	store := newIsolatedConfigStore(t)
	const cpID = "cp-reclaim-owner"
	const orgID = "org-reclaim-exact"
	upsertActiveCP(t, store, cpID)
	seedAuthoritativeOrgConnectionLimits(t, store, orgID, 1, map[string]int{"alice": 0})

	now := time.Now()
	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-reclaim-exact", OrgID: orgID, Username: "alice", CPInstanceID: cpID,
		PID: 1001, Protocol: "postgres", RequestedVCPUs: 1,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(request); err != nil {
		t.Fatalf("enqueue request: %v", err)
	}
	ref := cpconfigstore.OrgConnectionAdmissionRef{
		RequestID: request.RequestID, OrgID: request.OrgID, CPInstanceID: request.CPInstanceID,
	}
	lease, err := store.ScheduleAndClaimOrgConnectionLeaseForRef(ref)
	if err != nil || lease == nil {
		t.Fatalf("grant request = %#v, err = %v", lease, err)
	}

	wrongOwner := ref
	wrongOwner.CPInstanceID = "cp-not-owner"
	if err := store.ReclaimOrgConnectionAdmissionContext(context.Background(), wrongOwner); err != nil {
		t.Fatalf("reclaim with wrong owner: %v", err)
	}
	assertOrgConnectionRequestGranted(t, store, request.RequestID)
	assertOrgConnectionActiveVCPUs(t, store, orgID, 1)

	schema := store.RuntimeSchema()
	if err := store.DB().Exec(
		"CREATE FUNCTION " + schema + ".fail_exact_reclaim_queue_delete() RETURNS trigger " +
			"LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION ''forced queue delete failure''; END'",
	).Error; err != nil {
		t.Fatalf("create reclaim rollback function: %v", err)
	}
	if err := store.DB().Exec(
		"CREATE TRIGGER fail_exact_reclaim_queue_delete BEFORE DELETE ON " + schema + ".org_connection_queue " +
			"FOR EACH ROW EXECUTE FUNCTION " + schema + ".fail_exact_reclaim_queue_delete()",
	).Error; err != nil {
		t.Fatalf("create reclaim rollback trigger: %v", err)
	}
	if err := store.ReclaimOrgConnectionAdmissionContext(context.Background(), ref); err == nil {
		t.Fatal("expected forced queue delete failure")
	}
	// Lease deletion happens first, so both rows remaining proves the transaction
	// rolled it back when queue deletion failed.
	assertOrgConnectionRequestGranted(t, store, request.RequestID)
	assertOrgConnectionActiveVCPUs(t, store, orgID, 1)
	if err := store.DB().Exec(
		"DROP TRIGGER fail_exact_reclaim_queue_delete ON " + schema + ".org_connection_queue",
	).Error; err != nil {
		t.Fatalf("drop reclaim rollback trigger: %v", err)
	}
	if err := store.DB().Exec(
		"DROP FUNCTION " + schema + ".fail_exact_reclaim_queue_delete()",
	).Error; err != nil {
		t.Fatalf("drop reclaim rollback function: %v", err)
	}

	if err := store.ReclaimOrgConnectionAdmissionContext(context.Background(), ref); err != nil {
		t.Fatalf("reclaim exact admission: %v", err)
	}
	assertOrgConnectionRequestAbsent(t, store, request.RequestID)
	assertOrgConnectionActiveVCPUs(t, store, orgID, 0)

	if err := store.ReclaimOrgConnectionAdmissionContext(context.Background(), ref); err != nil {
		t.Fatalf("repeat idempotent reclaim: %v", err)
	}
}

func TestReclaimOrgConnectionAdmissionDoesNotDeleteReusedRequest(t *testing.T) {
	storeA, storeB, _, appB := newSharedConfigStores(t)
	upsertActiveCP(t, storeA, "cp-old")
	upsertActiveCP(t, storeA, "cp-new")
	seedAuthoritativeOrgConnectionLimits(t, storeA, "org-new", 1, map[string]int{"alice": 0})

	now := time.Now()
	oldRequest := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-reused-during-reclaim", OrgID: "org-old", Username: "alice", CPInstanceID: "cp-old",
		PID: 1001, Protocol: "postgres", RequestedVCPUs: 1,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := storeA.EnqueueOrgConnectionRequest(oldRequest); err != nil {
		t.Fatalf("enqueue old request: %v", err)
	}
	oldRef := cpconfigstore.OrgConnectionAdmissionRef{
		RequestID: oldRequest.RequestID, OrgID: oldRequest.OrgID, CPInstanceID: oldRequest.CPInstanceID,
	}

	blocker := storeA.DB().Begin()
	if blocker.Error != nil {
		t.Fatalf("begin old-org lock blocker: %v", blocker.Error)
	}
	defer func() { _ = blocker.Rollback().Error }()
	var blockerPID int
	if err := blocker.Raw("SELECT pg_backend_pid()").Scan(&blockerPID).Error; err != nil {
		t.Fatalf("read blocker pid: %v", err)
	}
	if err := cpconfigstore.LockOrgConnectionAdmissionTx(blocker, oldRequest.OrgID); err != nil {
		t.Fatalf("hold old-org admission lock: %v", err)
	}

	reclaimDone := make(chan error, 1)
	go func() {
		reclaimDone <- storeB.ReclaimOrgConnectionAdmissionContext(context.Background(), oldRef)
	}()
	waitForAdvisoryLockWaiterFromApplication(t, configStoreSQLDB(t, storeA), blockerPID, appB)

	if err := storeA.DB().Table(storeA.RuntimeSchema()+".org_connection_queue").
		Where("request_id = ?", oldRequest.RequestID).
		Delete(&cpconfigstore.OrgConnectionQueueEntry{}).Error; err != nil {
		t.Fatalf("delete old request before reuse: %v", err)
	}
	newRequest := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: oldRequest.RequestID, OrgID: "org-new", Username: "alice", CPInstanceID: "cp-new",
		PID: 1002, Protocol: "postgres", RequestedVCPUs: 1,
		EnqueuedAt: now.Add(time.Millisecond), ExpiresAt: now.Add(time.Minute),
	}
	if err := storeA.EnqueueOrgConnectionRequest(newRequest); err != nil {
		t.Fatalf("enqueue reused request: %v", err)
	}
	newRef := cpconfigstore.OrgConnectionAdmissionRef{
		RequestID: newRequest.RequestID, OrgID: newRequest.OrgID, CPInstanceID: newRequest.CPInstanceID,
	}
	lease, err := storeA.ScheduleAndClaimOrgConnectionLeaseForRef(newRef)
	if err != nil || lease == nil {
		t.Fatalf("grant reused request = %#v, err = %v", lease, err)
	}

	if err := blocker.Commit().Error; err != nil {
		t.Fatalf("release old-org admission lock: %v", err)
	}
	select {
	case err := <-reclaimDone:
		if err != nil {
			t.Fatalf("stale reclaim: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stale reclaim")
	}

	assertOrgConnectionRequestGranted(t, storeA, newRequest.RequestID)
	assertOrgConnectionActiveVCPUs(t, storeA, newRequest.OrgID, 1)
}

func TestScheduleAndClaimContextCancellationInterruptsOrgLockWait(t *testing.T) {
	storeA, storeB, _, appB := newSharedConfigStores(t)
	upsertActiveCP(t, storeA, "cp-a")

	const orgID = "org-context-lock-cancel"
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 1, map[string]int{"alice": 0})
	now := time.Now()
	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-context-lock-cancel", OrgID: orgID, Username: "alice", CPInstanceID: "cp-a",
		PID: 1001, Protocol: "postgres", RequestedVCPUs: 1,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := storeA.EnqueueOrgConnectionRequest(request); err != nil {
		t.Fatalf("enqueue request: %v", err)
	}

	blocker := storeA.DB().Begin()
	if blocker.Error != nil {
		t.Fatalf("begin lock blocker: %v", blocker.Error)
	}
	defer func() { _ = blocker.Rollback().Error }()
	var blockerPID int
	if err := blocker.Raw("SELECT pg_backend_pid()").Scan(&blockerPID).Error; err != nil {
		t.Fatalf("read lock blocker pid: %v", err)
	}
	if err := cpconfigstore.LockOrgConnectionAdmissionTx(blocker, orgID); err != nil {
		t.Fatalf("hold org admission lock: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := storeB.ScheduleAndClaimOrgConnectionLeaseContext(ctx, request.RequestID, request.CPInstanceID)
		result <- err
	}()
	waitForAdvisoryLockWaiterFromApplication(t, configStoreSQLDB(t, storeA), blockerPID, appB)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("schedule error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("context cancellation did not interrupt PostgreSQL advisory-lock wait")
	}

	if err := blocker.Rollback().Error; err != nil {
		t.Fatalf("release org admission lock: %v", err)
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
	defer cleanupCancel()
	if err := storeA.CancelOrgConnectionRequestContext(cleanupCtx, request.RequestID, time.Now()); err != nil {
		t.Fatalf("cancel interrupted request: %v", err)
	}
	assertOrgConnectionRequestAbsent(t, storeA, request.RequestID)
	assertOrgConnectionActiveVCPUs(t, storeA, orgID, 0)
}

func TestScheduleAndClaimSerializesWithOwnerLeavingActiveState(t *testing.T) {
	storeA, storeB, _, appB := newSharedConfigStores(t)
	sqlDB := configStoreSQLDB(t, storeA)
	const cpID = "cp-owner-state-race"
	upsertActiveCP(t, storeA, cpID)

	const orgID = "org-owner-state-race"
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 1, map[string]int{"alice": 0})
	now := time.Now()
	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-owner-state-race", OrgID: orgID, Username: "alice", CPInstanceID: cpID,
		PID: 1001, Protocol: "postgres", RequestedVCPUs: 1,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := storeA.EnqueueOrgConnectionRequest(request); err != nil {
		t.Fatalf("enqueue request: %v", err)
	}

	stateChange := storeA.DB().Begin()
	if stateChange.Error != nil {
		t.Fatalf("begin owner state change: %v", stateChange.Error)
	}
	defer func() { _ = stateChange.Rollback().Error }()
	if err := stateChange.Table(storeA.RuntimeSchema()+".cp_instances").
		Where("id = ?", cpID).
		Update("state", cpconfigstore.ControlPlaneInstanceStateDraining).Error; err != nil {
		t.Fatalf("stage owner draining state: %v", err)
	}
	var stateChangePID int
	if err := stateChange.Raw("SELECT pg_backend_pid()").Scan(&stateChangePID).Error; err != nil {
		t.Fatalf("read owner state-change pid: %v", err)
	}

	type scheduleResult struct {
		lease *cpconfigstore.OrgConnectionLease
		err   error
	}
	resultCh := make(chan scheduleResult, 1)
	go func() {
		lease, err := storeB.ScheduleAndClaimOrgConnectionLease(request.RequestID, cpID)
		resultCh <- scheduleResult{lease: lease, err: err}
	}()
	waitForBlockedApplicationPID(t, sqlDB, stateChangePID, appB)

	if err := stateChange.Commit().Error; err != nil {
		t.Fatalf("commit owner draining state: %v", err)
	}
	var result scheduleResult
	select {
	case result = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("schedule did not resume after owner state commit")
	}

	if cancelErr := storeA.CancelOrgConnectionRequest(request.RequestID, time.Now()); cancelErr != nil {
		t.Fatalf("cleanup raced request: %v", cancelErr)
	}
	if result.err != nil {
		t.Fatalf("schedule after owner started draining: %v", result.err)
	}
	if result.lease != nil {
		t.Fatalf("non-active owner received lease %#v", result.lease)
	}

	// A grant whose commit response was lost follows the existing-lease path on
	// retry. That claim must serialize with the same owner-state transition too.
	upsertActiveCP(t, storeA, cpID)
	existingRequest := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-existing-owner-state-race", OrgID: orgID, Username: "alice", CPInstanceID: cpID,
		PID: 1002, Protocol: "postgres", RequestedVCPUs: 1,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	if err := storeA.EnqueueOrgConnectionRequest(existingRequest); err != nil {
		t.Fatalf("enqueue existing-lease request: %v", err)
	}
	existingLease, err := storeA.ScheduleAndClaimOrgConnectionLease(existingRequest.RequestID, cpID)
	if err != nil || existingLease == nil {
		t.Fatalf("seed existing lease = %#v, err = %v", existingLease, err)
	}

	retryStateChange := storeA.DB().Begin()
	if retryStateChange.Error != nil {
		t.Fatalf("begin retry owner state change: %v", retryStateChange.Error)
	}
	defer func() { _ = retryStateChange.Rollback().Error }()
	if err := retryStateChange.Table(storeA.RuntimeSchema()+".cp_instances").
		Where("id = ?", cpID).
		Update("state", cpconfigstore.ControlPlaneInstanceStateDraining).Error; err != nil {
		t.Fatalf("stage retry owner draining state: %v", err)
	}
	var retryStateChangePID int
	if err := retryStateChange.Raw("SELECT pg_backend_pid()").Scan(&retryStateChangePID).Error; err != nil {
		t.Fatalf("read retry owner state-change pid: %v", err)
	}

	retryResultCh := make(chan scheduleResult, 1)
	go func() {
		lease, err := storeB.ScheduleAndClaimOrgConnectionLease(existingRequest.RequestID, cpID)
		retryResultCh <- scheduleResult{lease: lease, err: err}
	}()
	waitForBlockedApplicationPID(t, sqlDB, retryStateChangePID, appB)
	if err := retryStateChange.Commit().Error; err != nil {
		t.Fatalf("commit retry owner draining state: %v", err)
	}
	select {
	case result = <-retryResultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("existing-lease retry did not resume after owner state commit")
	}
	if cancelErr := storeA.CancelOrgConnectionRequest(existingRequest.RequestID, time.Now()); cancelErr != nil {
		t.Fatalf("cleanup existing-lease request: %v", cancelErr)
	}
	if result.err != nil {
		t.Fatalf("existing-lease retry after owner started draining: %v", result.err)
	}
	if result.lease != nil {
		t.Fatalf("non-active owner reclaimed existing lease %#v", result.lease)
	}
}

func TestScheduleAndClaimDoesNotGrantPastForeignHead(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	const orgID = "org-request-owned-head"
	seedAuthoritativeOrgConnectionLimits(t, store, orgID, 3, map[string]int{
		"alice": 0,
		"bob":   0,
	})

	now := time.Now()
	alice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-alice-head", OrgID: orgID, Username: "alice", CPInstanceID: "cp-a",
		PID: 1001, Protocol: "postgres", RequestedVCPUs: 2,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-bob-behind", OrgID: orgID, Username: "bob", CPInstanceID: "cp-b",
		PID: 1002, Protocol: "postgres", RequestedVCPUs: 1,
		EnqueuedAt: now.Add(time.Millisecond), ExpiresAt: now.Add(time.Minute),
	}
	for _, request := range []*cpconfigstore.OrgConnectionQueueEntry{alice, bob} {
		if err := store.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}

	lease, err := store.ScheduleAndClaimOrgConnectionLease(bob.RequestID, bob.CPInstanceID)
	if err != nil {
		t.Fatalf("Bob admission poll: %v", err)
	}
	if lease != nil {
		t.Fatalf("Bob's poll granted past Alice's eligible head: %#v", lease)
	}
	assertOrgConnectionActiveVCPUs(t, store, orgID, 0)

	aliceLease, err := store.ScheduleAndClaimOrgConnectionLease(alice.RequestID, alice.CPInstanceID)
	if err != nil || aliceLease == nil {
		t.Fatalf("Alice admission = %#v, err = %v; want own lease", aliceLease, err)
	}
	bobLease, err := store.ScheduleAndClaimOrgConnectionLease(bob.RequestID, bob.CPInstanceID)
	if err != nil || bobLease == nil {
		t.Fatalf("Bob admission after Alice = %#v, err = %v; want own lease", bobLease, err)
	}
}

func TestScheduleAndClaimRejectsPermanentlyImpossibleRequest(t *testing.T) {
	tests := []struct {
		name       string
		orgMax     int
		userMax    int
		wantReason cpconfigstore.OrgConnectionAdmissionRejectionReason
	}{
		{name: "org limit", orgMax: 2, userMax: 0, wantReason: cpconfigstore.OrgConnectionAdmissionRejectedOrgVCPU},
		{name: "user limit", orgMax: 10, userMax: 2, wantReason: cpconfigstore.OrgConnectionAdmissionRejectedUserVCPU},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newIsolatedConfigStore(t)
			upsertActiveCP(t, store, "cp-a")

			const orgID = "org-impossible-request"
			seedAuthoritativeOrgConnectionLimits(t, store, orgID, tt.orgMax, map[string]int{"alice": tt.userMax})
			now := time.Now()
			request := &cpconfigstore.OrgConnectionQueueEntry{
				RequestID: "request-too-large", OrgID: orgID, Username: "alice", CPInstanceID: "cp-a",
				PID: 1001, Protocol: "postgres", RequestedVCPUs: 3,
				EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
			}
			if err := store.EnqueueOrgConnectionRequest(request); err != nil {
				t.Fatalf("enqueue request: %v", err)
			}

			lease, err := store.ScheduleAndClaimOrgConnectionLease(request.RequestID, request.CPInstanceID)
			if lease != nil {
				t.Fatalf("impossible request returned lease %#v", lease)
			}
			var rejection *cpconfigstore.OrgConnectionAdmissionRejectedError
			if !errors.As(err, &rejection) {
				t.Fatalf("admission error = %v, want typed rejection", err)
			}
			if rejection.Reason != tt.wantReason || rejection.RequestedVCPUs != 3 {
				t.Fatalf("rejection = %+v, want reason %q and requested_vcpus=3", rejection, tt.wantReason)
			}
			assertOrgConnectionRequestAbsent(t, store, request.RequestID)
			assertOrgConnectionActiveVCPUs(t, store, orgID, 0)
		})
	}
}

func TestScheduleAndClaimKeepsTemporarilySaturatedRequestPending(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	const orgID = "org-temporary-saturation"
	seedAuthoritativeOrgConnectionLimits(t, store, orgID, 3, map[string]int{
		"alice": 0,
		"bob":   0,
	})
	now := time.Now()
	alice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-active-alice", OrgID: orgID, Username: "alice", CPInstanceID: "cp-a",
		PID: 1001, Protocol: "postgres", RequestedVCPUs: 2,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-waiting-bob", OrgID: orgID, Username: "bob", CPInstanceID: "cp-b",
		PID: 1002, Protocol: "postgres", RequestedVCPUs: 2,
		EnqueuedAt: now.Add(time.Millisecond), ExpiresAt: now.Add(time.Minute),
	}
	for _, request := range []*cpconfigstore.OrgConnectionQueueEntry{alice, bob} {
		if err := store.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}

	aliceLease, err := store.ScheduleAndClaimOrgConnectionLease(alice.RequestID, alice.CPInstanceID)
	if err != nil || aliceLease == nil {
		t.Fatalf("Alice admission = %#v, err = %v", aliceLease, err)
	}
	bobLease, err := store.ScheduleAndClaimOrgConnectionLease(bob.RequestID, bob.CPInstanceID)
	if err != nil {
		t.Fatalf("temporarily saturated Bob admission: %v", err)
	}
	if bobLease != nil {
		t.Fatalf("temporarily saturated Bob received lease %#v", bobLease)
	}
	assertOrgConnectionRequestPending(t, store, bob.RequestID)

	if err := store.ReleaseOrgConnectionLease(aliceLease.LeaseID); err != nil {
		t.Fatalf("release Alice: %v", err)
	}
	bobLease, err = store.ScheduleAndClaimOrgConnectionLease(bob.RequestID, bob.CPInstanceID)
	if err != nil || bobLease == nil {
		t.Fatalf("Bob admission after capacity release = %#v, err = %v", bobLease, err)
	}
}
