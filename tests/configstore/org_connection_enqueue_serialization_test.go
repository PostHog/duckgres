//go:build linux || darwin

package configstore_test

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
)

const blockedOrgConnectionRequestID = "request-blocked-insert"

func TestEnqueueOrgConnectionRequestSerializesFIFOWithDispatcherPostgres(t *testing.T) {
	storeA, storeB, appA, appB := newSharedConfigStores(t)
	const (
		cpID  = "cp-enqueue-fifo"
		orgID = "org-enqueue-fifo"
	)
	upsertActiveCP(t, storeA, cpID)
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 1, map[string]int{
		"alice": 0,
		"bob":   0,
	})
	if err := storeA.ActivateOrgConnectionAdmissionOffers(); err != nil {
		t.Fatalf("activate admission offers: %v", err)
	}

	blockKey := installBlockingOrgConnectionQueueInsertTrigger(t, storeA)
	holder, holderPID := holdTestAdvisoryLock(t, storeA, blockKey)
	defer func() { _ = holder.Rollback() }()

	now := time.Now()
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- storeA.EnqueueOrgConnectionRequest(&cpconfigstore.OrgConnectionQueueEntry{
			RequestID: blockedOrgConnectionRequestID, OrgID: orgID, Username: "alice", CPInstanceID: cpID,
			PID: 1001, Protocol: "postgres", RequestedVCPUs: 1,
			EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
		})
	}()

	sqlDB := configStoreSQLDB(t, storeA)
	firstPID := waitForBlockedApplicationPID(t, sqlDB, holderPID, appA)

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- storeB.EnqueueOrgConnectionRequest(&cpconfigstore.OrgConnectionQueueEntry{
			RequestID: "request-enqueue-second", OrgID: orgID, Username: "bob", CPInstanceID: cpID,
			PID: 1002, Protocol: "postgres", RequestedVCPUs: 1,
			EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
		})
	}()

	// The first enqueue already owns the org admission linearization point even
	// though its INSERT is uncommitted. A later enqueue must wait instead of
	// becoming visible to a dispatcher with a later enqueued_at timestamp.
	waitForBlockedApplicationPID(t, sqlDB, firstPID, appB)

	if err := holder.Commit(); err != nil {
		t.Fatalf("release queue-insert blocker: %v", err)
	}
	waitForAsyncError(t, firstDone, "first enqueue")
	waitForAsyncError(t, secondDone, "second enqueue")

	if offered, err := storeA.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 1 {
		t.Fatalf("dispatch after serialized enqueues = %d, err = %v; want one offer", offered, err)
	}
	first := assertOrgConnectionOffer(t, storeA, blockedOrgConnectionRequestID)
	second := assertOrgConnectionRequestState(t, storeA, "request-enqueue-second", cpconfigstore.OrgConnectionRequestStatePending)
	if !first.EnqueuedAt.Before(second.EnqueuedAt) {
		t.Fatalf("serialized enqueue timestamps: first=%s second=%s, want first before second", first.EnqueuedAt, second.EnqueuedAt)
	}
}

func TestAdmissionOfferActivationWaitsForInFlightQueueInsertPostgres(t *testing.T) {
	storeA, storeB, appA, appB := newSharedConfigStores(t)
	now := time.Now()
	upsertActiveCP(t, storeA, "cp-activation-capable")

	blockKey := installBlockingOrgConnectionQueueInsertTrigger(t, storeA)
	holder, holderPID := holdTestAdvisoryLock(t, storeA, blockKey)
	defer func() { _ = holder.Rollback() }()

	enqueueDone := make(chan error, 1)
	go func() {
		enqueueDone <- storeB.EnqueueOrgConnectionRequest(&cpconfigstore.OrgConnectionQueueEntry{
			RequestID: blockedOrgConnectionRequestID, OrgID: "org-activation-in-flight", Username: "alice",
			CPInstanceID: "cp-missing", PID: 1001, Protocol: "postgres", RequestedVCPUs: 1,
			EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
		})
	}()

	sqlDB := configStoreSQLDB(t, storeA)
	enqueuePID := waitForBlockedApplicationPID(t, sqlDB, holderPID, appB)

	activateDone := make(chan error, 1)
	go func() { activateDone <- storeA.ActivateOrgConnectionAdmissionOffers() }()

	// The queue INSERT's BEFORE trigger holds the disabled protocol singleton
	// FOR SHARE until commit. Activation must wait, then include the committed
	// row in its incompatible-owner validation.
	waitForBlockedApplicationPID(t, sqlDB, enqueuePID, appA)
	if err := holder.Commit(); err != nil {
		t.Fatalf("release queue-insert blocker: %v", err)
	}
	waitForAsyncError(t, enqueueDone, "pre-activation enqueue")

	select {
	case err := <-activateDone:
		if !errors.Is(err, cpconfigstore.ErrAdmissionOfferProtocolActivationBlocked) {
			t.Fatalf("activation error = %v, want activation-blocked sentinel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for activation after queue insert committed")
	}
}

func TestQueueInsertRechecksOwnerAfterConcurrentAdmissionOfferActivationPostgres(t *testing.T) {
	storeA, storeB, appA, appB := newSharedConfigStores(t)
	now := time.Now()
	upsertActiveCP(t, storeA, "cp-activation-capable")

	sqlDB := configStoreSQLDB(t, storeA)
	cpTableHolder, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin CP-table blocker: %v", err)
	}
	defer func() { _ = cpTableHolder.Rollback() }()
	var holderPID int
	if err := cpTableHolder.QueryRow("SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatalf("read CP-table blocker pid: %v", err)
	}
	if _, err := cpTableHolder.Exec("LOCK TABLE " + storeA.RuntimeSchema() + ".cp_instances IN ROW EXCLUSIVE MODE"); err != nil {
		t.Fatalf("lock CP table: %v", err)
	}

	activateDone := make(chan error, 1)
	go func() { activateDone <- storeA.ActivateOrgConnectionAdmissionOffers() }()
	activationPID := waitForBlockedApplicationPID(t, sqlDB, holderPID, appA)

	enqueueDone := make(chan error, 1)
	go func() {
		enqueueDone <- storeB.EnqueueOrgConnectionRequest(&cpconfigstore.OrgConnectionQueueEntry{
			RequestID: "request-concurrent-activation", OrgID: "org-concurrent-activation", Username: "alice",
			CPInstanceID: "cp-missing", PID: 1001, Protocol: "postgres", RequestedVCPUs: 1,
			EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
		})
	}()

	// Activation owns the protocol row while waiting for its CP-table fence.
	// The INSERT must wait on that row, then observe the enabled value and reject
	// its missing owner rather than committing against its pre-wait snapshot.
	waitForBlockedApplicationPID(t, sqlDB, activationPID, appB)
	if err := cpTableHolder.Commit(); err != nil {
		t.Fatalf("release CP-table blocker: %v", err)
	}
	waitForAsyncError(t, activateDone, "admission offer activation")

	select {
	case err := <-enqueueDone:
		if err == nil {
			t.Fatal("missing-owner queue insert succeeded after concurrent protocol activation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for queue insert after protocol activation")
	}
}

func installBlockingOrgConnectionQueueInsertTrigger(t *testing.T, store *cpconfigstore.ConfigStore) int64 {
	t.Helper()

	blockKey := orgConnectionAdvisoryLockKey("test:block-queue-insert:" + store.RuntimeSchema())
	schema := store.RuntimeSchema()
	if err := store.DB().Exec(fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.block_selected_org_connection_insert() RETURNS trigger AS $fn$
		BEGIN
			IF NEW.request_id = '%s' THEN
				PERFORM pg_advisory_xact_lock(%d);
			END IF;
			RETURN NEW;
		END;
		$fn$ LANGUAGE plpgsql
	`, schema, blockedOrgConnectionRequestID, blockKey)).Error; err != nil {
		t.Fatalf("create queue-insert blocker function: %v", err)
	}
	if err := store.DB().Exec(
		"CREATE TRIGGER block_selected_org_connection_insert " +
			"AFTER INSERT ON " + schema + ".org_connection_queue " +
			"FOR EACH ROW EXECUTE FUNCTION " + schema + ".block_selected_org_connection_insert()",
	).Error; err != nil {
		t.Fatalf("create queue-insert blocker trigger: %v", err)
	}
	return blockKey
}

func holdTestAdvisoryLock(t *testing.T, store *cpconfigstore.ConfigStore, key int64) (*sql.Tx, int) {
	t.Helper()

	sqlDB := configStoreSQLDB(t, store)
	holder, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin test advisory-lock holder: %v", err)
	}
	var holderPID int
	if err := holder.QueryRow("SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		_ = holder.Rollback()
		t.Fatalf("read test advisory-lock holder pid: %v", err)
	}
	if _, err := holder.Exec("SELECT pg_advisory_xact_lock($1)", key); err != nil {
		_ = holder.Rollback()
		t.Fatalf("take test advisory lock: %v", err)
	}
	return holder, holderPID
}

func configStoreSQLDB(t *testing.T, store *cpconfigstore.ConfigStore) *sql.DB {
	t.Helper()

	sqlDB, err := store.DB().DB()
	if err != nil {
		t.Fatalf("config store SQL DB: %v", err)
	}
	return sqlDB
}

func waitForAsyncError(t *testing.T, done <-chan error, operation string) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}
