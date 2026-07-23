//go:build linux || darwin

package configstore_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
)

const blockedOrgConnectionRequestID = "request-blocked-insert"

func TestEnqueueOrgConnectionRequestSerializesFIFOWithAdmissionPostgres(t *testing.T) {
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
	// though its INSERT is uncommitted. A later enqueue must wait and receive a
	// later database timestamp.
	waitForBlockedApplicationPID(t, sqlDB, firstPID, appB)

	if err := holder.Commit(); err != nil {
		t.Fatalf("release queue-insert blocker: %v", err)
	}
	waitForAsyncError(t, firstDone, "first enqueue")
	waitForAsyncError(t, secondDone, "second enqueue")

	var first, second cpconfigstore.OrgConnectionQueueEntry
	if err := storeA.DB().Table(storeA.RuntimeSchema()+".org_connection_queue").
		Where("request_id = ?", blockedOrgConnectionRequestID).Take(&first).Error; err != nil {
		t.Fatalf("load first request: %v", err)
	}
	if err := storeA.DB().Table(storeA.RuntimeSchema()+".org_connection_queue").
		Where("request_id = ?", "request-enqueue-second").Take(&second).Error; err != nil {
		t.Fatalf("load second request: %v", err)
	}
	if !first.EnqueuedAt.Before(second.EnqueuedAt) {
		t.Fatalf("serialized enqueue timestamps: first=%s second=%s, want first before second", first.EnqueuedAt, second.EnqueuedAt)
	}

	lease, err := storeA.ScheduleAndClaimOrgConnectionLease(first.RequestID, cpID)
	if err != nil || lease == nil || lease.RequestID != first.RequestID {
		t.Fatalf("admit serialized head = %#v, err = %v", lease, err)
	}
	waiting, err := storeB.ScheduleAndClaimOrgConnectionLease(second.RequestID, cpID)
	if err != nil || waiting != nil {
		t.Fatalf("second request admission = %#v, err = %v; want pending at org cap", waiting, err)
	}
	assertOrgConnectionRequestPending(t, storeA, second.RequestID)
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
