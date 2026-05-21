//go:build linux || darwin

package configstore_test

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"strings"
	"testing"
	"time"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
)

func upsertActiveCP(t *testing.T, store *cpconfigstore.ConfigStore, id string) {
	t.Helper()
	now := time.Now()
	if err := store.UpsertControlPlaneInstance(&cpconfigstore.ControlPlaneInstance{
		ID:              id,
		PodName:         id,
		PodUID:          id,
		BootID:          "boot-" + id,
		State:           cpconfigstore.ControlPlaneInstanceStateActive,
		StartedAt:       now,
		LastHeartbeatAt: now,
	}); err != nil {
		t.Fatalf("upsert control-plane instance %q: %v", id, err)
	}
}

func TestOrgConnectionLeasesEnforceClusterWideLimit(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	first := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:    "request-a",
		OrgID:        "org-1",
		CPInstanceID: "cp-a",
		PID:          1001,
		Protocol:     "postgres",
		EnqueuedAt:   now,
		ExpiresAt:    now.Add(time.Minute),
	}
	second := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:    "request-b",
		OrgID:        "org-1",
		CPInstanceID: "cp-b",
		PID:          1002,
		Protocol:     "postgres",
		EnqueuedAt:   now.Add(time.Millisecond),
		ExpiresAt:    now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(first); err != nil {
		t.Fatalf("enqueue first request: %v", err)
	}
	if err := store.EnqueueOrgConnectionRequest(second); err != nil {
		t.Fatalf("enqueue second request: %v", err)
	}

	lease, err := store.TryAcquireOrgConnectionLease(first.RequestID, 1, now)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected first request to acquire a lease")
	}

	blocked, err := store.TryAcquireOrgConnectionLease(second.RequestID, 1, now)
	if err != nil {
		t.Fatalf("acquire second lease: %v", err)
	}
	if blocked != nil {
		t.Fatalf("expected second request to be blocked by cluster-wide limit, got lease %q", blocked.LeaseID)
	}

	count, err := store.ActiveOrgConnectionLeaseCount("org-1")
	if err != nil {
		t.Fatalf("count active leases: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one active cluster-wide lease, got %d", count)
	}

	if err := store.ReleaseOrgConnectionLease(lease.LeaseID); err != nil {
		t.Fatalf("release first lease: %v", err)
	}

	lease, err = store.TryAcquireOrgConnectionLease(second.RequestID, 1, now)
	if err != nil {
		t.Fatalf("acquire second lease after release: %v", err)
	}
	if lease == nil {
		t.Fatal("expected second request to acquire after release")
	}
}

func TestOrgConnectionLeasesPreserveFIFOAcrossControlPlanes(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	entries := []*cpconfigstore.OrgConnectionQueueEntry{
		{
			RequestID:    "request-a",
			OrgID:        "org-1",
			CPInstanceID: "cp-a",
			PID:          1001,
			Protocol:     "postgres",
			EnqueuedAt:   now,
			ExpiresAt:    now.Add(time.Minute),
		},
		{
			RequestID:    "request-b",
			OrgID:        "org-1",
			CPInstanceID: "cp-b",
			PID:          1002,
			Protocol:     "postgres",
			EnqueuedAt:   now.Add(time.Millisecond),
			ExpiresAt:    now.Add(time.Minute),
		},
	}
	for _, entry := range entries {
		if err := store.EnqueueOrgConnectionRequest(entry); err != nil {
			t.Fatalf("enqueue %s: %v", entry.RequestID, err)
		}
	}

	outOfOrder, err := store.TryAcquireOrgConnectionLease("request-b", 1, now)
	if err != nil {
		t.Fatalf("out-of-order acquire: %v", err)
	}
	if outOfOrder != nil {
		t.Fatalf("expected FIFO to block request-b while request-a is pending, got lease %q", outOfOrder.LeaseID)
	}

	first, err := store.TryAcquireOrgConnectionLease("request-a", 1, now)
	if err != nil {
		t.Fatalf("acquire request-a: %v", err)
	}
	if first == nil {
		t.Fatal("expected request-a to acquire first")
	}
}

func TestOrgConnectionQueueUsesDatabaseInsertionTimeForFIFO(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	first := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:    "request-first-inserted",
		OrgID:        "org-clock-skew",
		CPInstanceID: "cp-a",
		PID:          1001,
		Protocol:     "postgres",
		EnqueuedAt:   now.Add(10 * time.Minute),
		ExpiresAt:    now.Add(70 * time.Minute),
	}
	second := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:    "request-second-inserted",
		OrgID:        "org-clock-skew",
		CPInstanceID: "cp-b",
		PID:          1002,
		Protocol:     "postgres",
		EnqueuedAt:   now.Add(-10 * time.Minute),
		ExpiresAt:    now.Add(50 * time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(first); err != nil {
		t.Fatalf("enqueue first request: %v", err)
	}
	if err := store.EnqueueOrgConnectionRequest(second); err != nil {
		t.Fatalf("enqueue second request: %v", err)
	}

	outOfOrder, err := store.TryAcquireOrgConnectionLease(second.RequestID, 1, now)
	if err != nil {
		t.Fatalf("out-of-order acquire: %v", err)
	}
	if outOfOrder != nil {
		t.Fatalf("expected database insertion order to block request-second-inserted, got lease %q", outOfOrder.LeaseID)
	}

	lease, err := store.TryAcquireOrgConnectionLease(first.RequestID, 1, now)
	if err != nil {
		t.Fatalf("acquire first request: %v", err)
	}
	if lease == nil {
		t.Fatal("expected first inserted request to acquire first")
	}
}

func TestOrgConnectionLeasesIgnoreExpiredControlPlaneOwners(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	first := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:    "request-a",
		OrgID:        "org-1",
		CPInstanceID: "cp-a",
		PID:          1001,
		Protocol:     "postgres",
		EnqueuedAt:   now,
		ExpiresAt:    now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(first); err != nil {
		t.Fatalf("enqueue first request: %v", err)
	}
	if _, err := store.TryAcquireOrgConnectionLease(first.RequestID, 1, now); err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}

	expiredAt := now.Add(time.Second)
	if err := store.UpsertControlPlaneInstance(&cpconfigstore.ControlPlaneInstance{
		ID:              "cp-a",
		PodName:         "cp-a",
		PodUID:          "cp-a",
		BootID:          "boot-cp-a",
		State:           cpconfigstore.ControlPlaneInstanceStateExpired,
		StartedAt:       now,
		LastHeartbeatAt: now,
		ExpiredAt:       &expiredAt,
	}); err != nil {
		t.Fatalf("expire cp-a: %v", err)
	}

	second := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:    "request-b",
		OrgID:        "org-1",
		CPInstanceID: "cp-b",
		PID:          1002,
		Protocol:     "postgres",
		EnqueuedAt:   now.Add(time.Millisecond),
		ExpiresAt:    now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(second); err != nil {
		t.Fatalf("enqueue second request: %v", err)
	}
	lease, err := store.TryAcquireOrgConnectionLease(second.RequestID, 1, now)
	if err != nil {
		t.Fatalf("acquire second lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected expired cp-a lease to stop counting against org limit")
	}
}

func TestTryAcquireOrgConnectionLeasePrunesStaleMissingControlPlaneOwner(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")

	now := time.Now()
	staleLease := &cpconfigstore.OrgConnectionLease{
		LeaseID:      "missing-owner-lease",
		RequestID:    "missing-owner-request",
		OrgID:        "org-missing-owner",
		CPInstanceID: "missing-cp",
		PID:          1001,
		Protocol:     "postgres",
		AcquiredAt:   now.Add(-10 * time.Minute),
	}
	if err := store.DB().Table(store.RuntimeSchema() + ".org_connection_leases").Create(staleLease).Error; err != nil {
		t.Fatalf("insert stale missing-owner lease: %v", err)
	}

	entry := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:    "replacement-request",
		OrgID:        staleLease.OrgID,
		CPInstanceID: "cp-a",
		PID:          1002,
		Protocol:     "postgres",
		EnqueuedAt:   now,
		ExpiresAt:    now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(entry); err != nil {
		t.Fatalf("enqueue replacement request: %v", err)
	}

	lease, err := store.TryAcquireOrgConnectionLease(entry.RequestID, 1, now)
	if err != nil {
		t.Fatalf("acquire replacement request: %v", err)
	}
	if lease == nil {
		t.Fatal("expected stale missing-owner lease not to block replacement request")
	}
}

func TestTryAcquireOrgConnectionLeaseRetryReturnsExistingLease(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")

	now := time.Now()
	entry := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:    "request-a",
		OrgID:        "org-1",
		CPInstanceID: "cp-a",
		PID:          1001,
		Protocol:     "postgres",
		EnqueuedAt:   now,
		ExpiresAt:    now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(entry); err != nil {
		t.Fatalf("enqueue request: %v", err)
	}

	first, err := store.TryAcquireOrgConnectionLease(entry.RequestID, 1, now)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if first == nil {
		t.Fatal("expected first acquire to grant lease")
	}

	retry, err := store.TryAcquireOrgConnectionLease(entry.RequestID, 1, now.Add(time.Second))
	if err != nil {
		t.Fatalf("retry acquire: %v", err)
	}
	if retry == nil {
		t.Fatal("expected retry acquire to return existing lease")
	}
	if retry.LeaseID != first.LeaseID {
		t.Fatalf("expected retry lease id %q, got %q", first.LeaseID, retry.LeaseID)
	}

	var activeLeases int64
	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_leases").
		Where("request_id = ?", entry.RequestID).
		Count(&activeLeases).Error; err != nil {
		t.Fatalf("count leases: %v", err)
	}
	if activeLeases != 1 {
		t.Fatalf("expected one active lease for retried request, got %d", activeLeases)
	}
}

func TestTryAcquireOrgConnectionLeaseTakesOrgLockBeforeExpiredRowLock(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")

	now := time.Now()
	entry := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:    "expired-request",
		OrgID:        "org-lock-order",
		CPInstanceID: "cp-a",
		PID:          1001,
		Protocol:     "postgres",
		EnqueuedAt:   now.Add(-2 * time.Minute),
		ExpiresAt:    now.Add(-time.Minute),
	}
	if err := store.DB().Table(store.RuntimeSchema() + ".org_connection_queue").Create(entry).Error; err != nil {
		t.Fatalf("insert expired request: %v", err)
	}

	sqlDB, err := store.DB().DB()
	if err != nil {
		t.Fatalf("store sql db: %v", err)
	}
	holder, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin advisory holder tx: %v", err)
	}
	defer func() { _ = holder.Rollback() }()

	var holderPID int
	if err := holder.QueryRow("SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatalf("get advisory holder backend pid: %v", err)
	}
	if _, err := holder.Exec("SELECT pg_advisory_xact_lock($1)", orgConnectionAdvisoryLockKey(entry.OrgID)); err != nil {
		t.Fatalf("take org advisory lock: %v", err)
	}

	acquireDone := make(chan error, 1)
	go func() {
		lease, err := store.TryAcquireOrgConnectionLease(entry.RequestID, 1, now)
		if lease != nil {
			acquireDone <- fmt.Errorf("expected expired request not to receive lease, got %q", lease.LeaseID)
			return
		}
		acquireDone <- err
	}()

	waitForAdvisoryLockWaiter(t, sqlDB, holderPID)

	if _, err := holder.Exec("SET LOCAL lock_timeout = '250ms'"); err != nil {
		t.Fatalf("set lock timeout: %v", err)
	}
	_, deleteErr := holder.Exec(
		"DELETE FROM "+store.RuntimeSchema()+".org_connection_queue WHERE request_id = $1",
		entry.RequestID,
	)

	if deleteErr != nil {
		_ = holder.Rollback()
	} else if err := holder.Commit(); err != nil {
		t.Fatalf("commit advisory holder tx: %v", err)
	}

	select {
	case err := <-acquireDone:
		if err != nil {
			t.Fatalf("acquire expired request: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for blocked acquire to finish")
	}

	if deleteErr != nil {
		t.Fatalf("delete expired queue row while holding org advisory lock should not block behind TryAcquire row lock: %v", deleteErr)
	}
}

func TestTryAcquireOrgConnectionLeaseRetriesWhenRequestIDReusedWithDifferentOrg(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")

	now := time.Now()
	oldEntry := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:    "reused-request",
		OrgID:        "org-old",
		CPInstanceID: "cp-a",
		PID:          1001,
		Protocol:     "postgres",
		EnqueuedAt:   now,
		ExpiresAt:    now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(oldEntry); err != nil {
		t.Fatalf("enqueue old request: %v", err)
	}

	sqlDB, err := store.DB().DB()
	if err != nil {
		t.Fatalf("store sql db: %v", err)
	}
	holder, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin advisory holder tx: %v", err)
	}
	defer func() { _ = holder.Rollback() }()

	var holderPID int
	if err := holder.QueryRow("SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatalf("get advisory holder backend pid: %v", err)
	}
	if _, err := holder.Exec("SELECT pg_advisory_xact_lock($1)", orgConnectionAdvisoryLockKey(oldEntry.OrgID)); err != nil {
		t.Fatalf("take old org advisory lock: %v", err)
	}

	acquireDone := make(chan acquireResult, 1)
	go func() {
		lease, err := store.TryAcquireOrgConnectionLease(oldEntry.RequestID, 1, now)
		acquireDone <- acquireResult{lease: lease, err: err}
	}()

	waitForAdvisoryLockWaiter(t, sqlDB, holderPID)

	if _, err := sqlDB.Exec(
		"DELETE FROM "+store.RuntimeSchema()+".org_connection_queue WHERE request_id = $1",
		oldEntry.RequestID,
	); err != nil {
		t.Fatalf("delete old request: %v", err)
	}
	newEntry := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:    oldEntry.RequestID,
		OrgID:        "org-new",
		CPInstanceID: "cp-a",
		PID:          1002,
		Protocol:     "postgres",
		EnqueuedAt:   now.Add(time.Millisecond),
		ExpiresAt:    now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(newEntry); err != nil {
		t.Fatalf("enqueue reused request: %v", err)
	}

	if err := holder.Commit(); err != nil {
		t.Fatalf("commit advisory holder tx: %v", err)
	}

	select {
	case result := <-acquireDone:
		if result.err != nil {
			t.Fatalf("acquire reused request: %v", result.err)
		}
		if result.lease == nil {
			t.Fatal("expected reused request to acquire after retrying under new org")
		}
		if result.lease.OrgID != newEntry.OrgID {
			t.Fatalf("expected lease for org %q, got %q", newEntry.OrgID, result.lease.OrgID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reused request acquire")
	}

	count, err := store.ActiveOrgConnectionLeaseCount(newEntry.OrgID)
	if err != nil {
		t.Fatalf("count active leases: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one active lease for new org, got %d", count)
	}
}

type acquireResult struct {
	lease *cpconfigstore.OrgConnectionLease
	err   error
}

func orgConnectionAdvisoryLockKey(orgID string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("duckgres:org-connections:" + orgID))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

func waitForAdvisoryLockWaiter(t *testing.T, db *sql.DB, holderPID int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiters int
		err := db.QueryRow(`
			SELECT count(*)
			FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND wait_event = 'advisory'
			  AND datname = current_database()
			  AND $1 = ANY(pg_blocking_pids(pid))
		`, holderPID).Scan(&waiters)
		if err != nil {
			t.Fatalf("poll advisory lock waiter: %v", err)
		}
		if waiters > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	var activities []string
	rows, err := db.Query(`
		SELECT COALESCE(wait_event_type, '') || '/' || COALESCE(wait_event, '') || ' ' || query
		FROM pg_stat_activity
		WHERE datname = current_database()
		  AND ($1 = ANY(pg_blocking_pids(pid)) OR pid = $1)
	`, holderPID)
	if err == nil {
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var activity string
			if err := rows.Scan(&activity); err == nil {
				activities = append(activities, activity)
			}
		}
	}
	t.Fatalf("timed out waiting for advisory lock waiter; activities: %s", strings.Join(activities, "; "))
}
