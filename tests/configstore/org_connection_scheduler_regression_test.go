//go:build linux || darwin

package configstore_test

import (
	"database/sql"
	"testing"
	"time"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
)

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
