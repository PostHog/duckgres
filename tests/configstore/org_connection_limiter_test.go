//go:build linux || darwin

package configstore_test

import (
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
