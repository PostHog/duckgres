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
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func upsertActiveCP(t *testing.T, store *cpconfigstore.ConfigStore, id string) {
	t.Helper()
	now := time.Now()
	if err := store.UpsertControlPlaneInstance(&cpconfigstore.ControlPlaneInstance{
		ID:              id,
		PodName:         id,
		State:           cpconfigstore.ControlPlaneInstanceStateActive,
		StartedAt:       now,
		LastHeartbeatAt: now,
	}); err != nil {
		t.Fatalf("upsert control-plane instance %q: %v", id, err)
	}
}

func TestOrgConnectionLeasesEnforceOrgVCPUBudget(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	first := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-a",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 4,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	second := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-b",
		OrgID:          "org-1",
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 3,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(first); err != nil {
		t.Fatalf("enqueue first request: %v", err)
	}
	if err := store.EnqueueOrgConnectionRequest(second); err != nil {
		t.Fatalf("enqueue second request: %v", err)
	}

	limits := cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 6}
	lease, err := store.TryAcquireOrgConnectionLease(first.RequestID, limits, now)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected first request to acquire a lease")
	}

	blocked, err := store.TryAcquireOrgConnectionLease(second.RequestID, limits, now)
	if err != nil {
		t.Fatalf("acquire second lease: %v", err)
	}
	if blocked != nil {
		t.Fatalf("expected second request to be blocked by org vCPU budget, got lease %q", blocked.LeaseID)
	}

	if err := store.ReleaseOrgConnectionLease(lease.LeaseID); err != nil {
		t.Fatalf("release first lease: %v", err)
	}
	lease, err = store.TryAcquireOrgConnectionLease(second.RequestID, limits, now)
	if err != nil {
		t.Fatalf("acquire second lease after release: %v", err)
	}
	if lease == nil {
		t.Fatal("expected second request to acquire after vCPUs are released")
	}
}

func TestOrgConnectionLeasesEnforceUserVCPUBudget(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	first := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-a",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 4,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	second := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-b",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 3,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(first); err != nil {
		t.Fatalf("enqueue first request: %v", err)
	}
	if err := store.EnqueueOrgConnectionRequest(second); err != nil {
		t.Fatalf("enqueue second request: %v", err)
	}

	limits := cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 20, UserMaxVCPUs: 6}
	lease, err := store.TryAcquireOrgConnectionLease(first.RequestID, limits, now)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected first request to acquire a lease")
	}

	blocked, err := store.TryAcquireOrgConnectionLease(second.RequestID, limits, now)
	if err != nil {
		t.Fatalf("acquire second lease: %v", err)
	}
	if blocked != nil {
		t.Fatalf("expected second request to be blocked by user vCPU budget, got lease %q", blocked.LeaseID)
	}
}

func TestOrgConnectionLeasesUserCapDoesNotBlockOtherUsers(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	activeAlice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-active-alice",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(activeAlice); err != nil {
		t.Fatalf("enqueue active alice request: %v", err)
	}
	limits := cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 10, UserMaxVCPUs: 1}
	lease, err := store.TryAcquireOrgConnectionLease(activeAlice.RequestID, limits, now)
	if err != nil {
		t.Fatalf("acquire active alice lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected active alice request to acquire a lease")
	}

	blockedAlice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-blocked-alice",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-bob",
		OrgID:          "org-1",
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1003,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(2 * time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(blockedAlice); err != nil {
		t.Fatalf("enqueue blocked alice request: %v", err)
	}
	if err := store.EnqueueOrgConnectionRequest(bob); err != nil {
		t.Fatalf("enqueue bob request: %v", err)
	}

	lease, err = store.TryAcquireOrgConnectionLease(bob.RequestID, limits, now)
	if err != nil {
		t.Fatalf("acquire bob request: %v", err)
	}
	if lease == nil {
		t.Fatal("expected bob request to bypass alice request blocked by alice's user vCPU budget")
	}
	if lease.RequestID != bob.RequestID {
		t.Fatalf("expected bob lease, got %#v", lease)
	}

	stillBlocked, err := store.TryAcquireOrgConnectionLease(blockedAlice.RequestID, limits, now)
	if err != nil {
		t.Fatalf("acquire blocked alice request: %v", err)
	}
	if stillBlocked != nil {
		t.Fatalf("expected alice request to remain blocked by alice's user vCPU budget, got lease %q", stillBlocked.LeaseID)
	}
}

func TestOrgConnectionLeasesQueueUsesEachUsersOwnLimit(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	activeAlice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-active-alice",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(activeAlice); err != nil {
		t.Fatalf("enqueue active alice request: %v", err)
	}
	limitLookup := func(username string) cpconfigstore.OrgResourceLimits {
		switch username {
		case "alice":
			return cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 10, UserMaxVCPUs: 1}
		case "bob":
			return cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 10}
		default:
			return cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 10}
		}
	}
	lease, err := store.TryAcquireOrgConnectionLeaseWithLimitLookup(activeAlice.RequestID, limitLookup, now)
	if err != nil {
		t.Fatalf("acquire active alice lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected active alice request to acquire a lease")
	}

	blockedAlice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-blocked-alice",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-bob",
		OrgID:          "org-1",
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1003,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(2 * time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(blockedAlice); err != nil {
		t.Fatalf("enqueue blocked alice request: %v", err)
	}
	if err := store.EnqueueOrgConnectionRequest(bob); err != nil {
		t.Fatalf("enqueue bob request: %v", err)
	}

	lease, err = store.TryAcquireOrgConnectionLeaseWithLimitLookup(bob.RequestID, limitLookup, now)
	if err != nil {
		t.Fatalf("acquire bob request: %v", err)
	}
	if lease == nil {
		t.Fatal("expected bob request to bypass alice using alice's saturated user limit, even though bob is unlimited")
	}
}

func TestOrgConnectionLeasesOutOfOrderPollGrantsOldestEligibleUserHead(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	alice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-alice",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-bob",
		OrgID:          "org-1",
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(alice); err != nil {
		t.Fatalf("enqueue alice request: %v", err)
	}
	if err := store.EnqueueOrgConnectionRequest(bob); err != nil {
		t.Fatalf("enqueue bob request: %v", err)
	}

	outOfOrder, err := store.TryAcquireOrgConnectionLease(bob.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 2}, now)
	if err != nil {
		t.Fatalf("out-of-order acquire: %v", err)
	}
	if outOfOrder != nil {
		t.Fatalf("expected bob to keep waiting while alice is granted first, got lease %q", outOfOrder.LeaseID)
	}

	aliceLease, err := store.TryAcquireOrgConnectionLease(alice.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 2}, now)
	if err != nil {
		t.Fatalf("acquire alice after scheduler grant: %v", err)
	}
	if aliceLease == nil || aliceLease.RequestID != alice.RequestID {
		t.Fatalf("expected alice lease to have been granted by bob's poll, got %#v", aliceLease)
	}
}

func TestCancelOrgConnectionRequestReleasesForeignGrantedLease(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	alice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-alice",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-bob",
		OrgID:          "org-1",
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	for _, entry := range []*cpconfigstore.OrgConnectionQueueEntry{alice, bob} {
		if err := store.EnqueueOrgConnectionRequest(entry); err != nil {
			t.Fatalf("enqueue %s: %v", entry.RequestID, err)
		}
	}

	outOfOrder, err := store.TryAcquireOrgConnectionLease(bob.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
	if err != nil {
		t.Fatalf("out-of-order acquire: %v", err)
	}
	if outOfOrder != nil {
		t.Fatalf("expected bob to keep waiting while alice is granted first, got lease %q", outOfOrder.LeaseID)
	}
	count, err := store.ActiveOrgConnectionLeaseCount(alice.OrgID)
	if err != nil {
		t.Fatalf("count active leases before cancel: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected alice foreign-granted lease to reserve capacity, got %d active leases", count)
	}

	if err := store.CancelOrgConnectionRequest(alice.RequestID, now.Add(time.Second)); err != nil {
		t.Fatalf("cancel alice request: %v", err)
	}
	count, err = store.ActiveOrgConnectionLeaseCount(alice.OrgID)
	if err != nil {
		t.Fatalf("count active leases after cancel: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected cancel to release unclaimed foreign-granted lease, got %d active leases", count)
	}
}

func TestOrgConnectionLeasesUserCapSkipDoesNotBypassSameUserFIFO(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	activeAlice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-active-alice",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 2,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(activeAlice); err != nil {
		t.Fatalf("enqueue active alice request: %v", err)
	}
	limitLookup := func(username string) cpconfigstore.OrgResourceLimits {
		switch username {
		case "alice":
			return cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 10, UserMaxVCPUs: 3}
		default:
			return cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 10}
		}
	}
	lease, err := store.TryAcquireOrgConnectionLeaseWithLimitLookup(activeAlice.RequestID, limitLookup, now)
	if err != nil {
		t.Fatalf("acquire active alice request: %v", err)
	}
	if lease == nil {
		t.Fatal("expected active alice request to acquire a lease")
	}

	aliceBig := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-alice-big",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 2,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	aliceSmall := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-alice-small",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1003,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(2 * time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-bob",
		OrgID:          "org-1",
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1004,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(3 * time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	for _, entry := range []*cpconfigstore.OrgConnectionQueueEntry{aliceBig, aliceSmall, bob} {
		if err := store.EnqueueOrgConnectionRequest(entry); err != nil {
			t.Fatalf("enqueue %s: %v", entry.RequestID, err)
		}
	}

	aliceSmallLease, err := store.TryAcquireOrgConnectionLeaseWithLimitLookup(aliceSmall.RequestID, limitLookup, now)
	if err != nil {
		t.Fatalf("acquire alice small request: %v", err)
	}
	if aliceSmallLease != nil {
		t.Fatalf("expected alice small request to wait behind alice big FIFO head, got lease %q", aliceSmallLease.LeaseID)
	}

	bobLease, err := store.TryAcquireOrgConnectionLeaseWithLimitLookup(bob.RequestID, limitLookup, now)
	if err != nil {
		t.Fatalf("acquire bob request: %v", err)
	}
	if bobLease == nil || bobLease.RequestID != bob.RequestID {
		t.Fatalf("expected bob to bypass alice's user-capped queue, got %#v", bobLease)
	}
}

func TestOrgConnectionLeasesOrgCapStopsAtFirstUnblockedUserHead(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")
	upsertActiveCP(t, store, "cp-c")

	now := time.Now()
	activeAlice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-active-alice",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	activeOrg := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-active-org",
		OrgID:          "org-1",
		Username:       "dave",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 2,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	limitLookup := func(username string) cpconfigstore.OrgResourceLimits {
		switch username {
		case "alice":
			return cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 4, UserMaxVCPUs: 1}
		default:
			return cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 4}
		}
	}
	for _, entry := range []*cpconfigstore.OrgConnectionQueueEntry{activeAlice, activeOrg} {
		if err := store.EnqueueOrgConnectionRequest(entry); err != nil {
			t.Fatalf("enqueue %s: %v", entry.RequestID, err)
		}
		lease, err := store.TryAcquireOrgConnectionLeaseWithLimitLookup(entry.RequestID, limitLookup, now)
		if err != nil {
			t.Fatalf("acquire %s: %v", entry.RequestID, err)
		}
		if lease == nil {
			t.Fatalf("expected %s to acquire a lease", entry.RequestID)
		}
	}

	blockedAlice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-blocked-alice",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1003,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(2 * time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-bob",
		OrgID:          "org-1",
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1004,
		Protocol:       "postgres",
		RequestedVCPUs: 2,
		EnqueuedAt:     now.Add(3 * time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	carol := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-carol",
		OrgID:          "org-1",
		Username:       "carol",
		CPInstanceID:   "cp-c",
		PID:            1005,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(4 * time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	for _, entry := range []*cpconfigstore.OrgConnectionQueueEntry{blockedAlice, bob, carol} {
		if err := store.EnqueueOrgConnectionRequest(entry); err != nil {
			t.Fatalf("enqueue %s: %v", entry.RequestID, err)
		}
	}

	carolLease, err := store.TryAcquireOrgConnectionLeaseWithLimitLookup(carol.RequestID, limitLookup, now)
	if err != nil {
		t.Fatalf("acquire carol request: %v", err)
	}
	if carolLease != nil {
		t.Fatalf("expected carol to wait behind bob's org-cap-blocked head, got lease %q", carolLease.LeaseID)
	}

	if err := store.ReleaseOrgConnectionLease(activeOrg.RequestID); err != nil {
		t.Fatalf("release active org lease: %v", err)
	}
	bobLease, err := store.TryAcquireOrgConnectionLeaseWithLimitLookup(bob.RequestID, limitLookup, now)
	if err != nil {
		t.Fatalf("acquire bob after release: %v", err)
	}
	if bobLease == nil || bobLease.RequestID != bob.RequestID {
		t.Fatalf("expected bob to acquire before carol after org capacity frees, got %#v", bobLease)
	}
}

func TestOrgConnectionLeasesReordersUserQueueAfterHeadGranted(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	aliceFirst := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-alice-1",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-bob-1",
		OrgID:          "org-1",
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	aliceSecond := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-alice-2",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1003,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(2 * time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	for _, entry := range []*cpconfigstore.OrgConnectionQueueEntry{aliceFirst, bob, aliceSecond} {
		if err := store.EnqueueOrgConnectionRequest(entry); err != nil {
			t.Fatalf("enqueue %s: %v", entry.RequestID, err)
		}
	}

	limits := cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 3}
	aliceFirstLease, err := store.TryAcquireOrgConnectionLease(aliceFirst.RequestID, limits, now)
	if err != nil {
		t.Fatalf("acquire alice first: %v", err)
	}
	if aliceFirstLease == nil {
		t.Fatal("expected alice first request to acquire")
	}

	aliceSecondLease, err := store.TryAcquireOrgConnectionLease(aliceSecond.RequestID, limits, now)
	if err != nil {
		t.Fatalf("acquire alice second before bob: %v", err)
	}
	if aliceSecondLease != nil {
		t.Fatalf("expected alice second to wait behind bob after alice first is granted, got lease %q", aliceSecondLease.LeaseID)
	}

	bobLease, err := store.TryAcquireOrgConnectionLease(bob.RequestID, limits, now)
	if err != nil {
		t.Fatalf("acquire bob: %v", err)
	}
	if bobLease == nil || bobLease.RequestID != bob.RequestID {
		t.Fatalf("expected bob to acquire before alice second, got %#v", bobLease)
	}
}

func TestOrgConnectionAdmissionMetricsObserveAttemptsAndQueueShape(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")

	durationBefore := configstoreMetricHistogramCount(t, "duckgres_org_connection_admission_duration_seconds")
	queueBefore := configstoreMetricHistogramCount(t, "duckgres_org_connection_admission_queue_depth")
	userQueuesBefore := configstoreMetricHistogramCount(t, "duckgres_org_connection_admission_user_queues")
	outcomesBefore := configstoreMetricCounterFamilyTotal(t, "duckgres_org_connection_admission_attempts_total")

	now := time.Now()
	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-metrics",
		OrgID:          "org-metrics",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(request); err != nil {
		t.Fatalf("enqueue metrics request: %v", err)
	}
	lease, err := store.TryAcquireOrgConnectionLease(request.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 2}, now)
	if err != nil {
		t.Fatalf("acquire metrics request: %v", err)
	}
	if lease == nil {
		t.Fatal("expected metrics request to acquire")
	}

	durationAfter := configstoreMetricHistogramCount(t, "duckgres_org_connection_admission_duration_seconds")
	queueAfter := configstoreMetricHistogramCount(t, "duckgres_org_connection_admission_queue_depth")
	userQueuesAfter := configstoreMetricHistogramCount(t, "duckgres_org_connection_admission_user_queues")
	outcomesAfter := configstoreMetricCounterFamilyTotal(t, "duckgres_org_connection_admission_attempts_total")

	if durationAfter-durationBefore != 1 {
		t.Fatalf("expected admission duration sample delta 1, got %d", durationAfter-durationBefore)
	}
	if queueAfter-queueBefore != 1 {
		t.Fatalf("expected queue depth sample delta 1, got %d", queueAfter-queueBefore)
	}
	if userQueuesAfter-userQueuesBefore != 1 {
		t.Fatalf("expected user queues sample delta 1, got %d", userQueuesAfter-userQueuesBefore)
	}
	if outcomesAfter-outcomesBefore != 1 {
		t.Fatalf("expected admission attempts counter delta 1, got %f", outcomesAfter-outcomesBefore)
	}
}

func TestOrgConnectionLeasesChargeLegacyZeroVCPULeaseAsOneVCPU(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	if err := store.DB().Exec(
		"INSERT INTO "+store.RuntimeSchema()+".org_connection_leases (lease_id, request_id, org_id, cp_instance_id, p_id, protocol, requested_vcpus, acquired_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"legacy-lease", "legacy-request", "org-1", "cp-a", 1001, "postgres", 0, now, now, now,
	).Error; err != nil {
		t.Fatalf("insert legacy lease: %v", err)
	}

	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-b",
		OrgID:          "org-1",
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(request); err != nil {
		t.Fatalf("enqueue request: %v", err)
	}

	blocked, err := store.TryAcquireOrgConnectionLease(request.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
	if err != nil {
		t.Fatalf("acquire request: %v", err)
	}
	if blocked != nil {
		t.Fatalf("expected legacy zero-vCPU lease to consume fallback capacity, got lease %q", blocked.LeaseID)
	}
}

func TestOrgConnectionLeasesChargeLegacyEmptyUserLeaseAgainstUserBudget(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	if err := store.DB().Exec(
		"INSERT INTO "+store.RuntimeSchema()+".org_connection_leases (lease_id, request_id, org_id, cp_instance_id, p_id, protocol, requested_vcpus, acquired_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"legacy-lease", "legacy-request", "org-1", "cp-a", 1001, "postgres", 0, now, now, now,
	).Error; err != nil {
		t.Fatalf("insert legacy lease: %v", err)
	}

	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-alice",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(request); err != nil {
		t.Fatalf("enqueue request: %v", err)
	}

	blocked, err := store.TryAcquireOrgConnectionLease(request.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 10, UserMaxVCPUs: 1}, now)
	if err != nil {
		t.Fatalf("acquire request: %v", err)
	}
	if blocked != nil {
		t.Fatalf("expected legacy empty-user lease to consume fallback user capacity, got lease %q", blocked.LeaseID)
	}
}

func TestOrgConnectionLeasesEnforceClusterWideLimit(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	first := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-a",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	second := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-b",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(first); err != nil {
		t.Fatalf("enqueue first request: %v", err)
	}
	if err := store.EnqueueOrgConnectionRequest(second); err != nil {
		t.Fatalf("enqueue second request: %v", err)
	}

	lease, err := store.TryAcquireOrgConnectionLease(first.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected first request to acquire a lease")
		return
	}

	blocked, err := store.TryAcquireOrgConnectionLease(second.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
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

	lease, err = store.TryAcquireOrgConnectionLease(second.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
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
			RequestID:      "request-a",
			OrgID:          "org-1",
			Username:       "alice",
			CPInstanceID:   "cp-a",
			PID:            1001,
			Protocol:       "postgres",
			RequestedVCPUs: 1,
			EnqueuedAt:     now,
			ExpiresAt:      now.Add(time.Minute),
		},
		{
			RequestID:      "request-b",
			OrgID:          "org-1",
			Username:       "alice",
			CPInstanceID:   "cp-b",
			PID:            1002,
			Protocol:       "postgres",
			RequestedVCPUs: 1,
			EnqueuedAt:     now.Add(time.Millisecond),
			ExpiresAt:      now.Add(time.Minute),
		},
	}
	for _, entry := range entries {
		if err := store.EnqueueOrgConnectionRequest(entry); err != nil {
			t.Fatalf("enqueue %s: %v", entry.RequestID, err)
		}
	}

	outOfOrder, err := store.TryAcquireOrgConnectionLease("request-b", cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
	if err != nil {
		t.Fatalf("out-of-order acquire: %v", err)
	}
	if outOfOrder != nil {
		t.Fatalf("expected FIFO to block request-b while request-a is pending, got lease %q", outOfOrder.LeaseID)
	}

	first, err := store.TryAcquireOrgConnectionLease("request-a", cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
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
		RequestID:      "request-first-inserted",
		OrgID:          "org-clock-skew",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(10 * time.Minute),
		ExpiresAt:      now.Add(70 * time.Minute),
	}
	second := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-second-inserted",
		OrgID:          "org-clock-skew",
		Username:       "alice",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(-10 * time.Minute),
		ExpiresAt:      now.Add(50 * time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(first); err != nil {
		t.Fatalf("enqueue first request: %v", err)
	}
	if err := store.EnqueueOrgConnectionRequest(second); err != nil {
		t.Fatalf("enqueue second request: %v", err)
	}

	outOfOrder, err := store.TryAcquireOrgConnectionLease(second.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
	if err != nil {
		t.Fatalf("out-of-order acquire: %v", err)
	}
	if outOfOrder != nil {
		t.Fatalf("expected database insertion order to block request-second-inserted, got lease %q", outOfOrder.LeaseID)
	}

	lease, err := store.TryAcquireOrgConnectionLease(first.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
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
		RequestID:      "request-a",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(first); err != nil {
		t.Fatalf("enqueue first request: %v", err)
	}
	if _, err := store.TryAcquireOrgConnectionLease(first.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now); err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}

	expiredAt := now.Add(time.Second)
	if err := store.UpsertControlPlaneInstance(&cpconfigstore.ControlPlaneInstance{
		ID:              "cp-a",
		PodName:         "cp-a",
		State:           cpconfigstore.ControlPlaneInstanceStateExpired,
		StartedAt:       now,
		LastHeartbeatAt: now,
		ExpiredAt:       &expiredAt,
	}); err != nil {
		t.Fatalf("expire cp-a: %v", err)
	}

	second := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-b",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(second); err != nil {
		t.Fatalf("enqueue second request: %v", err)
	}
	lease, err := store.TryAcquireOrgConnectionLease(second.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
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
		LeaseID:        "missing-owner-lease",
		RequestID:      "missing-owner-request",
		OrgID:          "org-missing-owner",
		Username:       "alice",
		CPInstanceID:   "missing-cp",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		AcquiredAt:     now.Add(-10 * time.Minute),
	}
	if err := store.DB().Table(store.RuntimeSchema() + ".org_connection_leases").Create(staleLease).Error; err != nil {
		t.Fatalf("insert stale missing-owner lease: %v", err)
	}

	entry := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "replacement-request",
		OrgID:          staleLease.OrgID,
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(entry); err != nil {
		t.Fatalf("enqueue replacement request: %v", err)
	}

	lease, err := store.TryAcquireOrgConnectionLease(entry.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
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
		RequestID:      "request-a",
		OrgID:          "org-1",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(entry); err != nil {
		t.Fatalf("enqueue request: %v", err)
	}

	first, err := store.TryAcquireOrgConnectionLease(entry.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if first == nil {
		t.Fatal("expected first acquire to grant lease")
		return
	}

	retry, err := store.TryAcquireOrgConnectionLease(entry.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now.Add(time.Second))
	if err != nil {
		t.Fatalf("retry acquire: %v", err)
	}
	if retry == nil {
		t.Fatal("expected retry acquire to return existing lease")
		return
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
		RequestID:      "expired-request",
		OrgID:          "org-lock-order",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(-2 * time.Minute),
		ExpiresAt:      now.Add(-time.Minute),
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
		lease, err := store.TryAcquireOrgConnectionLease(entry.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
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
		RequestID:      "reused-request",
		OrgID:          "org-old",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
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
		lease, err := store.TryAcquireOrgConnectionLease(oldEntry.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
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
		RequestID:      oldEntry.RequestID,
		OrgID:          "org-new",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
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

func configstoreMetricHistogramCount(t *testing.T, metricName string) uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != metricName {
			continue
		}
		if fam.GetType() != dto.MetricType_HISTOGRAM {
			t.Fatalf("metric %q is not a histogram", metricName)
		}
		var total uint64
		for _, metric := range fam.GetMetric() {
			total += metric.GetHistogram().GetSampleCount()
		}
		return total
	}
	return 0
}

func configstoreMetricCounterFamilyTotal(t *testing.T, metricName string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != metricName {
			continue
		}
		if fam.GetType() != dto.MetricType_COUNTER {
			t.Fatalf("metric %q is not a counter", metricName)
		}
		var total float64
		for _, metric := range fam.GetMetric() {
			total += metric.GetCounter().GetValue()
		}
		return total
	}
	return 0
}
