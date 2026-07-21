//go:build linux || darwin

package configstore_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
)

func enqueueOrgConnectionRequests(t *testing.T, store *cpconfigstore.ConfigStore, requests ...*cpconfigstore.OrgConnectionQueueEntry) {
	t.Helper()
	for _, request := range requests {
		if err := store.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}
}

func newOrgConnectionRequest(id, orgID, username, cpID string, requestedVCPUs int, enqueuedAt time.Time) *cpconfigstore.OrgConnectionQueueEntry {
	return &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      id,
		OrgID:          orgID,
		Username:       username,
		CPInstanceID:   cpID,
		PID:            int32(1000 + requestedVCPUs),
		Protocol:       "postgres",
		RequestedVCPUs: requestedVCPUs,
		EnqueuedAt:     enqueuedAt,
		ExpiresAt:      enqueuedAt.Add(time.Minute),
	}
}

func TestScheduleAndClaimBatchesOffersButClaimsOnlyCaller(t *testing.T) {
	storeA, storeB, _, _ := newSharedConfigStores(t)
	for _, cpID := range []string{"cp-a", "cp-b", "cp-c"} {
		upsertActiveCP(t, storeA, cpID)
	}
	activateAdmissionOffers(t, storeA)
	const orgID = "org-schedule-and-claim-batch"
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 3, map[string]int{
		"alice": 0,
		"bob":   0,
		"carol": 0,
	})

	now := time.Now()
	alice := newOrgConnectionRequest("request-alice", orgID, "alice", "cp-a", 1, now)
	bob := newOrgConnectionRequest("request-bob", orgID, "bob", "cp-b", 1, now.Add(time.Millisecond))
	carol := newOrgConnectionRequest("request-carol", orgID, "carol", "cp-c", 1, now.Add(2*time.Millisecond))
	enqueueOrgConnectionRequests(t, storeA, alice, bob, carol)

	// Bob happens to win the org scheduler lock. Queue order controls which
	// reservations are made, while Bob may claim only Bob's own reservation.
	bobLease, err := storeB.ScheduleAndClaimOrgConnectionLease(bob.RequestID, "cp-b")
	if err != nil || bobLease == nil || bobLease.RequestID != bob.RequestID {
		t.Fatalf("bob schedule-and-claim = %#v, err = %v", bobLease, err)
	}
	assertOrgConnectionOffer(t, storeA, alice.RequestID)
	assertOrgConnectionRequestGranted(t, storeA, bob.RequestID)
	assertOrgConnectionOffer(t, storeA, carol.RequestID)
	assertOrgConnectionOfferedVCPUs(t, storeA, orgID, 2)
	assertOrgConnectionActiveVCPUs(t, storeA, orgID, 1)

	aliceLease, err := storeA.ScheduleAndClaimOrgConnectionLease(alice.RequestID, "cp-a")
	if err != nil || aliceLease == nil || aliceLease.RequestID != alice.RequestID {
		t.Fatalf("alice claims existing offer = %#v, err = %v", aliceLease, err)
	}
	assertOrgConnectionRequestGranted(t, storeB, alice.RequestID)
	assertOrgConnectionOffer(t, storeB, carol.RequestID)
}

func TestOrgConnectionDispatcherDoesNotBypassOldestOrgBlockedRequest(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")
	activateAdmissionOffers(t, store)
	const orgID = "org-no-size-bypass"
	seedAuthoritativeOrgConnectionLimits(t, store, orgID, 3, map[string]int{"alice": 0, "bob": 0})

	now := time.Now()
	alice := newOrgConnectionRequest("request-too-large", orgID, "alice", "cp-a", 4, now)
	bob := newOrgConnectionRequest("request-small-later", orgID, "bob", "cp-b", 1, now.Add(time.Millisecond))
	enqueueOrgConnectionRequests(t, store, alice, bob)

	offered, err := store.DispatchOrgConnectionAdmissions(orgID)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if offered != 0 {
		t.Fatalf("dispatcher bypassed oldest org-blocked request with %d later offers", offered)
	}
	assertOrgConnectionRequestState(t, store, alice.RequestID, cpconfigstore.OrgConnectionRequestStatePending)
	assertOrgConnectionRequestState(t, store, bob.RequestID, cpconfigstore.OrgConnectionRequestStatePending)
}

func TestOrgConnectionDispatcherReconcilesOffersAfterLimitDecrease(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")
	activateAdmissionOffers(t, store)
	const orgID = "org-limit-decrease"
	seedAuthoritativeOrgConnectionLimits(t, store, orgID, 2, map[string]int{"alice": 0, "bob": 0})

	now := time.Now()
	alice := newOrgConnectionRequest("request-alice", orgID, "alice", "cp-a", 1, now)
	bob := newOrgConnectionRequest("request-bob", orgID, "bob", "cp-b", 1, now.Add(time.Millisecond))
	enqueueOrgConnectionRequests(t, store, alice, bob)
	if offered, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 2 {
		t.Fatalf("initial dispatch = %d, err = %v; want two", offered, err)
	}

	if err := store.DB().Model(&cpconfigstore.Org{}).
		Where("name = ?", orgID).
		Update("max_vcpus", 1).Error; err != nil {
		t.Fatalf("lower authoritative org limit: %v", err)
	}
	if offered, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 0 {
		t.Fatalf("dispatch after limit decrease = %d, err = %v; want no new offers", offered, err)
	}
	assertOrgConnectionOffer(t, store, alice.RequestID)
	assertOrgConnectionRequestState(t, store, bob.RequestID, cpconfigstore.OrgConnectionRequestStatePending)
	assertOrgConnectionOfferedVCPUs(t, store, orgID, 1)

	if lease, err := store.ClaimOrgConnectionOffer(bob.RequestID, "cp-b"); err != nil || lease != nil {
		t.Fatalf("revoked offer claim = %#v, err = %v; want clean miss", lease, err)
	}
}

func TestOrgConnectionDispatcherReoffersExpiredReservation(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	activateAdmissionOffers(t, store)
	const orgID = "org-offer-expiry"
	seedAuthoritativeOrgConnectionLimits(t, store, orgID, 1, map[string]int{"alice": 0})

	now := time.Now()
	request := newOrgConnectionRequest("request-alice", orgID, "alice", "cp-a", 1, now)
	enqueueOrgConnectionRequests(t, store, request)
	if offered, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 1 {
		t.Fatalf("initial dispatch = %d, err = %v", offered, err)
	}
	first := assertOrgConnectionOffer(t, store, request.RequestID)

	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_queue").
		Where("request_id = ?", request.RequestID).
		Update("offer_expires_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatalf("expire offer: %v", err)
	}
	if offered, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 1 {
		t.Fatalf("redispatch expired offer = %d, err = %v; want one replacement", offered, err)
	}
	second := assertOrgConnectionOffer(t, store, request.RequestID)
	if first.OfferedAt == nil || second.OfferedAt == nil || !second.OfferedAt.After(*first.OfferedAt) {
		t.Fatalf("replacement offer time = %v, first = %v; want a newer durable offer", second.OfferedAt, first.OfferedAt)
	}
}

func TestOrgConnectionCancelAndReleaseImmediatelyRefillOffers(t *testing.T) {
	tests := []struct {
		name string
		free func(*testing.T, *cpconfigstore.ConfigStore, *cpconfigstore.OrgConnectionQueueEntry, *cpconfigstore.OrgConnectionLease)
	}{
		{
			name: "cancel offered request",
			free: func(t *testing.T, store *cpconfigstore.ConfigStore, request *cpconfigstore.OrgConnectionQueueEntry, _ *cpconfigstore.OrgConnectionLease) {
				t.Helper()
				if err := store.CancelOrgConnectionRequest(request.RequestID, time.Now()); err != nil {
					t.Fatalf("cancel offered request: %v", err)
				}
			},
		},
		{
			name: "release active lease",
			free: func(t *testing.T, store *cpconfigstore.ConfigStore, _ *cpconfigstore.OrgConnectionQueueEntry, lease *cpconfigstore.OrgConnectionLease) {
				t.Helper()
				if err := store.ReleaseOrgConnectionLease(lease.LeaseID); err != nil {
					t.Fatalf("release active lease: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newIsolatedConfigStore(t)
			upsertActiveCP(t, store, "cp-a")
			upsertActiveCP(t, store, "cp-b")
			activateAdmissionOffers(t, store)
			orgID := "org-refill-" + tt.name
			seedAuthoritativeOrgConnectionLimits(t, store, orgID, 1, map[string]int{"alice": 0, "bob": 0})

			now := time.Now()
			alice := newOrgConnectionRequest("request-alice", orgID, "alice", "cp-a", 1, now)
			bob := newOrgConnectionRequest("request-bob", orgID, "bob", "cp-b", 1, now.Add(time.Millisecond))
			enqueueOrgConnectionRequests(t, store, alice, bob)
			if offered, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 1 {
				t.Fatalf("initial dispatch = %d, err = %v", offered, err)
			}

			var aliceLease *cpconfigstore.OrgConnectionLease
			if tt.name == "release active lease" {
				var err error
				aliceLease, err = store.ClaimOrgConnectionOffer(alice.RequestID, "cp-a")
				if err != nil || aliceLease == nil {
					t.Fatalf("claim alice = %#v, err = %v", aliceLease, err)
				}
			}
			tt.free(t, store, alice, aliceLease)

			// No explicit dispatcher call: freeing reserved capacity triggers a
			// best-effort scheduling pass before the API returns.
			assertOrgConnectionOffer(t, store, bob.RequestID)
		})
	}
}

func TestOrgConnectionReshardFlipFencesOutstandingOfferClaim(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	activateAdmissionOffers(t, store)
	const orgID = "org-offer-reshard"
	seedReadyWarehouse(t, store, orgID)
	if err := store.DB().Create(&cpconfigstore.OrgUser{
		OrgID: orgID, Username: "alice", Password: "test-password-hash",
	}).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}

	now := time.Now()
	request := newOrgConnectionRequest("request-alice", orgID, "alice", "cp-a", 1, now)
	enqueueOrgConnectionRequests(t, store, request)
	if offered, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 1 {
		t.Fatalf("dispatch offer = %d, err = %v", offered, err)
	}
	if err := store.SetWarehouseResharding(orgID); err != nil {
		t.Fatalf("set warehouse resharding: %v", err)
	}
	if lease, err := store.ClaimOrgConnectionOffer(request.RequestID, "cp-a"); err != nil || lease != nil {
		t.Fatalf("claim after reshard flip = %#v, err = %v; want clean miss", lease, err)
	}
	assertOrgConnectionOffer(t, store, request.RequestID)
	assertOrgConnectionActiveVCPUs(t, store, orgID, 0)
}

func TestOrgConnectionMixedVersionGateUsesLegacyPathBeforeActivation(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")
	const orgID = "org-mixed-version-cutover"
	seedAuthoritativeOrgConnectionLimits(t, store, orgID, 2, map[string]int{"alice": 0, "bob": 0})

	now := time.Now()
	// Register the old binary before the monotonic protocol transition. While
	// it remains live, new binaries keep using the request-owned legacy path.
	if err := store.UpsertControlPlaneInstance(&cpconfigstore.ControlPlaneInstance{
		ID: "cp-legacy", PodName: "cp-legacy", State: cpconfigstore.ControlPlaneInstanceStateActive,
		StartedAt: now, LastHeartbeatAt: now,
	}); err != nil {
		t.Fatalf("register legacy CP: %v", err)
	}
	if err := store.ActivateOrgConnectionAdmissionOffers(); !errors.Is(err, cpconfigstore.ErrAdmissionOfferProtocolActivationBlocked) {
		t.Fatalf("activation with live legacy CP error = %v, want activation-blocked sentinel", err)
	}
	alice := newOrgConnectionRequest("request-alice", orgID, "alice", "cp-a", 1, now)
	bob := newOrgConnectionRequest("request-bob", orgID, "bob", "cp-b", 1, now.Add(time.Millisecond))
	enqueueOrgConnectionRequests(t, store, alice, bob)
	if offered, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 0 {
		t.Fatalf("dispatch with live legacy CP = %d, err = %v; want disabled", offered, err)
	}
	aliceLease, err := store.ScheduleAndClaimOrgConnectionLease(alice.RequestID, "cp-a")
	if err != nil || aliceLease == nil {
		t.Fatalf("compatibility grant = %#v, err = %v", aliceLease, err)
	}
	assertOrgConnectionRequestGranted(t, store, alice.RequestID)
	assertOrgConnectionRequestState(t, store, bob.RequestID, cpconfigstore.OrgConnectionRequestStatePending)
	assertOrgConnectionOfferedVCPUs(t, store, orgID, 0)

	expiredAt := time.Now()
	if err := store.DB().Table(store.RuntimeSchema()+".cp_instances").
		Where("id = ?", "cp-legacy").
		Updates(map[string]any{
			"state":      cpconfigstore.ControlPlaneInstanceStateExpired,
			"expired_at": expiredAt,
		}).Error; err != nil {
		t.Fatalf("expire legacy CP: %v", err)
	}
	activateAdmissionOffers(t, store)
	if offered, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 1 {
		t.Fatalf("dispatch offers after explicit activation = %d, err = %v", offered, err)
	}
	assertOrgConnectionOffer(t, store, bob.RequestID)
}

func TestOrgConnectionConcurrentOwnerClaimsAreIdempotent(t *testing.T) {
	storeA, storeB, _, _ := newSharedConfigStores(t)
	upsertActiveCP(t, storeA, "cp-a")
	activateAdmissionOffers(t, storeA)
	const orgID = "org-concurrent-owner-claim"
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 1, map[string]int{"alice": 0})

	now := time.Now()
	request := newOrgConnectionRequest("request-alice", orgID, "alice", "cp-a", 1, now)
	enqueueOrgConnectionRequests(t, storeA, request)
	if offered, err := storeA.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 1 {
		t.Fatalf("dispatch = %d, err = %v", offered, err)
	}

	var wg sync.WaitGroup
	results := make(chan *cpconfigstore.OrgConnectionLease, 2)
	errs := make(chan error, 2)
	for _, store := range []*cpconfigstore.ConfigStore{storeA, storeB} {
		wg.Add(1)
		go func(store *cpconfigstore.ConfigStore) {
			defer wg.Done()
			lease, err := store.ClaimOrgConnectionOffer(request.RequestID, "cp-a")
			results <- lease
			errs <- err
		}(store)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent owner claim: %v", err)
		}
	}
	for lease := range results {
		if lease == nil || lease.LeaseID != request.RequestID {
			t.Fatalf("concurrent owner claim returned %#v", lease)
		}
	}
	assertOrgConnectionRequestGranted(t, storeA, request.RequestID)
	assertOrgConnectionActiveVCPUs(t, storeA, orgID, 1)
}
