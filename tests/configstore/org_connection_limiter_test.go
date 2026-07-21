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
		ID:                      id,
		PodName:                 id,
		State:                   cpconfigstore.ControlPlaneInstanceStateActive,
		StartedAt:               now,
		LastHeartbeatAt:         now,
		SupportsAdmissionOffers: true,
	}); err != nil {
		t.Fatalf("upsert control-plane instance %q: %v", id, err)
	}
}

func activateAdmissionOffers(t *testing.T, store *cpconfigstore.ConfigStore) {
	t.Helper()
	if err := store.ActivateOrgConnectionAdmissionOffers(); err != nil {
		t.Fatalf("activate org connection admission offers: %v", err)
	}
}

func newSharedConfigStores(t *testing.T) (*cpconfigstore.ConfigStore, *cpconfigstore.ConfigStore, string, string) {
	t.Helper()

	_, connStr := newIsolatedConfigStoreSchema(t)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	appA := "admission-a-" + suffix
	appB := "admission-b-" + suffix

	newStore := func(applicationName string) *cpconfigstore.ConfigStore {
		t.Helper()
		store, err := cpconfigStoreNew(connStr + " application_name=" + applicationName)
		if err != nil {
			t.Fatalf("new shared config store %q: %v", applicationName, err)
		}
		sqlDB, err := store.DB().DB()
		if err != nil {
			t.Fatalf("shared config store %q sql db: %v", applicationName, err)
		}
		t.Cleanup(func() { _ = sqlDB.Close() })
		return store
	}

	return newStore(appA), newStore(appB), appA, appB
}

func assertOrgConnectionRequestPending(t *testing.T, store *cpconfigstore.ConfigStore, requestID string) {
	t.Helper()

	var leaseCount int64
	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_leases").
		Where("request_id = ?", requestID).
		Count(&leaseCount).Error; err != nil {
		t.Fatalf("count leases for request %q: %v", requestID, err)
	}
	if leaseCount != 0 {
		t.Fatalf("expected request %q to have no lease, got %d", requestID, leaseCount)
	}

	var pendingCount int64
	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_queue").
		Where("request_id = ? AND granted_at IS NULL", requestID).
		Count(&pendingCount).Error; err != nil {
		t.Fatalf("count pending queue rows for request %q: %v", requestID, err)
	}
	if pendingCount != 1 {
		t.Fatalf("expected request %q to remain pending, got %d pending rows", requestID, pendingCount)
	}
}

func assertOrgConnectionRequestAbsent(t *testing.T, store *cpconfigstore.ConfigStore, requestID string) {
	t.Helper()

	var leaseCount int64
	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_leases").
		Where("request_id = ?", requestID).
		Count(&leaseCount).Error; err != nil {
		t.Fatalf("count leases for request %q: %v", requestID, err)
	}
	if leaseCount != 0 {
		t.Fatalf("expected request %q to have no lease, got %d", requestID, leaseCount)
	}

	var queueCount int64
	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_queue").
		Where("request_id = ?", requestID).
		Count(&queueCount).Error; err != nil {
		t.Fatalf("count queue rows for request %q: %v", requestID, err)
	}
	if queueCount != 0 {
		t.Fatalf("expected request %q to have no queue row, got %d", requestID, queueCount)
	}
}

func assertOrgConnectionRequestGranted(t *testing.T, store *cpconfigstore.ConfigStore, requestID string) {
	t.Helper()

	var leaseCount int64
	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_leases").
		Where("request_id = ?", requestID).
		Count(&leaseCount).Error; err != nil {
		t.Fatalf("count leases for request %q: %v", requestID, err)
	}
	if leaseCount != 1 {
		t.Fatalf("expected request %q to have one lease, got %d", requestID, leaseCount)
	}

	var grantedCount int64
	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_queue").
		Where("request_id = ? AND granted_at IS NOT NULL", requestID).
		Count(&grantedCount).Error; err != nil {
		t.Fatalf("count granted queue rows for request %q: %v", requestID, err)
	}
	if grantedCount != 1 {
		t.Fatalf("expected request %q to have one granted queue row, got %d", requestID, grantedCount)
	}
}

func assertOrgConnectionActiveVCPUs(t *testing.T, store *cpconfigstore.ConfigStore, orgID string, want int64) {
	t.Helper()

	var got int64
	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_leases").
		Select("COALESCE(SUM(requested_vcpus), 0)").
		Where("org_id = ?", orgID).
		Scan(&got).Error; err != nil {
		t.Fatalf("sum active vCPUs for org %q: %v", orgID, err)
	}
	if got != want {
		t.Fatalf("active vCPUs for org %q = %d, want %d", orgID, got, want)
	}
}

func seedAuthoritativeOrgConnectionLimits(t *testing.T, store *cpconfigstore.ConfigStore, orgID string, orgMaxVCPUs int, userMaxVCPUs map[string]int) {
	t.Helper()

	if err := store.DB().Create(&cpconfigstore.Org{
		Name:         orgID,
		DatabaseName: orgID,
		MaxVCPUs:     orgMaxVCPUs,
	}).Error; err != nil {
		t.Fatalf("seed org %q limits: %v", orgID, err)
	}
	for username, maxVCPUs := range userMaxVCPUs {
		if err := store.DB().Create(&cpconfigstore.OrgUser{
			OrgID:    orgID,
			Username: username,
			Password: "test-password-hash",
			MaxVCPUs: maxVCPUs,
		}).Error; err != nil {
			t.Fatalf("seed limits for %s/%s: %v", orgID, username, err)
		}
	}
}

func assertOrgConnectionRequestState(t *testing.T, store *cpconfigstore.ConfigStore, requestID string, want cpconfigstore.OrgConnectionRequestState) cpconfigstore.OrgConnectionQueueEntry {
	t.Helper()

	var request cpconfigstore.OrgConnectionQueueEntry
	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_queue").
		Where("request_id = ?", requestID).
		Take(&request).Error; err != nil {
		t.Fatalf("load org connection request %q: %v", requestID, err)
	}
	if request.State != want {
		t.Fatalf("request %q state = %q, want %q", requestID, request.State, want)
	}
	return request
}

func assertOrgConnectionOffer(t *testing.T, store *cpconfigstore.ConfigStore, requestID string) cpconfigstore.OrgConnectionQueueEntry {
	t.Helper()

	request := assertOrgConnectionRequestState(t, store, requestID, cpconfigstore.OrgConnectionRequestStateOffered)
	if request.OfferedAt == nil {
		t.Fatalf("request %q is offered without offered_at", requestID)
	}
	if request.OfferExpiresAt == nil {
		t.Fatalf("request %q is offered without offer_expires_at", requestID)
	}
	if !request.OfferExpiresAt.After(*request.OfferedAt) {
		t.Fatalf("request %q offer expiry %s is not after offer time %s", requestID, request.OfferExpiresAt, request.OfferedAt)
	}
	if request.OfferExpiresAt.After(request.ExpiresAt) {
		t.Fatalf("request %q offer expiry %s exceeds request expiry %s", requestID, request.OfferExpiresAt, request.ExpiresAt)
	}
	if request.GrantedAt != nil {
		t.Fatalf("request %q was marked granted before its owner claimed the offer", requestID)
	}

	var leaseCount int64
	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_leases").
		Where("request_id = ?", requestID).
		Count(&leaseCount).Error; err != nil {
		t.Fatalf("count active leases for offered request %q: %v", requestID, err)
	}
	if leaseCount != 0 {
		t.Fatalf("offered request %q created %d active leases before owner claim", requestID, leaseCount)
	}
	return request
}

func assertOrgConnectionOfferedVCPUs(t *testing.T, store *cpconfigstore.ConfigStore, orgID string, want int64) {
	t.Helper()

	var got int64
	if err := store.DB().Table(store.RuntimeSchema()+".org_connection_queue").
		Select("COALESCE(SUM(requested_vcpus), 0)").
		Where("org_id = ? AND state = ?", orgID, cpconfigstore.OrgConnectionRequestStateOffered).
		Scan(&got).Error; err != nil {
		t.Fatalf("sum offered vCPUs for org %q: %v", orgID, err)
	}
	if got != want {
		t.Fatalf("offered vCPUs for org %q = %d, want %d", orgID, got, want)
	}
}

func TestOrgConnectionDispatcherCreatesDurableBatchOffers(t *testing.T) {
	storeA, storeB, _, _ := newSharedConfigStores(t)
	for _, cpID := range []string{"cp-a", "cp-b", "cp-c"} {
		upsertActiveCP(t, storeA, cpID)
	}
	activateAdmissionOffers(t, storeA)
	const orgID = "org-dispatch-batch"
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 3, map[string]int{
		"alice": 0,
		"bob":   0,
		"carol": 0,
	})

	now := time.Now()
	requests := []*cpconfigstore.OrgConnectionQueueEntry{
		{RequestID: "request-alice", OrgID: orgID, Username: "alice", CPInstanceID: "cp-a", PID: 1001, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now, ExpiresAt: now.Add(time.Minute)},
		{RequestID: "request-bob", OrgID: orgID, Username: "bob", CPInstanceID: "cp-b", PID: 1002, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now.Add(time.Millisecond), ExpiresAt: now.Add(time.Minute)},
		{RequestID: "request-carol", OrgID: orgID, Username: "carol", CPInstanceID: "cp-c", PID: 1003, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now.Add(2 * time.Millisecond), ExpiresAt: now.Add(time.Minute)},
	}
	for _, request := range requests {
		if err := storeA.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}

	offered, err := storeA.DispatchOrgConnectionAdmissions(orgID)
	if err != nil {
		t.Fatalf("dispatch org connection admissions: %v", err)
	}
	if offered != len(requests) {
		t.Fatalf("dispatcher created %d offers, want one batch of %d", offered, len(requests))
	}

	// Read through an independent ConfigStore connection: offers are durable
	// database state, not scheduler-local wakeups or promises.
	for _, request := range requests {
		assertOrgConnectionOffer(t, storeB, request.RequestID)
	}
	assertOrgConnectionOfferedVCPUs(t, storeB, orgID, 3)
	assertOrgConnectionActiveVCPUs(t, storeB, orgID, 0)
}

func TestOrgConnectionOfferCanOnlyBeClaimedByItsOwner(t *testing.T) {
	storeA, storeB, _, _ := newSharedConfigStores(t)
	upsertActiveCP(t, storeA, "cp-a")
	upsertActiveCP(t, storeA, "cp-b")
	activateAdmissionOffers(t, storeA)
	const orgID = "org-owner-claim"
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 1, map[string]int{"alice": 0})

	now := time.Now()
	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-alice",
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
	if offered, err := storeA.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 1 {
		t.Fatalf("dispatch offer = %d, err = %v; want one offer", offered, err)
	}
	assertOrgConnectionOffer(t, storeB, request.RequestID)

	foreignLease, err := storeB.ClaimOrgConnectionOffer(request.RequestID, "cp-b")
	if err != nil {
		t.Fatalf("foreign claim should be a clean miss: %v", err)
	}
	if foreignLease != nil {
		t.Fatalf("foreign CP claimed owner cp-a's offer: %#v", foreignLease)
	}
	assertOrgConnectionOffer(t, storeA, request.RequestID)
	assertOrgConnectionActiveVCPUs(t, storeA, orgID, 0)

	ownerLease, err := storeA.ClaimOrgConnectionOffer(request.RequestID, "cp-a")
	if err != nil {
		t.Fatalf("owner claim: %v", err)
	}
	if ownerLease == nil || ownerLease.RequestID != request.RequestID || ownerLease.CPInstanceID != "cp-a" {
		t.Fatalf("owner claim returned %#v, want cp-a's request", ownerLease)
	}
	assertOrgConnectionRequestGranted(t, storeB, request.RequestID)
	assertOrgConnectionActiveVCPUs(t, storeB, orgID, 1)
}

func TestOrgConnectionDispatcherPrunesExpiredOwnerPendingHead(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-dead")
	upsertActiveCP(t, store, "cp-live")
	activateAdmissionOffers(t, store)
	const orgID = "org-dead-owner-head"
	seedAuthoritativeOrgConnectionLimits(t, store, orgID, 1, map[string]int{"alice": 0, "bob": 0})

	now := time.Now()
	deadOwner := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-dead-owner", OrgID: orgID, Username: "alice", CPInstanceID: "cp-dead",
		PID: 1001, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	liveOwner := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-live-owner", OrgID: orgID, Username: "bob", CPInstanceID: "cp-live",
		PID: 1002, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now.Add(time.Millisecond), ExpiresAt: now.Add(time.Minute),
	}
	for _, request := range []*cpconfigstore.OrgConnectionQueueEntry{deadOwner, liveOwner} {
		if err := store.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}
	expiredAt := now.Add(time.Second)
	if err := store.UpsertControlPlaneInstance(&cpconfigstore.ControlPlaneInstance{
		ID: "cp-dead", PodName: "cp-dead", SupportsAdmissionOffers: true, State: cpconfigstore.ControlPlaneInstanceStateExpired,
		StartedAt: now, LastHeartbeatAt: now, ExpiredAt: &expiredAt,
	}); err != nil {
		t.Fatalf("expire dead owner: %v", err)
	}

	offered, err := store.DispatchOrgConnectionAdmissions(orgID)
	if err != nil {
		t.Fatalf("dispatch behind expired owner: %v", err)
	}
	if offered != 1 {
		t.Fatalf("dispatcher created %d offers, want live owner's one offer", offered)
	}
	assertOrgConnectionRequestAbsent(t, store, deadOwner.RequestID)
	assertOrgConnectionOffer(t, store, liveOwner.RequestID)
	assertOrgConnectionActiveVCPUs(t, store, orgID, 0)
}

func TestOrgConnectionDispatcherUsesAuthoritativeDatabaseLimits(t *testing.T) {
	storeA, storeB, _, _ := newSharedConfigStores(t)
	for _, cpID := range []string{"cp-a", "cp-b", "cp-c"} {
		upsertActiveCP(t, storeA, cpID)
	}
	activateAdmissionOffers(t, storeA)
	const orgID = "org-authoritative-limits"

	// Both stores loaded their snapshots before these rows existed. Dispatch
	// must read the authoritative rows in PostgreSQL, not either CP's stale
	// local snapshot or a request-owner callback.
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 4, map[string]int{
		"alice": 1,
		"bob":   0,
		"carol": 0,
	})
	if _, found := storeA.Snapshot().Orgs[orgID]; found {
		t.Fatalf("test setup unexpectedly refreshed store A snapshot for %q", orgID)
	}
	if _, found := storeB.Snapshot().Orgs[orgID]; found {
		t.Fatalf("test setup unexpectedly refreshed store B snapshot for %q", orgID)
	}

	now := time.Now()
	requests := []*cpconfigstore.OrgConnectionQueueEntry{
		{RequestID: "request-alice-1", OrgID: orgID, Username: "alice", CPInstanceID: "cp-a", PID: 1001, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now, ExpiresAt: now.Add(time.Minute)},
		{RequestID: "request-alice-2", OrgID: orgID, Username: "alice", CPInstanceID: "cp-a", PID: 1002, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now.Add(time.Millisecond), ExpiresAt: now.Add(time.Minute)},
		{RequestID: "request-bob", OrgID: orgID, Username: "bob", CPInstanceID: "cp-b", PID: 1003, Protocol: "postgres", RequestedVCPUs: 2, EnqueuedAt: now.Add(2 * time.Millisecond), ExpiresAt: now.Add(time.Minute)},
		{RequestID: "request-carol", OrgID: orgID, Username: "carol", CPInstanceID: "cp-c", PID: 1004, Protocol: "postgres", RequestedVCPUs: 2, EnqueuedAt: now.Add(3 * time.Millisecond), ExpiresAt: now.Add(time.Minute)},
	}
	for _, request := range requests {
		if err := storeA.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}

	offered, err := storeB.DispatchOrgConnectionAdmissions(orgID)
	if err != nil {
		t.Fatalf("dispatch with stale local snapshot: %v", err)
	}
	if offered != 2 {
		t.Fatalf("dispatcher created %d offers, want alice-1 and bob", offered)
	}
	assertOrgConnectionOffer(t, storeA, "request-alice-1")
	assertOrgConnectionOffer(t, storeA, "request-bob")
	assertOrgConnectionRequestState(t, storeA, "request-alice-2", cpconfigstore.OrgConnectionRequestStatePending)
	assertOrgConnectionRequestState(t, storeA, "request-carol", cpconfigstore.OrgConnectionRequestStatePending)
	assertOrgConnectionOfferedVCPUs(t, storeA, orgID, 3)
	assertOrgConnectionActiveVCPUs(t, storeA, orgID, 0)
}

func TestOrgConnectionDispatcherCountsOffersAgainstCapacity(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")
	activateAdmissionOffers(t, store)
	const orgID = "org-offered-capacity"
	seedAuthoritativeOrgConnectionLimits(t, store, orgID, 2, map[string]int{"alice": 0, "bob": 0})

	now := time.Now()
	alice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-alice", OrgID: orgID, Username: "alice", CPInstanceID: "cp-a",
		PID: 1001, Protocol: "postgres", RequestedVCPUs: 2, EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID: "request-bob", OrgID: orgID, Username: "bob", CPInstanceID: "cp-b",
		PID: 1002, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now.Add(time.Millisecond), ExpiresAt: now.Add(time.Minute),
	}
	if err := store.EnqueueOrgConnectionRequest(alice); err != nil {
		t.Fatalf("enqueue alice: %v", err)
	}
	if offered, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 1 {
		t.Fatalf("dispatch alice offer = %d, err = %v; want one", offered, err)
	}
	assertOrgConnectionOffer(t, store, alice.RequestID)
	assertOrgConnectionOfferedVCPUs(t, store, orgID, 2)
	assertOrgConnectionActiveVCPUs(t, store, orgID, 0)

	if err := store.EnqueueOrgConnectionRequest(bob); err != nil {
		t.Fatalf("enqueue bob: %v", err)
	}
	if offered, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 0 {
		t.Fatalf("dispatch over reserved capacity = %d, err = %v; want zero", offered, err)
	}
	assertOrgConnectionRequestState(t, store, bob.RequestID, cpconfigstore.OrgConnectionRequestStatePending)

	aliceLease, err := store.ClaimOrgConnectionOffer(alice.RequestID, "cp-a")
	if err != nil || aliceLease == nil {
		t.Fatalf("claim alice offer = %#v, err = %v", aliceLease, err)
	}
	assertOrgConnectionOfferedVCPUs(t, store, orgID, 0)
	assertOrgConnectionActiveVCPUs(t, store, orgID, 2)
	if offered, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 0 {
		t.Fatalf("dispatch over active capacity = %d, err = %v; want zero", offered, err)
	}

	if err := store.ReleaseOrgConnectionLease(aliceLease.LeaseID); err != nil {
		t.Fatalf("release alice: %v", err)
	}
	if _, err := store.DispatchOrgConnectionAdmissions(orgID); err != nil {
		t.Fatalf("dispatch bob after capacity release: %v", err)
	}
	assertOrgConnectionOffer(t, store, bob.RequestID)
}

func TestOrgConnectionDispatcherDuplicateRunsAreIdempotent(t *testing.T) {
	storeA, storeB, _, _ := newSharedConfigStores(t)
	upsertActiveCP(t, storeA, "cp-a")
	upsertActiveCP(t, storeA, "cp-b")
	activateAdmissionOffers(t, storeA)
	const orgID = "org-duplicate-dispatch"
	seedAuthoritativeOrgConnectionLimits(t, storeA, orgID, 2, map[string]int{"alice": 0, "bob": 0})

	now := time.Now()
	for _, request := range []*cpconfigstore.OrgConnectionQueueEntry{
		{RequestID: "request-alice", OrgID: orgID, Username: "alice", CPInstanceID: "cp-a", PID: 1001, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now, ExpiresAt: now.Add(time.Minute)},
		{RequestID: "request-bob", OrgID: orgID, Username: "bob", CPInstanceID: "cp-b", PID: 1002, Protocol: "postgres", RequestedVCPUs: 1, EnqueuedAt: now.Add(time.Millisecond), ExpiresAt: now.Add(time.Minute)},
	} {
		if err := storeA.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}

	type dispatchResult struct {
		offered int
		err     error
	}
	results := make(chan dispatchResult, 2)
	for _, store := range []*cpconfigstore.ConfigStore{storeA, storeB} {
		go func(store *cpconfigstore.ConfigStore) {
			offered, err := store.DispatchOrgConnectionAdmissions(orgID)
			results <- dispatchResult{offered: offered, err: err}
		}(store)
	}

	totalOffered := 0
	for range 2 {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("concurrent duplicate dispatch: %v", result.err)
			}
			totalOffered += result.offered
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for duplicate dispatchers")
		}
	}
	if totalOffered != 2 {
		t.Fatalf("duplicate dispatchers created %d offers in total, want exactly 2", totalOffered)
	}
	assertOrgConnectionOffer(t, storeA, "request-alice")
	assertOrgConnectionOffer(t, storeA, "request-bob")
	assertOrgConnectionOfferedVCPUs(t, storeA, orgID, 2)
	assertOrgConnectionActiveVCPUs(t, storeA, orgID, 0)

	if offered, err := storeA.DispatchOrgConnectionAdmissions(orgID); err != nil || offered != 0 {
		t.Fatalf("idempotent follow-up dispatch = %d, err = %v; want zero", offered, err)
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

func TestOrgConnectionLeasesOutOfOrderPollDoesNotGrantForeignRequest(t *testing.T) {
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
		t.Fatalf("expected bob to keep waiting behind alice, got lease %q", outOfOrder.LeaseID)
	}
	count, err := store.ActiveOrgConnectionLeaseCount(alice.OrgID)
	if err != nil {
		t.Fatalf("count active leases after bob poll: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected bob's poll not to reserve capacity for alice, got %d active leases", count)
	}
	assertOrgConnectionRequestPending(t, store, alice.RequestID)

	aliceLease, err := store.TryAcquireOrgConnectionLease(alice.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 2}, now)
	if err != nil {
		t.Fatalf("acquire alice: %v", err)
	}
	if aliceLease == nil || aliceLease.RequestID != alice.RequestID {
		t.Fatalf("expected alice to grant only her own request, got %#v", aliceLease)
	}

	bobLease, err := store.TryAcquireOrgConnectionLease(bob.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 2}, now)
	if err != nil {
		t.Fatalf("acquire bob: %v", err)
	}
	if bobLease == nil || bobLease.RequestID != bob.RequestID {
		t.Fatalf("expected bob to grant only his own request, got %#v", bobLease)
	}
}

func TestOrgConnectionLeasesConcurrentCrossCPOwnerPolls(t *testing.T) {
	for _, orgMaxVCPUs := range []int{1, 2} {
		t.Run(fmt.Sprintf("org_cap_%d", orgMaxVCPUs), func(t *testing.T) {
			storeA, storeB, appA, appB := newSharedConfigStores(t)
			upsertActiveCP(t, storeA, "cp-a")
			upsertActiveCP(t, storeB, "cp-b")

			now := time.Now()
			alice := &cpconfigstore.OrgConnectionQueueEntry{
				RequestID:      "request-alice",
				OrgID:          "org-concurrent",
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
				OrgID:          alice.OrgID,
				Username:       "bob",
				CPInstanceID:   "cp-b",
				PID:            1002,
				Protocol:       "postgres",
				RequestedVCPUs: 1,
				EnqueuedAt:     now.Add(time.Millisecond),
				ExpiresAt:      now.Add(time.Minute),
			}
			if err := storeA.EnqueueOrgConnectionRequest(alice); err != nil {
				t.Fatalf("enqueue alice: %v", err)
			}
			if err := storeB.EnqueueOrgConnectionRequest(bob); err != nil {
				t.Fatalf("enqueue bob: %v", err)
			}

			sqlDB, err := storeA.DB().DB()
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
			if _, err := holder.Exec("SELECT pg_advisory_xact_lock($1)", orgConnectionAdvisoryLockKey(alice.OrgID)); err != nil {
				t.Fatalf("take org advisory lock: %v", err)
			}
			aliceRowHolder, err := sqlDB.Begin()
			if err != nil {
				t.Fatalf("begin alice row holder tx: %v", err)
			}
			defer func() { _ = aliceRowHolder.Rollback() }()
			var lockedRequestID string
			if err := aliceRowHolder.QueryRow(
				"SELECT request_id FROM "+storeA.RuntimeSchema()+".org_connection_queue WHERE request_id = $1 FOR UPDATE",
				alice.RequestID,
			).Scan(&lockedRequestID); err != nil {
				t.Fatalf("lock alice queue row: %v", err)
			}

			limits := cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: orgMaxVCPUs}
			aliceDone := make(chan acquireResult, 1)
			bobDone := make(chan acquireResult, 1)
			go func() {
				lease, err := storeB.TryAcquireOrgConnectionLease(bob.RequestID, limits, now)
				bobDone <- acquireResult{lease: lease, err: err}
			}()
			waitForAdvisoryLockWaiterFromApplication(t, sqlDB, holderPID, appB)
			go func() {
				lease, err := storeA.TryAcquireOrgConnectionLease(alice.RequestID, limits, now)
				aliceDone <- acquireResult{lease: lease, err: err}
			}()

			waitForAdvisoryLockWaitersFromApplications(t, sqlDB, holderPID, appA, appB)
			if err := holder.Commit(); err != nil {
				t.Fatalf("release org advisory lock: %v", err)
			}

			bobResult := waitForAcquireResult(t, bobDone, "bob concurrent poll")
			if bobResult.err != nil {
				t.Fatalf("bob concurrent poll: %v", bobResult.err)
			}
			if bobResult.lease != nil {
				t.Fatalf("bob's earlier poll granted a lease: %#v", bobResult.lease)
			}
			assertOrgConnectionRequestPending(t, storeA, alice.RequestID)
			assertOrgConnectionActiveVCPUs(t, storeA, alice.OrgID, 0)

			if err := aliceRowHolder.Commit(); err != nil {
				t.Fatalf("release alice queue row: %v", err)
			}
			aliceResult := waitForAcquireResult(t, aliceDone, "alice concurrent poll")
			if aliceResult.err != nil {
				t.Fatalf("alice concurrent poll: %v", aliceResult.err)
			}
			if aliceResult.lease == nil || aliceResult.lease.RequestID != alice.RequestID {
				t.Fatalf("alice result = %#v, want alice's own lease", aliceResult.lease)
			}

			assertOrgConnectionRequestGranted(t, storeA, alice.RequestID)
			if orgMaxVCPUs == 1 {
				if bobResult.lease != nil {
					t.Fatalf("bob acquired over the org cap: %#v", bobResult.lease)
				}
				assertOrgConnectionRequestPending(t, storeB, bob.RequestID)
				assertOrgConnectionActiveVCPUs(t, storeA, alice.OrgID, 1)

				if err := storeA.ReleaseOrgConnectionLease(aliceResult.lease.LeaseID); err != nil {
					t.Fatalf("release alice: %v", err)
				}
				bobResult.lease, bobResult.err = storeB.TryAcquireOrgConnectionLease(bob.RequestID, limits, now)
				if bobResult.err != nil {
					t.Fatalf("bob retry after release: %v", bobResult.err)
				}
			}

			if bobResult.lease == nil {
				bobResult.lease, bobResult.err = storeB.TryAcquireOrgConnectionLease(bob.RequestID, limits, now)
				if bobResult.err != nil {
					t.Fatalf("bob owner retry: %v", bobResult.err)
				}
			}
			if bobResult.lease == nil || bobResult.lease.RequestID != bob.RequestID {
				t.Fatalf("bob result after owner retry = %#v, want bob's own lease", bobResult.lease)
			}
			assertOrgConnectionRequestGranted(t, storeB, bob.RequestID)
			if orgMaxVCPUs == 2 {
				assertOrgConnectionActiveVCPUs(t, storeA, alice.OrgID, 2)
			} else {
				assertOrgConnectionActiveVCPUs(t, storeA, alice.OrgID, 1)
			}
		})
	}
}

func TestCancelOrgConnectionRequestReleasesUnclaimedOwnLease(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")

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
	if err := store.EnqueueOrgConnectionRequest(alice); err != nil {
		t.Fatalf("enqueue alice: %v", err)
	}

	lease, err := store.TryAcquireOrgConnectionLease(alice.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
	if err != nil {
		t.Fatalf("acquire alice: %v", err)
	}
	if lease == nil || lease.RequestID != alice.RequestID {
		t.Fatalf("expected alice lease, got %#v", lease)
	}
	count, err := store.ActiveOrgConnectionLeaseCount(alice.OrgID)
	if err != nil {
		t.Fatalf("count active leases before cancel: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected alice lease to reserve capacity, got %d active leases", count)
	}

	if err := store.CancelOrgConnectionRequest(alice.RequestID, now.Add(time.Second)); err != nil {
		t.Fatalf("cancel alice request: %v", err)
	}
	count, err = store.ActiveOrgConnectionLeaseCount(alice.OrgID)
	if err != nil {
		t.Fatalf("count active leases after cancel: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected cancel to release unclaimed own lease, got %d active leases", count)
	}
	assertOrgConnectionRequestAbsent(t, store, alice.RequestID)
}

func TestCancelPendingOrgConnectionRequestUnblocksNextOwner(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	alice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-alice",
		OrgID:          "org-cancel-pending",
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
		OrgID:          alice.OrgID,
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	for _, request := range []*cpconfigstore.OrgConnectionQueueEntry{alice, bob} {
		if err := store.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}

	limits := cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}
	lease, err := store.TryAcquireOrgConnectionLease(bob.RequestID, limits, now)
	if err != nil {
		t.Fatalf("bob poll behind alice: %v", err)
	}
	if lease != nil {
		t.Fatalf("bob acquired before alice was canceled: %#v", lease)
	}
	assertOrgConnectionRequestPending(t, store, alice.RequestID)
	assertOrgConnectionRequestPending(t, store, bob.RequestID)
	assertOrgConnectionActiveVCPUs(t, store, alice.OrgID, 0)

	if err := store.CancelOrgConnectionRequest(alice.RequestID, now); err != nil {
		t.Fatalf("cancel pending alice: %v", err)
	}
	assertOrgConnectionRequestAbsent(t, store, alice.RequestID)

	lease, err = store.TryAcquireOrgConnectionLease(bob.RequestID, limits, now)
	if err != nil {
		t.Fatalf("bob poll after alice cancellation: %v", err)
	}
	if lease == nil || lease.RequestID != bob.RequestID {
		t.Fatalf("bob result after alice cancellation = %#v, want bob's own lease", lease)
	}
	assertOrgConnectionRequestGranted(t, store, bob.RequestID)
	assertOrgConnectionActiveVCPUs(t, store, alice.OrgID, 1)
}

func TestCancelOrgConnectionRequestWinsWhileAcquireWaitsForOrgLock(t *testing.T) {
	storeA, storeB, appA, appB := newSharedConfigStores(t)
	upsertActiveCP(t, storeA, "cp-a")

	now := time.Now()
	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-cancel-race",
		OrgID:          "org-cancel-race",
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
	holder, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin advisory holder tx: %v", err)
	}
	defer func() { _ = holder.Rollback() }()

	var holderPID int
	if err := holder.QueryRow("SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatalf("get advisory holder backend pid: %v", err)
	}
	if _, err := holder.Exec("SELECT pg_advisory_xact_lock($1)", orgConnectionAdvisoryLockKey(request.OrgID)); err != nil {
		t.Fatalf("take org advisory lock: %v", err)
	}

	cancelDone := make(chan error, 1)
	go func() {
		cancelDone <- storeB.CancelOrgConnectionRequest(request.RequestID, now)
	}()
	waitForAdvisoryLockWaiterFromApplication(t, sqlDB, holderPID, appB)

	acquireDone := make(chan acquireResult, 1)
	go func() {
		lease, err := storeA.TryAcquireOrgConnectionLease(request.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
		acquireDone <- acquireResult{lease: lease, err: err}
	}()
	waitForAdvisoryLockWaitersFromApplications(t, sqlDB, holderPID, appA, appB)

	if err := holder.Commit(); err != nil {
		t.Fatalf("release org advisory lock: %v", err)
	}
	select {
	case err := <-cancelDone:
		if err != nil {
			t.Fatalf("cancel request while acquire waits: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancel-first transaction")
	}
	result := waitForAcquireResult(t, acquireDone, "acquire after cancellation")
	if result.err != nil {
		t.Fatalf("acquire after cancellation: %v", result.err)
	}
	if result.lease != nil {
		t.Fatalf("canceled request acquired lease %#v", result.lease)
	}
	assertOrgConnectionRequestAbsent(t, storeB, request.RequestID)
	assertOrgConnectionRequestAbsent(t, storeA, request.RequestID)
	assertOrgConnectionActiveVCPUs(t, storeA, request.OrgID, 0)
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
	assertOrgConnectionRequestPending(t, store, bob.RequestID)
	count, err := store.ActiveOrgConnectionLeaseCount(activeAlice.OrgID)
	if err != nil {
		t.Fatalf("count active leases after alice small poll: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected alice small poll not to grant bob, got %d active leases", count)
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
	assertOrgConnectionRequestPending(t, store, bob.RequestID)
	count, err := store.ActiveOrgConnectionLeaseCount(aliceFirst.OrgID)
	if err != nil {
		t.Fatalf("count active leases after alice second poll: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected alice second poll not to grant bob, got %d active leases", count)
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

func TestOrgConnectionAdmissionMetricsDoNotCountDisabledUserAsVCPULimit(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-disabled-metrics")
	activateAdmissionOffers(t, store)

	const (
		orgID     = "org-disabled-metrics"
		username  = "alice"
		requestID = "request-disabled-metrics"
	)
	seedAuthoritativeOrgConnectionLimits(t, store, orgID, 2, map[string]int{username: 1})
	if err := store.DB().Model(&cpconfigstore.OrgUser{}).
		Where("org_id = ? AND username = ?", orgID, username).
		Update("disabled", true).Error; err != nil {
		t.Fatalf("disable metrics user: %v", err)
	}

	now := time.Now()
	if err := store.EnqueueOrgConnectionRequest(&cpconfigstore.OrgConnectionQueueEntry{
		RequestID: requestID, OrgID: orgID, Username: username, CPInstanceID: "cp-disabled-metrics",
		PID: 1001, Protocol: "postgres", RequestedVCPUs: 1,
		EnqueuedAt: now, ExpiresAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("enqueue disabled-user metrics request: %v", err)
	}

	userLimitBefore := configstoreMetricCounterFamilyTotal(t, "duckgres_org_connection_admission_user_limit_skips_total")
	ineligibleBefore := configstoreMetricCounterFamilyTotal(t, "duckgres_org_connection_admission_ineligible_user_skips_total")
	outcomeBefore := configstoreMetricCounterValue(t, "duckgres_org_connection_admission_attempts_total", "outcome", "ineligible_user")

	lease, err := store.ScheduleAndClaimOrgConnectionLease(requestID, "cp-disabled-metrics")
	if err != nil {
		t.Fatalf("admit disabled-user metrics request: %v", err)
	}
	if lease != nil {
		t.Fatalf("disabled user received lease %#v", lease)
	}

	if delta := configstoreMetricCounterFamilyTotal(t, "duckgres_org_connection_admission_user_limit_skips_total") - userLimitBefore; delta != 0 {
		t.Fatalf("disabled user changed vCPU-limit skip counter by %f, want 0", delta)
	}
	if delta := configstoreMetricCounterFamilyTotal(t, "duckgres_org_connection_admission_ineligible_user_skips_total") - ineligibleBefore; delta != 1 {
		t.Fatalf("disabled user changed ineligible-user skip counter by %f, want 1", delta)
	}
	if delta := configstoreMetricCounterValue(t, "duckgres_org_connection_admission_attempts_total", "outcome", "ineligible_user") - outcomeBefore; delta != 1 {
		t.Fatalf("disabled user changed ineligible outcome counter by %f, want 1", delta)
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
	assertOrgConnectionRequestPending(t, store, "request-a")
	count, err := store.ActiveOrgConnectionLeaseCount("org-1")
	if err != nil {
		t.Fatalf("count active leases after request-b poll: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected request-b poll not to grant request-a, got %d active leases", count)
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
	assertOrgConnectionRequestPending(t, store, first.RequestID)
	count, err := store.ActiveOrgConnectionLeaseCount(first.OrgID)
	if err != nil {
		t.Fatalf("count active leases after second request poll: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected second request poll not to grant first request, got %d active leases", count)
	}

	lease, err := store.TryAcquireOrgConnectionLease(first.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
	if err != nil {
		t.Fatalf("acquire first request: %v", err)
	}
	if lease == nil {
		t.Fatal("expected first inserted request to acquire first")
	}
}

func TestOrgConnectionLeasesExpiredForeignHeadDoesNotBlockOwner(t *testing.T) {
	store := newIsolatedConfigStore(t)
	upsertActiveCP(t, store, "cp-a")
	upsertActiveCP(t, store, "cp-b")

	now := time.Now()
	alice := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-expired-alice",
		OrgID:          "org-expired-head",
		Username:       "alice",
		CPInstanceID:   "cp-a",
		PID:            1001,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now,
		ExpiresAt:      now.Add(time.Minute),
	}
	bob := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-live-bob",
		OrgID:          alice.OrgID,
		Username:       "bob",
		CPInstanceID:   "cp-b",
		PID:            1002,
		Protocol:       "postgres",
		RequestedVCPUs: 1,
		EnqueuedAt:     now.Add(time.Millisecond),
		ExpiresAt:      now.Add(time.Minute),
	}
	for _, request := range []*cpconfigstore.OrgConnectionQueueEntry{alice, bob} {
		if err := store.EnqueueOrgConnectionRequest(request); err != nil {
			t.Fatalf("enqueue %s: %v", request.RequestID, err)
		}
	}
	if err := store.DB().Exec(
		"UPDATE "+store.RuntimeSchema()+".org_connection_queue SET expires_at = clock_timestamp() - interval '1 second' WHERE request_id = ?",
		alice.RequestID,
	).Error; err != nil {
		t.Fatalf("expire alice request: %v", err)
	}

	lease, err := store.TryAcquireOrgConnectionLease(bob.RequestID, cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}, now)
	if err != nil {
		t.Fatalf("bob poll after alice expiry: %v", err)
	}
	if lease == nil || lease.RequestID != bob.RequestID {
		t.Fatalf("bob result after alice expiry = %#v, want bob's own lease", lease)
	}
	assertOrgConnectionRequestAbsent(t, store, alice.RequestID)
	assertOrgConnectionRequestGranted(t, store, bob.RequestID)
	assertOrgConnectionActiveVCPUs(t, store, alice.OrgID, 1)
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

func TestTryAcquireOrgConnectionLeaseConcurrentRetriesReturnSameOwnLease(t *testing.T) {
	storeA, storeB, appA, appB := newSharedConfigStores(t)
	upsertActiveCP(t, storeA, "cp-a")

	now := time.Now()
	request := &cpconfigstore.OrgConnectionQueueEntry{
		RequestID:      "request-concurrent-retry",
		OrgID:          "org-concurrent-retry",
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
	holder, err := sqlDB.Begin()
	if err != nil {
		t.Fatalf("begin advisory holder tx: %v", err)
	}
	defer func() { _ = holder.Rollback() }()

	var holderPID int
	if err := holder.QueryRow("SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatalf("get advisory holder backend pid: %v", err)
	}
	if _, err := holder.Exec("SELECT pg_advisory_xact_lock($1)", orgConnectionAdvisoryLockKey(request.OrgID)); err != nil {
		t.Fatalf("take org advisory lock: %v", err)
	}

	limits := cpconfigstore.OrgResourceLimits{OrgMaxVCPUs: 1}
	firstDone := make(chan acquireResult, 1)
	secondDone := make(chan acquireResult, 1)
	go func() {
		lease, err := storeA.TryAcquireOrgConnectionLease(request.RequestID, limits, now)
		firstDone <- acquireResult{lease: lease, err: err}
	}()
	go func() {
		lease, err := storeB.TryAcquireOrgConnectionLease(request.RequestID, limits, now)
		secondDone <- acquireResult{lease: lease, err: err}
	}()

	waitForAdvisoryLockWaitersFromApplications(t, sqlDB, holderPID, appA, appB)
	if err := holder.Commit(); err != nil {
		t.Fatalf("release org advisory lock: %v", err)
	}

	first := waitForAcquireResult(t, firstDone, "first concurrent retry")
	second := waitForAcquireResult(t, secondDone, "second concurrent retry")
	for i, result := range []acquireResult{first, second} {
		if result.err != nil {
			t.Fatalf("concurrent retry %d: %v", i, result.err)
		}
		if result.lease == nil || result.lease.RequestID != request.RequestID {
			t.Fatalf("concurrent retry %d lease = %#v, want own lease", i, result.lease)
		}
	}
	if first.lease.LeaseID != second.lease.LeaseID {
		t.Fatalf("concurrent retries returned different leases %q and %q", first.lease.LeaseID, second.lease.LeaseID)
	}
	assertOrgConnectionRequestGranted(t, storeA, request.RequestID)
	assertOrgConnectionActiveVCPUs(t, storeA, request.OrgID, 1)
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

func waitForAcquireResult(t *testing.T, results <-chan acquireResult, description string) acquireResult {
	t.Helper()

	select {
	case result := <-results:
		return result
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return acquireResult{}
	}
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

func waitForAdvisoryLockWaiterFromApplication(t *testing.T, db *sql.DB, holderPID int, applicationName string) {
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
			  AND application_name = $2
		`, holderPID, applicationName).Scan(&waiters)
		if err != nil {
			t.Fatalf("poll advisory lock waiter by application: %v", err)
		}
		if waiters > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timed out waiting for advisory lock waiter from application %q", applicationName)
}

func waitForAdvisoryLockWaitersFromApplications(
	t *testing.T,
	db *sql.DB,
	holderPID int,
	applicationNameA string,
	applicationNameB string,
) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiters int
		err := db.QueryRow(`
			SELECT count(DISTINCT application_name)
			FROM pg_stat_activity
			WHERE wait_event_type = 'Lock'
			  AND wait_event = 'advisory'
			  AND datname = current_database()
			  AND $1 = ANY(pg_blocking_pids(pid))
			  AND application_name IN ($2, $3)
		`, holderPID, applicationNameA, applicationNameB).Scan(&waiters)
		if err != nil {
			t.Fatalf("poll advisory lock waiters by application: %v", err)
		}
		if waiters == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	var activities []string
	rows, err := db.Query(`
		SELECT application_name || ' ' || COALESCE(wait_event_type, '') || '/' || COALESCE(wait_event, '') || ' ' || query
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
	t.Fatalf(
		"timed out waiting for advisory lock waiters from applications %q; activities: %s",
		[]string{applicationNameA, applicationNameB},
		strings.Join(activities, "; "),
	)
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

func configstoreMetricCounterValue(t *testing.T, metricName, labelName, labelValue string) float64 {
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
		for _, metric := range fam.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelName && label.GetValue() == labelValue {
					return metric.GetCounter().GetValue()
				}
			}
		}
		return 0
	}
	return 0
}
