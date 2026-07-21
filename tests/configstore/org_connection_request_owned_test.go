//go:build linux || darwin

package configstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
)

func TestScheduleAndClaimContextCancellationInterruptsOrgLockWait(t *testing.T) {
	storeA, storeB, _, _ := newSharedConfigStores(t)
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
	if err := cpconfigstore.LockOrgConnectionAdmissionTx(blocker, orgID); err != nil {
		t.Fatalf("hold org admission lock: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := storeB.ScheduleAndClaimOrgConnectionLeaseContext(ctx, request.RequestID, request.CPInstanceID)
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("schedule returned before cancellation while lock was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
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
	storeA, storeB, _, _ := newSharedConfigStores(t)
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

	type scheduleResult struct {
		lease *cpconfigstore.OrgConnectionLease
		err   error
	}
	resultCh := make(chan scheduleResult, 1)
	go func() {
		lease, err := storeB.ScheduleAndClaimOrgConnectionLease(request.RequestID, cpID)
		resultCh <- scheduleResult{lease: lease, err: err}
	}()

	var (
		result               scheduleResult
		returnedBeforeCommit bool
	)
	select {
	case result = <-resultCh:
		returnedBeforeCommit = true
	case <-time.After(150 * time.Millisecond):
	}

	if err := stateChange.Commit().Error; err != nil {
		t.Fatalf("commit owner draining state: %v", err)
	}
	if !returnedBeforeCommit {
		select {
		case result = <-resultCh:
		case <-time.After(2 * time.Second):
			t.Fatal("schedule did not resume after owner state commit")
		}
	}

	if cancelErr := storeA.CancelOrgConnectionRequest(request.RequestID, time.Now()); cancelErr != nil {
		t.Fatalf("cleanup raced request: %v", cancelErr)
	}
	if returnedBeforeCommit {
		t.Fatal("admission committed without serializing against the owner's in-flight draining transition")
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

	retryResultCh := make(chan scheduleResult, 1)
	go func() {
		lease, err := storeB.ScheduleAndClaimOrgConnectionLease(existingRequest.RequestID, cpID)
		retryResultCh <- scheduleResult{lease: lease, err: err}
	}()
	returnedBeforeCommit = false
	select {
	case result = <-retryResultCh:
		returnedBeforeCommit = true
	case <-time.After(150 * time.Millisecond):
	}
	if err := retryStateChange.Commit().Error; err != nil {
		t.Fatalf("commit retry owner draining state: %v", err)
	}
	if !returnedBeforeCommit {
		select {
		case result = <-retryResultCh:
		case <-time.After(2 * time.Second):
			t.Fatal("existing-lease retry did not resume after owner state commit")
		}
	}
	if cancelErr := storeA.CancelOrgConnectionRequest(existingRequest.RequestID, time.Now()); cancelErr != nil {
		t.Fatalf("cleanup existing-lease request: %v", cancelErr)
	}
	if returnedBeforeCommit {
		t.Fatal("existing-lease claim committed without serializing against the owner's draining transition")
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
