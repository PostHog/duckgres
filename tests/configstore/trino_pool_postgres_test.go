//go:build linux || darwin

package configstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

const poolID = "registered:cell-001"

func poolSpec() cpconfigstore.TrinoPoolSpec {
	return cpconfigstore.TrinoPoolSpec{
		PoolID:                 poolID,
		PublicID:               "cell-001",
		APIMode:                cpconfigstore.TrinoPoolAPIModeShared,
		DesiredReleaseID:       "r1",
		DesiredBlueprintDigest: "d1",
		DesiredInstances:       3,
		MinServing:             3,
		MaxSurge:               1,
		MaxRepair:              1,
	}
}

func newPoolStore(t *testing.T) *cpconfigstore.ConfigStore {
	t.Helper()
	store := newIsolatedConfigStore(t)
	if err := store.UpsertTrinoPoolSpec(context.Background(), poolSpec()); err != nil {
		t.Fatalf("upsert pool spec: %v", err)
	}
	return store
}

func claimPool(t *testing.T, store *cpconfigstore.ConfigStore, owner string) cpconfigstore.TrinoPoolLease {
	t.Helper()
	lease, err := store.AcquireTrinoPoolAuthority(context.Background(), poolID, owner)
	if err != nil {
		t.Fatalf("acquire authority: %v", err)
	}
	return lease
}

func newInstance(id string, phase trinopool.Phase) cpconfigstore.TrinoPoolInstanceSpec {
	return cpconfigstore.TrinoPoolInstanceSpec{
		InstanceID:        id,
		PoolID:            poolID,
		ReleaseID:         "r1",
		SpecDigest:        "spec-" + id,
		BlueprintSnapshot: `{"blueprint_version":1}`,
		Phase:             phase,
	}
}

// The desired spec is upserted from configuration on every startup; it must not
// disturb the authority epoch, the freeze flag or anything the operator owns.
func TestUpsertTrinoPoolSpecPreservesRuntimeState(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	if err := store.FreezeTrinoPool(ctx, poolID, "blueprint unreadable"); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	spec := poolSpec()
	spec.DesiredReleaseID = "r2"
	if err := store.UpsertTrinoPoolSpec(ctx, spec); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	pool, err := store.GetTrinoPool(ctx, poolID)
	if err != nil || pool == nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.DesiredReleaseID != "r2" {
		t.Fatalf("desired release = %q, want r2", pool.DesiredReleaseID)
	}
	if pool.AuthorityEpoch != lease.Epoch {
		t.Fatalf("epoch changed to %d, want %d", pool.AuthorityEpoch, lease.Epoch)
	}
	if !pool.Frozen || pool.FrozenReason == "" {
		t.Fatal("a config upsert cleared the freeze")
	}
}

// Missing desired configuration freezes the pool. It must never be able to
// express itself as a desired count of zero.
func TestTrinoPoolRejectsAnEmptyDesiredCount(t *testing.T) {
	store := newIsolatedConfigStore(t)
	spec := poolSpec()
	spec.DesiredInstances = 0
	if err := store.UpsertTrinoPoolSpec(context.Background(), spec); err == nil {
		t.Fatal("a desired count of zero was accepted")
	}
}

func TestAcquireTrinoPoolAuthorityIsMonotonic(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)

	first := claimPool(t, store, "cp-a")
	second := claimPool(t, store, "cp-b")
	if second.Epoch <= first.Epoch {
		t.Fatalf("epoch did not advance: %d -> %d", first.Epoch, second.Epoch)
	}

	// The superseded leader keeps running until it notices. Its writes must be
	// refused rather than silently applied on top of the new leader's.
	err := store.CreateTrinoPoolInstance(ctx, first, newInstance("i-stale", trinopool.PhasePending))
	if !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale leader create error = %v, want ErrTrinoPoolConflict", err)
	}
	instances, err := store.ListTrinoPoolInstances(ctx, poolID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(instances) != 0 {
		t.Fatalf("stale leader created %d instances", len(instances))
	}
	if err := store.CreateTrinoPoolInstance(ctx, second, newInstance("i-fresh", trinopool.PhasePending)); err != nil {
		t.Fatalf("current leader create: %v", err)
	}
}

// Two control planes racing for the pool must not both believe they own it.
func TestConcurrentAuthorityAcquisitionYieldsDistinctEpochs(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)

	const racers = 6
	epochs := make([]int64, racers)
	errs := make([]error, racers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for index := 0; index < racers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			lease, err := store.AcquireTrinoPoolAuthority(ctx, poolID, fmt.Sprintf("cp-%d", index))
			epochs[index], errs[index] = lease.Epoch, err
		}(index)
	}
	close(start)
	wg.Wait()

	seen := map[int64]bool{}
	for index, err := range errs {
		if err != nil {
			t.Fatalf("racer %d: %v", index, err)
		}
		if seen[epochs[index]] {
			t.Fatalf("epoch %d was handed to two owners", epochs[index])
		}
		seen[epochs[index]] = true
	}
}

func TestInstancePhaseTransitionsAreCASAndValidated(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")
	if err := store.CreateTrinoPoolInstance(ctx, lease, newInstance("i-1", trinopool.PhasePending)); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := store.AdvanceTrinoPoolInstance(ctx, lease, "i-1", trinopool.PhasePending, trinopool.PhaseCreating, nil); err != nil {
		t.Fatalf("advance: %v", err)
	}
	// Wrong expected phase: somebody else moved it first.
	err := store.AdvanceTrinoPoolInstance(ctx, lease, "i-1", trinopool.PhasePending, trinopool.PhaseCreating, nil)
	if !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale CAS error = %v, want ErrTrinoPoolConflict", err)
	}
	// Illegal transition, rejected before it reaches the database.
	if err := store.AdvanceTrinoPoolInstance(ctx, lease, "i-1", trinopool.PhaseCreating, trinopool.PhaseServing, nil); err == nil {
		t.Fatal("CREATING -> SERVING was accepted")
	}
}

// Retirement is irreversible. The store is the last line of defence: even a
// buggy operator must not be able to bring a retired incarnation back.
func TestRetiredInstanceCannotResume(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")
	if err := store.CreateTrinoPoolInstance(ctx, lease, newInstance("i-1", trinopool.PhaseSealed)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.AdvanceTrinoPoolInstance(ctx, lease, "i-1", trinopool.PhaseSealed, trinopool.PhaseRetiring, nil); err != nil {
		t.Fatalf("retire: %v", err)
	}
	for _, target := range []trinopool.Phase{trinopool.PhaseServing, trinopool.PhaseDraining, trinopool.PhaseSealed} {
		if err := store.AdvanceTrinoPoolInstance(ctx, lease, "i-1", trinopool.PhaseRetiring, target, nil); err == nil {
			t.Fatalf("RETIRING -> %s was accepted", target)
		}
	}
}

// An instance id is never reused, including after retirement: a recycled id
// would let a stale Kubernetes or Gateway reference resolve to a live instance.
func TestInstanceIdentitiesAreNeverReused(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")
	if err := store.CreateTrinoPoolInstance(ctx, lease, newInstance("i-1", trinopool.PhaseRetiring)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.AdvanceTrinoPoolInstance(ctx, lease, "i-1", trinopool.PhaseRetiring, trinopool.PhaseRetired, nil); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if err := store.CreateTrinoPoolInstance(ctx, lease, newInstance("i-1", trinopool.PhasePending)); err == nil {
		t.Fatal("a retired instance id was reused")
	}
}

func TestLiveEndpointsAreUnique(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	first := newInstance("i-1", trinopool.PhasePreparing)
	first.EndpointURL = "https://i-1.trino-cell-001.svc.cluster.local:8443"
	if err := store.CreateTrinoPoolInstance(ctx, lease, first); err != nil {
		t.Fatalf("create: %v", err)
	}
	second := newInstance("i-2", trinopool.PhasePreparing)
	second.EndpointURL = first.EndpointURL
	if err := store.CreateTrinoPoolInstance(ctx, lease, second); err == nil {
		t.Fatal("two live instances shared one endpoint")
	}
}

// A lost response must be resolvable by reading the operation back under the
// same id. The same id with different content is a conflict, not a replay.
func TestOperationsAreIdempotentByIntent(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	operation := cpconfigstore.TrinoPoolOperationSpec{
		OperationID: "op-1", PoolID: poolID, InstanceID: "i-1",
		Kind: cpconfigstore.TrinoPoolOperationReplace, IntentHash: "hash-a",
	}
	first, err := store.BeginTrinoPoolOperation(ctx, lease, operation)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if first.Replayed {
		t.Fatal("a fresh operation reported itself as a replay")
	}
	replay, err := store.BeginTrinoPoolOperation(ctx, lease, operation)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Replayed {
		t.Fatal("an identical operation was not recognized as a replay")
	}

	changed := operation
	changed.IntentHash = "hash-b"
	if _, err := store.BeginTrinoPoolOperation(ctx, lease, changed); !errors.Is(err, cpconfigstore.ErrTrinoPoolIntentChanged) {
		t.Fatalf("changed intent error = %v, want ErrTrinoPoolIntentChanged", err)
	}
}

func TestOperationStepsAreIdempotentByPayload(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")
	if _, err := store.BeginTrinoPoolOperation(ctx, lease, cpconfigstore.TrinoPoolOperationSpec{
		OperationID: "op-1", PoolID: poolID, Kind: cpconfigstore.TrinoPoolOperationReplace, IntentHash: "hash-a",
	}); err != nil {
		t.Fatalf("begin: %v", err)
	}

	recorded, err := store.RecordTrinoPoolOperationStep(ctx, "op-1", "register", "payload-a", "OK", `{"instanceId":"i-1"}`)
	if err != nil {
		t.Fatalf("record step: %v", err)
	}
	if recorded.Replayed {
		t.Fatal("a fresh step reported itself as a replay")
	}
	again, err := store.RecordTrinoPoolOperationStep(ctx, "op-1", "register", "payload-a", "OK", `{"instanceId":"i-1"}`)
	if err != nil {
		t.Fatalf("replay step: %v", err)
	}
	// jsonb re-serializes, so compare the decoded documents rather than bytes.
	if !again.Replayed || !sameJSON(t, again.Result, recorded.Result) {
		t.Fatalf("step replay = %+v, want the recorded result %+v", again, recorded)
	}
	if _, err := store.RecordTrinoPoolOperationStep(ctx, "op-1", "register", "payload-b", "OK", `{}`); !errors.Is(err, cpconfigstore.ErrTrinoPoolIntentChanged) {
		t.Fatalf("changed step payload error = %v, want ErrTrinoPoolIntentChanged", err)
	}
}

func sameJSON(t *testing.T, left, right string) bool {
	t.Helper()
	var leftValue, rightValue any
	if err := json.Unmarshal([]byte(left), &leftValue); err != nil {
		t.Fatalf("decode %q: %v", left, err)
	}
	if err := json.Unmarshal([]byte(right), &rightValue); err != nil {
		t.Fatalf("decode %q: %v", right, err)
	}
	return reflect.DeepEqual(leftValue, rightValue)
}

// The projection watermark is the fence that stops a stale replica from serving
// a regressing authorization bundle. It only ever moves forward.
func TestProjectionWatermarkNeverRegresses(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	if err := store.AdvanceTrinoPoolProjection(ctx, lease, 5, "digest-5"); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := store.AdvanceTrinoPoolProjection(ctx, lease, 4, "digest-4"); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("regressing advance error = %v, want ErrTrinoPoolConflict", err)
	}
	projection, err := store.GetTrinoPoolProjection(ctx, poolID)
	if err != nil {
		t.Fatalf("get projection: %v", err)
	}
	if projection.AcceptedRevision != 5 || projection.AcceptedDigest != "digest-5" {
		t.Fatalf("projection = %+v, want revision 5", projection)
	}

	// A superseded leader cannot move the watermark at all.
	stale := lease
	stale.Epoch--
	if err := store.AdvanceTrinoPoolProjection(ctx, stale, 6, "digest-6"); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale leader advance error = %v, want ErrTrinoPoolConflict", err)
	}
}

// Desired, published and admitted are three separate facts. A tenant that is
// enabled but not yet admitted must be distinguishable from one that is live.
func TestPublicationTracksDesiredAndAdmittedSeparately(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	if err := store.SetTrinoPoolPublicationDesired(ctx, lease, poolID, "org-a", 7); err != nil {
		t.Fatalf("set desired: %v", err)
	}
	publication, err := store.GetTrinoPoolPublication(ctx, poolID, "org-a")
	if err != nil || publication == nil {
		t.Fatalf("get publication: %v", err)
	}
	if publication.DesiredRevision != 7 || publication.AdmittedRevision != 0 || publication.State != cpconfigstore.TrinoPublicationPending {
		t.Fatalf("publication = %+v, want a pending tenant at desired revision 7", publication)
	}

	if err := store.RecordTrinoPoolPublicationAdmitted(ctx, lease, poolID, "org-a", 7, "pub-1", `{"phase":"ADMITTED"}`); err != nil {
		t.Fatalf("record admitted: %v", err)
	}
	publication, err = store.GetTrinoPoolPublication(ctx, poolID, "org-a")
	if err != nil || publication == nil {
		t.Fatalf("get publication: %v", err)
	}
	if publication.AdmittedRevision != 7 || publication.State != cpconfigstore.TrinoPublicationAdmitted {
		t.Fatalf("publication = %+v, want an admitted tenant", publication)
	}
}
