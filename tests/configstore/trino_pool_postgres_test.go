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
	"time"

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
	// Seeding is the one unfenced write: a fence needs a row to lock, so the
	// first publication has to create one. It can only INSERT.
	if err := store.SeedTrinoPool(context.Background(), poolSpec()); err != nil {
		t.Fatalf("seed pool: %v", err)
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

	if err := store.FreezeTrinoPool(ctx, lease, poolID, "blueprint unreadable"); err != nil {
		t.Fatalf("freeze: %v", err)
	}
	spec := poolSpec()
	spec.DesiredReleaseID = "r2"
	if err := store.UpsertTrinoPoolSpec(ctx, lease, spec); err != nil {
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

// Publishing desired state is lifecycle-affecting, so it is fenced: a delayed
// old leader must not be able to overwrite a newer desired spec or clear
// another leader's freeze.
func TestDesiredStatePublicationIsFenced(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	stale := claimPool(t, store, "cp-a")
	current := claimPool(t, store, "cp-b")

	spec := poolSpec()
	spec.DesiredReleaseID = "stale-release"
	if err := store.UpsertTrinoPoolSpec(ctx, stale, spec); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale desired publication error = %v, want ErrTrinoPoolConflict", err)
	}
	if err := store.FreezeTrinoPool(ctx, stale, poolID, "stale freeze"); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale freeze error = %v, want ErrTrinoPoolConflict", err)
	}
	if err := store.ThawTrinoPool(ctx, stale, poolID); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale thaw error = %v, want ErrTrinoPoolConflict", err)
	}

	pool, err := store.GetTrinoPool(ctx, poolID)
	if err != nil || pool == nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.DesiredReleaseID == "stale-release" || pool.Frozen {
		t.Fatalf("a superseded leader changed desired state: %+v", pool)
	}

	// The current leader still writes normally.
	spec.DesiredReleaseID = "current-release"
	if err := store.UpsertTrinoPoolSpec(ctx, current, spec); err != nil {
		t.Fatalf("current leader publication: %v", err)
	}
}

// Missing desired configuration freezes the pool. It must never be able to
// express itself as a desired count of zero.
func TestTrinoPoolRejectsAnEmptyDesiredCount(t *testing.T) {
	store := newIsolatedConfigStore(t)
	spec := poolSpec()
	spec.DesiredInstances = 0
	if err := store.SeedTrinoPool(context.Background(), spec); err == nil {
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

	recorded, err := store.RecordTrinoPoolOperationStep(ctx, lease, "op-1", "register", "payload-a", "OK", `{"instanceId":"i-1"}`)
	if err != nil {
		t.Fatalf("record step: %v", err)
	}
	if recorded.Replayed {
		t.Fatal("a fresh step reported itself as a replay")
	}
	again, err := store.RecordTrinoPoolOperationStep(ctx, lease, "op-1", "register", "payload-a", "OK", `{"instanceId":"i-1"}`)
	if err != nil {
		t.Fatalf("replay step: %v", err)
	}
	// jsonb re-serializes, so compare the decoded documents rather than bytes.
	if !again.Replayed || !sameJSON(t, again.Result, recorded.Result) {
		t.Fatalf("step replay = %+v, want the recorded result %+v", again, recorded)
	}
	if _, err := store.RecordTrinoPoolOperationStep(ctx, lease, "op-1", "register", "payload-b", "OK", `{}`); !errors.Is(err, cpconfigstore.ErrTrinoPoolIntentChanged) {
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

// A step is recorded UNKNOWN before the external effect and re-recorded with
// the outcome after it. If the second call returned the stored row unchanged,
// a step could never leave UNKNOWN: the cross-leader read-back that keys on a
// completed step would be unreachable and every retry would repeat the call.
func TestOperationStepOutcomeAdvancesOutOfUnknown(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")
	if _, err := store.BeginTrinoPoolOperation(ctx, lease, cpconfigstore.TrinoPoolOperationSpec{
		OperationID: "op-1", PoolID: poolID, Kind: cpconfigstore.TrinoPoolOperationReplace, IntentHash: "hash-a",
	}); err != nil {
		t.Fatalf("begin: %v", err)
	}

	if _, err := store.RecordTrinoPoolOperationStep(ctx, lease, "op-1", "admit", "payload-a",
		cpconfigstore.TrinoPoolStepOutcomeUnknown, "{}"); err != nil {
		t.Fatalf("record intent: %v", err)
	}
	recorded, err := store.RecordTrinoPoolOperationStep(ctx, lease, "op-1", "admit", "payload-a",
		cpconfigstore.TrinoPoolStepOutcomeOK, `{"phase":"ACTIVE"}`)
	if err != nil {
		t.Fatalf("record outcome: %v", err)
	}
	if recorded.Outcome != cpconfigstore.TrinoPoolStepOutcomeOK || !recorded.Replayed {
		t.Fatalf("step = %+v, want a replayed step recorded OK", recorded)
	}

	// A DECIDED outcome is never re-decided: rewriting it is exactly the loss of
	// history the journal exists to prevent.
	again, err := store.RecordTrinoPoolOperationStep(ctx, lease, "op-1", "admit", "payload-a",
		cpconfigstore.TrinoPoolStepOutcomeFailed, `{"phase":"REFUSED"}`)
	if err != nil {
		t.Fatalf("re-record: %v", err)
	}
	if again.Outcome != cpconfigstore.TrinoPoolStepOutcomeOK {
		t.Fatalf("outcome = %q, want the recorded OK to stand", again.Outcome)
	}
}

// The candidate gate compares against the pool's published catalog revision, so
// that revision has to be written by whatever publishes catalogs. It only moves
// forward: two publications can report out of order, and moving the gate
// backwards would certify a coordinator missing the newest tenant.
func TestPublicationRevisionOnlyAdvances(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	if err := store.RecordTrinoPoolPublicationRevision(ctx, lease, poolID, 7); err != nil {
		t.Fatalf("record revision: %v", err)
	}
	if err := store.RecordTrinoPoolPublicationRevision(ctx, lease, poolID, 5); err != nil {
		t.Fatalf("record older revision: %v", err)
	}
	pool, err := store.GetTrinoPool(ctx, poolID)
	if err != nil || pool == nil {
		t.Fatalf("get pool: %v", err)
	}
	if pool.PublicationRevision != 7 {
		t.Fatalf("publication revision = %d, want the highest published (7)", pool.PublicationRevision)
	}

	stale := lease
	stale.Epoch--
	if err := store.RecordTrinoPoolPublicationRevision(ctx, stale, poolID, 9); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale leader error = %v, want ErrTrinoPoolConflict", err)
	}
}

// A desired generation that went backwards is a CONFIGURATION problem, not a
// lost fence. Reporting it as a conflict ended the leadership term on every
// tick and handed the pool to a replica that did the same.
func TestBackwardsGenerationIsNotAFenceConflict(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	spec := poolSpec()
	spec.Generation = 5
	if err := store.UpsertTrinoPoolSpec(ctx, lease, spec); err != nil {
		t.Fatalf("publish generation 5: %v", err)
	}
	spec.Generation = 4
	err := store.UpsertTrinoPoolSpec(ctx, lease, spec)
	if !errors.Is(err, cpconfigstore.ErrTrinoPoolStaleGeneration) {
		t.Fatalf("backwards generation error = %v, want ErrTrinoPoolStaleGeneration", err)
	}
	if errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatal("a stale generation was reported as a lost fence")
	}
}

// The publication barrier's record is what a NEW leader reads instead of its
// own memory: which binding was published, which barrier is open, and which
// target actually committed.
func TestTenantPublicationRecordsBindingAndAdmission(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	if err := store.RecordTrinoPoolTenantPrincipals(ctx, lease, poolID, "org-a", "binding-1"); err != nil {
		t.Fatalf("record principals: %v", err)
	}
	if err := store.RecordTrinoPoolPublicationOpen(ctx, lease, poolID, "org-a", "pub-1", "c7.pabc"); err != nil {
		t.Fatalf("record open: %v", err)
	}
	if err := store.RecordTrinoPoolPublicationCommitted(ctx, lease, poolID, "org-a", "c7.pabc", `{"receipts":3}`); err != nil {
		t.Fatalf("record commit: %v", err)
	}

	publications, err := store.ListTrinoPoolPublications(ctx, poolID)
	if err != nil || len(publications) != 1 {
		t.Fatalf("list publications: %v (%d rows)", err, len(publications))
	}
	publication := publications[0]
	if publication.PrincipalRevision != "binding-1" || publication.AdmittedTargetRevision != "c7.pabc" ||
		publication.State != cpconfigstore.TrinoPublicationAdmitted {
		t.Fatalf("publication = %+v, want an admitted tenant at the committed target", publication)
	}
	// The committed barrier stops being the LIVE one. Leaving it named here
	// would make the driver select a finished publication as the attempt in
	// flight forever, and the Gateway never retracts an opened admission gate.
	if publication.PublicationID != "" || publication.TargetRevision != "" {
		t.Fatalf("publication = %+v, want the finished barrier cleared", publication)
	}

	// Revocation KEEPS the row. Deleting it would read as "never published", and
	// the next tick would republish the binding of a tenant meant to be gone.
	if err := store.RecordTrinoPoolTenantRevoked(ctx, lease, poolID, "org-a", "warehouse removed"); err != nil {
		t.Fatalf("record revocation: %v", err)
	}
	publications, err = store.ListTrinoPoolPublications(ctx, poolID)
	if err != nil || len(publications) != 1 {
		t.Fatalf("list after revocation: %v (%d rows)", err, len(publications))
	}
	if publications[0].State != cpconfigstore.TrinoPublicationRevoked ||
		publications[0].AdmittedTargetRevision != "" {
		t.Fatalf("publication = %+v, want a revoked tenant with no admitted target", publications[0])
	}

	stale := lease
	stale.Epoch--
	if err := store.RecordTrinoPoolTenantPrincipals(ctx, stale, poolID, "org-a", "binding-2"); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale leader error = %v, want ErrTrinoPoolConflict", err)
	}
}

// A new barrier does not retract an admission, and clearing a dead one does not
// either.
//
// Both matter outside the driver: an operator surface keyed on the state would
// otherwise flap a serving warehouse back to Provisioning every time one of its
// logins changed, and an attempt that was abandoned has to leave the durable
// record without taking the tenant's admission with it.
func TestPublicationBarrierLivenessIsSeparateFromAdmission(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	if err := store.RecordTrinoPoolTenantPrincipals(ctx, lease, poolID, "org-a", "binding-1"); err != nil {
		t.Fatalf("record principals: %v", err)
	}
	// A tenant that has never been admitted reports that it is being admitted.
	if err := store.RecordTrinoPoolPublicationOpen(ctx, lease, poolID, "org-a", "pub-1", "b1.a1"); err != nil {
		t.Fatalf("record open: %v", err)
	}
	if got := onePublication(t, store, "org-a"); got.State != cpconfigstore.TrinoPublicationAdmitting {
		t.Fatalf("state = %q, want admitting for a tenant that was never admitted", got.State)
	}
	if err := store.RecordTrinoPoolPublicationCommitted(ctx, lease, poolID, "org-a", "b1.a1", `{"receipts":3}`); err != nil {
		t.Fatalf("record commit: %v", err)
	}

	// Its next barrier - a new login - leaves the admission alone.
	if err := store.RecordTrinoPoolTenantPrincipals(ctx, lease, poolID, "org-a", "binding-2"); err != nil {
		t.Fatalf("record second principals: %v", err)
	}
	if err := store.RecordTrinoPoolPublicationOpen(ctx, lease, poolID, "org-a", "pub-2", "b2.a2"); err != nil {
		t.Fatalf("record second open: %v", err)
	}
	got := onePublication(t, store, "org-a")
	if got.State != cpconfigstore.TrinoPublicationAdmitted || got.AdmittedTargetRevision != "b1.a1" {
		t.Fatalf("publication = %+v, want the serving tenant to stay admitted at its previous target", got)
	}
	if got.PublicationID != "pub-2" {
		t.Fatalf("live barrier = %q, want pub-2", got.PublicationID)
	}

	// Clearing the attempt takes the barrier, not the admission.
	if err := store.ClearTrinoPoolPublicationBarrier(ctx, lease, poolID, "org-a"); err != nil {
		t.Fatalf("clear barrier: %v", err)
	}
	got = onePublication(t, store, "org-a")
	if got.PublicationID != "" || got.TargetRevision != "" {
		t.Fatalf("publication = %+v, want no live barrier", got)
	}
	if got.State != cpconfigstore.TrinoPublicationAdmitted || got.AdmittedTargetRevision != "b1.a1" {
		t.Fatalf("publication = %+v, want the admission preserved", got)
	}

	// A tenant that was never admitted falls back to whether its binding was
	// published at all.
	if err := store.RecordTrinoPoolTenantPrincipals(ctx, lease, poolID, "org-b", "binding-1"); err != nil {
		t.Fatalf("record principals for org-b: %v", err)
	}
	if err := store.RecordTrinoPoolPublicationOpen(ctx, lease, poolID, "org-b", "pub-3", "b1.a1"); err != nil {
		t.Fatalf("record open for org-b: %v", err)
	}
	if err := store.ClearTrinoPoolPublicationBarrier(ctx, lease, poolID, "org-b"); err != nil {
		t.Fatalf("clear barrier for org-b: %v", err)
	}
	if got := onePublication(t, store, "org-b"); got.State != cpconfigstore.TrinoPublicationPublished {
		t.Fatalf("state = %q, want published for a tenant whose binding is current but was never admitted", got.State)
	}

	stale := lease
	stale.Epoch--
	if err := store.ClearTrinoPoolPublicationBarrier(ctx, stale, poolID, "org-a"); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale leader error = %v, want ErrTrinoPoolConflict", err)
	}
}

func onePublication(t *testing.T, store *cpconfigstore.ConfigStore, orgID string) cpconfigstore.TrinoPoolPublication {
	t.Helper()
	publications, err := store.ListTrinoPoolPublications(context.Background(), poolID)
	if err != nil {
		t.Fatalf("list publications: %v", err)
	}
	for _, publication := range publications {
		if publication.OrgID == orgID {
			return publication
		}
	}
	t.Fatalf("no publication row for %s", orgID)
	return cpconfigstore.TrinoPoolPublication{}
}

// A tenant's occurrence counter is what makes its NEXT barrier - or its next
// revocation - a new operation. Sharing an identity with the previous one would
// replay that one's recorded outcome, which for a revocation means a tenant
// nobody revoked stays admitted.

// An occurrence that stands for a request in flight is durable, and only a
// DEFINITE outcome closes it.
//
// A lost response leaves the request possibly still executing at the Gateway,
// so the next pass has to reissue that exact step rather than mint a new one -
// which it can only do if what the occurrence stands for survived the restart
// that may have happened in between.
func TestPendingPublicationIntentIsDurable(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	first, err := store.BeginTrinoPoolPublicationIntent(ctx, lease, poolID, "org-a", cpconfigstore.TrinoPublicationIntentPrincipals)
	if err != nil || first != 1 {
		t.Fatalf("begin intent = %d, %v; want occurrence 1", first, err)
	}
	if got := onePublication(t, store, "org-a"); got.PendingIntent != cpconfigstore.TrinoPublicationIntentPrincipals {
		t.Fatalf("pending intent = %q, want the publication it stands for", got.PendingIntent)
	}

	// The Gateway answered: the checkpoint closes the occurrence in the same
	// write, so a crash cannot leave a checkpointed binding with an occurrence
	// still claiming to be in flight.
	if err := store.RecordTrinoPoolTenantPrincipals(ctx, lease, poolID, "org-a", "binding-1"); err != nil {
		t.Fatalf("record principals: %v", err)
	}
	if got := onePublication(t, store, "org-a"); got.PendingIntent != "" || got.PrincipalRevision != "binding-1" {
		t.Fatalf("publication = %+v, want a checkpointed binding and no open occurrence", got)
	}

	// A revocation takes its own occurrence, and a definite refusal closes it
	// without moving the checkpoint.
	second, err := store.BeginTrinoPoolPublicationIntent(ctx, lease, poolID, "org-a", cpconfigstore.TrinoPublicationIntentRevoke)
	if err != nil || second != 2 {
		t.Fatalf("begin revoke intent = %d, %v; want occurrence 2", second, err)
	}
	if err := store.ResolveTrinoPoolPublicationIntent(ctx, lease, poolID, "org-a"); err != nil {
		t.Fatalf("resolve intent: %v", err)
	}
	got := onePublication(t, store, "org-a")
	if got.PendingIntent != "" || got.Attempt != 2 || got.PrincipalRevision != "binding-1" {
		t.Fatalf("publication = %+v, want the occurrence closed and the checkpoint untouched", got)
	}

	// A revocation clears it too, so a revoked tenant never looks like one with
	// a request outstanding.
	if _, err := store.BeginTrinoPoolPublicationIntent(ctx, lease, poolID, "org-a", cpconfigstore.TrinoPublicationIntentRevoke); err != nil {
		t.Fatalf("begin second revoke intent: %v", err)
	}
	if err := store.RecordTrinoPoolTenantRevoked(ctx, lease, poolID, "org-a", "warehouse removed"); err != nil {
		t.Fatalf("record revocation: %v", err)
	}
	if got := onePublication(t, store, "org-a"); got.PendingIntent != "" {
		t.Fatalf("publication = %+v, want no open occurrence after a revocation", got)
	}

	if _, err := store.BeginTrinoPoolPublicationIntent(ctx, lease, poolID, "org-a", "something-else"); err == nil {
		t.Fatal("an unknown intent kind was accepted")
	}
	stale := lease
	stale.Epoch--
	if _, err := store.BeginTrinoPoolPublicationIntent(ctx, stale, poolID, "org-a", cpconfigstore.TrinoPublicationIntentPrincipals); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale leader error = %v, want ErrTrinoPoolConflict", err)
	}
}
func TestPublicationAttemptsAreMonotonePerTenant(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	first, err := store.BeginTrinoPoolPublicationAttempt(ctx, lease, poolID, "org-a")
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	second, err := store.BeginTrinoPoolPublicationAttempt(ctx, lease, poolID, "org-a")
	if err != nil {
		t.Fatalf("begin second attempt: %v", err)
	}
	if first != 1 || second != 2 {
		t.Fatalf("attempts = %d then %d, want 1 then 2", first, second)
	}
	// Another tenant counts independently.
	other, err := store.BeginTrinoPoolPublicationAttempt(ctx, lease, poolID, "org-b")
	if err != nil {
		t.Fatalf("begin attempt for another tenant: %v", err)
	}
	if other != 1 {
		t.Fatalf("attempt for a second tenant = %d, want its own 1", other)
	}

	stale := lease
	stale.Epoch--
	if _, err := store.BeginTrinoPoolPublicationAttempt(ctx, stale, poolID, "org-a"); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale leader error = %v, want ErrTrinoPoolConflict", err)
	}
}

// One unserviceable tenant must not busy-loop or starve the tenants behind it:
// the driver takes one tenant per tick, so the wait a failure earns has to be
// durable and per tenant.
func TestPublicationFailureRecordsADurableWait(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	next := time.Now().UTC().Add(45 * time.Second)
	if err := store.RecordTrinoPoolPublicationFailure(ctx, lease, poolID, "org-a", next, "principal conflict"); err != nil {
		t.Fatalf("record failure: %v", err)
	}
	if err := store.RecordTrinoPoolPublicationFailure(ctx, lease, poolID, "org-a", next, "principal conflict"); err != nil {
		t.Fatalf("record second failure: %v", err)
	}
	publication, err := store.GetTrinoPoolPublication(ctx, poolID, "org-a")
	if err != nil || publication == nil {
		t.Fatalf("get publication: %v", err)
	}
	if publication.Attempts != 2 || publication.NextAttemptAt == nil || publication.LastError == "" {
		t.Fatalf("publication = %+v, want two recorded attempts and a next attempt time", publication)
	}

	if err := store.ClearTrinoPoolPublicationFailure(ctx, lease, poolID, "org-a"); err != nil {
		t.Fatalf("clear failure: %v", err)
	}
	publication, err = store.GetTrinoPoolPublication(ctx, poolID, "org-a")
	if err != nil || publication == nil {
		t.Fatalf("get publication: %v", err)
	}
	if publication.Attempts != 0 || publication.NextAttemptAt != nil {
		t.Fatalf("publication = %+v, want the backoff cleared after a step that worked", publication)
	}
}

// The projection fence needs an ORDER, not just a fingerprint: every control
// plane builds the projection from its own view, so a replica that is behind
// cannot tell that it is from a digest alone. The authority assigns it.
func TestAcceptedProjectionIsOrderedAndStableForUnchangedContent(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	first, err := store.AcceptTrinoPoolProjection(ctx, lease, "digest-1")
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	same, err := store.AcceptTrinoPoolProjection(ctx, lease, "digest-1")
	if err != nil {
		t.Fatalf("re-accept: %v", err)
	}
	if first != 1 || same != first {
		t.Fatalf("revisions = %d then %d, want an unchanged projection to keep its revision", first, same)
	}
	next, err := store.AcceptTrinoPoolProjection(ctx, lease, "digest-2")
	if err != nil {
		t.Fatalf("accept a changed projection: %v", err)
	}
	if next != first+1 {
		t.Fatalf("revision = %d, want %d", next, first+1)
	}

	projection, err := store.GetTrinoPoolProjection(ctx, poolID)
	if err != nil {
		t.Fatalf("read projection: %v", err)
	}
	if projection.AcceptedDigest != "digest-2" || projection.AcceptedRevision != next {
		t.Fatalf("projection = %+v, want the accepted digest at revision %d", projection, next)
	}

	// A superseded leader cannot move it - which is the case that matters: a
	// delayed write from the previous authority must not reinstate an older
	// projection after a newer one is in effect.
	stale := lease
	stale.Epoch--
	if _, err := store.AcceptTrinoPoolProjection(ctx, stale, "digest-old"); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale leader error = %v, want ErrTrinoPoolConflict", err)
	}
	projection, err = store.GetTrinoPoolProjection(ctx, poolID)
	if err != nil {
		t.Fatalf("re-read projection: %v", err)
	}
	if projection.AcceptedDigest != "digest-2" {
		t.Fatalf("a superseded leader changed the accepted projection: %+v", projection)
	}
}

// The projection a pooled cell accepts must describe ONE state of the database.
//
// The pool row lock serializes acceptances against each other, but nothing
// stops the org/user/team writers - so under the default isolation the separate
// reads this builds from could straddle such a write and be accepted as a
// coherent projection that never existed.
func TestAcceptedProjectionReadsOneSnapshotOfItsSources(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	seedTrinoOrg(t, store, "acme")
	if err := store.EnableTrino("acme", cpconfigstore.TrinoSettings{}); err != nil {
		t.Fatalf("EnableTrino: %v", err)
	}

	// A writer commits a NEW tenant while the builder is between its reads.
	var seenFirst, seenSecond int
	_, _, err := store.AcceptTrinoPoolProjectionFrom(ctx, lease, func(sources cpconfigstore.TrinoProjectionSources) (string, error) {
		seenFirst = len(sources.Orgs)
		// This commits in another session, after the transaction's snapshot was
		// taken. A repeatable-read transaction must not see it.
		seedTrinoOrg(t, store, "beta")
		if err := store.EnableTrino("beta", cpconfigstore.TrinoSettings{}); err != nil {
			return "", err
		}
		again, err := sources.Reread()
		if err != nil {
			return "", err
		}
		seenSecond = len(again)
		return "digest-1", nil
	})
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if seenFirst != 1 {
		t.Fatalf("the builder saw %d orgs, want the one that existed", seenFirst)
	}
	if seenSecond != seenFirst {
		t.Fatalf("a second read inside the same acceptance saw %d orgs after %d: the projection is built from two different states",
			seenSecond, seenFirst)
	}
}

// A project-scoped login's policy must come from the SAME read as the rest of
// the projection.
//
// The snapshot-backed resolver is refreshed on a poll, so a scope taken from it
// can be older than the rows the acceptance transaction just read - and the
// result would be accepted as one coherent projection. Here the team is
// disabled in the database and the snapshot is deliberately NOT reloaded: the
// accepted projection must reflect the database.
func TestAcceptedProjectionDerivesScopesFromItsOwnRead(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "cp-a")

	seedTrinoOrg(t, store, "acme")
	if err := store.EnableTrino("acme", cpconfigstore.TrinoSettings{}); err != nil {
		t.Fatalf("EnableTrino: %v", err)
	}
	if _, err := cpconfigstore.UpsertOrgTeamTx(store.DB(), "acme", cpconfigstore.OrgTeamUpsert{
		TeamID: 7, SchemaName: "posthog_7",
	}); err != nil {
		t.Fatalf("UpsertOrgTeamTx: %v", err)
	}
	if err := store.CreateOrgUser("acme", "posthog_team_7", "$2a$10$team7"); err != nil {
		t.Fatalf("CreateOrgUser: %v", err)
	}
	if err := store.DB().Exec(
		`UPDATE duckgres_org_users SET access_mode = 'project_reader', team_id = 7
		  WHERE org_id = 'acme' AND username = 'posthog_team_7'`).Error; err != nil {
		t.Fatalf("bind the project login: %v", err)
	}
	if err := store.ReloadSnapshot(); err != nil {
		t.Fatalf("ReloadSnapshot: %v", err)
	}

	// The team is disabled in the database. Nothing reloads the snapshot, so
	// the cache still reports the login as scoped to an enabled team.
	if err := store.DB().Exec(
		`UPDATE duckgres_org_teams SET enabled = false WHERE org_id = 'acme' AND team_id = 7`).Error; err != nil {
		t.Fatalf("disable the team: %v", err)
	}

	var scoped *cpconfigstore.TrinoOrgUser
	if _, _, err := store.AcceptTrinoPoolProjectionWith(ctx, lease, func(orgs []cpconfigstore.TrinoEnabledOrg) (string, error) {
		for i := range orgs {
			for j := range orgs[i].Users {
				if orgs[i].Users[j].Username == "posthog_team_7" {
					scoped = &orgs[i].Users[j]
				}
			}
		}
		return "digest-1", nil
	}); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if scoped == nil || scoped.Scope == nil {
		t.Fatalf("the project login was dropped or unscoped: %+v", scoped)
	}
	if len(scoped.Scope.AllowedSchemas) != 0 || !scoped.Scope.ReadOnly {
		t.Fatalf("scope = %+v, want the fail-closed policy the DISABLED team implies, not the cached one",
			*scoped.Scope)
	}

	// The cache, unreloaded, still reports the old policy - which is exactly
	// why the projection must not be built from it.
	cached, ok := store.OrgUserQueryAccess("acme", "posthog_team_7")
	if !ok || len(cached.AllowedSchemas) == 0 {
		t.Fatalf("the snapshot cache no longer holds the stale policy (%+v, ok=%v); this test proves nothing", cached, ok)
	}
}
