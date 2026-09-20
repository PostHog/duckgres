//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

// The shared-pool operator.
//
// It runs under the EXISTING janitor leader lease, but leadership only decides
// who executes. Correctness comes from the fences: the pool's authority epoch
// on every durable write, the same epoch on every Kubernetes object, and the
// Gateway's own epoch/generation CAS. A superseded leader that has not noticed
// yet is refused at each of those, not trusted to stop on its own.
//
// One step per instance per tick, and one lifecycle action per pool per tick.
// A stuck rollout must never be able to spawn a chain of replacements.
const (
	trinoPoolReconcileInterval = 5 * time.Second
	trinoPoolInstanceIDBytes   = 4
)

// trinoPoolStore is the durable state the operator needs. It is an interface so
// the loop can be tested without a database; the implementation's own semantics
// (fencing, CAS, replay) are covered by real-PostgreSQL tests.
type trinoPoolStore interface {
	SeedTrinoPool(context.Context, configstore.TrinoPoolSpec) error
	UpsertTrinoPoolSpec(context.Context, configstore.TrinoPoolLease, configstore.TrinoPoolSpec) error
	FreezeTrinoPool(ctx context.Context, lease configstore.TrinoPoolLease, poolID, reason string) error
	ThawTrinoPool(ctx context.Context, lease configstore.TrinoPoolLease, poolID string) error
	GetTrinoPool(ctx context.Context, poolID string) (*configstore.TrinoPool, error)
	AcquireTrinoPoolAuthority(ctx context.Context, poolID, owner string) (configstore.TrinoPoolLease, error)
	ListTrinoPoolInstances(ctx context.Context, poolID string) ([]configstore.TrinoPoolInstance, error)
	CreateTrinoPoolInstance(context.Context, configstore.TrinoPoolLease, configstore.TrinoPoolInstanceSpec) error
	AdvanceTrinoPoolInstance(ctx context.Context, lease configstore.TrinoPoolLease, instanceID string, from, to trinopool.Phase, updates map[string]any) error
	RecordTrinoPoolInstanceFields(ctx context.Context, lease configstore.TrinoPoolLease, instanceID string, updates map[string]any) error
	// RecordTrinoPoolPublicationRevision checkpoints the published catalog
	// revision the admission gate certifies members against. The operator needs
	// it directly, not only through the catalog writer: a revision the writer
	// committed but failed to record has to be recoverable by whoever next holds
	// the authority.
	RecordTrinoPoolPublicationRevision(ctx context.Context, lease configstore.TrinoPoolLease, poolID string, revision int64) error
}

// trinoPoolGateway is the Gateway surface the operator uses.
type trinoPoolGateway interface {
	EnsureInactiveBackend(context.Context, trinogateway.Backend) error
	PublishTenantPrincipals(ctx context.Context, poolID, tenant string, request trinogateway.PublishPrincipalsRequest) (trinogateway.TenantAdmission, error)
	ConfigurePool(context.Context, string, trinogateway.ConfigurePoolRequest) (trinogateway.PoolState, error)
	GetPool(ctx context.Context, poolID string) (trinogateway.PoolState, error)
	OpenPublication(ctx context.Context, poolID string, request trinogateway.OpenPublicationRequest) (trinogateway.Publication, error)
	AbandonPublication(ctx context.Context, poolID, publicationID string, step trinogateway.Step) (trinogateway.Publication, error)
	GetPublication(ctx context.Context, poolID, publicationID string) (trinogateway.Publication, error)
	RecordPublicationReceipt(ctx context.Context, poolID, publicationID string, request trinogateway.PublicationReceiptRequest) (trinogateway.Publication, error)
	CommitPublication(ctx context.Context, poolID, publicationID string, request trinogateway.CommitPublicationRequest) (trinogateway.Publication, error)
	RevokeTenant(ctx context.Context, poolID, tenant string, request trinogateway.RevokeTenantRequest) (trinogateway.TenantAdmission, error)
	RegisterMember(context.Context, string, trinogateway.RegisterMemberRequest) (trinogateway.Member, error)
	AdmitMember(ctx context.Context, poolID, instanceID string, request trinogateway.AdmitMemberRequest) (trinogateway.Member, error)
	GetMember(ctx context.Context, poolID, instanceID string) (trinogateway.Member, error)
	GetObligations(ctx context.Context, poolID, instanceID string) (trinogateway.Obligations, error)
	DrainMember(ctx context.Context, poolID, instanceID string, request trinogateway.MemberStepRequest) (trinogateway.Member, error)
	SealMember(ctx context.Context, poolID, instanceID string, request trinogateway.MemberStepRequest) (trinogateway.Member, error)
	SuspectMember(ctx context.Context, poolID, instanceID string, request trinogateway.SuspectMemberRequest) (trinogateway.Member, error)
	LostMember(ctx context.Context, poolID, instanceID string, request trinogateway.LostMemberRequest) (trinogateway.Member, error)
	RetireMember(ctx context.Context, poolID, instanceID string, request trinogateway.MemberStepRequest) (trinogateway.Member, error)
	MemberRetired(ctx context.Context, poolID, instanceID string, request trinogateway.MemberStepRequest) (trinogateway.Member, error)
}

// trinoPoolKube is the Kubernetes surface the operator uses.
type trinoPoolKube interface {
	Apply(context.Context, trinopool.Objects) (trinoPoolInventory, error)
	Observe(context.Context, trinoPoolInventory) (trinoPoolObservation, error)
	Delete(context.Context, trinoPoolInventory) error
	ResourcesAbsent(context.Context, trinoPoolInventory) (bool, error)
}

// trinoPoolValidator probes a candidate through its own endpoint.
type trinoPoolValidator func(ctx context.Context, endpoint string, observed trinoPoolObservation, expected trinoPoolExpectation) (trinoPoolValidation, error)

// trinoPoolIdentityProbe reads a candidate's coordinator process identity.
type trinoPoolIdentityProbe func(ctx context.Context, endpoint string) (string, error)

type trinoPoolOperator struct {
	config   trinoPoolConfig
	store    trinoPoolStore
	gateway  trinoPoolGateway
	kube     func(epoch int64) trinoPoolKube
	validate trinoPoolValidator
	identity trinoPoolIdentityProbe
	// projection reports what this control plane currently serves: the
	// authorization bundle's revision and the fingerprints of the projected
	// password and group files. It is what a member is compared against when
	// its acknowledgement is recorded.
	projection func() trinoPoolProjectionRevisions
	// acceptedProjection reports the projection the pool's DURABLE record
	// accepts. Candidate admission compares against this rather than against
	// what this process last published, because a replica's own memory is
	// exactly the thing in question when replicas disagree.
	acceptedProjection func() string
	// resolveConfig re-reads this pool's desired configuration from the
	// authoritative source. It runs on every tick, immediately before the
	// desired state is published, so a process that has been idle since boot
	// cannot publish what it read then. Nil in tests that drive a fixed config.
	resolveConfig func() (trinoPoolConfig, error)
	owner         string
	interval      time.Duration
	// operatorEnabled gates every external effect. With it off the operator
	// keeps the durable desired state in sync and touches nothing else, which
	// is how the feature ships disabled without the code path rotting.
	operatorEnabled bool
	newInstanceID   func() string
	// installWriter claims the catalog store's writer fence under the lease
	// just acquired and installs it as the cell's catalog write path.
	installWriter func(context.Context, configstore.TrinoPoolLease) error
	// releaseWriter drops the authority the catalog writer publishes under. It
	// runs when the leadership term ends, so a superseded process stops
	// attempting writes rather than discovering the fence one publication at a
	// time.
	releaseWriter func()
	// operations records durable intents around external effects, so a lost
	// response is resolved by read-back rather than repeated blind.
	operations trinoPoolOperationStore
	// tenants is the org projection the principal binding is derived from.
	tenants trinoPoolTenantStore
	// publications is the durable record of which tenant is published and
	// admitted at which revision. It is durable rather than remembered because
	// a restart or a leadership move must not republish blindly, nor assume an
	// admission that never committed.
	publications trinoPoolPublicationStore
	// acknowledgement asks ONE member what configuration it is serving, which
	// is what a publication receipt asserts.
	acknowledgement func(ctx context.Context, endpoint string, expected trinoPoolProjectionRevisions, catalogRevision int64) (trinoPoolAcknowledgement, error)
	// catalogWatermark reports the revision the CATALOG STORE is at, which is
	// the authority for the admission gate. The pool row's publication_revision
	// is a cache of it, and a cache whose write failed is indistinguishable from
	// "nothing new was published" unless the store is asked. Nil for a cell that
	// publishes through a coordinator, where the store is not duckgres-side and
	// this question has no local answer.
	catalogWatermark func(ctx context.Context) (int64, error)
	// bindingCursor and barrierCursor rotate which tenant is worked on. The
	// driver performs one external step per tick, so a fixed order lets one
	// permanently failing tenant hold the front of the queue forever - and with
	// thousands of warehouses, "forever" is not hyperbole.
	bindingCursor uint64
	barrierCursor uint64
	// identityObservedAt paces the per-member process-identity probe. It is
	// memory rather than durable state because it only spaces out a question
	// whose answer is re-read anyway; a restart simply asks again.
	identityObservedAt map[string]time.Time
	// tenantTurn alternates the two long queues - publishing a changed binding
	// and opening the next barrier - so neither can starve the other at fleet
	// scale.
	tenantTurn uint64
	// barrierBasis is the configuration the live publication attempt was opened
	// against, so every receipt it collects attests to ONE configuration rather
	// than to whatever was current when each was taken. At most one attempt is
	// live, so this holds at most one entry. It is deliberately per-process: an
	// attempt this process did not open is released and reopened rather than
	// completed on evidence nobody can describe.
	barrierBasis map[string]trinoPoolBarrierBasis

	lease configstore.TrinoPoolLease
	// fenced records that this term lost the fence. It ends the loop rather
	// than letting a superseded controller re-acquire.
	fenced bool
	// pool is the durable row read at the start of the tick, so the steps agree
	// on one view of the desired state.
	pool *configstore.TrinoPool
}

// Run is the leader-attached loop. It is started fresh on every leadership
// acquisition and cancelled on loss, so it re-acquires authority each time
// rather than trusting a lease it held before.
func (o *trinoPoolOperator) Run(ctx context.Context) {
	interval := o.interval
	if interval <= 0 {
		interval = trinoPoolReconcileInterval
	}
	o.fenced = false

	// A new Run is a NEW leadership term. Any lease left on the struct belongs
	// to the previous term and must not be reused: the janitor lease may have
	// moved away and back, and another control plane may have taken the pool's
	// authority in between.
	o.lease = configstore.TrinoPoolLease{}
	if o.releaseWriter != nil {
		// The catalog writer publishes under THIS term's authority, so it has
		// none until the term acquires one, and none again once it ends.
		o.releaseWriter()
		defer o.releaseWriter()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := o.reconcileOnce(ctx); err != nil && ctx.Err() == nil {
			if errors.Is(err, errTrinoPoolBackoff) {
				// Nothing was attempted: an operation is serving out the wait a
				// previous failure earned. That is the retry schedule working,
				// not a fault to alert on.
				slog.Debug("Trino pool operation is waiting for its next attempt.",
					"pool", o.config.PublicID, "reason", err)
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
				continue
			}
			slog.Warn("Trino pool reconcile failed.", "pool", o.config.PublicID, "error", err)
			if o.fenced {
				// The fence refused this leader. Ending the term is the correct
				// response: re-acquiring here would ratchet the epoch against a
				// valid new leader on every tick, and two controllers taking
				// turns raising the epoch is worse than one stepping aside. The
				// janitor lease decides when this process leads again.
				slog.Warn("Trino pool leadership term ended after a fence refusal.",
					"pool", o.config.PublicID, "epoch", o.lease.Epoch)
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (o *trinoPoolOperator) reconcileOnce(ctx context.Context) error {
	// A pool whose desired configuration is unreadable freezes at its last-good
	// state. No creates, no drains, no deletes - and explicitly not a desired
	// count of zero.
	// Desired-state publication is a lifecycle-affecting write, so it is fenced
	// like every other one. Without the lease, a delayed old leader - or a
	// replica still holding stale configuration - could overwrite a newer
	// desired spec or clear another leader's freeze. A read-only operator does
	// not write it at all: it has no authority to speak for the pool.
	if !o.operatorEnabled {
		return nil
	}
	// Desired configuration is re-read from the authoritative source on every
	// tick, not taken from a snapshot made when this process booted.
	//
	// The startup snapshot was the actual staleness hazard. A replica that
	// booted before a configuration change, sat idle, and then won the janitor
	// lease would publish what it read at boot - a perfectly legal fenced write
	// of old content, and one the generation guard cannot catch, because the
	// value that orders generations does not change for a settings-only edit
	// and two different configurations can legitimately carry the same one.
	// Reading immediately before writing makes the published spec current by
	// construction, and bounds the window to a single tick.
	o.refreshConfig()
	// Seeding is the one unfenced write, and it can only INSERT: a fence needs
	// a row to lock, so the very first publication has to create one.
	if err := o.store.SeedTrinoPool(ctx, o.config.Spec); err != nil {
		return fmt.Errorf("seed pool row: %w", err)
	}
	if err := o.ensureAuthority(ctx); err != nil {
		return err
	}
	if o.config.Frozen {
		return o.dropAuthority(o.store.FreezeTrinoPool(ctx, o.lease, o.config.PoolID, o.config.FrozenReason))
	}
	if err := o.store.UpsertTrinoPoolSpec(ctx, o.lease, o.config.Spec); err != nil {
		if errors.Is(err, configstore.ErrTrinoPoolStaleGeneration) {
			// The configuration this leader holds carries a generation the
			// store has already passed. That is a configuration problem, not a
			// lost fence: ending the term would hand the pool to a replica
			// reading the same file and doing the same thing. Hold the last-good
			// state and say why, which is what every other unusable desired
			// configuration does.
			slog.Error("Trino pool desired configuration went backwards; holding the last-good state.",
				"pool", o.config.PublicID, "error", err)
			return o.dropAuthority(o.store.FreezeTrinoPool(ctx, o.lease, o.config.PoolID,
				"desired configuration is older than the published one: "+err.Error()))
		}
		return o.dropAuthority(fmt.Errorf("record desired pool spec: %w", err))
	}
	if err := o.store.ThawTrinoPool(ctx, o.lease, o.config.PoolID); err != nil {
		return o.dropAuthority(fmt.Errorf("clear pool freeze: %w", err))
	}

	pool, err := o.store.GetTrinoPool(ctx, o.config.PoolID)
	if err != nil {
		return fmt.Errorf("read pool state: %w", err)
	}
	if pool == nil {
		return fmt.Errorf("pool %s has no durable row", o.config.PoolID)
	}
	o.pool = pool
	if err := o.configureGatewayPool(ctx); err != nil {
		return err
	}
	// The binding has to be current BEFORE the gate can refuse anything on its
	// basis, and a tenant is only dispatchable once its barrier has committed,
	// so both run before any lifecycle step.
	//
	// A tenant's failure is NOT allowed to stop the pool. One warehouse whose
	// publication can never succeed used to end the tick here, so no instance
	// was repaired, drained or replaced for as long as it stayed broken - the
	// pool's compute lifecycle held hostage by one row. The failure is recorded
	// against that tenant (with its own backoff) and reported at the end; only
	// a lost fence stops the tick, because after that nothing this process
	// writes can land anyway.
	tenantErr := o.advanceTenantAdmissions(ctx)
	if tenantErr != nil {
		if o.fenced {
			return tenantErr
		}
		slog.Warn("Trino pool tenant admission step failed; continuing with the instance lifecycle.",
			"pool", o.config.PublicID, "error", tenantErr)
	}

	instances, err := o.store.ListTrinoPoolInstances(ctx, o.config.PoolID)
	if err != nil {
		return fmt.Errorf("list pool instances: %w", err)
	}
	// Advance the instances already in flight before starting anything new, so
	// a slow rollout cannot be overtaken by its own successor.
	//
	// An instance failure is reported but does not end the tick: the planner is
	// what repairs and replaces members, and holding it back because one member
	// is stuck is how a single unrecoverable instance took the whole pool's
	// lifecycle with it.
	progressed, instanceErr := o.progressInstances(ctx, instances)
	if instanceErr != nil && o.fenced {
		return errors.Join(tenantErr, instanceErr)
	}
	if progressed {
		return errors.Join(tenantErr, instanceErr)
	}
	return errors.Join(tenantErr, instanceErr, o.applyPlan(ctx, pool, instances))
}

// refreshConfig replaces the desired configuration with what the authoritative
// source says NOW.
//
// An unreadable source freezes the pool at its last-good state rather than
// failing the tick: the pool keeps serving, nothing is created, drained or
// deleted, and the operator is told why. It is explicitly NOT a desired count
// of zero and explicitly NOT a startup abort - a bad mount must not empty a
// fleet or take the control plane down.
//
// A resolution that names a DIFFERENT pool is refused outright: it would mean
// this loop is about to publish another pool's desired state under this pool's
// authority.
func (o *trinoPoolOperator) refreshConfig() {
	if o.resolveConfig == nil {
		return
	}
	fresh, err := o.resolveConfig()
	if err != nil {
		o.config.Frozen = true
		o.config.FrozenReason = "desired configuration is unreadable: " + err.Error()
		slog.Warn("Trino pool desired configuration is unreadable; holding the last-good state.",
			"pool", o.config.PublicID, "error", err)
		return
	}
	if fresh.PoolID != o.config.PoolID {
		o.config.Frozen = true
		o.config.FrozenReason = fmt.Sprintf("desired configuration now names pool %q", fresh.PoolID)
		slog.Error("Trino pool desired configuration names a different pool; holding the last-good state.",
			"pool", o.config.PublicID, "resolved", fresh.PoolID)
		return
	}
	o.config = fresh
}

// ensureAuthority acquires the pool's authority epoch once per leadership term.
// Losing it is not something to retry harder: the next tick re-acquires, and
// until then every fenced write correctly refuses.
func (o *trinoPoolOperator) ensureAuthority(ctx context.Context) error {
	if o.lease.Epoch != 0 {
		return nil
	}
	lease, err := o.store.AcquireTrinoPoolAuthority(ctx, o.config.PoolID, o.owner)
	if err != nil {
		return fmt.Errorf("acquire pool authority: %w", err)
	}
	o.lease = lease
	slog.Info("Trino pool authority acquired.", "pool", o.config.PublicID, "epoch", lease.Epoch)

	// The catalog writer's fence IS this authority: claim it now, under the
	// epoch we just won, and install it as the cell's write path. Claiming at
	// startup instead would have every replica take the cell on boot, which
	// would make the writer fence agree with everyone and distinguish nobody.
	if o.installWriter != nil {
		if err := o.installWriter(ctx, lease); err != nil {
			// Authority is held but catalogs cannot be published. Publishing
			// through a stale path would be worse, so the pool keeps serving and
			// the failure is surfaced for the next tick to retry.
			return fmt.Errorf("claim catalog writer for pool %s: %w", o.config.PublicID, err)
		}
	}
	return nil
}

// dropAuthority is called when a fenced write is refused. The leader has been
// superseded, so it marks the term finished and stops. It does NOT re-acquire:
// a stale controller that immediately bumps the epoch again would fence the
// valid leader right back, and the two would trade the pool forever. Ending the
// term hands the decision back to the janitor lease, which is the only thing
// that knows who should be leading.
func (o *trinoPoolOperator) dropAuthority(err error) error {
	if errors.Is(err, configstore.ErrTrinoPoolConflict) || errors.Is(err, trinogateway.ErrStaleEpoch) {
		slog.Warn("Trino pool authority lost.", "pool", o.config.PublicID, "epoch", o.lease.Epoch, "error", err)
		o.lease = configstore.TrinoPoolLease{}
		o.fenced = true
		if o.releaseWriter != nil {
			o.releaseWriter()
		}
	}
	return err
}

func (o *trinoPoolOperator) configureGatewayPool(ctx context.Context) error {
	_, err := o.gateway.ConfigurePool(ctx, o.config.RoutingGroup, trinogateway.ConfigurePoolRequest{
		Step: trinogateway.Step{
			OperationID: "pool-config:" + o.config.PublicID,
			// The step identity carries BOTH the desired shape and this
			// leader's epoch.
			//
			// The Gateway hashes the whole request body, epoch included, so a
			// new leader re-sending an unchanged configuration under the same
			// step id would hash differently and conflict forever. Putting the
			// epoch in the step id makes each leadership term its own step:
			// a repeat within one term is still a replay, and a new term is a
			// new step rather than a permanent conflict.
			StepID:          fmt.Sprintf("configure.e%d.%s", o.lease.Epoch, o.configDigest()),
			ControllerEpoch: o.lease.Epoch,
			// The owner is what makes an EQUAL epoch from a different process
			// refusable. Without it the Gateway's recorded owner stays NULL and
			// the epoch alone fences, which admits a second controller at the
			// same epoch.
			OwnerIdentity: o.owner,
		},
		APIMode:         "POOLED",
		MinServing:      o.config.Spec.MinServing,
		DesiredMembers:  o.config.Spec.DesiredInstances,
		MaxSurge:        o.config.Spec.MaxSurge,
		MaxRepair:       o.config.Spec.MaxRepair,
		DesiredRevision: o.config.Spec.DesiredReleaseID,
		// The gate is a deliberate per-pool choice. It is deny-only: with it on,
		// a tenant whose principals this controller has not published yet
		// cannot dispatch work. Publishing the binding is therefore part of the
		// same loop (see publishTenantBindings).
		TenantAdmissionEnabled: o.config.Pool.TenantAdmission,
	})
	if err != nil {
		return o.dropAuthority(fmt.Errorf("configure gateway pool: %w", err))
	}
	return nil
}

func (o *trinoPoolOperator) configDigest() string {
	digest := o.config.Spec.DesiredBlueprintDigest
	if len(digest) > 8 {
		digest = digest[:8]
	}
	if digest == "" {
		digest = "none"
	}
	return fmt.Sprintf("%s-%d-%d-%d-%d", digest,
		o.config.Spec.DesiredInstances, o.config.Spec.MinServing, o.config.Spec.MaxSurge, o.config.Spec.MaxRepair)
}

// applyPlan starts at most one lifecycle action.
func (o *trinoPoolOperator) applyPlan(ctx context.Context, pool *configstore.TrinoPool, instances []configstore.TrinoPoolInstance) error {
	views := make([]trinopool.InstanceView, 0, len(instances))
	for _, instance := range instances {
		views = append(views, instance.View())
	}
	plan := trinopool.PlanNext(trinopool.PoolState{
		DesiredInstances: pool.DesiredInstances,
		MinServing:       pool.MinServing,
		MaxSurge:         pool.MaxSurge,
		MaxRepair:        pool.MaxRepair,
		DesiredReleaseID: pool.DesiredReleaseID,
		Frozen:           pool.Frozen,
		FrozenReason:     pool.FrozenReason,
		Instances:        views,
	})
	switch plan.Action {
	case trinopool.PlanActionCreate:
		return o.createInstance(ctx, plan)
	case trinopool.PlanActionDrain:
		return o.beginDrain(ctx, plan)
	default:
		return nil
	}
}

// createInstance persists the identity BEFORE anything exists in Kubernetes.
// That ordering is what makes a lost create response recoverable: the name is
// deterministic and already recorded, so the next tick reads it back instead of
// creating a second instance.
func (o *trinoPoolOperator) createInstance(ctx context.Context, plan trinopool.Plan) error {
	suffix := o.newInstanceID()
	if suffix == "" {
		// A random suffix is what keeps instance identities from being reused.
		// Without one, this create would mint "<pool>-" and collide with itself
		// on the next attempt.
		return errors.New("could not generate an instance identity")
	}
	instanceID := o.config.PublicID + "-" + suffix
	identity := o.identityFor(instanceID)
	objects, err := o.config.Blueprint.Instantiate(identity)
	if err != nil {
		return fmt.Errorf("instantiate instance %s: %w", instanceID, err)
	}
	spec := configstore.TrinoPoolInstanceSpec{
		InstanceID:        instanceID,
		RepairFor:         plan.RepairFor,
		PoolID:            o.config.PoolID,
		ReleaseID:         o.config.Blueprint.ReleaseID,
		SpecDigest:        o.config.Blueprint.SpecDigest(identity),
		BlueprintSnapshot: o.blueprintSnapshot(),
		Phase:             trinopool.PhasePending,
		Repair:            plan.Repair,
		EndpointURL:       o.endpointFor(instanceID),
	}
	if err := o.store.CreateTrinoPoolInstance(ctx, o.lease, spec); err != nil {
		return o.dropAuthority(fmt.Errorf("record instance %s: %w", instanceID, err))
	}
	slog.Info("Trino pool instance created.", "pool", o.config.PublicID, "instance", instanceID,
		"repair", plan.Repair, "repairFor", plan.RepairFor, "reason", plan.Reason)
	// objects is discarded on purpose: this call is a pre-flight that the
	// identity CAN be instantiated before the row is written. The objects
	// themselves are created on the next tick, from the instance's own stored
	// snapshot rather than from live configuration.
	_ = objects
	return nil
}

func (o *trinoPoolOperator) beginDrain(ctx context.Context, plan trinopool.Plan) error {
	instance, err := o.instanceByID(ctx, plan.InstanceID)
	if err != nil {
		return err
	}
	member, err := o.gateway.DrainMember(ctx, o.config.RoutingGroup, instance.InstanceID, trinogateway.MemberStepRequest{
		Step:               o.step(instance.InstanceID, "drain"),
		ExpectedGeneration: instance.GatewayGeneration,
	})
	if err != nil {
		// A serving-floor refusal is the Gateway doing its job. It is recorded
		// and retried on a later tick, never overridden.
		return o.dropAuthority(fmt.Errorf("drain member %s: %w", instance.InstanceID, err))
	}
	return o.dropAuthority(o.store.AdvanceTrinoPoolInstance(ctx, o.lease, instance.InstanceID,
		trinopool.PhaseServing, trinopool.PhaseDraining, map[string]any{
			"gateway_state":      member.Phase,
			"gateway_generation": member.Generation,
		}))
}

func (o *trinoPoolOperator) instanceByID(ctx context.Context, instanceID string) (configstore.TrinoPoolInstance, error) {
	instances, err := o.store.ListTrinoPoolInstances(ctx, o.config.PoolID)
	if err != nil {
		return configstore.TrinoPoolInstance{}, err
	}
	for _, instance := range instances {
		if instance.InstanceID == instanceID {
			return instance, nil
		}
	}
	return configstore.TrinoPoolInstance{}, fmt.Errorf("instance %s is unknown", instanceID)
}

func (o *trinoPoolOperator) identityFor(instanceID string) trinopool.Identity {
	return trinopool.Identity{
		PoolID:           o.config.PoolID,
		PoolLabelValue:   o.config.PublicID,
		InstanceID:       instanceID,
		NodeEnvironment:  o.config.Pool.NodeEnvironment,
		AuthorityEpoch:   o.lease.Epoch,
		CoordinatorPort:  o.config.Pool.CoordinatorServicePort,
		DiscoveryURIHost: o.serviceHost(instanceID),
	}
}

func (o *trinoPoolOperator) serviceHost(instanceID string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", instanceID, o.config.Namespace)
}

// endpointFor is the instance's own in-cluster Service, over plain HTTP.
//
// TLS terminates at the Gateway; there is no per-instance certificate, no
// private CA and no trust distribution. Everything that talks to a coordinator
// this way declares the forwarded HTTPS hop instead of relaxing
// authentication. The tradeoff is explicit: credentials and query data cross
// the cluster network unencrypted between Gateway/operator and coordinator.
func (o *trinoPoolOperator) endpointFor(instanceID string) string {
	return fmt.Sprintf("http://%s:%d", o.serviceHost(instanceID), o.config.Pool.CoordinatorServicePort)
}

func (o *trinoPoolOperator) backendName(instanceID string) string {
	return o.config.RoutingGroup + "-" + instanceID
}

// step builds the idempotency envelope. The operation id is the instance's, so
// every step of one instance's lifecycle is resolvable as a single history.
func (o *trinoPoolOperator) step(instanceID, stepID string) trinogateway.Step {
	return trinogateway.Step{
		OperationID:     "instance:" + instanceID,
		StepID:          stepID,
		ControllerEpoch: o.lease.Epoch,
		OwnerIdentity:   o.owner,
	}
}

func (o *trinoPoolOperator) blueprintSnapshot() string {
	encoded, err := o.config.Blueprint.MarshalSnapshot()
	if err != nil {
		return "{}"
	}
	return encoded
}

func newTrinoPoolInstanceID() string {
	buffer := make([]byte, trinoPoolInstanceIDBytes)
	if _, err := rand.Read(buffer); err != nil {
		// A random suffix is what keeps instance identities from being reused.
		// Falling back to something predictable would break that, so fail the
		// creation instead and let the next tick try again.
		return ""
	}
	return hex.EncodeToString(buffer)
}
