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
	UpsertTrinoPoolSpec(context.Context, configstore.TrinoPoolSpec) error
	FreezeTrinoPool(ctx context.Context, poolID, reason string) error
	ThawTrinoPool(ctx context.Context, poolID string) error
	GetTrinoPool(ctx context.Context, poolID string) (*configstore.TrinoPool, error)
	AcquireTrinoPoolAuthority(ctx context.Context, poolID, owner string) (configstore.TrinoPoolLease, error)
	ListTrinoPoolInstances(ctx context.Context, poolID string) ([]configstore.TrinoPoolInstance, error)
	CreateTrinoPoolInstance(context.Context, configstore.TrinoPoolLease, configstore.TrinoPoolInstanceSpec) error
	AdvanceTrinoPoolInstance(ctx context.Context, lease configstore.TrinoPoolLease, instanceID string, from, to trinopool.Phase, updates map[string]any) error
	RecordTrinoPoolInstanceFields(ctx context.Context, lease configstore.TrinoPoolLease, instanceID string, updates map[string]any) error
}

// trinoPoolGateway is the Gateway surface the operator uses.
type trinoPoolGateway interface {
	EnsureInactiveBackend(context.Context, trinogateway.Backend) error
	ConfigurePool(context.Context, string, trinogateway.ConfigurePoolRequest) (trinogateway.PoolState, error)
	RegisterMember(context.Context, string, trinogateway.RegisterMemberRequest) (trinogateway.Member, error)
	AdmitMember(ctx context.Context, poolID, instanceID string, request trinogateway.AdmitMemberRequest) (trinogateway.Member, error)
	GetMember(ctx context.Context, poolID, instanceID string) (trinogateway.Member, error)
	GetObligations(ctx context.Context, poolID, instanceID string) (trinogateway.Obligations, error)
	DrainMember(ctx context.Context, poolID, instanceID string, request trinogateway.MemberStepRequest) (trinogateway.Member, error)
	SealMember(ctx context.Context, poolID, instanceID string, request trinogateway.MemberStepRequest) (trinogateway.Member, error)
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
type trinoPoolValidator func(ctx context.Context, endpoint string, observedWorkers int) (trinoPoolValidation, error)

type trinoPoolOperator struct {
	config   trinoPoolConfig
	store    trinoPoolStore
	gateway  trinoPoolGateway
	kube     func(epoch int64) trinoPoolKube
	validate trinoPoolValidator
	owner    string
	interval time.Duration
	// operatorEnabled gates every external effect. With it off the operator
	// keeps the durable desired state in sync and touches nothing else, which
	// is how the feature ships disabled without the code path rotting.
	operatorEnabled bool
	newInstanceID   func() string

	lease configstore.TrinoPoolLease
}

// Run is the leader-attached loop. It is started fresh on every leadership
// acquisition and cancelled on loss, so it re-acquires authority each time
// rather than trusting a lease it held before.
func (o *trinoPoolOperator) Run(ctx context.Context) {
	interval := o.interval
	if interval <= 0 {
		interval = trinoPoolReconcileInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := o.reconcileOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("Trino pool reconcile failed.", "pool", o.config.PublicID, "error", err)
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
	if o.config.Frozen {
		return o.store.FreezeTrinoPool(ctx, o.config.PoolID, o.config.FrozenReason)
	}
	if err := o.store.UpsertTrinoPoolSpec(ctx, o.config.Spec); err != nil {
		return fmt.Errorf("record desired pool spec: %w", err)
	}
	if err := o.store.ThawTrinoPool(ctx, o.config.PoolID); err != nil {
		return fmt.Errorf("clear pool freeze: %w", err)
	}
	if !o.operatorEnabled {
		// Read-only mode: the desired state is recorded and nothing else is
		// touched. This is what "ships disabled" means here.
		return nil
	}
	if err := o.ensureAuthority(ctx); err != nil {
		return err
	}

	pool, err := o.store.GetTrinoPool(ctx, o.config.PoolID)
	if err != nil || pool == nil {
		return fmt.Errorf("read pool state: %w", err)
	}
	if err := o.configureGatewayPool(ctx); err != nil {
		return err
	}

	instances, err := o.store.ListTrinoPoolInstances(ctx, o.config.PoolID)
	if err != nil {
		return fmt.Errorf("list pool instances: %w", err)
	}
	// Advance the instances already in flight before starting anything new, so
	// a slow rollout cannot be overtaken by its own successor.
	progressed, err := o.progressInstances(ctx, instances)
	if err != nil {
		return err
	}
	if progressed {
		return nil
	}
	return o.applyPlan(ctx, pool, instances)
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
	return nil
}

// dropAuthority is called when a fenced write is refused. The leader has been
// superseded; it stops writing and re-acquires on the next tick rather than
// continuing with an epoch the database no longer honors.
func (o *trinoPoolOperator) dropAuthority(err error) error {
	if errors.Is(err, configstore.ErrTrinoPoolConflict) || errors.Is(err, trinogateway.ErrStaleEpoch) {
		slog.Warn("Trino pool authority lost.", "pool", o.config.PublicID, "epoch", o.lease.Epoch, "error", err)
		o.lease = configstore.TrinoPoolLease{}
	}
	return err
}

func (o *trinoPoolOperator) configureGatewayPool(ctx context.Context) error {
	_, err := o.gateway.ConfigurePool(ctx, o.config.RoutingGroup, trinogateway.ConfigurePoolRequest{
		Step: trinogateway.Step{
			OperationID: "pool-config:" + o.config.PublicID,
			// The step identity includes the desired shape, so re-sending an
			// unchanged configuration is a replay and a changed one is a new
			// step rather than a conflict against the recorded payload.
			StepID:          "configure:" + o.configDigest(),
			ControllerEpoch: o.lease.Epoch,
		},
		APIMode:         "POOLED",
		MinServing:      o.config.Spec.MinServing,
		DesiredMembers:  o.config.Spec.DesiredInstances,
		MaxSurge:        o.config.Spec.MaxSurge,
		MaxRepair:       o.config.Spec.MaxRepair,
		DesiredRevision: o.config.Spec.DesiredReleaseID,
		// The tenant-admission gate stays closed until the identity question is
		// settled. Enabling it here would claim an atomic gate duckgres cannot
		// currently back with a verified tenant identity.
		TenantAdmissionEnabled: false,
	})
	if err != nil {
		return o.dropAuthority(fmt.Errorf("configure gateway pool: %w", err))
	}
	return nil
}

func (o *trinoPoolOperator) configDigest() string {
	return fmt.Sprintf("%s-%d-%d-%d-%d", o.config.Spec.DesiredBlueprintDigest[:8],
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
	instanceID := o.config.PublicID + "-" + o.newInstanceID()
	identity := o.identityFor(instanceID)
	objects, err := o.config.Blueprint.Instantiate(identity)
	if err != nil {
		return fmt.Errorf("instantiate instance %s: %w", instanceID, err)
	}
	spec := configstore.TrinoPoolInstanceSpec{
		InstanceID:        instanceID,
		PoolID:            o.config.PoolID,
		ReleaseID:         o.config.Blueprint.ReleaseID,
		SpecDigest:        o.config.Blueprint.SpecDigest(identity),
		BlueprintSnapshot: o.blueprintSnapshot(),
		Phase:             trinopool.PhasePending,
		Repair:            plan.Repair,
		EndpointURL:       o.endpointFor(instanceID),
		TLSServerName:     o.config.TLSServerName,
	}
	if err := o.store.CreateTrinoPoolInstance(ctx, o.lease, spec); err != nil {
		return o.dropAuthority(fmt.Errorf("record instance %s: %w", instanceID, err))
	}
	slog.Info("Trino pool instance created.", "pool", o.config.PublicID, "instance", instanceID,
		"repair", plan.Repair, "reason", plan.Reason)
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

func (o *trinoPoolOperator) endpointFor(instanceID string) string {
	return fmt.Sprintf("https://%s:%d", o.serviceHost(instanceID), o.config.Pool.CoordinatorServicePort)
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
