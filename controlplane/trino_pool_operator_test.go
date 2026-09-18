//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

// ---------------------------------------------------------------------------
// Fakes. The store's own semantics (fencing, CAS, replay) are covered by
// real-PostgreSQL tests; these fakes exist so the LOOP's decisions can be
// asserted without a database.
// ---------------------------------------------------------------------------

type fakePoolStore struct {
	pool      *configstore.TrinoPool
	instances map[string]*configstore.TrinoPoolInstance
	order     []string
	epoch     int64
	frozen    string
	// failAdvance simulates losing authority mid-tick.
	failAdvance bool
}

func newFakePoolStore(spec configstore.TrinoPoolSpec) *fakePoolStore {
	return &fakePoolStore{
		pool: &configstore.TrinoPool{
			PoolID: spec.PoolID, PublicID: spec.PublicID, APIMode: spec.APIMode,
			DesiredInstances: spec.DesiredInstances, MinServing: spec.MinServing,
			MaxSurge: spec.MaxSurge, MaxRepair: spec.MaxRepair,
			DesiredReleaseID: spec.DesiredReleaseID,
		},
		instances: map[string]*configstore.TrinoPoolInstance{},
	}
}

func (f *fakePoolStore) UpsertTrinoPoolSpec(_ context.Context, spec configstore.TrinoPoolSpec) error {
	f.pool.DesiredInstances, f.pool.MinServing = spec.DesiredInstances, spec.MinServing
	f.pool.MaxSurge, f.pool.MaxRepair = spec.MaxSurge, spec.MaxRepair
	f.pool.DesiredReleaseID = spec.DesiredReleaseID
	return nil
}

func (f *fakePoolStore) FreezeTrinoPool(_ context.Context, _, reason string) error {
	f.frozen = reason
	f.pool.Frozen, f.pool.FrozenReason = true, reason
	return nil
}

func (f *fakePoolStore) ThawTrinoPool(context.Context, string) error {
	f.pool.Frozen, f.pool.FrozenReason = false, ""
	return nil
}

func (f *fakePoolStore) GetTrinoPool(context.Context, string) (*configstore.TrinoPool, error) {
	return f.pool, nil
}

func (f *fakePoolStore) AcquireTrinoPoolAuthority(_ context.Context, poolID, owner string) (configstore.TrinoPoolLease, error) {
	f.epoch++
	f.pool.AuthorityEpoch, f.pool.AuthorityOwner = f.epoch, owner
	return configstore.TrinoPoolLease{PoolID: poolID, Owner: owner, Epoch: f.epoch}, nil
}

func (f *fakePoolStore) ListTrinoPoolInstances(context.Context, string) ([]configstore.TrinoPoolInstance, error) {
	instances := make([]configstore.TrinoPoolInstance, 0, len(f.order))
	for _, id := range f.order {
		instances = append(instances, *f.instances[id])
	}
	return instances, nil
}

func (f *fakePoolStore) CreateTrinoPoolInstance(_ context.Context, lease configstore.TrinoPoolLease, spec configstore.TrinoPoolInstanceSpec) error {
	if lease.Epoch != f.epoch {
		return configstore.ErrTrinoPoolConflict
	}
	if _, exists := f.instances[spec.InstanceID]; exists {
		return fmt.Errorf("instance %s already exists", spec.InstanceID)
	}
	f.instances[spec.InstanceID] = &configstore.TrinoPoolInstance{
		InstanceID: spec.InstanceID, PoolID: spec.PoolID, ReleaseID: spec.ReleaseID,
		SpecDigest: spec.SpecDigest, BlueprintSnapshot: spec.BlueprintSnapshot,
		Phase: string(spec.Phase), Repair: spec.Repair, EndpointURL: spec.EndpointURL,
		ValidationReceipt: "{}", RetirementReceipt: "{}",
	}
	f.order = append(f.order, spec.InstanceID)
	return nil
}

func (f *fakePoolStore) AdvanceTrinoPoolInstance(_ context.Context, lease configstore.TrinoPoolLease, instanceID string, from, to trinopool.Phase, updates map[string]any) error {
	if f.failAdvance || lease.Epoch != f.epoch {
		return configstore.ErrTrinoPoolConflict
	}
	if err := trinopool.ValidateTransition(from, to); err != nil {
		return err
	}
	instance, exists := f.instances[instanceID]
	if !exists || instance.Phase != string(from) {
		return configstore.ErrTrinoPoolConflict
	}
	instance.Phase = string(to)
	applyFakeUpdates(instance, updates)
	return nil
}

func (f *fakePoolStore) RecordTrinoPoolInstanceFields(_ context.Context, lease configstore.TrinoPoolLease, instanceID string, updates map[string]any) error {
	if lease.Epoch != f.epoch {
		return configstore.ErrTrinoPoolConflict
	}
	applyFakeUpdates(f.instances[instanceID], updates)
	return nil
}

func applyFakeUpdates(instance *configstore.TrinoPoolInstance, updates map[string]any) {
	for key, value := range updates {
		switch key {
		case "service_name":
			instance.ServiceName = value.(string)
		case "service_uid":
			instance.ServiceUID = value.(string)
		case "config_map_name":
			instance.ConfigMapName = value.(string)
		case "config_map_uid":
			instance.ConfigMapUID = value.(string)
		case "coordinator_deployment_name":
			instance.CoordinatorDeploymentName = value.(string)
		case "coordinator_deployment_uid":
			instance.CoordinatorDeploymentUID = value.(string)
		case "worker_deployment_name":
			instance.WorkerDeploymentName = value.(string)
		case "worker_deployment_uid":
			instance.WorkerDeploymentUID = value.(string)
		case "coordinator_pod_uid":
			instance.CoordinatorPodUID = value.(string)
		case "coordinator_node_id":
			instance.CoordinatorNodeID = value.(string)
		case "coordinator_boot_id":
			instance.CoordinatorBootID = value.(string)
		case "gateway_incarnation":
			instance.GatewayIncarnation = value.(string)
		case "gateway_backend_name":
			instance.GatewayBackendName = value.(string)
		case "gateway_state":
			instance.GatewayState = value.(string)
		case "gateway_generation":
			instance.GatewayGeneration = value.(int64)
		case "applied_catalog_revision":
			instance.AppliedCatalogRevision = value.(int64)
		case "validation_receipt":
			instance.ValidationReceipt = value.(string)
		case "retirement_receipt":
			instance.RetirementReceipt = value.(string)
		}
	}
}

type fakePoolGateway struct {
	members     map[string]*trinogateway.Member
	obligations map[string]trinogateway.Obligations
	backends    map[string]trinogateway.Backend
	calls       []string
	drainErr    error
	configured  *trinogateway.ConfigurePoolRequest
}

func newFakePoolGateway() *fakePoolGateway {
	return &fakePoolGateway{
		members:     map[string]*trinogateway.Member{},
		obligations: map[string]trinogateway.Obligations{},
		backends:    map[string]trinogateway.Backend{},
	}
}

func (f *fakePoolGateway) record(call string) { f.calls = append(f.calls, call) }

func (f *fakePoolGateway) EnsureInactiveBackend(_ context.Context, backend trinogateway.Backend) error {
	f.record("backend:" + backend.Name)
	if backend.Active {
		return errors.New("a pooled backend must be inactive")
	}
	f.backends[backend.Name] = backend
	return nil
}

func (f *fakePoolGateway) ConfigurePool(_ context.Context, _ string, request trinogateway.ConfigurePoolRequest) (trinogateway.PoolState, error) {
	f.record("configure")
	f.configured = &request
	return trinogateway.PoolState{}, nil
}

func (f *fakePoolGateway) RegisterMember(_ context.Context, poolID string, request trinogateway.RegisterMemberRequest) (trinogateway.Member, error) {
	f.record("register:" + request.InstanceID)
	member := &trinogateway.Member{
		PoolID: poolID, InstanceID: request.InstanceID, BackendName: request.BackendName,
		Incarnation: "incarnation-" + request.InstanceID, Phase: "PREPARING", Generation: 1,
	}
	f.members[request.InstanceID] = member
	return *member, nil
}

func (f *fakePoolGateway) AdmitMember(_ context.Context, _, instanceID string, _ trinogateway.AdmitMemberRequest) (trinogateway.Member, error) {
	f.record("admit:" + instanceID)
	member := f.members[instanceID]
	member.Phase, member.Generation, member.Eligible = "ACTIVE", member.Generation+1, true
	return *member, nil
}

func (f *fakePoolGateway) GetMember(_ context.Context, _, instanceID string) (trinogateway.Member, error) {
	member, exists := f.members[instanceID]
	if !exists {
		return trinogateway.Member{}, errors.New("unknown member")
	}
	return *member, nil
}

func (f *fakePoolGateway) GetObligations(_ context.Context, _, instanceID string) (trinogateway.Obligations, error) {
	return f.obligations[instanceID], nil
}

func (f *fakePoolGateway) DrainMember(_ context.Context, _, instanceID string, _ trinogateway.MemberStepRequest) (trinogateway.Member, error) {
	f.record("drain:" + instanceID)
	if f.drainErr != nil {
		return trinogateway.Member{}, f.drainErr
	}
	member := f.members[instanceID]
	member.Phase, member.Generation = "DRAINING", member.Generation+1
	return *member, nil
}

func (f *fakePoolGateway) SealMember(_ context.Context, _, instanceID string, _ trinogateway.MemberStepRequest) (trinogateway.Member, error) {
	f.record("seal:" + instanceID)
	member := f.members[instanceID]
	member.Phase, member.Generation = "SEALED", member.Generation+1
	return *member, nil
}

func (f *fakePoolGateway) RetireMember(_ context.Context, _, instanceID string, _ trinogateway.MemberStepRequest) (trinogateway.Member, error) {
	f.record("retire:" + instanceID)
	member := f.members[instanceID]
	member.Phase, member.Generation, member.RetirementKind = "RETIRING", member.Generation+1, "PLANNED"
	return *member, nil
}

func (f *fakePoolGateway) MemberRetired(_ context.Context, _, instanceID string, request trinogateway.MemberStepRequest) (trinogateway.Member, error) {
	f.record("retired:" + instanceID)
	if !request.ResourcesAbsent {
		return trinogateway.Member{}, errors.New("retirement reported without asserting absence")
	}
	member := f.members[instanceID]
	member.Phase = "RETIRED"
	return *member, nil
}

type fakePoolKube struct {
	applied    map[string]trinoPoolInventory
	deleted    map[string]bool
	observed   trinoPoolObservation
	absent     bool
	epochsSeen []int64
}

func newFakePoolKube() *fakePoolKube {
	return &fakePoolKube{
		applied: map[string]trinoPoolInventory{},
		deleted: map[string]bool{},
		observed: trinoPoolObservation{
			CoordinatorReady: true, ReadyWorkers: 4, DesiredWorkers: 4,
			CoordinatorPodUID: "pod-uid-1", PodsPresent: 5,
		},
	}
}

func (f *fakePoolKube) forEpoch(epoch int64) trinoPoolKube {
	f.epochsSeen = append(f.epochsSeen, epoch)
	return f
}

func (f *fakePoolKube) Apply(_ context.Context, objects trinopool.Objects) (trinoPoolInventory, error) {
	inventory := trinoPoolInventory{
		Namespace:                 objects.Service.Namespace,
		ConfigMapName:             objects.ConfigMap.Name,
		ConfigMapUID:              "cm-uid",
		ServiceName:               objects.Service.Name,
		ServiceUID:                "svc-uid",
		CoordinatorDeploymentName: objects.CoordinatorDeployment.Name,
		CoordinatorDeploymentUID:  "coord-uid",
		WorkerDeploymentName:      objects.WorkerDeployment.Name,
		WorkerDeploymentUID:       "worker-uid",
	}
	f.applied[objects.Service.Name] = inventory
	return inventory, nil
}

func (f *fakePoolKube) Observe(context.Context, trinoPoolInventory) (trinoPoolObservation, error) {
	return f.observed, nil
}

func (f *fakePoolKube) Delete(_ context.Context, inventory trinoPoolInventory) error {
	f.deleted[inventory.ServiceName] = true
	return nil
}

func (f *fakePoolKube) ResourcesAbsent(context.Context, trinoPoolInventory) (bool, error) {
	return f.absent, nil
}

// ---------------------------------------------------------------------------
// The loop.
// ---------------------------------------------------------------------------

type operatorHarness struct {
	operator *trinoPoolOperator
	store    *fakePoolStore
	gateway  *fakePoolGateway
	kube     *fakePoolKube
}

func newOperatorHarness(t *testing.T) *operatorHarness {
	t.Helper()
	blueprint, err := trinopool.ParseBlueprint(testBlueprintJSON(t))
	if err != nil {
		t.Fatalf("parse blueprint: %v", err)
	}
	config := trinoPoolConfig{
		PoolID: "registered:cell-001", PublicID: "cell-001",
		RoutingGroup: "cell-001", Namespace: blueprint.Namespace,
		Blueprint: blueprint,
		Pool: trinoRegisteredPool{
			DesiredInstances: 3, MinServing: 3, MaxSurge: 1, MaxRepair: 1,
			CoordinatorServicePort: 8443, NodeEnvironment: "mw_dev_pool_001",
		},
		Spec: configstore.TrinoPoolSpec{
			PoolID: "registered:cell-001", PublicID: "cell-001",
			APIMode: configstore.TrinoPoolAPIModeShared, DesiredInstances: 3,
			MinServing: 3, MaxSurge: 1, MaxRepair: 1,
			DesiredReleaseID: blueprint.ReleaseID, DesiredBlueprintDigest: blueprint.Digest(),
		},
	}
	store := newFakePoolStore(config.Spec)
	gateway := newFakePoolGateway()
	kube := newFakePoolKube()

	sequence := 0
	return &operatorHarness{
		store: store, gateway: gateway, kube: kube,
		operator: &trinoPoolOperator{
			config: config, store: store, gateway: gateway,
			kube:            kube.forEpoch,
			owner:           "cp-test",
			operatorEnabled: true,
			validate: func(context.Context, string, int) (trinoPoolValidation, error) {
				return trinoPoolValidation{
					NodeID: "node-1", ProcessID: "process-1", CoordinatorID: "abcde",
					AppliedRevision: 42, AuthRevision: "auth", ReadyWorkers: 4,
					Checks: []string{trinoPoolCheckImage}, CertificateHash: "hash",
				}, nil
			},
			newInstanceID: func() string {
				sequence++
				return fmt.Sprintf("%08x", sequence)
			},
		},
	}
}

func (h *operatorHarness) tick(t *testing.T, times int) {
	t.Helper()
	for index := 0; index < times; index++ {
		if err := h.operator.reconcileOnce(context.Background()); err != nil {
			t.Fatalf("tick %d: %v", index, err)
		}
	}
}

func (h *operatorHarness) phases() map[string]string {
	phases := map[string]string{}
	for id, instance := range h.store.instances {
		phases[id] = instance.Phase
	}
	return phases
}

// With the operator disabled the desired state is still recorded, and nothing
// in Kubernetes or the Gateway is touched. That is what shipping this feature
// disabled has to mean: the wiring exists and stays inert.
func TestOperatorDisabledTouchesNothingExternal(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.operatorEnabled = false

	harness.tick(t, 3)

	if harness.store.pool.DesiredInstances != 3 {
		t.Fatal("the desired spec was not recorded")
	}
	if len(harness.gateway.calls) != 0 {
		t.Fatalf("the disabled operator called the gateway: %v", harness.gateway.calls)
	}
	if len(harness.kube.applied) != 0 {
		t.Fatalf("the disabled operator created %d instances", len(harness.kube.applied))
	}
	if harness.operator.lease.Epoch != 0 {
		t.Fatal("the disabled operator claimed authority")
	}
}

// A frozen pool holds its last-good state: nothing is created, nothing is
// deleted, and the freeze reason reaches the durable record for the operator.
func TestFrozenPoolMakesNoExternalChanges(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Frozen = true
	harness.operator.config.FrozenReason = "blueprint is unreadable"

	harness.tick(t, 2)

	if harness.store.frozen != "blueprint is unreadable" {
		t.Fatalf("freeze reason = %q", harness.store.frozen)
	}
	if len(harness.kube.applied) != 0 || len(harness.gateway.calls) != 0 {
		t.Fatal("a frozen pool produced external effects")
	}
}

// The full happy path: three instances reach SERVING, each through create,
// register, validate, admit.
func TestOperatorBringsThePoolToTheDesiredCount(t *testing.T) {
	harness := newOperatorHarness(t)
	// Each instance needs several ticks; the loop deliberately advances one
	// instance per tick.
	harness.tick(t, 20)

	serving := 0
	for _, phase := range harness.phases() {
		if phase == string(trinopool.PhaseServing) {
			serving++
		}
	}
	if serving != 3 {
		t.Fatalf("phases = %v, want three serving", harness.phases())
	}
	// Registration must have created the backend record first and left it
	// inactive; otherwise the member would be routable before it is certified.
	for name, backend := range harness.gateway.backends {
		if backend.Active {
			t.Fatalf("backend %s was registered active", name)
		}
	}
	if len(harness.gateway.backends) != 3 {
		t.Fatalf("registered %d backends", len(harness.gateway.backends))
	}
}

// A pool at its desired count and release does nothing further. A reconcile
// loop that keeps acting on a converged pool is how fleets get churned.
func TestOperatorIsQuietWhenConverged(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)
	before := len(harness.gateway.calls)

	harness.tick(t, 5)
	after := len(harness.gateway.calls)

	// Only the idempotent pool configuration call repeats.
	for _, call := range harness.gateway.calls[before:after] {
		if call != "configure" {
			t.Fatalf("a converged pool issued %q", call)
		}
	}
	if len(harness.kube.applied) != 3 {
		t.Fatalf("a converged pool created %d instances", len(harness.kube.applied))
	}
}

// A new release surges ONE replacement, and only drains an old instance once
// the replacement is actually serving.
func TestOperatorSurgesThenDrainsForANewRelease(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)

	harness.store.pool.DesiredReleaseID = "next-release"
	harness.operator.config.Spec.DesiredReleaseID = "next-release"
	harness.operator.config.Blueprint.ReleaseID = "next-release"

	// One tick creates the surge instance; it then needs ticks to reach
	// SERVING before any drain may start.
	harness.tick(t, 1)
	if drained := countCalls(harness.gateway.calls, "drain:"); drained != 0 {
		t.Fatal("a drain started before the replacement was serving")
	}
	if len(harness.store.instances) != 4 {
		t.Fatalf("surge created %d instances", len(harness.store.instances))
	}

	harness.tick(t, 10)
	if drained := countCalls(harness.gateway.calls, "drain:"); drained != 1 {
		t.Fatalf("expected exactly one drain, got %d", drained)
	}
}

// The Gateway's serving-floor refusal is authoritative. The operator records it
// and retries later; it never forces the drain.
func TestServingFloorRefusalIsNotOverridden(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)
	harness.gateway.drainErr = trinogateway.ErrServingFloor

	harness.store.pool.DesiredReleaseID = "next-release"
	harness.operator.config.Spec.DesiredReleaseID = "next-release"
	harness.operator.config.Blueprint.ReleaseID = "next-release"
	// The refusal surfaces as an error from the tick that attempts the drain,
	// so these ticks are allowed to fail.
	for index := 0; index < 11; index++ {
		_ = harness.operator.reconcileOnce(context.Background())
	}

	err := harness.operator.reconcileOnce(context.Background())
	if err == nil || !errors.Is(err, trinogateway.ErrServingFloor) {
		t.Fatalf("error = %v, want the serving-floor refusal to surface", err)
	}
	for _, instance := range harness.store.instances {
		if instance.Phase == string(trinopool.PhaseDraining) {
			t.Fatal("an instance was drained despite the refusal")
		}
	}
}

// Nothing is deleted before the Gateway has irreversibly claimed retirement,
// and retirement completes only once the resources are verifiably absent.
func TestRetirementRequiresAClaimThenVerifiedAbsence(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)

	instanceID := harness.store.order[0]
	instance := harness.store.instances[instanceID]
	instance.Phase = string(trinopool.PhaseSealed)

	// Sealed -> the operator claims retirement. Still nothing deleted.
	harness.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseRetiring) {
		t.Fatalf("phase = %s, want RETIRING", instance.Phase)
	}
	if len(harness.kube.deleted) != 0 {
		t.Fatal("resources were deleted before the retirement claim")
	}
	if instance.RetirementReceipt == "{}" || instance.RetirementReceipt == "" {
		t.Fatal("the retirement claim was not recorded")
	}

	// Deletion starts, but the pods are still terminating: the instance stays
	// RETIRING and keeps its slot.
	harness.kube.absent = false
	harness.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseRetiring) {
		t.Fatalf("phase = %s, want the instance to hold RETIRING while pods remain", instance.Phase)
	}
	if countCalls(harness.gateway.calls, "retired:") != 0 {
		t.Fatal("retirement was reported before the resources were gone")
	}

	harness.kube.absent = true
	harness.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseRetired) {
		t.Fatalf("phase = %s, want RETIRED", instance.Phase)
	}
}

// A member is not sealed while anything is still pinned to it. There is no
// drain deadline: sealing on a timer would be a decision to lose that work.
func TestSealWaitsForObligations(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)

	instanceID := harness.store.order[0]
	instance := harness.store.instances[instanceID]
	instance.Phase = string(trinopool.PhaseDraining)
	harness.gateway.obligations[instanceID] = trinogateway.Obligations{
		Generation: 5, OpenTransactions: 1, Drained: false,
	}

	harness.tick(t, 3)
	if instance.Phase != string(trinopool.PhaseDraining) {
		t.Fatalf("phase = %s, want the instance to stay DRAINING", instance.Phase)
	}
	if countCalls(harness.gateway.calls, "seal:") != 0 {
		t.Fatal("a member with an open transaction was sealed")
	}

	harness.gateway.obligations[instanceID] = trinogateway.Obligations{Generation: 5, Drained: true}
	harness.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseSealed) {
		t.Fatalf("phase = %s, want SEALED once drained", instance.Phase)
	}
}

// Losing the authority CAS means this leader has been superseded. It must stop
// writing and re-acquire, not retry the same epoch harder.
func TestLostAuthorityIsDroppedAndReacquired(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 1)
	first := harness.operator.lease.Epoch

	harness.store.failAdvance = true
	if err := harness.operator.reconcileOnce(context.Background()); err == nil {
		t.Fatal("a fenced write failure was not surfaced")
	}
	if harness.operator.lease.Epoch != 0 {
		t.Fatal("the operator kept an epoch the store refused")
	}

	harness.store.failAdvance = false
	harness.tick(t, 1)
	if harness.operator.lease.Epoch <= first {
		t.Fatalf("epoch %d did not advance past %d", harness.operator.lease.Epoch, first)
	}
}

// Every Kubernetes effect is made under the CURRENT authority epoch, so a
// superseded leader's writes are refused at the object level too.
func TestKubernetesEffectsCarryTheCurrentEpoch(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 5)
	if len(harness.kube.epochsSeen) == 0 {
		t.Fatal("no kubernetes effect was attempted")
	}
	for _, epoch := range harness.kube.epochsSeen {
		if epoch != harness.operator.lease.Epoch {
			t.Fatalf("an effect used epoch %d, current is %d", epoch, harness.operator.lease.Epoch)
		}
	}
}

// The tenant-admission gate stays closed while the identity question is open.
// Turning it on would claim an atomic gate duckgres cannot currently back.
func TestTenantAdmissionGateStaysClosed(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 1)
	if harness.gateway.configured == nil {
		t.Fatal("the pool was never configured")
	}
	if harness.gateway.configured.TenantAdmissionEnabled {
		t.Fatal("the operator enabled tenant admission")
	}
	if harness.gateway.configured.DesiredMembers != 3 || harness.gateway.configured.MaxRepair != 1 {
		t.Fatalf("configure request = %+v", harness.gateway.configured)
	}
}

func countCalls(calls []string, prefix string) int {
	total := 0
	for _, call := range calls {
		if len(call) >= len(prefix) && call[:len(prefix)] == prefix {
			total++
		}
	}
	return total
}
