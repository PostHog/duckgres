//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

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
	// staleGeneration simulates a desired spec whose generation is behind the
	// published one.
	staleGeneration bool
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

func (f *fakePoolStore) SeedTrinoPool(_ context.Context, _ configstore.TrinoPoolSpec) error {
	return nil
}

func (f *fakePoolStore) UpsertTrinoPoolSpec(_ context.Context, lease configstore.TrinoPoolLease, spec configstore.TrinoPoolSpec) error {
	if lease.Epoch != f.epoch {
		return configstore.ErrTrinoPoolConflict
	}
	if f.staleGeneration {
		return fmt.Errorf("%w: 1 is behind the published 2", configstore.ErrTrinoPoolStaleGeneration)
	}
	f.pool.DesiredInstances, f.pool.MinServing = spec.DesiredInstances, spec.MinServing
	f.pool.MaxSurge, f.pool.MaxRepair = spec.MaxSurge, spec.MaxRepair
	f.pool.DesiredReleaseID = spec.DesiredReleaseID
	return nil
}

func (f *fakePoolStore) FreezeTrinoPool(_ context.Context, _ configstore.TrinoPoolLease, _, reason string) error {
	f.frozen = reason
	f.pool.Frozen, f.pool.FrozenReason = true, reason
	return nil
}

func (f *fakePoolStore) ThawTrinoPool(context.Context, configstore.TrinoPoolLease, string) error {
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
		case "coordinator_id":
			instance.CoordinatorID = value.(string)
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
		case "repair_for":
			instance.RepairFor = value.(string)
		case "failure_reason":
			instance.FailureReason = value.(string)
		case "worker_config_map_name":
			instance.WorkerConfigMapName = value.(string)
		case "worker_config_map_uid":
			instance.WorkerConfigMapUID = value.(string)
		case "last_error":
			instance.LastError = value.(string)
		case "validation_receipt":
			instance.ValidationReceipt = value.(string)
		case "retirement_receipt":
			instance.RetirementReceipt = value.(string)
		}
	}
}

type fakePoolGateway struct {
	members      map[string]*trinogateway.Member
	obligations  map[string]trinogateway.Obligations
	backends     map[string]trinogateway.Backend
	principals   map[string][]string
	calls        []string
	drainErr     error
	membership   int64
	publications map[string]*fakePublication
	admitted     map[string]string
	revoked      map[string]bool
	configured   *trinogateway.ConfigurePoolRequest
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

func (f *fakePoolGateway) PublishTenantPrincipals(_ context.Context, _, tenant string, request trinogateway.PublishPrincipalsRequest) (trinogateway.TenantAdmission, error) {
	f.record("principals:" + tenant)
	if request.Revision == "" || len(request.Principals) == 0 {
		return trinogateway.TenantAdmission{}, errors.New("a binding needs a revision and at least one principal")
	}
	if f.principals == nil {
		f.principals = map[string][]string{}
	}
	f.principals[tenant] = request.Principals
	return trinogateway.TenantAdmission{Tenant: tenant, State: "PENDING", PrincipalRevision: request.Revision}, nil
}

func (f *fakePoolGateway) ConfigurePool(_ context.Context, _ string, request trinogateway.ConfigurePoolRequest) (trinogateway.PoolState, error) {
	f.record("configure")
	f.configured = &request
	return trinogateway.PoolState{}, nil
}

func (f *fakePoolGateway) RegisterMember(_ context.Context, poolID string, request trinogateway.RegisterMemberRequest) (trinogateway.Member, error) {
	f.record("register:" + request.InstanceID)
	// The Gateway probes the coordinator itself at registration and binds the
	// member to the identity it observed, so the response - not the request -
	// is where those values come from.
	member := &trinogateway.Member{
		PoolID: poolID, InstanceID: request.InstanceID, BackendName: request.BackendName,
		Incarnation: "incarnation-" + request.InstanceID, Phase: "PREPARING", Generation: 1,
		NodeID: "node-1", CoordinatorID: "abcde",
		// Bound at registration, and a later receipt or loss claim has to
		// present the identical pair.
		PodUID: request.PodUID, BootID: request.BootID,
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

func (f *fakePoolGateway) SuspectMember(_ context.Context, _, instanceID string, request trinogateway.SuspectMemberRequest) (trinogateway.Member, error) {
	f.record("suspect:" + instanceID)
	if request.Reason == "" {
		return trinogateway.Member{}, errors.New("a suspicion must carry a reason")
	}
	member := f.members[instanceID]
	member.Phase, member.Generation = "SUSPECT", member.Generation+1
	return *member, nil
}

func (f *fakePoolGateway) LostMember(_ context.Context, _, instanceID string, request trinogateway.LostMemberRequest) (trinogateway.Member, error) {
	f.record("lost:" + instanceID)
	if request.Evidence == "" || request.Termination.Source == "" {
		return trinogateway.Member{}, errors.New("a loss claim needs termination evidence")
	}
	member := f.members[instanceID]
	member.Phase, member.Generation, member.RetirementKind = "LOST", member.Generation+1, "FAILED"
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
	operator     *trinoPoolOperator
	store        *fakePoolStore
	gateway      *fakePoolGateway
	kube         *fakePoolKube
	publications *fakePublicationStore
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
	publications := newFakePublicationStore()
	return &operatorHarness{
		store: store, gateway: gateway, kube: kube, publications: publications,
		operator: &trinoPoolOperator{
			config: config, store: store, gateway: gateway,
			kube:            kube.forEpoch,
			owner:           "cp-test",
			operatorEnabled: true,
			validate: func(_ context.Context, _ string, _ trinoPoolObservation, _ trinoPoolExpectation) (trinoPoolValidation, error) {
				return trinoPoolValidation{
					NodeID: "node-1", ProcessID: "process-1", CoordinatorID: "abcde",
					AppliedRevision: 42, AuthRevision: "auth", ReadyWorkers: 4,
					Checks: []string{trinoPoolCheckImage}, CertificateHash: "hash",
				}, nil
			},
			identity:     func(context.Context, string) (string, error) { return "process-1", nil },
			publications: publications,
			// Every member is serving the projection the control plane is
			// publishing. Tests that need the opposite override this.
			acknowledgement: func(_ context.Context, _ string, _ trinoPoolProjectionRevisions, _ int64) (trinoPoolAcknowledgement, error) {
				return trinoPoolAcknowledgement{ProcessID: "process-1", AppliedRevision: 42, ProjectionCurrent: true}, nil
			},
			projection: func() trinoPoolProjectionRevisions {
				return trinoPoolProjectionRevisions{Policy: "policy-1", Password: "password-1", Group: "group-1"}
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

// ---------------------------------------------------------------------------
// Tenant binding, failure branch and authority lifecycle.
// ---------------------------------------------------------------------------

type fakeTenantStore struct{ orgs []configstore.TrinoEnabledOrg }

func (f *fakeTenantStore) ListTrinoEnabledOrgs() ([]configstore.TrinoEnabledOrg, error) {
	return f.orgs, nil
}

func poolOrg(users ...string) configstore.TrinoEnabledOrg {
	org := configstore.TrinoEnabledOrg{
		OrgID: "org-a", DatabaseName: "acme",
		CellID: "registered:cell-001", RootPasswordHash: "hash",
	}
	for _, username := range users {
		org.Users = append(org.Users, configstore.TrinoOrgUser{Username: username, PasswordHash: "hash"})
	}
	return org
}

// With the gate on, the binding must reach the Gateway BEFORE any lifecycle
// step: the restriction refuses work whose principal it cannot place, so a pool
// that admitted members before publishing would deny its own tenants.
func TestTenantBindingIsPublishedWhenTheGateIsOn(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}

	harness.tick(t, 1)

	if harness.gateway.configured == nil || !harness.gateway.configured.TenantAdmissionEnabled {
		t.Fatal("the gate knob was not passed to the Gateway")
	}
	published := harness.gateway.principals["org-a"]
	if len(published) != 2 {
		t.Fatalf("published principals = %v, want the root login and the user", published)
	}
	// The bare database name is the root login and carries no separator; a gate
	// that inferred the tenant from a dotted prefix would refuse it.
	var sawBare bool
	for _, principal := range published {
		if principal == "acme" {
			sawBare = true
		}
	}
	if !sawBare {
		t.Fatalf("published principals = %v, want the bare root login included", published)
	}
}

// An unchanged tenant is not republished; a changed login set is, because the
// Gateway replaces the set whole and a removed login must stop being admitted.
func TestTenantBindingIsRepublishedOnlyWhenItChanges(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants

	harness.tick(t, 3)
	if count := countCalls(harness.gateway.calls, "principals:"); count != 1 {
		t.Fatalf("published %d times for an unchanged tenant", count)
	}

	tenants.orgs = []configstore.TrinoEnabledOrg{poolOrg("analyst", "dagster")}
	harness.tick(t, 1)
	if count := countCalls(harness.gateway.calls, "principals:"); count != 2 {
		t.Fatalf("a changed login set published %d times", count)
	}
}

// With the gate off nothing is published: the pool is not making that promise.
func TestNoTenantBindingWhenTheGateIsOff(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}

	harness.tick(t, 2)
	if countCalls(harness.gateway.calls, "principals:") != 0 {
		t.Fatal("a pool with the gate off published a binding")
	}
}

// A serving member whose coordinator is gone did NOT drain. It is excluded from
// new work first, and only declared lost once the resources are verifiably
// absent - a failing probe is not evidence of death.
func TestUnhealthyMemberIsSuspectedThenLostOnlyWithEvidence(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)

	instanceID := harness.store.order[0]
	instance := harness.store.instances[instanceID]
	instance.PhaseChangedAt = nowUTC().Add(-time.Hour)
	harness.kube.observed.CoordinatorReady = false

	harness.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseSuspect) {
		t.Fatalf("phase = %s, want SUSPECT", instance.Phase)
	}
	if countCalls(harness.gateway.calls, "lost:") != 0 {
		t.Fatal("a member was declared lost on a failing probe alone")
	}

	// Pods still present: nothing is declared and nothing is deleted.
	harness.kube.absent = false
	harness.kube.observed.PodsPresent = 3
	harness.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseSuspect) {
		t.Fatalf("phase = %s, want the member to stay SUSPECT while its pods exist", instance.Phase)
	}

	// Verified absence is the evidence.
	harness.kube.absent = true
	harness.kube.observed.PodsPresent = 0
	harness.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseLost) {
		t.Fatalf("phase = %s, want LOST once the resources are gone", instance.Phase)
	}
	if countCalls(harness.gateway.calls, "lost:") != 1 {
		t.Fatal("the loss was not recorded with the Gateway")
	}
	// Reported as failed, never as a clean drain.
	if countCalls(harness.gateway.calls, "seal:") != 0 {
		t.Fatal("a lost member was sealed as if it had drained")
	}
}

// A member excluded on suspicion returns to service when it recovers: one bad
// minute must not retire a healthy cluster.
func TestSuspectedMemberRecovers(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)

	instanceID := harness.store.order[0]
	instance := harness.store.instances[instanceID]
	instance.Phase = string(trinopool.PhaseSuspect)

	harness.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseServing) {
		t.Fatalf("phase = %s, want the recovered member back in service", instance.Phase)
	}
}

// Desired-state publication is lifecycle-affecting, so it is fenced too. A
// read-only operator has no authority and must not write it at all.
func TestReadOnlyOperatorDoesNotPublishDesiredState(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.operatorEnabled = false
	harness.store.pool.DesiredReleaseID = "someone-elses-release"

	harness.tick(t, 2)

	if harness.store.pool.DesiredReleaseID != "someone-elses-release" {
		t.Fatal("a read-only operator overwrote authority-owned desired state")
	}
	if harness.store.epoch != 0 {
		t.Fatal("a read-only operator claimed authority")
	}
}

// ---------------------------------------------------------------------------
// Durable operation recording.
// ---------------------------------------------------------------------------

type fakeOperationStore struct {
	operations map[string]configstore.TrinoPoolOperation
	steps      map[string]configstore.TrinoPoolOperationStep
	epoch      func() int64
}

func newFakeOperationStore(epoch func() int64) *fakeOperationStore {
	return &fakeOperationStore{
		operations: map[string]configstore.TrinoPoolOperation{},
		steps:      map[string]configstore.TrinoPoolOperationStep{},
		epoch:      epoch,
	}
}

func (f *fakeOperationStore) BeginTrinoPoolOperation(_ context.Context, lease configstore.TrinoPoolLease, spec configstore.TrinoPoolOperationSpec) (configstore.TrinoPoolOperation, error) {
	if lease.Epoch != f.epoch() {
		return configstore.TrinoPoolOperation{}, configstore.ErrTrinoPoolConflict
	}
	if existing, ok := f.operations[spec.OperationID]; ok {
		if existing.IntentHash != spec.IntentHash {
			return configstore.TrinoPoolOperation{}, configstore.ErrTrinoPoolIntentChanged
		}
		existing.Replayed = true
		return existing, nil
	}
	created := configstore.TrinoPoolOperation{OperationID: spec.OperationID, IntentHash: spec.IntentHash}
	f.operations[spec.OperationID] = created
	return created, nil
}

func (f *fakeOperationStore) RecordTrinoPoolOperationStep(_ context.Context, lease configstore.TrinoPoolLease, operationID, stepID, payloadHash, outcome, result string) (configstore.TrinoPoolOperationStep, error) {
	if lease.Epoch != f.epoch() {
		return configstore.TrinoPoolOperationStep{}, configstore.ErrTrinoPoolConflict
	}
	key := operationID + "/" + stepID
	if existing, ok := f.steps[key]; ok {
		if existing.PayloadHash != payloadHash {
			return configstore.TrinoPoolOperationStep{}, configstore.ErrTrinoPoolIntentChanged
		}
		existing.Replayed = true
		// A later call with a real outcome replaces the provisional UNKNOWN.
		if outcome != "" && outcome != "UNKNOWN" {
			existing.Outcome, existing.Result = outcome, result
			f.steps[key] = existing
		}
		return existing, nil
	}
	created := configstore.TrinoPoolOperationStep{
		OperationID: operationID, StepID: stepID, PayloadHash: payloadHash,
		Outcome: outcome, Result: result,
	}
	f.steps[key] = created
	return created, nil
}

func (f *fakeOperationStore) FinishTrinoPoolOperation(context.Context, configstore.TrinoPoolLease, string, string, string) error {
	return nil
}

// An admission whose response is lost is UNKNOWN, not failed: the member may
// already be ACTIVE. The intent is recorded before the call, so the next
// attempt resolves it by read-back instead of deciding from nothing.
func TestAdmissionRecordsItsIntentAndOutcome(t *testing.T) {
	harness := newOperatorHarness(t)
	operations := newFakeOperationStore(func() int64 { return harness.store.epoch })
	harness.operator.operations = operations

	harness.tick(t, 20)

	var admitStep configstore.TrinoPoolOperationStep
	for key, step := range operations.steps {
		if step.StepID == "admit" {
			admitStep = step
			_ = key
			break
		}
	}
	if admitStep.StepID == "" {
		t.Fatalf("no admit step was recorded: %v", operations.steps)
	}
	if admitStep.Outcome != "OK" {
		t.Fatalf("admit outcome = %q, want OK once the Gateway answered", admitStep.Outcome)
	}
	if admitStep.PayloadHash == "" {
		t.Fatal("the admit step recorded no payload identity")
	}
	if len(operations.operations) == 0 {
		t.Fatal("no durable operation was recorded")
	}
}

// A step already recorded OK is not performed again: a second admission call
// would be a duplicate effect this controller can avoid entirely.
func TestCompletedStepIsNotRepeated(t *testing.T) {
	harness := newOperatorHarness(t)
	operations := newFakeOperationStore(func() int64 { return harness.store.epoch })
	harness.operator.operations = operations
	harness.tick(t, 20)

	before := countCalls(harness.gateway.calls, "admit:")
	instanceID := harness.store.order[0]
	instance := harness.store.instances[instanceID]
	// Force the instance back to the admitting step with the record intact.
	instance.Phase = string(trinopool.PhaseValidating)

	harness.tick(t, 1)
	if countCalls(harness.gateway.calls, "admit:") != before {
		t.Fatal("a step already recorded OK was performed again")
	}
	if instance.Phase != string(trinopool.PhaseAdmitted) {
		t.Fatalf("phase = %s, want the recorded outcome to advance the instance", instance.Phase)
	}
}

// A candidate that can never be admitted must not be abandoned in place.
//
// FAILED_PREPARING used to be terminal, so the instance's Deployments, Service
// and ConfigMaps kept running and its Gateway member stayed PREPARING - which
// the Gateway counts as LIVE. One such candidate at desired+surge refused every
// later registration: no repair, no rollout, and a whole leaked Trino cluster.
func TestFailedCandidateIsCleanedUpAndReleasesItsSlot(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Spec.DesiredInstances, harness.operator.config.Spec.MinServing = 1, 1
	harness.store.pool.DesiredInstances, harness.store.pool.MinServing = 1, 1

	// create -> CREATING -> PREPARING (registered)
	harness.tick(t, 3)
	instanceID := harness.store.order[0]
	if phase := harness.store.instances[instanceID].Phase; phase != string(trinopool.PhasePreparing) {
		t.Fatalf("instance phase = %s, want PREPARING", phase)
	}
	// The Gateway observes the coordinator identity itself at registration, and
	// a later loss claim has to present exactly what it recorded.
	if harness.store.instances[instanceID].CoordinatorID == "" {
		t.Fatal("the coordinator identity the Gateway recorded was not kept; a loss claim can never be accepted")
	}

	// The coordinator restarts before admission: the registered incarnation is
	// gone, so this candidate can never be admitted.
	harness.operator.validate = func(context.Context, string, trinoPoolObservation, trinoPoolExpectation) (trinoPoolValidation, error) {
		return trinoPoolValidation{
			NodeID: "node-1", ProcessID: "process-restarted", CoordinatorID: "abcde",
			AppliedRevision: 42, AuthRevision: "auth", ReadyWorkers: 4,
			Checks: []string{trinoPoolCheckImage}, CertificateHash: "hash",
		}, nil
	}
	harness.tick(t, 1)
	if phase := harness.store.instances[instanceID].Phase; phase != string(trinopool.PhaseFailedPreparing) {
		t.Fatalf("instance phase = %s, want FAILED_PREPARING", phase)
	}

	// Nothing may be declared lost while the pods are still there.
	harness.kube.absent = false
	harness.tick(t, 1)
	for _, call := range harness.gateway.calls {
		if call == "lost:"+instanceID {
			t.Fatal("a loss was claimed while the resources were still present")
		}
	}

	harness.kube.absent = true
	harness.tick(t, 3)
	if !harness.kube.deleted[instanceID] {
		t.Fatal("the failed candidate's Kubernetes objects were never deleted")
	}
	if phase := harness.store.instances[instanceID].Phase; phase != string(trinopool.PhaseFailureRetired) {
		t.Fatalf("instance phase = %s, want FAILURE_RETIRED", phase)
	}
	if member, _ := harness.gateway.GetMember(context.Background(), "cell-001", instanceID); member.Phase != "LOST" {
		t.Fatalf("gateway member phase = %s, want LOST so the live slot is released", member.Phase)
	}

	// With the slot released the pool replaces the failed candidate instead of
	// stalling behind it.
	harness.tick(t, 1)
	if len(harness.store.order) != 2 {
		t.Fatalf("the pool created %d instances; a failed candidate blocked the replacement", len(harness.store.order))
	}
}

// The staleness hazard is a process that resolved its configuration at boot and
// only later won the lease: publishing that snapshot is a legal fenced write of
// old content, and no generation ordering catches it, because a settings-only
// edit need not move the generation at all. The desired state is therefore
// re-read immediately before it is published.
func TestDesiredStateIsResolvedOnEveryTick(t *testing.T) {
	harness := newOperatorHarness(t)
	current := harness.operator.config
	harness.operator.resolveConfig = func() (trinoPoolConfig, error) { return current, nil }

	harness.tick(t, 1)
	if harness.store.pool.DesiredInstances != 3 {
		t.Fatalf("desired instances = %d, want the resolved 3", harness.store.pool.DesiredInstances)
	}

	// The cluster's configuration changes with no change to the generation,
	// which is exactly the case an ordering check cannot see.
	changed := current
	changed.Spec.DesiredInstances, changed.Spec.MinServing = 5, 4
	current = changed

	harness.tick(t, 1)
	if harness.store.pool.DesiredInstances != 5 || harness.store.pool.MinServing != 4 {
		t.Fatalf("desired = %d/%d, want the configuration the source holds now (5/4)",
			harness.store.pool.DesiredInstances, harness.store.pool.MinServing)
	}
}

// An unreadable configuration holds the last-good state. It is never a desired
// count of zero and never a failed tick that stops the loop: the pool keeps
// serving while somebody fixes the mount.
func TestUnreadableConfigurationFreezesRatherThanEmptiesThePool(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.resolveConfig = func() (trinoPoolConfig, error) {
		return trinoPoolConfig{}, errors.New("blueprint is unreadable")
	}

	harness.tick(t, 2)

	if harness.store.pool.DesiredInstances != 3 {
		t.Fatalf("desired instances = %d, want the last-good 3", harness.store.pool.DesiredInstances)
	}
	if !harness.store.pool.Frozen {
		t.Fatal("an unreadable configuration did not freeze the pool")
	}
	if len(harness.kube.applied) != 0 {
		t.Fatalf("the frozen pool created %d instances", len(harness.kube.applied))
	}
}

// A generation that went backwards is a configuration problem, not a lost
// fence. Ending the leadership term over it handed the pool to a replica
// reading the same file, which did the same thing: the pool never converged and
// the epoch ratcheted on every tick.
func TestBackwardsGenerationFreezesAndKeepsTheLease(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.store.staleGeneration = true

	harness.tick(t, 1)

	if !harness.store.pool.Frozen {
		t.Fatal("a backwards desired generation did not freeze the pool")
	}
	if harness.operator.fenced {
		t.Fatal("a stale generation was treated as a lost fence")
	}
	if harness.operator.lease.Epoch == 0 {
		t.Fatal("the leadership term ended over a configuration problem")
	}
}

// Leadership can move to a replica whose own copy of the configuration is old.
// Desired state must still be the cluster's, so the publication is derived from
// the API object at the moment of the write - by both replicas, in either
// order.
func TestLeadershipSwitchPublishesTheAPIObjectNotTheReplicaSnapshot(t *testing.T) {
	t.Setenv(envTrinoRegistryOnly, "true")
	t.Setenv(envTrinoPoolEnabled, "true")
	t.Setenv(envTrinoCellsFile, mountedRegistry(t, 3, 3))
	client := poolConfigMap(t, 5, 4)
	reader := poolAPIReader(t, client)

	// One durable pool, two control planes. Each booted with a different copy
	// of the configuration, which is what independent projected volumes look
	// like in practice.
	shared := newOperatorHarness(t)
	stale := newOperatorHarness(t)
	stale.operator.store = shared.store
	stale.store = shared.store
	stale.operator.config.Spec.DesiredInstances, stale.operator.config.Spec.MinServing = 3, 3
	shared.operator.config.Spec.DesiredInstances, shared.operator.config.Spec.MinServing = 2, 2
	for _, harness := range []*operatorHarness{shared, stale} {
		harness.operator.resolveConfig = func() (trinoPoolConfig, error) {
			return resolveTrinoPoolConfigByID(context.Background(), reader, "cell-001")
		}
	}

	shared.tick(t, 1)
	if shared.store.pool.DesiredInstances != 5 || shared.store.pool.MinServing != 4 {
		t.Fatalf("first leader published %d/%d, want the API object's 5/4",
			shared.store.pool.DesiredInstances, shared.store.pool.MinServing)
	}

	// The lease moves. The new leader's own snapshot says 3/3 and must not
	// revert the pool to it.
	stale.tick(t, 1)
	if shared.store.pool.DesiredInstances != 5 || shared.store.pool.MinServing != 4 {
		t.Fatalf("the new leader reverted desired state to %d/%d",
			shared.store.pool.DesiredInstances, shared.store.pool.MinServing)
	}

	// And a change made while the first leader is idle is picked up by whoever
	// is leading, without a restart.
	setPoolConfigMap(t, client, 4, 3)
	stale.tick(t, 1)
	if shared.store.pool.DesiredInstances != 4 || shared.store.pool.MinServing != 3 {
		t.Fatalf("desired = %d/%d, want the updated 4/3",
			shared.store.pool.DesiredInstances, shared.store.pool.MinServing)
	}
}

// ---------------------------------------------------------------------------
// Publication barrier fakes. The Gateway's own rules (receipt identity,
// membership CAS, serving floor) are its tests' business; these model the
// parts the operator's decisions depend on.
// ---------------------------------------------------------------------------

type fakePublication struct {
	trinogateway.Publication
	received map[string]bool
}

func (f *fakePoolGateway) GetPool(context.Context, string) (trinogateway.PoolState, error) {
	serving := int64(0)
	for _, member := range f.members {
		if member.Phase == "ACTIVE" {
			serving++
		}
	}
	return trinogateway.PoolState{
		ServingMembers:       serving,
		MembershipGeneration: f.membership,
	}, nil
}

func (f *fakePoolGateway) activeInstanceIDs() []string {
	var active []string
	for id, member := range f.members {
		if member.Phase == "ACTIVE" {
			active = append(active, id)
		}
	}
	sort.Strings(active)
	return active
}

func (f *fakePoolGateway) OpenPublication(_ context.Context, _ string, request trinogateway.OpenPublicationRequest) (trinogateway.Publication, error) {
	f.record("open:" + request.Tenant)
	if f.publications == nil {
		f.publications = map[string]*fakePublication{}
	}
	if existing, found := f.publications[request.PublicationID]; found {
		return f.publicationView(existing), nil
	}
	active := f.activeInstanceIDs()
	publication := &fakePublication{
		Publication: trinogateway.Publication{
			PublicationID:        request.PublicationID,
			Tenant:               request.Tenant,
			TargetRevision:       request.TargetRevision,
			MembershipGeneration: request.ExpectedMembershipGeneration,
			Phase:                "OPEN",
			RequiredMembers:      active,
			TenantState:          "PENDING",
		},
		received: map[string]bool{},
	}
	f.publications[request.PublicationID] = publication
	return f.publicationView(publication), nil
}

func (f *fakePoolGateway) GetPublication(_ context.Context, _, publicationID string) (trinogateway.Publication, error) {
	publication, found := f.publications[publicationID]
	if !found {
		return trinogateway.Publication{}, fmt.Errorf("%w: %s", trinogateway.ErrNotFound, publicationID)
	}
	return f.publicationView(publication), nil
}

func (f *fakePoolGateway) RecordPublicationReceipt(_ context.Context, _, publicationID string, request trinogateway.PublicationReceiptRequest) (trinogateway.Publication, error) {
	f.record("receipt:" + request.InstanceID)
	publication, found := f.publications[publicationID]
	if !found {
		return trinogateway.Publication{}, fmt.Errorf("%w: %s", trinogateway.ErrNotFound, publicationID)
	}
	if request.AppliedRevision != publication.TargetRevision {
		return trinogateway.Publication{}, fmt.Errorf("%w: applied revision does not match the target", trinogateway.ErrPublicationBarrier)
	}
	member := f.members[request.InstanceID]
	if member == nil || member.BootID != request.BootID {
		return trinogateway.Publication{}, fmt.Errorf("%w: the acknowledgement does not identify this member's process", trinogateway.ErrPublicationBarrier)
	}
	publication.received[request.InstanceID] = true
	return f.publicationView(publication), nil
}

func (f *fakePoolGateway) CommitPublication(_ context.Context, _, publicationID string, _ trinogateway.CommitPublicationRequest) (trinogateway.Publication, error) {
	f.record("commit:" + publicationID)
	publication, found := f.publications[publicationID]
	if !found {
		return trinogateway.Publication{}, fmt.Errorf("%w: %s", trinogateway.ErrNotFound, publicationID)
	}
	view := f.publicationView(publication)
	if len(view.MissingMembers) > 0 {
		return trinogateway.Publication{}, fmt.Errorf("%w: %v", trinogateway.ErrReceiptsIncomplete, view.MissingMembers)
	}
	publication.Phase, publication.TenantState = "ADMITTED", "ADMITTED"
	publication.AdmittedRevision = publication.TargetRevision
	if f.admitted == nil {
		f.admitted = map[string]string{}
	}
	f.admitted[publication.Tenant] = publication.TargetRevision
	return f.publicationView(publication), nil
}

func (f *fakePoolGateway) RevokeTenant(_ context.Context, _, tenant string, request trinogateway.RevokeTenantRequest) (trinogateway.TenantAdmission, error) {
	f.record("revoke:" + tenant)
	if request.Reason == "" {
		return trinogateway.TenantAdmission{}, errors.New("a revocation must carry a reason")
	}
	delete(f.admitted, tenant)
	delete(f.principals, tenant)
	if f.revoked == nil {
		f.revoked = map[string]bool{}
	}
	f.revoked[tenant] = true
	return trinogateway.TenantAdmission{Tenant: tenant, State: "REVOKED"}, nil
}

func (f *fakePoolGateway) publicationView(publication *fakePublication) trinogateway.Publication {
	view := publication.Publication
	view.Receipts = nil
	view.MissingMembers = nil
	for _, instanceID := range publication.RequiredMembers {
		if publication.received[instanceID] {
			view.Receipts = append(view.Receipts, trinogateway.PublicationReceipt{InstanceID: instanceID})
			continue
		}
		view.MissingMembers = append(view.MissingMembers, instanceID)
	}
	return view
}

// fakePublicationStore is the durable publication record. It is a map, but the
// operator must treat it as the only source of "already published" - not its
// own memory - so the tests below restart the operator and assert that.
type fakePublicationStore struct {
	rows map[string]*configstore.TrinoPoolPublication
}

func newFakePublicationStore() *fakePublicationStore {
	return &fakePublicationStore{rows: map[string]*configstore.TrinoPoolPublication{}}
}

func (f *fakePublicationStore) ListTrinoPoolPublications(_ context.Context, poolID string) ([]configstore.TrinoPoolPublication, error) {
	ids := make([]string, 0, len(f.rows))
	for id := range f.rows {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	publications := make([]configstore.TrinoPoolPublication, 0, len(ids))
	for _, id := range ids {
		if f.rows[id].PoolID == poolID {
			publications = append(publications, *f.rows[id])
		}
	}
	return publications, nil
}

func (f *fakePublicationStore) row(poolID, orgID string) *configstore.TrinoPoolPublication {
	if f.rows[orgID] == nil {
		f.rows[orgID] = &configstore.TrinoPoolPublication{PoolID: poolID, OrgID: orgID}
	}
	return f.rows[orgID]
}

func (f *fakePublicationStore) RecordTrinoPoolTenantPrincipals(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID, revision string) error {
	row := f.row(poolID, orgID)
	row.PrincipalRevision = revision
	if row.State == configstore.TrinoPublicationRevoked || row.State == "" {
		row.State = configstore.TrinoPublicationPublished
	}
	return nil
}

func (f *fakePublicationStore) RecordTrinoPoolPublicationOpen(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID, publicationID, target string) error {
	row := f.row(poolID, orgID)
	row.PublicationID, row.TargetRevision = publicationID, target
	row.State = configstore.TrinoPublicationAdmitting
	return nil
}

func (f *fakePublicationStore) RecordTrinoPoolPublicationCommitted(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID, target, receipt string) error {
	row := f.row(poolID, orgID)
	row.AdmittedTargetRevision, row.GatewayReceipt = target, receipt
	row.State = configstore.TrinoPublicationAdmitted
	return nil
}

func (f *fakePublicationStore) RecordTrinoPoolTenantRevoked(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID, reason string) error {
	row := f.row(poolID, orgID)
	row.State, row.LastError = configstore.TrinoPublicationRevoked, reason
	row.AdmittedTargetRevision, row.TargetRevision, row.PublicationID = "", "", ""
	return nil
}

// servingPool brings the pool to a state where every instance is ACTIVE and
// serving, which is what a publication barrier requires.
func (h *operatorHarness) servingPool(t *testing.T) {
	t.Helper()
	h.tick(t, 12)
	for id, instance := range h.store.instances {
		if instance.Phase != string(trinopool.PhaseServing) {
			t.Fatalf("instance %s is %s, want SERVING before a publication", id, instance.Phase)
		}
	}
}

// Publishing a tenant's principals does NOT admit it. The Gateway dispatches
// work only for a tenant in state ADMITTED, and only a committed barrier puts
// it there - so a gate enabled without this driver denies every tenant forever.
func TestTenantIsAdmittedOnlyThroughACommittedBarrier(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.servingPool(t)

	// Principals first, then the barrier: open, one receipt per member, commit.
	harness.tick(t, 10)

	if harness.gateway.admitted["org-a"] == "" {
		t.Fatalf("the tenant was never admitted; gateway calls: %v", harness.gateway.calls)
	}
	row := harness.publications.rows["org-a"]
	if row == nil || row.State != configstore.TrinoPublicationAdmitted {
		t.Fatalf("durable publication = %+v, want an admitted tenant", row)
	}
	if row.AdmittedTargetRevision == "" || row.AdmittedTargetRevision != harness.gateway.admitted["org-a"] {
		t.Fatalf("durable target %q disagrees with the Gateway's %q",
			row.AdmittedTargetRevision, harness.gateway.admitted["org-a"])
	}
	// Every serving member had to acknowledge, one receipt each.
	receipts := 0
	for _, call := range harness.gateway.calls {
		if strings.HasPrefix(call, "receipt:") {
			receipts++
		}
	}
	if receipts != len(harness.store.instances) {
		t.Fatalf("%d receipts recorded for %d members", receipts, len(harness.store.instances))
	}
}

// A member that is not serving the tenant's configuration yet must not be
// acknowledged on its behalf. Committing without it would admit the tenant to a
// coordinator that cannot resolve its catalog or authenticate its login.
func TestBarrierWaitsForAMemberThatIsNotCurrent(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.acknowledgement = func(context.Context, string, trinoPoolProjectionRevisions, int64) (trinoPoolAcknowledgement, error) {
		return trinoPoolAcknowledgement{ProcessID: "process-1", AppliedRevision: 42, ProjectionCurrent: false}, nil
	}
	harness.servingPool(t)

	harness.tick(t, 10)

	if harness.gateway.admitted["org-a"] != "" {
		t.Fatal("a tenant was admitted while a member was not serving its configuration")
	}
	for _, call := range harness.gateway.calls {
		if strings.HasPrefix(call, "receipt:") {
			t.Fatalf("a receipt was recorded for a member that is not current: %v", harness.gateway.calls)
		}
	}
}

// "Already published" cannot live in the leader's memory. A new leadership term
// - or a different replica - reads the durable record, so an admitted tenant is
// not republished and an unpublished one is not skipped.
func TestPublicationStateSurvivesALeadershipChange(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.servingPool(t)
	harness.tick(t, 10)
	if harness.gateway.admitted["org-a"] == "" {
		t.Fatalf("setup: the tenant was never admitted; calls: %v", harness.gateway.calls)
	}

	// A different control plane takes over: same durable state, no memory.
	successor := newOperatorHarness(t)
	successor.operator.store = harness.store
	successor.operator.publications = harness.publications
	successor.operator.gateway = harness.gateway
	successor.operator.config.Pool.TenantAdmission = true
	successor.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.gateway.calls = nil

	successor.tick(t, 3)

	for _, call := range harness.gateway.calls {
		if strings.HasPrefix(call, "principals:") || strings.HasPrefix(call, "open:") || strings.HasPrefix(call, "commit:") {
			t.Fatalf("the new leader republished an already admitted tenant: %v", harness.gateway.calls)
		}
	}
}

// A tenant that disappears from the projection is REVOKED, not forgotten. The
// Gateway replaces a principal set only when it is published, so a removed
// warehouse would otherwise keep its logins dispatchable indefinitely.
func TestDepartedTenantIsRevoked(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.servingPool(t)
	harness.tick(t, 10)
	if harness.gateway.admitted["org-a"] == "" {
		t.Fatalf("setup: the tenant was never admitted; calls: %v", harness.gateway.calls)
	}

	tenants.orgs = nil
	harness.tick(t, 2)

	if !harness.gateway.revoked["org-a"] {
		t.Fatalf("the departed tenant was not revoked: %v", harness.gateway.calls)
	}
	if row := harness.publications.rows["org-a"]; row == nil || row.State != configstore.TrinoPublicationRevoked {
		t.Fatalf("durable publication = %+v, want a revoked tenant", row)
	}

	// And it is revoked exactly once: the row is kept precisely so the next
	// tick does not repeat an external mutation.
	harness.gateway.calls = nil
	harness.tick(t, 2)
	for _, call := range harness.gateway.calls {
		if strings.HasPrefix(call, "revoke:") {
			t.Fatalf("the tenant was revoked again: %v", harness.gateway.calls)
		}
	}
}
