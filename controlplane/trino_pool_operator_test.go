//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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
	// failRevisionCheckpoint simulates the write of the published catalog
	// revision failing after the catalog itself committed.
	failRevisionCheckpoint bool
	// recordedRevisions is every checkpoint the operator wrote, in order.
	recordedRevisions []int64
}

func (f *fakePoolStore) RecordTrinoPoolPublicationRevision(_ context.Context, lease configstore.TrinoPoolLease, _ string, revision int64) error {
	if lease.Epoch != f.epoch {
		return configstore.ErrTrinoPoolConflict
	}
	if f.failRevisionCheckpoint {
		return errors.New("checkpoint refused")
	}
	f.recordedRevisions = append(f.recordedRevisions, revision)
	f.pool.PublicationRevision = revision
	return nil
}

func newFakePoolStore(spec configstore.TrinoPoolSpec) *fakePoolStore {
	return &fakePoolStore{
		pool: &configstore.TrinoPool{
			PoolID: spec.PoolID, PublicID: spec.PublicID, APIMode: spec.APIMode,
			DesiredInstances: spec.DesiredInstances, MinServing: spec.MinServing,
			MaxSurge: spec.MaxSurge, MaxRepair: spec.MaxRepair,
			DesiredReleaseID: spec.DesiredReleaseID, DesiredBlueprintDigest: spec.DesiredBlueprintDigest,
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
	f.pool.DesiredBlueprintDigest = spec.DesiredBlueprintDigest
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
		RepairFor: spec.RepairFor,
		Phase:     string(spec.Phase), Repair: spec.Repair, EndpointURL: spec.EndpointURL,
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
	// The store stamps this on every transition; the operator's failure timers
	// measure from it, so a fake that left it alone would make a member look
	// like it had been in its new phase since whenever it entered the previous
	// one.
	instance.PhaseChangedAt = time.Now().UTC()
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
		case "coordinator_container_id":
			instance.CoordinatorContainerID = value.(string)
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
	members     map[string]*trinogateway.Member
	obligations map[string]trinogateway.Obligations
	backends    map[string]trinogateway.Backend
	principals  map[string][]string
	calls       []string
	drainErr    error
	sealErr     error
	admitErr    error
	lostErr     error
	membership  int64
	// lastLost is the loss claim the Gateway recorded, so a test can assert
	// WHICH evidence a repair rested on.
	lastLost     trinogateway.LostMemberRequest
	principalErr map[string]error
	publications map[string]*fakePublication
	admitted     map[string]string
	revoked      map[string]bool
	configured   *trinogateway.ConfigurePoolRequest

	// memberJournal mirrors the Gateway's request journal for the MEMBER
	// lifecycle steps, keyed by step identity. The recorded payload INCLUDES
	// the expected generation, because the Gateway hashes the whole request
	// body: a repeat under the same step id carrying a different generation is
	// POOL_INTENT_CHANGED, not a replay. It also records the response, so a
	// test can make an effect land while the caller sees a transport failure.
	//
	// The publication steps use `journal` + guardStep/recordStep below. They
	// are kept separate because they answer different questions: this one is
	// about resolving a lost response, that one about refusing a changed
	// intent.
	memberJournal map[string]fakeJournalEntry
	// loseResponse names step ids whose effect must land while the caller sees
	// a transport failure - the ambiguity every lifecycle retry has to survive.
	loseResponse map[string]bool

	journal     map[string]fakeStep
	principalOf map[string]string
	clock       int64
	// deferPublish holds the next publication for a tenant at the server: the
	// caller sees a lost response, and the effect commits when deliverDeferred
	// runs.
	deferPublish map[string]bool
	deferred     []func()
	deferRevoke  map[string]bool
	// intentConflicts counts refusals of a step identity that carries content
	// the journal did not record for it. Settling a request must never depend
	// on provoking one: it is an anomaly, not a protocol.
	intentConflicts int
	// minServing is the floor the Gateway itself enforces on open and commit,
	// taken from the pool configuration the operator publishes.
	minServing int64
}

type fakeJournalEntry struct {
	payload  string
	response trinogateway.Member
}

func newFakePoolGateway() *fakePoolGateway {
	return &fakePoolGateway{
		members:     map[string]*trinogateway.Member{},
		obligations: map[string]trinogateway.Obligations{},
		backends:    map[string]trinogateway.Backend{},
	}
}

// replay answers a repeated step from the journal, exactly as the Gateway does.
// A repeat with a DIFFERENT payload is refused: that is the failure mode a
// retry hits when it rebuilds its request from freshly read state instead of
// from what it recorded when it first formed the intent.
func (f *fakePoolGateway) replay(step trinogateway.Step, payload string) (trinogateway.Member, bool, error) {
	entry, recorded := f.memberJournal[step.OperationID+"/"+step.StepID]
	if !recorded {
		return trinogateway.Member{}, false, nil
	}
	if entry.payload != payload {
		return trinogateway.Member{}, true, fmt.Errorf("%w: recorded %s, received %s",
			trinogateway.ErrIntentChanged, entry.payload, payload)
	}
	return entry.response, true, nil
}

// commit records the outcome and then, when the test asked for it, hides the
// response from the caller.
func (f *fakePoolGateway) commit(step trinogateway.Step, payload string, member trinogateway.Member) (trinogateway.Member, error) {
	if f.memberJournal == nil {
		f.memberJournal = map[string]fakeJournalEntry{}
	}
	f.memberJournal[step.OperationID+"/"+step.StepID] = fakeJournalEntry{payload: payload, response: member}
	if f.loseResponse[step.StepID] {
		delete(f.loseResponse, step.StepID)
		return trinogateway.Member{}, errors.New("connection reset before the response was read")
	}
	return member, nil
}

// fakeRequestPayload mirrors what the Gateway actually hashes under a step
// identity: the WHOLE canonical request body, minus the authority envelope it
// strips (controllerEpoch, ownerIdentity) so a successor leader can resume a
// step its predecessor committed.
//
// Hashing a stand-in - the call name and the expected generation - modelled
// generation drift and nothing else, so a retry that rebuilt any OTHER field
// (a re-stamped observation time, a re-read boot identity, a reason chosen by
// whichever check fired first) looked like a clean replay here and was refused
// by the real Gateway.
func fakeRequestPayload(request any) string {
	encoded, err := json.Marshal(request)
	if err != nil {
		panic(fmt.Sprintf("marshal gateway request: %v", err))
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		panic(fmt.Sprintf("decode gateway request: %v", err))
	}
	delete(body, "controllerEpoch")
	delete(body, "ownerIdentity")
	canonical, err := json.Marshal(body)
	if err != nil {
		panic(fmt.Sprintf("canonicalize gateway request: %v", err))
	}
	return string(canonical)
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
	if err := f.principalErr[tenant]; err != nil {
		return trinogateway.TenantAdmission{}, err
	}
	if request.Revision == "" || len(request.Principals) == 0 {
		return trinogateway.TenantAdmission{}, fmt.Errorf("%w: a binding needs a revision and at least one principal", trinogateway.ErrValidation)
	}
	if f.deferPublish[tenant] {
		// The request REACHED the Gateway and is still executing there - behind
		// the pool row lock, say - while the caller's response is lost. Its
		// effect commits later, in arrival order at the Gateway, and is subject
		// to the journal at THAT moment rather than at call time. This is the
		// case an immediate retry of identical bytes does not cover.
		delete(f.deferPublish, tenant)
		captured := request
		f.deferred = append(f.deferred, func() { _, _ = f.applyPublish(tenant, captured) })
		return trinogateway.TenantAdmission{}, fmt.Errorf("%w: the response was lost", trinogateway.ErrUnavailable)
	}
	return f.applyPublish(tenant, request)
}

// deliverDeferred commits the effects that were left executing at the Gateway,
// in the order they arrived.
func (f *fakePoolGateway) deliverDeferred() {
	deferred := f.deferred
	f.deferred = nil
	for _, apply := range deferred {
		apply()
	}
}

func (f *fakePoolGateway) applyPublish(tenant string, request trinogateway.PublishPrincipalsRequest) (trinogateway.TenantAdmission, error) {
	intent := request.Revision + "|" + strings.Join(request.Principals, ",")
	replayed, err := f.guardStep(request.Step, intent)
	if err != nil {
		return trinogateway.TenantAdmission{}, err
	}
	if replayed {
		// PoolStore.inPool resolves the recorded step and applies NOTHING: the
		// principal rows keep whatever the earlier publication left there.
		return f.principalAdmission(tenant, request), nil
	}
	if f.principals == nil {
		f.principals = map[string][]string{}
	}
	if f.principalOf == nil {
		f.principalOf = map[string]string{}
	}
	// pool_tenant_principal is one flat namespace per pool: a principal already
	// bound to another tenant is a conflict, never an ambiguous admission.
	for _, principal := range request.Principals {
		if owner, bound := f.principalOf[principal]; bound && owner != tenant {
			return trinogateway.TenantAdmission{}, fmt.Errorf("%w: %s already belongs to %s",
				trinogateway.ErrPrincipalConflict, principal, owner)
		}
	}
	for _, principal := range f.principals[tenant] {
		delete(f.principalOf, principal)
	}
	for _, principal := range request.Principals {
		f.principalOf[principal] = tenant
	}
	f.principals[tenant] = request.Principals
	f.recordStep(request.Step, intent)
	return f.principalAdmission(tenant, request), nil
}

func (f *fakePoolGateway) principalAdmission(tenant string, request trinogateway.PublishPrincipalsRequest) trinogateway.TenantAdmission {
	return trinogateway.TenantAdmission{Tenant: tenant, State: "PENDING", PrincipalRevision: request.Revision}
}

func (f *fakePoolGateway) ConfigurePool(_ context.Context, poolID string, request trinogateway.ConfigurePoolRequest) (trinogateway.PoolState, error) {
	f.record("configure")
	f.configured = &request
	f.minServing = int64(request.MinServing)
	return trinogateway.PoolState{
		PoolID: poolID, ControllerEpoch: request.ControllerEpoch,
		APIMode: request.APIMode, MinServing: request.MinServing,
		DesiredMembers: request.DesiredMembers, MaxSurge: request.MaxSurge,
		MaxRepair: request.MaxRepair, DesiredRevision: request.DesiredRevision,
		TenantAdmissionEnabled: request.TenantAdmissionEnabled,
	}, nil
}

func (f *fakePoolGateway) RegisterMember(_ context.Context, poolID string, request trinogateway.RegisterMemberRequest) (trinogateway.Member, error) {
	f.record("register:" + request.InstanceID)
	payload := fakeRequestPayload(request)
	if replayed, done, err := f.replay(request.Step, payload); done {
		return replayed, err
	}
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
	return f.commit(request.Step, payload, *member)
}

func (f *fakePoolGateway) AdmitMember(_ context.Context, _, instanceID string, request trinogateway.AdmitMemberRequest) (trinogateway.Member, error) {
	f.record("admit:" + instanceID)
	if f.admitErr != nil {
		return trinogateway.Member{}, f.admitErr
	}
	payload := fakeRequestPayload(request)
	if replayed, done, err := f.replay(request.Step, payload); done {
		return replayed, err
	}
	member := f.members[instanceID]
	if err := requireGeneration(member, request.ExpectedGeneration); err != nil {
		return trinogateway.Member{}, err
	}
	if member == nil {
		return trinogateway.Member{}, fmt.Errorf("%w: %s", trinogateway.ErrNotFound, instanceID)
	}
	// PoolStore.admitMember: a member joining while ANY publication is open must
	// acknowledge that publication's target revision. A candidate registers
	// under its RELEASE id, so it never can - which is what makes an open
	// barrier block the pool's own compute lifecycle.
	if barrier := f.oldestOpenPublication(); barrier != nil {
		if request.Receipt.ConfigRevision != barrier.TargetRevision {
			return trinogateway.Member{}, fmt.Errorf(
				"%w: a member joining during a publication must acknowledge its target revision",
				trinogateway.ErrPublicationBarrier)
		}
		barrier.received[instanceID] = fakeReceipt{bootID: member.BootID, fingerprint: request.Receipt.CertificateHash}
	}
	member.Phase, member.Generation, member.Eligible = "ACTIVE", member.Generation+1, true
	// Admission changes the serving set, so the membership generation moves and
	// every open barrier's commit CAS now fails.
	f.membership++
	return f.commit(request.Step, payload, *member)
}

func (f *fakePoolGateway) GetMember(_ context.Context, _, instanceID string) (trinogateway.Member, error) {
	member, exists := f.members[instanceID]
	if !exists {
		// The real client maps the Gateway's POOL_NOT_FOUND to this sentinel,
		// and a read-back that resolves a lost response has to tell "nothing was
		// ever recorded" from "the Gateway could not be reached".
		return trinogateway.Member{}, fmt.Errorf("%w: %s", trinogateway.ErrNotFound, instanceID)
	}
	return *member, nil
}

func (f *fakePoolGateway) GetObligations(_ context.Context, _, instanceID string) (trinogateway.Obligations, error) {
	member, exists := f.members[instanceID]
	if !exists {
		return trinogateway.Obligations{}, trinogateway.ErrNotFound
	}
	obligations := f.obligations[instanceID]
	obligations.InstanceID, obligations.Incarnation = instanceID, member.Incarnation
	obligations.Phase, obligations.Generation = member.Phase, member.Generation
	obligations.ReadyToSeal = member.Phase == "DRAINING" && obligations.Outstanding() == 0
	obligations.Drained = member.Phase == "SEALED" ||
		((member.Phase == "RETIRING" || member.Phase == "RETIRED") && member.RetirementKind == "DRAINED")
	return obligations, nil
}

// requirePhase mirrors the Gateway's own phase preconditions. Without them a
// fake accepts transitions the real PoolStore refuses with POOL_PHASE, and the
// tests prove the operator can drive a protocol nobody implements.
func (f *fakePoolGateway) requirePhase(instanceID, call string, allowed ...string) (*trinogateway.Member, error) {
	member, known := f.members[instanceID]
	if !known {
		return nil, fmt.Errorf("%w: %s", trinogateway.ErrNotFound, instanceID)
	}
	if !slices.Contains(allowed, member.Phase) {
		return nil, fmt.Errorf("%w: a %s member cannot %s", trinogateway.ErrPhase, member.Phase, call)
	}
	return member, nil
}

// requireGeneration mirrors the member CAS. A step carrying a stale generation
// is refused, which is what makes a read-back before each step necessary rather
// than optional.
func requireGeneration(member *trinogateway.Member, expected int64) error {
	if member.Generation != expected {
		return fmt.Errorf("%w: member is at generation %d, step carries %d",
			trinogateway.ErrStaleGeneration, member.Generation, expected)
	}
	return nil
}

func (f *fakePoolGateway) DrainMember(_ context.Context, _, instanceID string, request trinogateway.MemberStepRequest) (trinogateway.Member, error) {
	f.record("drain:" + instanceID)
	if f.drainErr != nil {
		return trinogateway.Member{}, f.drainErr
	}
	payload := fakeRequestPayload(request)
	if replayed, done, err := f.replay(request.Step, payload); done {
		return replayed, err
	}
	// ACTIVE is the planned drain. SUSPECT is the extension agreed with the
	// Gateway for a member that is excluded but not provably dead: it is the
	// only way such a member can ever leave, since a loss claim needs evidence
	// a crash-looping pod never provides.
	member, err := f.requirePhase(instanceID, "be drained", "ACTIVE", "SUSPECT")
	if err != nil {
		return trinogateway.Member{}, err
	}
	if err := requireGeneration(member, request.ExpectedGeneration); err != nil {
		return trinogateway.Member{}, err
	}
	member.Phase, member.Generation = "DRAINING", member.Generation+1
	f.membership++
	return f.commit(request.Step, payload, *member)
}

func (f *fakePoolGateway) SealMember(_ context.Context, _, instanceID string, request trinogateway.MemberStepRequest) (trinogateway.Member, error) {
	f.record("seal:" + instanceID)
	payload := fakeRequestPayload(request)
	if replayed, done, err := f.replay(request.Step, payload); done {
		return replayed, err
	}
	member, err := f.requirePhase(instanceID, "be sealed", "DRAINING")
	if err != nil {
		return trinogateway.Member{}, err
	}
	if err := requireGeneration(member, request.ExpectedGeneration); err != nil {
		return trinogateway.Member{}, err
	}
	if f.sealErr != nil {
		return trinogateway.Member{}, f.sealErr
	}
	if f.obligations[instanceID].Outstanding() != 0 {
		return trinogateway.Member{}, trinogateway.ErrNotDrained
	}
	member.Phase, member.Generation = "SEALED", member.Generation+1
	// Obligations are reported with the member's CURRENT generation, so a step
	// that rebuilds its request from a fresh obligations read carries a
	// different generation than the one the journal recorded.
	obligations := f.obligations[instanceID]
	obligations.Generation = member.Generation
	f.obligations[instanceID] = obligations
	return f.commit(request.Step, payload, *member)
}

func (f *fakePoolGateway) SuspectMember(_ context.Context, _, instanceID string, request trinogateway.SuspectMemberRequest) (trinogateway.Member, error) {
	f.record("suspect:" + instanceID)
	if request.Reason == "" {
		return trinogateway.Member{}, errors.New("a suspicion must carry a reason")
	}
	payload := fakeRequestPayload(request)
	if replayed, done, err := f.replay(request.Step, payload); done {
		return replayed, err
	}
	member, err := f.requirePhase(instanceID, "become suspect", "PREPARING", "ACTIVE", "DRAINING", "SEALED")
	if err != nil {
		return trinogateway.Member{}, err
	}
	if err := requireGeneration(member, request.ExpectedGeneration); err != nil {
		return trinogateway.Member{}, err
	}
	wasActive := member.Phase == "ACTIVE"
	member.Phase, member.Generation = "SUSPECT", member.Generation+1
	if wasActive {
		f.membership++
	}
	return f.commit(request.Step, payload, *member)
}

func (f *fakePoolGateway) LostMember(_ context.Context, _, instanceID string, request trinogateway.LostMemberRequest) (trinogateway.Member, error) {
	f.record("lost:" + instanceID)
	if f.lostErr != nil {
		return trinogateway.Member{}, f.lostErr
	}
	if request.Evidence == "" || request.Termination.Source == "" {
		return trinogateway.Member{}, errors.New("a loss claim needs termination evidence")
	}
	payload := fakeRequestPayload(request)
	if replayed, done, err := f.replay(request.Step, payload); done {
		return replayed, err
	}
	member, err := f.requirePhase(instanceID, "be declared lost", "SUSPECT")
	if err != nil {
		return trinogateway.Member{}, err
	}
	if err := requireGeneration(member, request.ExpectedGeneration); err != nil {
		return trinogateway.Member{}, err
	}
	// The evidence must identify the exact incarnation the Gateway recorded.
	if request.Termination.PodUID != member.PodUID || request.Termination.BootID != member.BootID ||
		request.Termination.NodeID != member.NodeID || request.Termination.CoordinatorID != member.CoordinatorID {
		return trinogateway.Member{}, fmt.Errorf("%w: termination evidence does not identify this incarnation",
			trinogateway.ErrEvidenceRequired)
	}
	member.Phase, member.Generation, member.RetirementKind = "LOST", member.Generation+1, "FAILED"
	f.lastLost = request
	return f.commit(request.Step, payload, *member)
}

func (f *fakePoolGateway) RetireMember(_ context.Context, _, instanceID string, request trinogateway.MemberStepRequest) (trinogateway.Member, error) {
	f.record("retire:" + instanceID)
	payload := fakeRequestPayload(request)
	if replayed, done, err := f.replay(request.Step, payload); done {
		return replayed, err
	}
	// SEALED is a completed drain, LOST a proven failure, and PREPARING a
	// candidate that never admitted work. Nothing else may claim retirement.
	member, err := f.requirePhase(instanceID, "be retired", "SEALED", "LOST", "PREPARING")
	if err != nil {
		return trinogateway.Member{}, err
	}
	if err := requireGeneration(member, request.ExpectedGeneration); err != nil {
		return trinogateway.Member{}, err
	}
	kind := "DRAINED"
	if member.Phase == "LOST" {
		kind = "FAILED"
	}
	member.Phase, member.Generation, member.RetirementKind = "RETIRING", member.Generation+1, kind
	return f.commit(request.Step, payload, *member)
}

func (f *fakePoolGateway) MemberRetired(_ context.Context, _, instanceID string, request trinogateway.MemberStepRequest) (trinogateway.Member, error) {
	f.record("retired:" + instanceID)
	if !request.ResourcesAbsent {
		return trinogateway.Member{}, errors.New("retirement reported without asserting absence")
	}
	payload := fakeRequestPayload(request)
	if replayed, done, err := f.replay(request.Step, payload); done {
		return replayed, err
	}
	member, err := f.requirePhase(instanceID, "complete retirement", "RETIRING")
	if err != nil {
		return trinogateway.Member{}, err
	}
	if err := requireGeneration(member, request.ExpectedGeneration); err != nil {
		return trinogateway.Member{}, err
	}
	member.Phase, member.Generation = "RETIRED", member.Generation+1
	return f.commit(request.Step, payload, *member)
}

type fakePoolKube struct {
	applied  map[string]trinoPoolInventory
	deleted  map[string]bool
	observed trinoPoolObservation
	absent   bool
	// absentAfterDelete makes deletion actually remove the objects, which is
	// what lets a test drive a teardown to its end rather than asserting only
	// the step that starts it.
	absentAfterDelete bool
	epochsSeen        []int64
}

func newFakePoolKube() *fakePoolKube {
	return &fakePoolKube{
		applied: map[string]trinoPoolInventory{},
		deleted: map[string]bool{},
		observed: trinoPoolObservation{
			CoordinatorReady: true, ReadyWorkers: 4, DesiredWorkers: 4,
			CoordinatorPodUID: "pod-uid-1", PodsPresent: 5,
			// A real Observe always reports the coordinator pod it read the
			// readiness from, including which container instance is running -
			// that is what registration records so a later termination record
			// can be correlated with the admitted process.
			CoordinatorPods: []trinoPoolCoordinatorPod{
				{UID: "pod-uid-1", RunningContainerID: "containerd://admitted"},
			},
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

func (f *fakePoolKube) ResourcesAbsent(_ context.Context, inventory trinoPoolInventory) (bool, error) {
	if f.absentAfterDelete && f.deleted[inventory.ServiceName] {
		return true, nil
	}
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

// placeInstance puts an instance into a phase on BOTH sides. The Gateway
// enforces its own phase preconditions, so a test that moved only the local row
// would be driving a protocol the real Gateway refuses.
func (h *operatorHarness) placeInstance(t *testing.T, instanceID string, local trinopool.Phase, gatewayPhase string) *configstore.TrinoPoolInstance {
	t.Helper()
	instance, known := h.store.instances[instanceID]
	if !known {
		t.Fatalf("instance %s does not exist", instanceID)
	}
	instance.Phase = string(local)
	member, known := h.gateway.members[instanceID]
	if !known {
		t.Fatalf("instance %s has no gateway member", instanceID)
	}
	member.Phase = gatewayPhase
	instance.GatewayGeneration = member.Generation
	return instance
}

// tickTolerant runs ticks that are EXPECTED to fail, which is what a refusing
// Gateway produces. The reconcile loop logs and carries on; these tests assert
// what was recorded while it did.
func (h *operatorHarness) tickTolerant(times int) {
	for index := 0; index < times; index++ {
		_ = h.operator.reconcileOnce(context.Background())
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
	harness.operator.config.Spec.DesiredBlueprintDigest = harness.operator.config.Blueprint.Digest()

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
	harness.operator.config.Spec.DesiredBlueprintDigest = harness.operator.config.Blueprint.Digest()
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
	instance := harness.placeInstance(t, instanceID, trinopool.PhaseSealed, "SEALED")

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
	instance := harness.placeInstance(t, instanceID, trinopool.PhaseDraining, "DRAINING")
	generation := harness.gateway.members[instanceID].Generation
	harness.gateway.obligations[instanceID] = trinogateway.Obligations{
		Generation: generation, OpenTransactions: 1,
	}

	harness.tick(t, 3)
	if instance.Phase != string(trinopool.PhaseDraining) {
		t.Fatalf("phase = %s, want the instance to stay DRAINING", instance.Phase)
	}
	if countCalls(harness.gateway.calls, "seal:") != 0 {
		t.Fatal("a member with an open transaction was sealed")
	}

	harness.gateway.obligations[instanceID] = trinogateway.Obligations{Generation: generation}
	harness.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseSealed) {
		t.Fatalf("phase = %s, want SEALED once drained", instance.Phase)
	}
}

// A lost response must not change the request the retry sends.
//
// The Gateway journals each step under its identity and hashes the whole
// request, expected generation included. So a step whose effect LANDED while
// its response was lost can only be resolved by repeating the identical
// request: rebuilding it from freshly read state sends the generation the
// committed effect produced, the journal reports a changed intent, and that
// member can never finish the transition - not on the next tick, not after a
// leader change, never.
func TestALostResponseDoesNotChangeTheRetriedRequest(t *testing.T) {
	t.Run("admit", func(t *testing.T) {
		harness := newOperatorHarness(t)
		harness.tick(t, 2)
		instanceID := harness.store.order[0]
		harness.gateway.loseResponse = map[string]bool{"admit": true}

		harness.tickTolerant(20)

		instance := harness.store.instances[instanceID]
		if instance.Phase != string(trinopool.PhaseServing) {
			t.Fatalf("phase = %s, want the admission to be resolved from the journal", instance.Phase)
		}
		if member := harness.gateway.members[instanceID]; member.Phase != "ACTIVE" {
			t.Fatalf("gateway phase = %s, want the admitted member to stay ACTIVE", member.Phase)
		}
	})

	t.Run("seal", func(t *testing.T) {
		harness := newOperatorHarness(t)
		harness.tick(t, 20)

		instanceID := harness.store.order[0]
		instance := harness.placeInstance(t, instanceID, trinopool.PhaseDraining, "DRAINING")
		harness.gateway.loseResponse = map[string]bool{"seal": true}

		harness.tickTolerant(5)

		if instance.Phase != string(trinopool.PhaseSealed) && instance.Phase != string(trinopool.PhaseRetiring) {
			t.Fatalf("phase = %s, want the seal to be resolved from the journal", instance.Phase)
		}
	})
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

func TestTrinoServiceCredentialAuthPreservesPublishedPrincipals(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.tick(t, 1)
	initial := strings.Join(harness.gateway.principals["org-a"], ",")
	t.Setenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE", "/synthetic/service-auth-token")
	harness.tick(t, 1)
	if got := strings.Join(harness.gateway.principals["org-a"], ","); got != initial {
		t.Fatalf("service authentication changed published principals: before=%s after=%s", initial, got)
	}
	t.Setenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE", "")
	harness.tick(t, 1)
	if got := strings.Join(harness.gateway.principals["org-a"], ","); got != initial {
		t.Fatalf("authentication rollback changed published principals: before=%s after=%s", initial, got)
	}
}

func TestTrinoServiceCredentialAuthAcceptsExistingGatewayReply(t *testing.T) {
	t.Setenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE", "/synthetic/service-auth-token")
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.tick(t, 1)
	row := harness.publications.rows["org-a"]
	if row == nil || row.PrincipalRevision == "" {
		t.Fatal("existing Gateway principal publication reply was not checkpointed")
	}
}

func TestTrinoServiceCredentialAuthDoesNotRepublishBindings(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.tick(t, 3)
	initial := countCalls(harness.gateway.calls, "principals:")
	revision := harness.publications.rows["org-a"].PrincipalRevision
	t.Setenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE", "/synthetic/service-auth-token")
	harness.tick(t, 3)
	t.Setenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE", "")
	harness.tick(t, 3)
	if got := countCalls(harness.gateway.calls, "principals:"); got != initial {
		t.Fatalf("service authentication toggles republished bindings: before=%d after=%d", initial, got)
	}
	if got := harness.publications.rows["org-a"].PrincipalRevision; got != revision {
		t.Fatalf("service authentication changed binding revision: before=%s after=%s", revision, got)
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

// A suspected member that starts looking healthy again is NOT returned to
// service locally.
//
// Suspicion is the Gateway's state as much as this row's: it excluded the
// member and only a fresh certified admission un-excludes it. Flipping the
// local row back to SERVING would leave duckgres believing a member serves
// while the Gateway routes nothing to it - the precise divergence the phase
// machine exists to prevent. The member leaves through the planned drain
// instead, and its replacement is certified from scratch.
func TestRecoveredSuspectIsNotReturnedToServiceLocally(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.tick(t, 20)

	instanceID := harness.store.order[0]
	instance := harness.placeInstance(t, instanceID, trinopool.PhaseSuspect, "SUSPECT")

	// Healthy again, as far as Kubernetes is concerned.
	harness.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseSuspect) {
		t.Fatalf("phase = %s, want the member to stay SUSPECT rather than be re-admitted locally", instance.Phase)
	}
	if harness.gateway.members[instanceID].Phase != "SUSPECT" {
		t.Fatal("the gateway member changed phase without an admission")
	}

	// The phase machine itself refuses the transition, so no future path can
	// reintroduce it by accident.
	if err := trinopool.ValidateTransition(trinopool.PhaseSuspect, trinopool.PhaseServing); err == nil {
		t.Fatal("SUSPECT -> SERVING is permitted; a local recovery would diverge from the Gateway")
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

func (f *fakeOperationStore) FinishTrinoPoolOperation(_ context.Context, _ configstore.TrinoPoolLease, operationID, phase, lastError string) error {
	operation, known := f.operations[operationID]
	if !known {
		return configstore.ErrTrinoPoolConflict
	}
	now := time.Now().UTC()
	operation.Phase, operation.LastError, operation.TerminalAt = phase, lastError, &now
	f.operations[operationID] = operation
	return nil
}

func (f *fakeOperationStore) UpdateTrinoPoolOperation(_ context.Context, lease configstore.TrinoPoolLease, operationID string, updates map[string]any) error {
	if lease.Epoch != f.epoch() {
		return configstore.ErrTrinoPoolConflict
	}
	operation, known := f.operations[operationID]
	if !known || operation.TerminalAt != nil {
		return configstore.ErrTrinoPoolConflict
	}
	if attempts, ok := updates["attempts"].(int64); ok {
		operation.Attempts = attempts
	}
	if next, ok := updates["next_attempt_at"].(time.Time); ok {
		operation.NextAttemptAt = &next
	}
	if lastError, ok := updates["last_error"].(string); ok {
		operation.LastError = lastError
	}
	f.operations[operationID] = operation
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
	// The Gateway's own never-admitted retirement is what releases the slot: a
	// PREPARING member that admitted no work may be retired directly, and the
	// Gateway verifies that rather than taking this controller's word for it.
	if member, _ := harness.gateway.GetMember(context.Background(), "cell-001", instanceID); member.Phase != "RETIRED" {
		t.Fatalf("gateway member phase = %s, want RETIRED so the live slot is released", member.Phase)
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
// Publication barrier fake.
//
// This models the rules PoolStore actually enforces, because the operator's
// decisions are only correct against those rules and a permissive fake proves
// the operator can drive a protocol nobody implements. Specifically:
//
//   - the operation/step JOURNAL: one row per (operationId, stepId); an
//     identical body resolves to the recorded result and APPLIES NOTHING, a
//     different body under the same identity is POOL_INTENT_CHANGED forever,
//     and a step is recorded only when the effect succeeded;
//   - the membership generation CAS on open and commit, and the serving floor;
//   - one open publication per tenant, and an immutable plan per publication id;
//   - receipts bound to the member's exact (podUid, bootId) and to the
//     publication's target revision, accepted only while it is OPEN;
//   - missing members computed from the CURRENT active membership, not from the
//     list frozen when the barrier opened;
//   - a joining member must acknowledge the open publication's target revision,
//     which is what makes an open barrier block admission;
//   - revocation keeps the tenant's principal rows (PoolStore only rewrites the
//     admission row and abandons that tenant's open publications).
// ---------------------------------------------------------------------------

type fakePublication struct {
	trinogateway.Publication
	received map[string]fakeReceipt
	opened   int64
}

// fakeReceipt is one row of pool_publication_receipt: what a member said it was
// serving, bound to the process that said it.
type fakeReceipt struct {
	bootID      string
	fingerprint string
}

// fakeStep is one row of the Gateway's pool_operation journal.
type fakeStep struct {
	payload string
}

// guardStep mirrors PoolStore.inPool's replay resolution. It returns true when
// the step was already recorded, in which case the caller must apply nothing.
func (f *fakePoolGateway) guardStep(step trinogateway.Step, intent string) (bool, error) {
	if f.journal == nil {
		f.journal = map[string]fakeStep{}
	}
	if step.OperationID == "" || step.StepID == "" {
		return false, fmt.Errorf("%w: a step needs an operation and a step id", trinogateway.ErrValidation)
	}
	if len(step.StepID) > 64 {
		// pool_operation.step_id is VARCHAR(64).
		return false, fmt.Errorf("%w: step id %q exceeds 64 characters", trinogateway.ErrValidation, step.StepID)
	}
	recorded, found := f.journal[step.OperationID+"\x00"+step.StepID]
	if !found {
		return false, nil
	}
	if recorded.payload != intent {
		f.intentConflicts++
		return false, fmt.Errorf("%w: step %s of %s was recorded with a different intent",
			trinogateway.ErrIntentChanged, step.StepID, step.OperationID)
	}
	return true, nil
}

// recordStep is the journal write PoolStore performs after a successful effect.
func (f *fakePoolGateway) recordStep(step trinogateway.Step, intent string) {
	f.journal[step.OperationID+"\x00"+step.StepID] = fakeStep{payload: intent}
}

// oldestOpenPublication is PoolStore.openPublicationRow: the pool's oldest OPEN
// publication, which is the one a joining member is measured against.
func (f *fakePoolGateway) oldestOpenPublication() *fakePublication {
	var oldest *fakePublication
	for _, publication := range f.publications {
		if publication.Phase != "OPEN" {
			continue
		}
		if oldest == nil || publication.opened < oldest.opened {
			oldest = publication
		}
	}
	return oldest
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
	intent := strings.Join([]string{request.PublicationID, request.Tenant, request.TargetRevision,
		request.PayloadHash, fmt.Sprint(request.ExpectedMembershipGeneration)}, "|")
	replayed, err := f.guardStep(request.Step, intent)
	if err != nil {
		return trinogateway.Publication{}, err
	}
	if replayed {
		return f.publicationView(f.publications[request.PublicationID]), nil
	}
	if request.ExpectedMembershipGeneration != f.membership {
		return trinogateway.Publication{}, fmt.Errorf("%w: the pool membership generation changed", trinogateway.ErrMembershipChanged)
	}
	active := f.activeInstanceIDs()
	if int64(len(active)) < f.minServing {
		return trinogateway.Publication{}, fmt.Errorf("%w: a publication requires the minimum serving membership", trinogateway.ErrServingFloor)
	}
	if existing, found := f.publications[request.PublicationID]; found {
		// The plan of a publication identity is immutable.
		if existing.Tenant != request.Tenant || existing.TargetRevision != request.TargetRevision {
			return trinogateway.Publication{}, fmt.Errorf("%w: this publication identity has a different plan", trinogateway.ErrIntentChanged)
		}
		f.recordStep(request.Step, intent)
		return f.publicationView(existing), nil
	}
	for _, publication := range f.publications {
		if publication.Tenant == request.Tenant && publication.Phase == "OPEN" {
			return trinogateway.Publication{}, fmt.Errorf("%w: this tenant already has an open publication", trinogateway.ErrPublicationBarrier)
		}
	}
	f.clock++
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
		received: map[string]fakeReceipt{},
		opened:   f.clock,
	}
	f.publications[request.PublicationID] = publication
	f.recordStep(request.Step, intent)
	return f.publicationView(publication), nil
}

func (f *fakePoolGateway) AbandonPublication(_ context.Context, _, publicationID string, step trinogateway.Step) (trinogateway.Publication, error) {
	f.record("abandon:" + publicationID)
	publication, found := f.publications[publicationID]
	if !found {
		return trinogateway.Publication{}, fmt.Errorf("%w: %s", trinogateway.ErrNotFound, publicationID)
	}
	replayed, err := f.guardStep(step, "abandon|"+publicationID)
	if err != nil {
		return trinogateway.Publication{}, err
	}
	if replayed {
		return f.publicationView(publication), nil
	}
	// An admitted gate is never retracted by abandoning it: the Gateway refuses,
	// and the caller reads it back as ADMITTED.
	if publication.Phase == "ADMITTED" {
		return trinogateway.Publication{}, fmt.Errorf("%w: a committed publication cannot be abandoned", trinogateway.ErrIrreversible)
	}
	publication.Phase = "ABANDONED"
	f.recordStep(step, "abandon|"+publicationID)
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
	intent := strings.Join([]string{publicationID, request.InstanceID, request.PodUID,
		request.BootID, request.AppliedRevision, request.AuthFingerprint}, "|")
	replayed, err := f.guardStep(request.Step, intent)
	if err != nil {
		return trinogateway.Publication{}, err
	}
	if replayed {
		return f.publicationView(publication), nil
	}
	if publication.Phase != "OPEN" {
		return trinogateway.Publication{}, fmt.Errorf("%w: only an open publication accepts receipts", trinogateway.ErrPhase)
	}
	if request.AppliedRevision != publication.TargetRevision {
		return trinogateway.Publication{}, fmt.Errorf("%w: applied revision does not match the target", trinogateway.ErrPublicationBarrier)
	}
	member := f.members[request.InstanceID]
	if member == nil || member.BootID != request.BootID || member.PodUID != request.PodUID {
		return trinogateway.Publication{}, fmt.Errorf("%w: the acknowledgement does not identify this member's process", trinogateway.ErrPublicationBarrier)
	}
	if member.Phase != "ACTIVE" && member.Phase != "PREPARING" {
		return trinogateway.Publication{}, fmt.Errorf("%w: a %s member cannot acknowledge a publication", trinogateway.ErrPhase, member.Phase)
	}
	publication.received[request.InstanceID] = fakeReceipt{bootID: request.BootID, fingerprint: request.AuthFingerprint}
	f.recordStep(request.Step, intent)
	return f.publicationView(publication), nil
}

func (f *fakePoolGateway) CommitPublication(_ context.Context, _, publicationID string, request trinogateway.CommitPublicationRequest) (trinogateway.Publication, error) {
	f.record("commit:" + publicationID)
	publication, found := f.publications[publicationID]
	if !found {
		return trinogateway.Publication{}, fmt.Errorf("%w: %s", trinogateway.ErrNotFound, publicationID)
	}
	intent := fmt.Sprintf("commit|%s|%d", publicationID, request.ExpectedMembershipGeneration)
	replayed, err := f.guardStep(request.Step, intent)
	if err != nil {
		return trinogateway.Publication{}, err
	}
	if replayed {
		return f.publicationView(publication), nil
	}
	if publication.Phase == "ADMITTED" {
		return f.publicationView(publication), nil
	}
	if publication.Phase != "OPEN" {
		return trinogateway.Publication{}, fmt.Errorf("%w: an abandoned publication cannot be committed", trinogateway.ErrPhase)
	}
	// Both generations are checked, exactly as PoolStore.commitPublication does:
	// the caller's view of the pool AND the membership this barrier was opened
	// against.
	if f.membership != request.ExpectedMembershipGeneration || publication.MembershipGeneration != request.ExpectedMembershipGeneration {
		return trinogateway.Publication{}, fmt.Errorf("%w: the membership generation changed during the publication", trinogateway.ErrMembershipChanged)
	}
	if int64(len(f.activeInstanceIDs())) < f.minServing {
		return trinogateway.Publication{}, fmt.Errorf("%w: the admitting membership fell below the minimum serving count", trinogateway.ErrServingFloor)
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
	// pool_tenant_admission holds ONE state per tenant: a commit moves a
	// revoked tenant back to ADMITTED.
	delete(f.revoked, publication.Tenant)
	f.recordStep(request.Step, intent)
	return f.publicationView(publication), nil
}

func (f *fakePoolGateway) RevokeTenant(_ context.Context, _, tenant string, request trinogateway.RevokeTenantRequest) (trinogateway.TenantAdmission, error) {
	f.record("revoke:" + tenant)
	if request.Reason == "" {
		return trinogateway.TenantAdmission{}, fmt.Errorf("%w: a revocation must carry a reason", trinogateway.ErrValidation)
	}
	if f.deferRevoke[tenant] {
		delete(f.deferRevoke, tenant)
		captured := request
		f.deferred = append(f.deferred, func() { _, _ = f.applyRevoke(tenant, captured) })
		return trinogateway.TenantAdmission{}, fmt.Errorf("%w: the response was lost", trinogateway.ErrUnavailable)
	}
	return f.applyRevoke(tenant, request)
}

func (f *fakePoolGateway) applyRevoke(tenant string, request trinogateway.RevokeTenantRequest) (trinogateway.TenantAdmission, error) {
	intent := "revoke|" + tenant + "|" + request.Reason
	replayed, err := f.guardStep(request.Step, intent)
	if err != nil {
		return trinogateway.TenantAdmission{}, err
	}
	if replayed {
		return trinogateway.TenantAdmission{Tenant: tenant, State: "REVOKED"}, nil
	}
	delete(f.admitted, tenant)
	// PoolStore.revokeTenant rewrites the ADMISSION row and abandons the
	// tenant's open publications. It does NOT delete pool_tenant_principal, so
	// the binding survives a revocation - which is precisely why re-publishing
	// an identical set after one is invisible unless it carries a new occurrence.
	for _, publication := range f.publications {
		if publication.Tenant == tenant && publication.Phase == "OPEN" {
			publication.Phase = "ABANDONED"
		}
	}
	if f.revoked == nil {
		f.revoked = map[string]bool{}
	}
	f.revoked[tenant] = true
	f.recordStep(request.Step, intent)
	return trinogateway.TenantAdmission{Tenant: tenant, State: "REVOKED"}, nil
}

// publicationView mirrors PoolStore.publication: requiredMembers is the list
// frozen when the barrier opened, but missingMembers is recomputed against the
// CURRENT active membership and each receipt's recorded process identity.
func (f *fakePoolGateway) publicationView(publication *fakePublication) trinogateway.Publication {
	view := publication.Publication
	view.Receipts = nil
	view.MissingMembers = nil
	for _, instanceID := range f.activeInstanceIDs() {
		member := f.members[instanceID]
		if receipt, acknowledged := publication.received[instanceID]; acknowledged && receipt.bootID == member.BootID {
			view.Receipts = append(view.Receipts, trinogateway.PublicationReceipt{
				InstanceID: instanceID, BootID: receipt.bootID, AppliedRevision: publication.TargetRevision,
				AuthFingerprint: receipt.fingerprint,
			})
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
	// failPrincipalCheckpoint models the half this control plane owns failing
	// on its own: the Gateway answered, the local write did not land.
	failPrincipalCheckpoint bool
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
	if f.failPrincipalCheckpoint {
		return errors.New("the checkpoint did not land")
	}
	row := f.row(poolID, orgID)
	row.PrincipalRevision = revision
	if row.State == configstore.TrinoPublicationRevoked || row.State == "" {
		row.State = configstore.TrinoPublicationPublished
	}
	// The Gateway answered: the occurrence is spent.
	row.PendingIntent, row.PendingPayload = "", "{}"
	return nil
}

// RecordTrinoPoolPublicationOpen mirrors the real statement: it records the
// LIVE barrier, and moves the state only for a tenant that has never been
// admitted. A tenant that is admitted today stays admitted while the barrier
// for its newest login runs.
func (f *fakePublicationStore) RecordTrinoPoolPublicationOpen(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID, publicationID, target string) error {
	row := f.row(poolID, orgID)
	row.PublicationID, row.TargetRevision = publicationID, target
	if row.AdmittedTargetRevision == "" {
		row.State = configstore.TrinoPublicationAdmitting
	}
	return nil
}

func (f *fakePublicationStore) RecordTrinoPoolPublicationCommitted(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID, target, receipt string) error {
	row := f.row(poolID, orgID)
	row.AdmittedTargetRevision, row.GatewayReceipt = target, receipt
	row.State = configstore.TrinoPublicationAdmitted
	// The barrier is finished; its outcome lives in the admitted revision.
	row.PublicationID, row.TargetRevision, row.LastError = "", "", ""
	return nil
}

// ClearTrinoPoolPublicationBarrier mirrors the durable half of abandoning an
// attempt: the barrier pointer goes, the tenant's admission does not.
func (f *fakePublicationStore) ClearTrinoPoolPublicationBarrier(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID string) error {
	row := f.row(poolID, orgID)
	row.PublicationID, row.TargetRevision = "", ""
	switch {
	case row.AdmittedTargetRevision != "":
	case row.PrincipalRevision != "":
		row.State = configstore.TrinoPublicationPublished
	default:
		row.State = configstore.TrinoPublicationPending
	}
	return nil
}

func (f *fakePublicationStore) RecordTrinoPoolTenantRevoked(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID, reason string) error {
	row := f.row(poolID, orgID)
	row.State, row.LastError = configstore.TrinoPublicationRevoked, reason
	row.AdmittedTargetRevision, row.TargetRevision, row.PublicationID = "", "", ""
	row.PendingIntent, row.PendingPayload = "", "{}"
	return nil
}

func (f *fakePublicationStore) BeginTrinoPoolPublicationAttempt(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID string) (int64, error) {
	row := f.row(poolID, orgID)
	row.Attempt++
	return row.Attempt, nil
}

func (f *fakePublicationStore) BeginTrinoPoolPublicationIntent(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID, kind, payload string) (int64, error) {
	if payload == "" {
		return 0, errors.New("a publication intent requires its request body")
	}
	row := f.row(poolID, orgID)
	row.Attempt++
	row.PendingIntent = kind
	row.PendingPayload = payload
	return row.Attempt, nil
}

func (f *fakePublicationStore) ResolveTrinoPoolPublicationIntent(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID string) error {
	row := f.row(poolID, orgID)
	row.PendingIntent, row.PendingPayload = "", "{}"
	return nil
}

func (f *fakePublicationStore) RecordTrinoPoolPublicationFailure(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID string, nextAttemptAt time.Time, lastError string) error {
	row := f.row(poolID, orgID)
	row.Attempts++
	next := nextAttemptAt
	row.NextAttemptAt, row.LastError = &next, lastError
	return nil
}

func (f *fakePublicationStore) ClearTrinoPoolPublicationFailure(_ context.Context, _ configstore.TrinoPoolLease, poolID, orgID string) error {
	row := f.row(poolID, orgID)
	row.Attempts, row.NextAttemptAt, row.LastError = 0, nil, ""
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

// A failing external call earns a wait, and the wait is DURABLE. Keeping it in
// the leader's memory would let a restart - or a leadership move - retry
// immediately, turning a persistent failure into a hot loop against the
// Gateway. A successful one closes its operation, so the table does not grow
// without bound and work in flight stays distinguishable from work that ended.
func TestFailedStepEarnsADurableWaitAndSuccessClosesTheOperation(t *testing.T) {
	harness := newOperatorHarness(t)
	operations := newFakeOperationStore(func() int64 { return harness.store.epoch })
	harness.operator.operations = operations

	refusal := &trinogateway.Error{Code: "POOL_NOT_CERTIFIED", Status: 409}
	harness.gateway.admitErr = refusal
	harness.tickTolerant(6)

	operation, known := operations.operations["instance:"+harness.store.order[0]]
	if !known {
		t.Fatalf("no operation was recorded: %v", operations.operations)
	}
	if operation.Attempts == 0 || operation.NextAttemptAt == nil {
		t.Fatalf("operation = %+v, want a recorded attempt and a next attempt time", operation)
	}
	if !operation.NextAttemptAt.After(time.Now().UTC()) {
		t.Fatalf("next attempt %s is not in the future", operation.NextAttemptAt)
	}
	if operation.TerminalAt != nil {
		t.Fatal("a failed attempt closed the operation; it must stay open to be retried")
	}

	// The wait is honoured rather than retried on the next tick. The count is
	// per INSTANCE: the backoff is recorded against this instance's operation,
	// and its siblings keep making their own attempts - one member's failure
	// does not stop the rest of the pool.
	waiting := "admit:" + harness.store.order[0]
	attempts := countGatewayCalls(harness.gateway.calls, waiting)
	harness.tickTolerant(3)
	if countGatewayCalls(harness.gateway.calls, waiting) != attempts {
		t.Fatalf("the admission was retried during its recorded wait: %v", harness.gateway.calls)
	}

	// Once the wait elapses and the call succeeds, the operation is closed.
	harness.gateway.admitErr = nil
	past := time.Now().UTC().Add(-time.Minute)
	stored := operations.operations["instance:"+harness.store.order[0]]
	stored.NextAttemptAt = &past
	operations.operations["instance:"+harness.store.order[0]] = stored

	harness.tickTolerant(3)
	closed := operations.operations["instance:"+harness.store.order[0]]
	if closed.TerminalAt == nil || closed.Phase != "completed" {
		t.Fatalf("operation = %+v, want a completed, terminal operation", closed)
	}
}

func countGatewayCalls(calls []string, prefix string) int {
	count := 0
	for _, call := range calls {
		if strings.HasPrefix(call, prefix) {
			count++
		}
	}
	return count
}

// SUSPECT used to have exactly one exit that freed the member's slot: LOST,
// which requires verified absence of every recorded object. A crash-looping
// coordinator keeps its Deployment forever, so it kept its slot forever, and a
// second such failure exhausted the repair budget and stalled the pool. A
// member that cannot be proven dead leaves through the planned drain instead.
func TestSuspectMemberThatCannotBeProvenDeadIsDrained(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.servingPool(t)
	instanceID := harness.store.order[0]

	// The coordinator is crash-looping: unhealthy, but its objects are present,
	// so nothing can claim it terminated.
	harness.kube.observed.CoordinatorPodUID = "pod-uid-replaced"
	harness.kube.absent = false
	harness.store.instances[instanceID].PhaseChangedAt = time.Now().Add(-trinoPoolSuspectAfter - time.Minute)
	harness.tick(t, 1)
	if phase := harness.store.instances[instanceID].Phase; phase != string(trinopool.PhaseSuspect) {
		t.Fatalf("instance phase = %s, want SUSPECT", phase)
	}

	// It is not dropped the moment it is suspected: suspicion is reversible.
	harness.tick(t, 1)
	if phase := harness.store.instances[instanceID].Phase; phase != string(trinopool.PhaseSuspect) {
		t.Fatalf("instance phase = %s, want it to stay SUSPECT while the grace lasts", phase)
	}

	harness.store.instances[instanceID].PhaseChangedAt = time.Now().Add(-trinoPoolSuspectDrainAfter - time.Minute)
	harness.tick(t, 1)
	if phase := harness.store.instances[instanceID].Phase; phase != string(trinopool.PhaseDraining) {
		t.Fatalf("instance phase = %s, want DRAINING so the slot can be released", phase)
	}
	for _, call := range harness.gateway.calls {
		if call == "lost:"+instanceID {
			t.Fatal("a loss was claimed for a member whose objects were still present")
		}
	}
}

// The Gateway's serving-floor refusal is authoritative even here: a pool at its
// floor keeps a flaky member rather than dropping below it.
func TestSuspectDrainRespectsTheServingFloor(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.servingPool(t)
	instanceID := harness.store.order[0]
	harness.kube.observed.CoordinatorPodUID = "pod-uid-replaced"
	harness.kube.absent = false
	harness.store.instances[instanceID].PhaseChangedAt = time.Now().Add(-trinoPoolSuspectAfter - time.Minute)
	harness.tick(t, 1)

	harness.gateway.drainErr = &trinogateway.Error{Code: "POOL_SERVING_FLOOR", Status: 409}
	harness.store.instances[instanceID].PhaseChangedAt = time.Now().Add(-trinoPoolSuspectDrainAfter - time.Minute)
	harness.tickTolerant(2)

	if phase := harness.store.instances[instanceID].Phase; phase != string(trinopool.PhaseSuspect) {
		t.Fatalf("instance phase = %s, want it to stay SUSPECT after a refused drain", phase)
	}
}

// poolOrgs builds a fleet of tenants, each with its own root login.
func poolOrgs(count int) []configstore.TrinoEnabledOrg {
	orgs := make([]configstore.TrinoEnabledOrg, 0, count)
	for i := 0; i < count; i++ {
		orgs = append(orgs, configstore.TrinoEnabledOrg{
			OrgID:            fmt.Sprintf("org-%04d", i),
			DatabaseName:     fmt.Sprintf("acme%04d", i),
			CellID:           "registered:cell-001",
			RootPasswordHash: "hash",
		})
	}
	return orgs
}

// admitAll drives the barrier until every tenant is admitted, or gives up.
func (h *operatorHarness) admitAll(t *testing.T, tenants int) {
	t.Helper()
	for tick := 0; tick < tenants*8+64; tick++ {
		h.tickTolerant(1)
		admitted := 0
		for _, row := range h.publications.rows {
			if row.State == configstore.TrinoPublicationAdmitted {
				admitted++
			}
		}
		if admitted == tenants {
			return
		}
	}
	t.Fatalf("not every tenant was admitted after many ticks")
}

// A new warehouse must not wait behind a re-admission of every existing one.
//
// The driver performs ONE external step per five-second tick, so a target that
// expired for the whole fleet whenever anything changed anywhere would have put
// a new tenant hours behind thousands of pointless re-admissions. A tenant's
// intent is its own: its principals, under its own attempt.
func TestANewTenantDoesNotWaitForTheWholeFleetToBeReadmitted(t *testing.T) {
	const existing = 1000
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: poolOrgs(existing)}
	harness.operator.tenants = tenants
	harness.servingPool(t)

	// A fleet that is already admitted, as the durable record would hold it
	// after those tenants were provisioned.
	for _, org := range tenants.orgs {
		binding := trinoPoolTenantBindingFor(org)
		row := harness.publications.row(harness.operator.config.PoolID, org.OrgID)
		row.Attempt = 1
		row.PrincipalRevision = binding.Revision
		// An admitted tenant holds no live barrier: the commit cleared it.
		row.AdmittedTargetRevision = trinoPoolTargetRevision(binding, row.Attempt)
		row.State = configstore.TrinoPublicationAdmitted
	}

	// Something changes that moves the pool's catalog revision and the whole
	// projection - exactly what provisioning a new warehouse does.
	harness.store.pool.PublicationRevision++
	harness.operator.projection = func() trinoPoolProjectionRevisions {
		return trinoPoolProjectionRevisions{Policy: "policy-2", Password: "password-2", Group: "group-2"}
	}
	newcomer := poolOrgs(existing + 1)[existing]
	tenants.orgs = append(tenants.orgs, newcomer)
	harness.gateway.calls = nil

	// The newcomer is admitted within a handful of steps: publish its binding,
	// open, one receipt per serving member, commit.
	steps := 0
	for ; steps < 64; steps++ {
		harness.tickTolerant(1)
		if harness.gateway.admitted[newcomer.OrgID] != "" {
			break
		}
	}
	if harness.gateway.admitted[newcomer.OrgID] == "" {
		t.Fatalf("the new tenant was not admitted in %d steps", steps)
	}
	// And nothing re-admitted the existing fleet to get there.
	commits := countGatewayCalls(harness.gateway.calls, "commit:")
	if commits > 2 {
		t.Fatalf("%d publications committed to admit one new tenant; the fleet was being re-admitted", commits)
	}
}

// A tenant whose publication always fails must not hold the queue. The driver
// rotates and honours each tenant's durable backoff, so its neighbours still
// get admitted.
func TestAFailingTenantDoesNotStarveTheOthers(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	orgs := poolOrgs(3)
	harness.operator.tenants = &fakeTenantStore{orgs: orgs}
	// The first tenant in order can never be published - from the start, so it
	// is never admitted and keeps failing.
	broken := orgs[0].OrgID
	harness.gateway.principalErr = map[string]error{broken: errors.New("principal conflict")}
	// The ticks that bring the pool up already carry the failing tenant, which
	// is the point: its failure must not stop them either.
	harness.tickTolerant(12)

	for tick := 0; tick < 80; tick++ {
		harness.tickTolerant(1)
	}

	for _, org := range orgs[1:] {
		if harness.gateway.admitted[org.OrgID] == "" {
			t.Fatalf("tenant %s was starved by the failing tenant %s: %v", org.OrgID, broken, harness.gateway.calls)
		}
	}
	if row := harness.publications.rows[broken]; row == nil || row.Attempts == 0 || row.NextAttemptAt == nil {
		t.Fatalf("the failing tenant recorded no durable backoff: %+v", row)
	}
}

// A tenant that is revoked, re-enabled and revoked again needs a NEW durable
// occurrence. A constant step identity would replay the FIRST revocation's
// recorded outcome, leaving a tenant everybody believes is revoked admitted and
// dispatchable.
func TestASecondRevocationIsItsOwnOccurrence(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: poolOrgs(1)}
	harness.operator.tenants = tenants
	org := tenants.orgs[0].OrgID
	harness.servingPool(t)
	harness.admitAll(t, 1)

	all := tenants.orgs
	tenants.orgs = nil
	harness.tickTolerant(3)
	if !harness.gateway.revoked[org] {
		t.Fatalf("the tenant was not revoked: %v", harness.gateway.calls)
	}
	firstRevoke := harness.publications.rows[org].Attempt

	// Re-enabled: published and admitted again.
	tenants.orgs = all
	harness.admitAll(t, 1)
	if harness.gateway.admitted[org] == "" {
		t.Fatal("the re-enabled tenant was not admitted again")
	}

	// Revoked a second time.
	tenants.orgs = nil
	harness.gateway.revoked = map[string]bool{}
	harness.tickTolerant(3)
	if !harness.gateway.revoked[org] {
		t.Fatalf("the second revocation never happened: %v", harness.gateway.calls)
	}
	if second := harness.publications.rows[org].Attempt; second <= firstRevoke {
		t.Fatalf("the second revocation reused occurrence %d (first was %d)", second, firstRevoke)
	}
}

// Membership changes during a rollout. An attempt opened against the old
// membership can never commit, and while it is open a joining member cannot be
// admitted - the cycle where the commit waits for a replacement the barrier
// itself refuses. The attempt is abandoned and a new one is opened.
func TestBarrierReopensWhenMembershipChanges(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: poolOrgs(1)}
	harness.operator.tenants = tenants
	org := tenants.orgs[0].OrgID
	harness.servingPool(t)

	// Get as far as an open barrier with at least one receipt.
	for tick := 0; tick < 8 && countGatewayCalls(harness.gateway.calls, "receipt:") == 0; tick++ {
		harness.tickTolerant(1)
	}
	if countGatewayCalls(harness.gateway.calls, "open:") == 0 {
		t.Fatalf("no barrier was opened: %v", harness.gateway.calls)
	}
	firstAttempt := harness.publications.rows[org].Attempt

	// The membership moves under it, as a replacement does.
	harness.gateway.membership++
	harness.gateway.calls = nil
	harness.tickTolerant(2)

	if countGatewayCalls(harness.gateway.calls, "abandon:") == 0 {
		t.Fatalf("the stale attempt was not abandoned: %v", harness.gateway.calls)
	}
	if next := harness.publications.rows[org].Attempt; next <= firstAttempt {
		t.Fatalf("no new attempt was started (attempt %d, was %d)", next, firstAttempt)
	}

	// And it converges: the tenant is admitted against the current membership.
	harness.admitAll(t, 1)
	if harness.gateway.admitted[org] == "" {
		t.Fatalf("the tenant never recovered after the membership change: %v", harness.gateway.calls)
	}
}

// One unserviceable tenant must not stop the pool's compute lifecycle: a
// warehouse that can never be published used to end the tick before any
// instance was repaired, drained or replaced.
func TestAFailingTenantDoesNotBlockInstanceProgress(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	harness.operator.tenants = &fakeTenantStore{orgs: poolOrgs(1)}
	harness.gateway.principalErr = map[string]error{"org-0000": errors.New("principal conflict")}

	// The pool still reaches its desired instance count.
	for tick := 0; tick < 40; tick++ {
		harness.tickTolerant(1)
	}
	serving := 0
	for _, instance := range harness.store.instances {
		if instance.Phase == string(trinopool.PhaseServing) {
			serving++
		}
	}
	if serving != harness.store.pool.DesiredInstances {
		t.Fatalf("%d serving instances with a failing tenant, want %d: the tenant blocked the lifecycle",
			serving, harness.store.pool.DesiredInstances)
	}
}

// A member joining while a tenant publication is open cannot acknowledge that
// publication's target - it registers under a release id - so the Gateway
// refuses the admission. Without a way out, the member waits for the barrier
// and the barrier waits for the membership that includes the member. The
// barrier gives way.
func TestAnOpenBarrierDoesNotBlockAMemberFromJoining(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	harness.operator.tenants = &fakeTenantStore{orgs: poolOrgs(1)}
	// The barrier will open and STAY open: no member is serving the current
	// projection yet, so no acknowledgement can be recorded.
	harness.operator.acknowledgement = func(context.Context, string, trinoPoolProjectionRevisions, int64) (trinoPoolAcknowledgement, error) {
		return trinoPoolAcknowledgement{ProcessID: "process-1", AppliedRevision: 42, ProjectionCurrent: false}, nil
	}
	harness.servingPool(t)
	for tick := 0; tick < 6 && countGatewayCalls(harness.gateway.calls, "open:") == 0; tick++ {
		harness.tickTolerant(1)
	}
	if countGatewayCalls(harness.gateway.calls, "open:") == 0 {
		t.Fatalf("no barrier was opened: %v", harness.gateway.calls)
	}

	// A replacement instance reaches admission and the Gateway refuses it on
	// the barrier.
	harness.gateway.admitErr = fmt.Errorf("%w: a member joining during a publication must acknowledge its target revision",
		trinogateway.ErrPublicationBarrier)
	harness.store.pool.DesiredInstances++
	harness.operator.config.Spec.DesiredInstances++
	harness.gateway.calls = nil
	harness.tickTolerant(12)

	if countGatewayCalls(harness.gateway.calls, "abandon:") == 0 {
		t.Fatalf("the open barrier was not reopened to let the member in: %v", harness.gateway.calls)
	}
}

// A container can restart inside a Pod and come back ready with the SAME pod
// UID and a NEW Trino process. The Gateway binds the member to the boot
// identity it registered and refuses to dispatch to anything else, so a health
// check that looks only at pod readiness reports SERVING while the pool has
// quietly lost that member's capacity.
func TestARestartedProcessInTheSamePodIsNotTheSameMember(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.servingPool(t)
	instanceID := harness.store.order[0]
	instance := harness.store.instances[instanceID]
	if instance.CoordinatorBootID == "" {
		t.Fatal("setup: the admitted boot identity was not recorded")
	}
	admitted := instance.CoordinatorBootID

	// Same pod, same UID, ready - and a different process behind it. Clearing
	// the pacing map stands in for the probe interval elapsing.
	harness.operator.identity = func(context.Context, string) (string, error) {
		return "process-restarted", nil
	}
	harness.operator.identityObservedAt = nil
	harness.tick(t, 1)

	if instance.Phase != string(trinopool.PhaseSuspect) {
		t.Fatalf("phase = %s, want SUSPECT: the admitted incarnation is gone", instance.Phase)
	}
	if harness.gateway.members[instanceID].Phase != "SUSPECT" {
		t.Fatalf("gateway member = %s, want the member excluded too", harness.gateway.members[instanceID].Phase)
	}
	// The recorded identity is what the Gateway admitted. Quietly adopting the
	// new one would relabel a member nobody certified.
	if instance.CoordinatorBootID != admitted {
		t.Fatalf("stored boot identity = %q, want the admitted %q left alone",
			instance.CoordinatorBootID, admitted)
	}
	// And it is replaced through the ordinary failure path.
	harness.kube.absent = true
	harness.kube.observed.PodsPresent = 0
	harness.tick(t, 4)
	if phase := harness.store.instances[instanceID].Phase; phase != string(trinopool.PhaseFailureRetired) {
		t.Fatalf("phase = %s, want the restarted member retired through the failure path", phase)
	}
}

// A probe that does not answer is not evidence about the member: the
// controller could not look, which is the same as a failed observation. A
// member is never suspected on a timeout.
func TestAnUnansweredIdentityProbeIsNotEvidence(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.servingPool(t)
	instanceID := harness.store.order[0]

	harness.operator.identity = func(context.Context, string) (string, error) {
		return "", errors.New("connection refused")
	}
	harness.operator.identityObservedAt = nil
	harness.tick(t, 3)

	if phase := harness.store.instances[instanceID].Phase; phase != string(trinopool.PhaseServing) {
		t.Fatalf("phase = %s, want SERVING: an unanswered probe is not evidence", phase)
	}
	for _, call := range harness.gateway.calls {
		if call == "suspect:"+instanceID {
			t.Fatal("a member was suspected because its probe timed out")
		}
	}
}

// fullMembership ticks until every desired instance is ACTIVE at the Gateway.
// A publication needs the serving floor, which the Gateway enforces on both
// open and commit.
func (h *operatorHarness) fullMembership(t *testing.T) {
	t.Helper()
	for tick := 0; tick < 120; tick++ {
		h.tickTolerant(1)
		if len(h.gateway.activeInstanceIDs()) >= h.store.pool.DesiredInstances {
			return
		}
	}
	t.Fatalf("the pool never reached %d active members: %v",
		h.store.pool.DesiredInstances, h.phases())
}

// admitTenantBeyond ticks until the tenant is admitted at a target other than
// the one it already held, and returns the new one.
func (h *operatorHarness) admitTenantBeyond(t *testing.T, tenant, previous string) string {
	t.Helper()
	for tick := 0; tick < 120; tick++ {
		h.tickTolerant(1)
		if current := h.gateway.admitted[tenant]; current != "" && current != previous {
			return current
		}
	}
	t.Fatalf("tenant %s was never admitted past %q: %v", tenant, previous, h.gateway.calls)
	return ""
}

// waitForPrincipals ticks until the Gateway's binding for a tenant has the
// expected size, and returns what it holds.
func (h *operatorHarness) waitForPrincipals(t *testing.T, tenant string, want int) []string {
	t.Helper()
	for tick := 0; tick < 120; tick++ {
		h.tickTolerant(1)
		if len(h.gateway.principals[tenant]) == want {
			return h.gateway.principals[tenant]
		}
	}
	t.Fatalf("tenant %s binds %v, want %d principals", tenant, h.gateway.principals[tenant], want)
	return nil
}

// openBarriers is how many publications the Gateway currently holds OPEN.
func (f *fakePoolGateway) openBarriers() int {
	open := 0
	for _, publication := range f.publications {
		if publication.Phase == "OPEN" {
			open++
		}
	}
	return open
}

// A tenant needs a SECOND barrier whenever its own logins change, against the
// very same members that acknowledged the first one.
//
// Every durable step identity therefore carries the publication it belongs to.
// Without that, the second attempt's receipt for a member reuses the first
// attempt's step identity while carrying a different applied revision, which
// the Gateway refuses as a changed intent - and refuses forever, because the
// identity never moves again. The tenant is then stranded: published, never
// admitted, retried until somebody notices.
func TestASecondBarrierForOneTenantAcknowledgesTheSameMembers(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.fullMembership(t)
	harness.admitAll(t, 1)
	first := harness.gateway.admitted["org-a"]
	if first == "" {
		t.Fatal("the tenant was not admitted by its first barrier")
	}

	// A second login: a new binding, a new barrier, the same members.
	tenants.orgs[0].Users = append(tenants.orgs[0].Users,
		configstore.TrinoOrgUser{Username: "engineer", PasswordHash: "hash"})
	second := harness.admitTenantBeyond(t, "org-a", first)
	if got := harness.gateway.principals["org-a"]; len(got) != 3 {
		t.Fatalf("the Gateway binds %v, want the three current logins", got)
	}
	row := harness.publications.rows["org-a"]
	if row.State != configstore.TrinoPublicationAdmitted || row.AdmittedTargetRevision != second {
		t.Fatalf("durable publication = %+v, want an admission at %q", row, second)
	}
	// And the finished attempt is not left behind as the live one.
	if row.PublicationID != "" {
		t.Fatalf("a committed barrier is still recorded as live: %q", row.PublicationID)
	}
}

// A login that is added and then removed again returns the tenant's principal
// set - and therefore its revision, a digest of that set - to a value it has
// held before.
//
// The publication body is then byte-identical to the earlier one, so under a
// constant step identity the Gateway resolves it as a replay and applies
// NOTHING: the removed login stays bound to the tenant, and the next tenant to
// be given that principal is refused for a conflict nobody can see. Each
// changed binding therefore takes a new durable occurrence first.
func TestAPrincipalSetReturningToAnEarlierValueIsRepublished(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg()}}
	harness.operator.tenants = tenants
	harness.fullMembership(t)
	harness.admitAll(t, 1)

	tenants.orgs[0].Users = []configstore.TrinoOrgUser{{Username: "analyst", PasswordHash: "hash"}}
	harness.waitForPrincipals(t, "org-a", 2)

	tenants.orgs[0].Users = nil
	if got := harness.waitForPrincipals(t, "org-a", 1); len(got) != 1 {
		t.Fatalf("the removed login is still bound to the tenant: %v", got)
	}
	// The principal is free again, which is what lets another tenant hold it.
	if owner, bound := harness.gateway.principalOf["acme.analyst"]; bound {
		t.Fatalf("the removed principal is still owned by %s", owner)
	}
}

// Every OPEN publication refuses a joining member, so leftovers - from an older
// version, or from a leader that died mid-attempt - are an obstacle to the
// pool's own compute lifecycle, not just to their tenants.
//
// The driver releases them ONE per pass, durably, and opens nothing new while a
// candidate is waiting. Releasing without recording it durably was the loop
// that re-abandoned one finished publication on every attempt while the barrier
// actually in the way stayed open.
func TestLeftoverBarriersDoNotBlockAMemberFromJoining(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	orgs := poolOrgs(3)
	harness.operator.tenants = &fakeTenantStore{orgs: orgs}
	harness.fullMembership(t)

	// Three tenants, each left holding an open barrier: the shape an earlier
	// scheduler produced by opening one barrier per eligible tenant.
	for index, org := range orgs {
		binding := trinoPoolTenantBindingFor(org)
		target := trinoPoolTargetRevision(binding, 1)
		publicationID := trinoPoolPublicationID(org.OrgID, target)
		if _, err := harness.gateway.OpenPublication(context.Background(), "pool", trinogateway.OpenPublicationRequest{
			Step:                         trinogateway.Step{OperationID: "seed:" + org.OrgID, StepID: fmt.Sprintf("open.%d", index)},
			PublicationID:                publicationID,
			Tenant:                       org.OrgID,
			TargetRevision:               target,
			ExpectedMembershipGeneration: harness.gateway.membership,
			PayloadHash:                  trinoPoolPublicationPlanHash(binding, target),
		}); err != nil {
			t.Fatalf("seeding a leftover barrier for %s: %v", org.OrgID, err)
		}
		row := harness.publications.row(harness.operator.config.PoolID, org.OrgID)
		row.Attempt = 1
		row.PrincipalRevision = binding.Revision
		row.PublicationID, row.TargetRevision = publicationID, target
		row.State = configstore.TrinoPublicationAdmitting
	}
	if harness.gateway.openBarriers() != 3 {
		t.Fatalf("expected three leftover barriers, got %d", harness.gateway.openBarriers())
	}

	// A replacement instance now has to join.
	harness.store.pool.DesiredInstances++
	harness.operator.config.Spec.DesiredInstances++
	joined := false
	for tick := 0; tick < 60 && !joined; tick++ {
		harness.tickTolerant(1)
		serving := 0
		for _, instance := range harness.store.instances {
			if trinopool.Phase(instance.Phase) == trinopool.PhaseServing {
				serving++
			}
		}
		joined = serving == harness.store.pool.DesiredInstances
	}
	if !joined {
		t.Fatalf("the joining member never got past the leftover barriers; phases %v, calls %v",
			harness.phases(), harness.gateway.calls)
	}
	// The releases are recorded durably, so no tenant is left naming a
	// publication that is finished.
	for _, org := range orgs {
		row := harness.publications.rows[org.OrgID]
		if row.PublicationID != "" && harness.gateway.publications[row.PublicationID].Phase != "OPEN" {
			t.Fatalf("tenant %s still names the finished publication %s", org.OrgID, row.PublicationID)
		}
	}
	// And they are still admitted afterwards: releasing an attempt is not
	// abandoning the tenant.
	harness.admitAll(t, len(orgs))
}

// One barrier is live at a time, and it is driven to completion.
//
// This is not tuning. Every open publication blocks every member admission, and
// an attempt that is only advanced once per full rotation of the fleet stays
// open long enough for ordinary membership churn to invalidate it before it can
// commit.
func TestOnlyOneBarrierIsLiveAtATime(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	orgs := poolOrgs(5)
	harness.operator.tenants = &fakeTenantStore{orgs: orgs}
	harness.fullMembership(t)

	for tick := 0; tick < 120; tick++ {
		harness.tickTolerant(1)
		if open := harness.gateway.openBarriers(); open > 1 {
			t.Fatalf("%d barriers were open at once after %d ticks: %v",
				open, tick+1, harness.gateway.calls)
		}
	}
	for _, org := range orgs {
		if harness.gateway.admitted[org.OrgID] == "" {
			t.Fatalf("tenant %s was never admitted: %v", org.OrgID, harness.gateway.calls)
		}
	}
}

// The receipts a commit rests on must describe ONE configuration.
//
// Each receipt is evidence that a member is serving what this control plane
// serves, so an attempt that collects one receipt against the old projection
// and one against the new admits the tenant on a mixture that no member ever
// had. The attempt is released and reopened on the current projection instead -
// and a tenant that is already admitted has no attempt in flight, so it is not
// re-admitted for somebody else's change.
func TestReceiptsRestOnTheProjectionTheAttemptWasOpenedAt(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	projection := trinoPoolProjectionRevisions{Policy: "policy-1", Password: "password-1", Group: "group-1"}
	harness.operator.projection = func() trinoPoolProjectionRevisions { return projection }
	harness.fullMembership(t)

	// Get the attempt as far as its first receipt.
	for tick := 0; tick < 8 && countGatewayCalls(harness.gateway.calls, "receipt:") == 0; tick++ {
		harness.tickTolerant(1)
	}
	if countGatewayCalls(harness.gateway.calls, "receipt:") == 0 {
		t.Fatalf("no receipt was recorded: %v", harness.gateway.calls)
	}

	// The projection moves under the open attempt.
	projection = trinoPoolProjectionRevisions{Policy: "policy-2", Password: "password-2", Group: "group-2"}
	harness.admitAll(t, 1)

	admittedID := ""
	for id, publication := range harness.gateway.publications {
		if publication.Phase == "ADMITTED" {
			admittedID = id
		}
	}
	if admittedID == "" {
		t.Fatalf("the tenant was never admitted: %v", harness.gateway.calls)
	}
	expected := trinoPoolProjectionFingerprint(projection)
	for instanceID, receipt := range harness.gateway.publications[admittedID].received {
		if receipt.fingerprint != expected {
			t.Fatalf("member %s acknowledged projection %s, the committed attempt requires %s",
				instanceID, receipt.fingerprint, expected)
		}
	}
}

// clearBackoff is the durable wait elapsing. Ticks are instantaneous in a test,
// so a tenant that earned a wait would otherwise never be reached again.
func (h *operatorHarness) clearBackoff(tenant string) {
	if row := h.publications.rows[tenant]; row != nil {
		row.NextAttemptAt = nil
	}
}

// A publication whose response was lost is NOT finished: it may still be
// executing at the Gateway, and it commits whenever it gets there.
//
// If the driver moves on to the next desired binding under a new step
// identity, the two are unordered at the Gateway - same controller, same epoch,
// different steps - so the older one can commit last and leave the tenant bound
// to a set nobody wants, while duckgres has checkpointed the newer one and will
// never publish it again. The step identity is therefore pinned to the tenant's
// open occurrence until that occurrence reaches a DEFINITE outcome, which makes
// the late duplicate a journal replay rather than a second effect.
func TestADelayedPublicationCannotOverwriteANewerBinding(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.fullMembership(t)
	harness.admitAll(t, 1)

	// A second login. Its publication reaches the Gateway but the response is
	// lost, and the effect is left executing there.
	tenants.orgs[0].Users = append(tenants.orgs[0].Users,
		configstore.TrinoOrgUser{Username: "engineer", PasswordHash: "hash"})
	harness.gateway.deferPublish = map[string]bool{"org-a": true}
	before := countGatewayCalls(harness.gateway.calls, "principals:")
	for tick := 0; tick < 12 && countGatewayCalls(harness.gateway.calls, "principals:") == before; tick++ {
		harness.tickTolerant(1)
	}
	if countGatewayCalls(harness.gateway.calls, "principals:") == before {
		t.Fatalf("the changed binding was never published: %v", harness.gateway.calls)
	}

	// Before that lands, the desired binding changes AGAIN: the first login is
	// removed, so the wanted set is neither the admitted one nor the one still
	// executing at the Gateway.
	tenants.orgs[0].Users = []configstore.TrinoOrgUser{{Username: "engineer", PasswordHash: "hash"}}
	wanted := trinoPoolTenantBindingFor(tenants.orgs[0]).Principals
	harness.clearBackoff("org-a")
	for tick := 0; tick < 8; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	// The delayed effect finally commits, after the newer one.
	harness.gateway.deliverDeferred()
	for tick := 0; tick < 12; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	if got := harness.gateway.principals["org-a"]; !slices.Equal(got, wanted) {
		t.Fatalf("the Gateway binds %v, want the current %v: a delayed publication overwrote a newer binding",
			got, wanted)
	}
	// And duckgres's checkpoint agrees with what the Gateway actually holds.
	row := harness.publications.rows["org-a"]
	if row.PrincipalRevision != trinoPoolTenantBindingFor(tenants.orgs[0]).Revision {
		t.Fatalf("checkpointed revision %q does not describe the Gateway's binding %v",
			row.PrincipalRevision, harness.gateway.principals["org-a"])
	}
}

// The same hazard, in the direction that silently takes a warehouse off the
// air: a revocation whose response was lost, a tenant that is re-enabled and
// admitted again, and then the old revocation commits.
//
// duckgres would hold an admitted checkpoint for a tenant the Gateway refuses
// to dispatch for, and nothing in the desired state ever changes again to
// correct it.
func TestADelayedRevocationCannotUnadmitAReenabledTenant(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.fullMembership(t)
	harness.admitAll(t, 1)

	// The warehouse leaves the projection; its revocation is left executing at
	// the Gateway.
	all := tenants.orgs
	tenants.orgs = nil
	harness.gateway.deferRevoke = map[string]bool{"org-a": true}
	for tick := 0; tick < 12 && countGatewayCalls(harness.gateway.calls, "revoke:") == 0; tick++ {
		harness.tickTolerant(1)
	}
	if countGatewayCalls(harness.gateway.calls, "revoke:") == 0 {
		t.Fatalf("the departed tenant was never revoked: %v", harness.gateway.calls)
	}

	// It comes back before that lands.
	tenants.orgs = all
	harness.clearBackoff("org-a")
	for tick := 0; tick < 24; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	// The old revocation finally commits.
	harness.gateway.deliverDeferred()
	for tick := 0; tick < 24; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	row := harness.publications.rows["org-a"]
	if row.State == configstore.TrinoPublicationAdmitted && harness.gateway.revoked["org-a"] {
		t.Fatalf("duckgres holds an admitted checkpoint for a tenant the Gateway has revoked: %+v", row)
	}
	if !harness.gateway.revoked["org-a"] && row.State != configstore.TrinoPublicationAdmitted {
		t.Fatalf("the re-enabled tenant is dispatchable at the Gateway but not checkpointed: %+v", row)
	}
}

// The checkpoint is the half of a publication this control plane owns, and it
// can be lost on its own: the Gateway answered, the local write did not land.
//
// The occurrence is still open, so the next pass reissues THAT step rather than
// a new one. The Gateway replays its recorded outcome, and the checkpoint
// catches up - without a second effect and without a new occurrence that an
// older request could still undercut.
func TestALostCheckpointIsSettledUnderTheSameOccurrence(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.fullMembership(t)
	harness.admitAll(t, 1)

	tenants.orgs[0].Users = append(tenants.orgs[0].Users,
		configstore.TrinoOrgUser{Username: "engineer", PasswordHash: "hash"})
	harness.publications.failPrincipalCheckpoint = true
	for tick := 0; tick < 12 && len(harness.gateway.principals["org-a"]) != 3; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}
	if len(harness.gateway.principals["org-a"]) != 3 {
		t.Fatalf("the binding never reached the Gateway: %v", harness.gateway.principals["org-a"])
	}
	row := harness.publications.rows["org-a"]
	if row.PendingIntent != configstore.TrinoPublicationIntentPrincipals {
		t.Fatalf("pending intent = %q, want the occurrence held open by the lost checkpoint", row.PendingIntent)
	}
	occurrence := row.Attempt

	// The checkpoint works again. The SAME occurrence settles it: the Gateway
	// replays, and no second effect is issued.
	harness.publications.failPrincipalCheckpoint = false
	published := countGatewayCalls(harness.gateway.calls, "principals:")
	harness.clearBackoff("org-a")
	harness.admitAll(t, 1)

	row = harness.publications.rows["org-a"]
	if row.PendingIntent != "" {
		t.Fatalf("the occurrence is still open after the checkpoint landed: %+v", row)
	}
	if row.Attempt != occurrence {
		t.Fatalf("occurrence moved from %d to %d to settle a lost checkpoint", occurrence, row.Attempt)
	}
	if row.PrincipalRevision != trinoPoolTenantBindingFor(tenants.orgs[0]).Revision {
		t.Fatalf("checkpoint = %q, want the published binding", row.PrincipalRevision)
	}
	if again := countGatewayCalls(harness.gateway.calls, "principals:") - published; again != 1 {
		t.Fatalf("%d publications were issued to settle a lost checkpoint, want exactly one replay", again)
	}
}

// A leadership move in the middle of an unresolved request changes nothing: the
// occurrence and what it stands for are durable, so the new leader reissues the
// same step rather than minting one the old request could undercut.
func TestANewLeaderSettlesTheOccurrenceItInherits(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.fullMembership(t)
	harness.admitAll(t, 1)

	tenants.orgs[0].Users = append(tenants.orgs[0].Users,
		configstore.TrinoOrgUser{Username: "engineer", PasswordHash: "hash"})
	harness.gateway.deferPublish = map[string]bool{"org-a": true}
	for tick := 0; tick < 12 && harness.publications.rows["org-a"].PendingIntent == ""; tick++ {
		harness.tickTolerant(1)
	}
	row := harness.publications.rows["org-a"]
	if row.PendingIntent != configstore.TrinoPublicationIntentPrincipals {
		t.Fatalf("pending intent = %q, want an occurrence in flight", row.PendingIntent)
	}
	occurrence := row.Attempt

	// A new leadership term: a fresh operator over the same durable record.
	successor := newOperatorHarness(t)
	successor.store, successor.publications = harness.store, harness.publications
	successor.gateway = harness.gateway
	successor.operator.store = harness.store
	successor.operator.publications = harness.publications
	successor.operator.gateway = harness.gateway
	successor.operator.tenants = tenants
	successor.operator.config.Pool.TenantAdmission = true
	successor.clearBackoff("org-a")
	for tick := 0; tick < 12 && successor.publications.rows["org-a"].PendingIntent != ""; tick++ {
		successor.tickTolerant(1)
		successor.clearBackoff("org-a")
	}

	row = successor.publications.rows["org-a"]
	if row.PendingIntent != "" {
		t.Fatalf("the new leader left the inherited occurrence open: %+v", row)
	}
	if row.Attempt != occurrence {
		t.Fatalf("the new leader minted occurrence %d instead of settling %d", row.Attempt, occurrence)
	}

	// And the original, still executing at the old Gateway connection, cannot
	// change anything now.
	harness.gateway.deliverDeferred()
	if got := harness.gateway.principals["org-a"]; !slices.Equal(got, trinoPoolTenantBindingFor(tenants.orgs[0]).Principals) {
		t.Fatalf("the late duplicate changed the binding to %v", got)
	}
}

// The progress bound has to hold at the size the pool is actually for.
//
// A live barrier is finished before any new binding is published, so closing it
// costs a number of passes that depends on the MEMBER count - not on how many
// tenants happen to be waiting for their first publication. Servicing bindings
// first meant a fleet's worth of pending tenants each took a pass before the
// open barrier advanced at all.
func TestManyPendingBindingsDoNotDelayTheLiveBarrier(t *testing.T) {
	const tenants = 400
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	orgs := poolOrgs(tenants)
	harness.operator.tenants = &fakeTenantStore{orgs: orgs}
	harness.fullMembership(t)

	// One tenant is published and has a barrier open; everybody else is still
	// waiting for their first publication.
	for tick := 0; tick < 200 && harness.gateway.openBarriers() == 0; tick++ {
		harness.tickTolerant(1)
	}
	if harness.gateway.openBarriers() == 0 {
		t.Fatalf("no barrier was opened in a fleet of %d tenants", tenants)
	}
	var open *fakePublication
	for _, publication := range harness.gateway.publications {
		if publication.Phase == "OPEN" {
			open = publication
		}
	}

	// One receipt per member, then the commit: the barrier closes within a
	// bound set by the membership, with hundreds of bindings still pending.
	bound := len(harness.gateway.activeInstanceIDs()) + 3
	for tick := 0; tick < bound && open.Phase == "OPEN"; tick++ {
		harness.tickTolerant(1)
	}
	if open.Phase != "ADMITTED" {
		t.Fatalf("the barrier for %s is %s after %d passes with %d tenants pending",
			open.Tenant, open.Phase, bound, tenants)
	}
}

// The same bound for the pool's compute: a member waiting to join is not behind
// the fleet's pending bindings either.
func TestManyPendingBindingsDoNotDelayAJoiningMember(t *testing.T) {
	const tenants = 400
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	harness.operator.tenants = &fakeTenantStore{orgs: poolOrgs(tenants)}
	harness.fullMembership(t)
	for tick := 0; tick < 200 && harness.gateway.openBarriers() == 0; tick++ {
		harness.tickTolerant(1)
	}

	harness.store.pool.DesiredInstances++
	harness.operator.config.Spec.DesiredInstances++
	joined := false
	// Create, observe, register, validate, admit, mark serving: a handful of
	// passes, plus one to release the open barrier. Nothing here scales with
	// the number of tenants waiting to be published.
	for tick := 0; tick < 20 && !joined; tick++ {
		harness.tickTolerant(1)
		serving := 0
		for _, instance := range harness.store.instances {
			if trinopool.Phase(instance.Phase) == trinopool.PhaseServing {
				serving++
			}
		}
		joined = serving == harness.store.pool.DesiredInstances
	}
	if !joined {
		t.Fatalf("the joining member was delayed behind %d pending bindings: %v", tenants, harness.phases())
	}
}

// A tenant can lose its last projectable login while its publication is still
// executing at the Gateway.
//
// The stored request is what makes that settleable: there is nothing left in
// the desired state to send, but the occurrence's own body is still there, so
// it is replayed byte for byte and reaches a definite outcome. Abandoning it
// instead - the only option before the body was stored - left the delayed
// original free to commit after the revocation and rebind principals for a
// tenant nobody serves.
func TestALastLoginRemovedWhileAPublicationIsInFlightIsStillSettled(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.fullMembership(t)
	harness.admitAll(t, 1)

	// A second login, whose publication is left executing at the Gateway.
	tenants.orgs[0].Users = append(tenants.orgs[0].Users,
		configstore.TrinoOrgUser{Username: "engineer", PasswordHash: "hash"})
	inFlight := trinoPoolTenantBindingFor(tenants.orgs[0]).Principals
	harness.gateway.deferPublish = map[string]bool{"org-a": true}
	for tick := 0; tick < 12 && harness.publications.rows["org-a"].PendingIntent == ""; tick++ {
		harness.tickTolerant(1)
	}
	if harness.publications.rows["org-a"].PendingIntent != configstore.TrinoPublicationIntentPrincipals {
		t.Fatalf("no publication is in flight: %+v", harness.publications.rows["org-a"])
	}

	// Now the warehouse leaves the projection entirely: there is no desired
	// binding left to send under that occurrence.
	tenants.orgs = nil
	for tick := 0; tick < 24; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	// It was settled by replaying its own stored request, and then revoked.
	if got := harness.gateway.principals["org-a"]; !slices.Equal(got, inFlight) {
		t.Fatalf("the Gateway binds %v, want the set the occurrence stood for %v", got, inFlight)
	}
	if !harness.gateway.revoked["org-a"] {
		t.Fatalf("the departed tenant was never revoked: %v", harness.gateway.calls)
	}
	row := harness.publications.rows["org-a"]
	if row.PendingIntent != "" || row.PendingPayload != "{}" {
		t.Fatalf("publication = %+v, want no request in flight", row)
	}

	// And the delayed original, arriving now, changes nothing.
	harness.gateway.deliverDeferred()
	if got := harness.gateway.principals["org-a"]; !slices.Equal(got, inFlight) {
		t.Fatalf("the late duplicate rebound the tenant to %v", got)
	}
}

// Revoked, then re-enabled with a DIFFERENT set, while the revocation is still
// executing at the Gateway.
//
// The revocation is settled from its own stored body first, so the new binding
// and the admission that follows are strictly after it. The late duplicate is a
// replay and cannot take the warehouse back off the air.
func TestAReenabledTenantWithADifferentSetSettlesTheOldRevocationFirst(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.fullMembership(t)
	harness.admitAll(t, 1)

	all := tenants.orgs
	tenants.orgs = nil
	harness.gateway.deferRevoke = map[string]bool{"org-a": true}
	for tick := 0; tick < 12 && harness.publications.rows["org-a"].PendingIntent == ""; tick++ {
		harness.tickTolerant(1)
	}
	if harness.publications.rows["org-a"].PendingIntent != configstore.TrinoPublicationIntentRevoke {
		t.Fatalf("no revocation is in flight: %+v", harness.publications.rows["org-a"])
	}

	// Back, with a different set of logins.
	all[0].Users = []configstore.TrinoOrgUser{{Username: "engineer", PasswordHash: "hash"}}
	tenants.orgs = all
	wanted := trinoPoolTenantBindingFor(all[0]).Principals
	for tick := 0; tick < 40; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}
	harness.gateway.deliverDeferred()
	for tick := 0; tick < 40; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	if got := harness.gateway.principals["org-a"]; !slices.Equal(got, wanted) {
		t.Fatalf("the Gateway binds %v, want the re-enabled tenant's set %v", got, wanted)
	}
	if harness.gateway.revoked["org-a"] {
		t.Fatalf("the late revocation took the re-enabled tenant off the air: %v", harness.gateway.calls)
	}
	if harness.gateway.admitted["org-a"] == "" {
		t.Fatalf("the re-enabled tenant was never admitted again: %v", harness.gateway.calls)
	}
	if row := harness.publications.rows["org-a"]; row.State != configstore.TrinoPublicationAdmitted {
		t.Fatalf("durable publication = %+v, want an admitted tenant", row)
	}
}

// A duplicate that arrives after the tenant is fully admitted must be inert:
// not a rebinding, not a re-admission, not a change of any kind.
func TestALateDuplicateAfterFinalAdmissionChangesNothing(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.fullMembership(t)
	harness.admitAll(t, 1)

	tenants.orgs[0].Users = append(tenants.orgs[0].Users,
		configstore.TrinoOrgUser{Username: "engineer", PasswordHash: "hash"})
	harness.gateway.deferPublish = map[string]bool{"org-a": true}
	for tick := 0; tick < 12 && harness.publications.rows["org-a"].PendingIntent == ""; tick++ {
		harness.tickTolerant(1)
	}
	for tick := 0; tick < 40; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}
	admitted := harness.gateway.admitted["org-a"]
	principals := append([]string(nil), harness.gateway.principals["org-a"]...)
	if admitted == "" {
		t.Fatalf("the tenant was not admitted at its new binding: %v", harness.gateway.calls)
	}

	harness.gateway.deliverDeferred()
	if got := harness.gateway.principals["org-a"]; !slices.Equal(got, principals) {
		t.Fatalf("the late duplicate rebound the tenant from %v to %v", principals, got)
	}
	if harness.gateway.admitted["org-a"] != admitted {
		t.Fatalf("the late duplicate moved the admission from %q to %q",
			admitted, harness.gateway.admitted["org-a"])
	}
	if harness.gateway.revoked["org-a"] {
		t.Fatal("the late duplicate revoked an admitted tenant")
	}
}

// An identifier a tenant no longer binds must be free for whoever legitimately
// holds it next - and freeing it must not depend on provoking a refusal.
//
// A publication carrying that identifier is in flight when the binding shrinks.
// Settling it byte-identically and then publishing the smaller set under a new
// occurrence releases the identifier through two ordinary successes. Sending the
// smaller set under the OLD occurrence instead reaches the same place only by
// way of a changed-intent refusal, which means the ordinary path depends on an
// error the Gateway raises for a genuine bug.
func TestAnIdentifierDroppedFromABindingCanBeTakenByAnotherTenant(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.fullMembership(t)
	harness.admitAll(t, 1)
	dropped := "acme.analyst"

	// A second login is added, and its publication is left executing at the
	// Gateway.
	tenants.orgs[0].Users = append(tenants.orgs[0].Users,
		configstore.TrinoOrgUser{Username: "engineer", PasswordHash: "hash"})
	harness.gateway.deferPublish = map[string]bool{"org-a": true}
	for tick := 0; tick < 12 && harness.publications.rows["org-a"].PendingIntent == ""; tick++ {
		harness.tickTolerant(1)
	}
	if harness.publications.rows["org-a"].PendingIntent == "" {
		t.Fatalf("no publication is in flight: %+v", harness.publications.rows["org-a"])
	}

	// Then the first login is removed: the desired set no longer has it.
	tenants.orgs[0].Users = []configstore.TrinoOrgUser{{Username: "engineer", PasswordHash: "hash"}}
	wanted := trinoPoolTenantBindingFor(tenants.orgs[0]).Principals
	conflicts := harness.gateway.intentConflicts
	for tick := 0; tick < 40; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}
	harness.gateway.deliverDeferred()
	for tick := 0; tick < 40; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	if got := harness.gateway.principals["org-a"]; !slices.Equal(got, wanted) {
		t.Fatalf("the Gateway binds %v, want the current %v", got, wanted)
	}
	if owner, bound := harness.gateway.principalOf[dropped]; bound {
		t.Fatalf("%s is still owned by %s after being dropped from the binding", dropped, owner)
	}
	if got := harness.gateway.intentConflicts - conflicts; got != 0 {
		t.Fatalf("%d changed-intent refusals were needed to settle the binding; that error is an anomaly, not a protocol", got)
	}
	// And a different tenant can hold it, which the ownership check would have
	// refused while it was still bound.
	if _, err := harness.gateway.PublishTenantPrincipals(context.Background(), "pool", "org-b",
		trinogateway.PublishPrincipalsRequest{
			Step:       trinogateway.Step{OperationID: "tenant.org-b", StepID: "principals.a1"},
			Revision:   "binding-b",
			Principals: []string{dropped},
		}); err != nil {
		t.Fatalf("another tenant cannot take the dropped identifier: %v", err)
	}
}

// A refusal is not an unknown outcome.
//
// The Gateway runs a step in one transaction and every check throws before the
// journal write, so a refused call applied nothing and never will: the request
// is over. Holding the occurrence open for it pins the tenant on a body the
// Gateway has already rejected, and since every other queue skips a tenant with
// a request in flight, the corrected binding an operator produces by removing
// the contested login can never be sent.
//
// The reachable case is a principal that belongs to another tenant, which is
// permanent rather than transient: a revocation leaves the Gateway's principal
// rows in place, so a departed tenant keeps owning its identifiers forever.
func TestADefinitelyRefusedBindingCanBeCorrected(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	// A departed tenant still owns one of this tenant's principals.
	contested := "acme.analyst"
	harness.gateway.principalOf = map[string]string{contested: "org-old"}
	harness.tickTolerant(20)
	for tick := 0; tick < 30; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	// The operator fixes the cause: the contested login is removed, so the
	// desired binding is publishable.
	tenants.orgs[0].Users = nil
	wanted := trinoPoolTenantBindingFor(tenants.orgs[0]).Principals
	for tick := 0; tick < 60; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}

	if got := harness.gateway.principals["org-a"]; !slices.Equal(got, wanted) {
		row := harness.publications.rows["org-a"]
		t.Fatalf("the Gateway binds %v, want the corrected %v (row: pending=%q payload=%s)",
			got, wanted, row.PendingIntent, row.PendingPayload)
	}
	// The refusal closed the occurrence, and it did NOT checkpoint a binding the
	// Gateway never accepted.
	if row := harness.publications.rows["org-a"]; row.PendingIntent != "" {
		t.Fatalf("publication = %+v, want no request in flight", row)
	}
}

// The same pin also makes a deleted warehouse unrevocable: the revocation pass
// skips a tenant with a request in flight, so its logins stay dispatchable at
// the Gateway - the exact failure that pass exists to prevent.
func TestADefinitelyRefusedTenantCanStillBeRevoked(t *testing.T) {
	harness := newOperatorHarness(t)
	harness.operator.config.Pool.TenantAdmission = true
	tenants := &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	harness.operator.tenants = tenants
	harness.gateway.principalOf = map[string]string{"acme.analyst": "org-old"}
	harness.tickTolerant(20)

	// The warehouse is deleted: it leaves the projection entirely.
	tenants.orgs = nil
	for tick := 0; tick < 60; tick++ {
		harness.tickTolerant(1)
		harness.clearBackoff("org-a")
	}
	if !harness.gateway.revoked["org-a"] {
		t.Fatalf("the departed tenant was never revoked; row: %+v", harness.publications.rows["org-a"])
	}
}

// Which answers close a tenant's occurrence, stated directly.
//
// The line is "did the Gateway decide?", not "which code was it": a refusal
// applied nothing and can never apply, an unknown outcome may still commit, and
// a lost fence is about this process rather than about the request.
func TestOnlyADefiniteRefusalClosesAnOccurrence(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		cause    error
		definite bool
	}{
		{"principal conflict", trinogateway.ErrPrincipalConflict, true},
		{"validation", trinogateway.ErrValidation, true},
		{"changed intent", trinogateway.ErrIntentChanged, true},
		{"unknown pool", trinogateway.ErrNotFound, true},
		{"wrong api mode", trinogateway.ErrAPIMode, true},
		{"protocol disabled", trinogateway.ErrPoolDisabled, true},
		// A 503 from the Gateway's handler and one from anything in front of it
		// are indistinguishable here, so the request may yet commit.
		{"unavailable", trinogateway.ErrUnavailable, false},
		// Definite for the request, but this process may no longer write: the
		// term ends and the next leader settles the occurrence by replaying it.
		{"stale epoch", trinogateway.ErrStaleEpoch, false},
		// No verdict at all.
		{"transport", errors.New("connection reset by peer"), false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			wrapped := fmt.Errorf("publish principals for org-a: %w", testCase.cause)
			if got := trinoPoolRefusedDefinitively(wrapped); got != testCase.definite {
				t.Fatalf("definite = %v, want %v for %v", got, testCase.definite, testCase.cause)
			}
		})
	}
}

func (f *fakePoolGateway) GetDrainCandidates(context.Context, string, string, string) ([]trinogateway.DrainQueryCandidate, error) {
	return nil, nil
}
func (f *fakePoolGateway) ReconcileQueries(context.Context, string, string, trinogateway.ReconcileQueriesRequest) (trinogateway.ReconcileQueriesResult, error) {
	return trinogateway.ReconcileQueriesResult{}, nil
}
