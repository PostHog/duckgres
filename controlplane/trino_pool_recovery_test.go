//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

type recoveryPoolStore struct {
	*fakePoolStore
	requests []configstore.TrinoPoolRecovery
	readErr  error
}

func (s *recoveryPoolStore) ListTrinoPoolRecoveries(context.Context, string) ([]configstore.TrinoPoolRecovery, error) {
	return s.requests, s.readErr
}

func recoveryHarness(t *testing.T) (*operatorHarness, *recoveryPoolStore, string) {
	t.Helper()
	h := newOperatorHarness(t)
	h.operator.config.Spec.DesiredInstances = 4
	h.tick(t, 30)
	h.operator.config.Spec.DesiredInstances = 3
	id := h.store.order[0]
	i := h.placeInstance(t, id, trinopool.PhaseDraining, "DRAINING")
	m := h.gateway.members[id]
	h.gateway.obligations[id] = trinogateway.Obligations{
		InstanceID: id, Incarnation: m.Incarnation, Phase: "DRAINING", Generation: m.Generation, ActiveQueries: 2,
	}
	s := &recoveryPoolStore{fakePoolStore: h.store, requests: []configstore.TrinoPoolRecovery{{
		OperationID: "recovery-test", PoolID: i.PoolID, InstanceID: id,
		ExpectedGeneration: m.Generation, Incarnation: m.Incarnation,
		PodUID: m.PodUID, BootID: m.BootID, NodeID: m.NodeID, CoordinatorID: m.CoordinatorID,
		RequestedBy: "operator@example.com", Reason: "Retire an obsolete instance", DestructiveAuthorization: true,
	}}}
	h.operator.store = s
	h.kube.absentAfterDelete = true
	return h, s, id
}

func TestTrinoPoolRecoveryRetiresOnlyAuthorizedInstance(t *testing.T) {
	h, _, id := recoveryHarness(t)
	h.tick(t, 12)
	if got := h.store.instances[id].Phase; got != string(trinopool.PhaseFailureRetired) {
		t.Fatalf("phase = %s, want FAILURE_RETIRED", got)
	}
	if got := h.gateway.members[id]; got.Phase != "RETIRED" || got.RetirementKind != "FAILED" {
		t.Fatalf("gateway retirement = %+v", got)
	}
	if h.gateway.lastLost.Evidence != trinogateway.EvidenceDestructiveOverride || !h.gateway.lastLost.DestructiveAuthorization {
		t.Fatalf("loss did not carry explicit authorization: %+v", h.gateway.lastLost)
	}
	if len(h.kube.deleted) != 1 || !h.kube.deleted[h.store.instances[id].ServiceName] {
		t.Fatalf("deleted inventory = %v", h.kube.deleted)
	}
	if h.gateway.obligations[id].ActiveQueries != 2 {
		t.Fatal("recovery erased obligations")
	}
}

func TestTrinoPoolRecoveryFailsClosed(t *testing.T) {
	for _, scenario := range []string{"unauthorized", "identity", "generation", "capacity", "pending", "transaction", "store", "restarted"} {
		t.Run(scenario, func(t *testing.T) {
			h, s, id := recoveryHarness(t)
			switch scenario {
			case "unauthorized":
				s.requests[0].DestructiveAuthorization = false
			case "identity":
				s.requests[0].BootID = "different-process"
			case "generation":
				s.requests[0].ExpectedGeneration++
			case "capacity":
				h.gateway.members[h.store.order[1]].Phase = "SUSPECT"
			case "pending":
				o := h.gateway.obligations[id]
				o.PendingRequests = 1
				h.gateway.obligations[id] = o
			case "transaction":
				o := h.gateway.obligations[id]
				o.OpenTransactions = 1
				h.gateway.obligations[id] = o
			case "store":
				s.readErr = errors.New("database unavailable")
			case "restarted":
				h.operator.identity = func(context.Context, string) (string, error) { return "new-process", nil }
			}
			h.tickTolerant(4)
			if len(h.kube.deleted) != 0 || h.gateway.members[id].Phase != "DRAINING" {
				t.Fatalf("unsafe progress: deleted=%v member=%+v", h.kube.deleted, h.gateway.members[id])
			}
		})
	}
}

func TestTrinoPoolRecoveryResumesLostRepliesAcrossLeadership(t *testing.T) {
	for _, step := range []string{"recovery-suspect", "recovery-lost", "recovery-retire", "recovery-retired"} {
		t.Run(step, func(t *testing.T) {
			h, s, id := recoveryHarness(t)
			h.gateway.loseResponse = map[string]bool{step: true}
			for range 12 {
				err := h.operator.reconcileOnce(context.Background())
				if err != nil {
					successor := newOperatorHarness(t).operator
					successor.config = h.operator.config
					successor.store = s
					successor.gateway = h.gateway
					successor.kube = h.kube.forEpoch
					successor.owner = "successor"
					h.operator = successor
					break
				}
			}
			h.tick(t, 12)
			if h.store.instances[id].Phase != string(trinopool.PhaseFailureRetired) {
				t.Fatalf("recovery stuck at %s", h.store.instances[id].Phase)
			}
		})
	}
}

func TestTrinoPoolRecoveryWaitsForRetirementClaimAndDeletion(t *testing.T) {
	h, _, id := recoveryHarness(t)
	h.gateway.lostErr = errors.New("gateway unavailable")
	h.tickTolerant(3)
	if len(h.kube.deleted) != 0 || h.store.instances[id].Phase != string(trinopool.PhaseDraining) {
		t.Fatal("local failure path started before retirement was claimed")
	}
	h.gateway.lostErr = nil
	h.kube.absentAfterDelete = false
	h.tick(t, 8)
	if h.gateway.members[id].Phase != "RETIRING" || h.store.instances[id].Phase == string(trinopool.PhaseFailureRetired) {
		t.Fatal("retired before resources disappeared")
	}
	h.kube.absentAfterDelete = true
	h.tick(t, 3)
	if h.store.instances[id].Phase != string(trinopool.PhaseFailureRetired) {
		t.Fatal("did not complete deletion")
	}
}

func TestTrinoPoolRecoveryRefusesIncompleteInventory(t *testing.T) {
	h, _, id := recoveryHarness(t)
	h.store.instances[id].CoordinatorDeploymentUID = ""
	h.tickTolerant(2)
	if h.gateway.members[id].Phase != "DRAINING" || len(h.kube.deleted) > 0 {
		t.Fatal("recovery accepted incomplete owned-resource identity")
	}
}

func TestTrinoPoolRecoveryAllowsConcurrentCleanDrain(t *testing.T) {
	h, _, id := recoveryHarness(t)
	i := h.store.instances[id]
	h.gateway.obligations[id] = trinogateway.Obligations{}
	_, err := h.gateway.SealMember(context.Background(), h.operator.config.RoutingGroup, id, trinogateway.MemberStepRequest{
		Step: h.operator.step(id, "seal"), ExpectedGeneration: i.GatewayGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.tick(t, 8)
	if i.Phase != string(trinopool.PhaseRetired) || h.gateway.members[id].RetirementKind != "DRAINED" {
		t.Fatalf("clean drain blocked: %s", i.Phase)
	}
	if h.gateway.lastLost.Evidence != "" {
		t.Fatal("recovery overwrote clean drain with failure")
	}
}

func TestTrinoPoolRecoveryCheckpointFailureCannotDelete(t *testing.T) {
	h, s, id := recoveryHarness(t)
	h.store.failAdvance = true
	if err := h.operator.reconcileOnce(context.Background()); err == nil {
		t.Fatal("expected failed durable checkpoint")
	}
	if h.gateway.members[id].Phase != "RETIRING" || h.store.instances[id].Phase != string(trinopool.PhaseDraining) || len(h.kube.deleted) != 0 {
		t.Fatal("deleted before durable recovery checkpoint")
	}
	h.store.failAdvance = false
	successor := newOperatorHarness(t).operator
	successor.config = h.operator.config
	successor.store = s
	successor.gateway = h.gateway
	successor.kube = h.kube.forEpoch
	successor.owner = "successor"
	h.operator = successor
	h.tick(t, 8)
	if h.store.instances[id].Phase != string(trinopool.PhaseFailureRetired) {
		t.Fatal("did not resume committed retirement")
	}
}

func TestTrinoPoolRecoveryAcceptsCompletedRetirementFromOlderLeader(t *testing.T) {
	h, _, id := recoveryHarness(t)
	h.tick(t, 2)
	i := h.store.instances[id]
	if i.Phase != string(trinopool.PhaseLost) {
		t.Fatalf("unexpected phase %s", i.Phase)
	}
	h.kube.deleted[i.ServiceName] = true
	_, err := h.gateway.MemberRetired(context.Background(), h.operator.config.RoutingGroup, id, trinogateway.MemberStepRequest{
		Step: h.operator.step(id, "retired"), ExpectedGeneration: h.gateway.members[id].Generation, ResourcesAbsent: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.tick(t, 2)
	if i.Phase != string(trinopool.PhaseFailureRetired) {
		t.Fatal("did not adopt completed failed retirement")
	}
}

func TestTrinoPoolRecoveryResumesAfterCoordinatorDisappears(t *testing.T) {
	h, _, id := recoveryHarness(t)
	h.gateway.loseResponse = map[string]bool{"recovery-suspect": true}
	if err := h.operator.reconcileOnce(context.Background()); err == nil {
		t.Fatal("expected lost response")
	}
	h.operator.identity = func(context.Context, string) (string, error) { return "", errors.New("coordinator unavailable") }
	h.kube.observed.CoordinatorPods = nil
	h.tick(t, 8)
	if h.store.instances[id].Phase != string(trinopool.PhaseFailureRetired) {
		t.Fatal("accepted recovery was stranded by process exit")
	}
}

type recoveryTerminationKube struct {
	*fakePoolKube
	absent bool
	err    error
	scans  int
	uids   map[string]bool
}

func (k *recoveryTerminationKube) NamespacePodUIDs(context.Context, string) (map[string]bool, error) {
	k.scans++
	if k.uids != nil {
		return k.uids, k.err
	}
	if k.absent {
		return map[string]bool{}, k.err
	}
	return map[string]bool{"pod-uid-1": true}, k.err
}

func TestTrinoPoolRecoveryLivePathDoesNotScanNamespace(t *testing.T) {
	h, _, id := recoveryHarness(t)
	kube := &recoveryTerminationKube{fakePoolKube: h.kube, err: errors.New("namespace listing unavailable")}
	h.operator.kube = func(int64) trinoPoolKube { return kube }
	h.tickTolerant(12)
	if h.store.instances[id].Phase != string(trinopool.PhaseFailureRetired) || kube.scans != 0 {
		t.Fatalf("live recovery depends on namespace scan: phase=%s scans=%d", h.store.instances[id].Phase, kube.scans)
	}
}

func TestTrinoPoolRecoveryRetainsAbsentProofAcrossRetries(t *testing.T) {
	h, _, id := recoveryHarness(t)
	kube := &recoveryTerminationKube{fakePoolKube: h.kube, absent: true}
	h.operator.kube = func(int64) trinoPoolKube { return kube }
	obligations := h.gateway.obligations[id]
	obligations.PendingRequests = 1
	h.gateway.obligations[id] = obligations
	h.gateway.lostErr = errors.New("gateway unavailable")
	h.tickTolerant(1)
	kube.err = errors.New("namespace listing unavailable after proof")
	h.gateway.lostErr = nil
	h.tickTolerant(12)
	if h.store.instances[id].Phase != string(trinopool.PhaseFailureRetired) || kube.scans != 1 {
		t.Fatalf("recovery rechecks immutable pod absence: phase=%s scans=%d", h.store.instances[id].Phase, kube.scans)
	}
}

func TestTrinoPoolRecoverySharesNamespaceProofOnlyAcrossKnownRequests(t *testing.T) {
	h, s, id := recoveryHarness(t)
	kube := &recoveryTerminationKube{fakePoolKube: h.kube, absent: true}
	h.operator.kube = func(int64) trinoPoolKube { return kube }
	first := *h.store.instances[id]
	second := first
	second.InstanceID = "another-instance"
	second.CoordinatorPodUID = "another-pod"
	secondRequest := s.requests[0]
	secondRequest.InstanceID = second.InstanceID
	secondRequest.PodUID = second.CoordinatorPodUID
	secondRequest.OperationID = "another-recovery"
	requests := map[string]configstore.TrinoPoolRecovery{first.InstanceID: s.requests[0], second.InstanceID: secondRequest}
	h.operator.recoveryEvidence.prepare([]configstore.TrinoPoolInstance{first, second}, requests)
	for _, instance := range []configstore.TrinoPoolInstance{first, second} {
		absent, err := h.operator.recoveryPodAbsent(context.Background(), instance, requests[instance.InstanceID])
		if !absent || err != nil {
			t.Fatalf("known request did not share complete inventory: absent=%v err=%v", absent, err)
		}
	}
	if kube.scans != 1 {
		t.Fatalf("same-namespace requests caused %d scans", kube.scans)
	}
	lateRequest := secondRequest
	lateRequest.OperationID = "later-recovery"
	if absent, err := h.operator.recoveryPodAbsent(context.Background(), second, lateRequest); absent || err == nil || kube.scans != 1 {
		t.Fatal("a later authorization reused an earlier snapshot or bypassed scan throttling")
	}
	namespace := instanceNamespace(first)
	inventory := h.operator.recoveryEvidence.inventory[namespace]
	inventory.nextAttempt = time.Now().Add(-time.Second)
	h.operator.recoveryEvidence.inventory[namespace] = inventory
	kube.uids = map[string]bool{second.CoordinatorPodUID: true}
	if absent, err := h.operator.recoveryPodAbsent(context.Background(), second, lateRequest); absent || err != nil || kube.scans != 2 {
		t.Fatalf("later request did not observe current pod: absent=%v err=%v scans=%d", absent, err, kube.scans)
	}
	first.Phase = string(trinopool.PhaseFailureRetired)
	second.Phase = string(trinopool.PhaseFailureRetired)
	h.operator.recoveryEvidence.prepare([]configstore.TrinoPoolInstance{first, second}, requests)
	if len(h.operator.recoveryEvidence.inventory) != 0 || len(h.operator.recoveryEvidence.absent) != 0 {
		t.Fatal("terminal recoveries retained namespace inventories or absence proofs")
	}
}

func TestTrinoPoolRecoveryThrottlesFailedInventoryWithoutAuthorizingAbsence(t *testing.T) {
	h, s, id := recoveryHarness(t)
	kube := &recoveryTerminationKube{fakePoolKube: h.kube, absent: true, err: errors.New("incomplete inventory")}
	h.operator.kube = func(int64) trinoPoolKube { return kube }
	i := *h.store.instances[id]
	for range 8 {
		if absent, err := h.operator.recoveryPodAbsent(context.Background(), i, s.requests[0]); absent || err == nil {
			t.Fatal("incomplete inventory authorized absence")
		}
	}
	if kube.scans != 1 || len(h.operator.recoveryEvidence.absent) != 0 {
		t.Fatal("failed inventory was repeated or cached as positive evidence")
	}
}

func TestTrinoPoolRecoveryEvidenceResetsOnLeadershipTerm(t *testing.T) {
	h, s, id := recoveryHarness(t)
	kube := &recoveryTerminationKube{fakePoolKube: h.kube, absent: true}
	h.operator.kube = func(int64) trinoPoolKube { return kube }
	i := *h.store.instances[id]
	h.operator.recoveryEvidence.prepare([]configstore.TrinoPoolInstance{i}, map[string]configstore.TrinoPoolRecovery{id: s.requests[0]})
	if absent, err := h.operator.recoveryPodAbsent(context.Background(), i, s.requests[0]); !absent || err != nil {
		t.Fatalf("initial absence proof failed: absent=%v err=%v", absent, err)
	}
	h.operator.operatorEnabled = false
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.operator.Run(ctx)
	e := &h.operator.recoveryEvidence
	if len(e.active) != 0 || len(e.absent) != 0 || len(e.inventory) != 0 {
		t.Fatal("new leadership term retained previous recovery evidence")
	}
	kube.err = errors.New("inventory unavailable after leadership change")
	if absent, err := h.operator.recoveryPodAbsent(context.Background(), i, s.requests[0]); absent || err == nil || kube.scans != 2 {
		t.Fatalf("new term trusted old evidence: absent=%v err=%v scans=%d", absent, err, kube.scans)
	}
}

func TestTrinoPoolRecoveryCachedProofCannotBypassServingFloor(t *testing.T) {
	h, _, id := recoveryHarness(t)
	kube := &recoveryTerminationKube{fakePoolKube: h.kube, absent: true}
	h.operator.kube = func(int64) trinoPoolKube { return kube }
	obligations := h.gateway.obligations[id]
	obligations.PendingRequests = 1
	h.gateway.obligations[id] = obligations
	h.gateway.lostErr = errors.New("gateway unavailable")
	h.tickTolerant(1)
	h.gateway.lostErr = nil
	h.gateway.members[h.store.order[1]].Phase = "SUSPECT"
	h.tickTolerant(5)
	if h.gateway.members[id].Phase != "SUSPECT" || len(h.kube.deleted) > 0 {
		t.Fatal("cached absence bypassed the current serving floor")
	}
}

func TestTrinoPoolRecoveryRetiresProvenAbsentCoordinatorWithObligations(t *testing.T) {
	for _, lostReply := range []string{"", "recovery-suspect", "recovery-lost", "recovery-retire", "recovery-retired"} {
		t.Run(lostReply, func(t *testing.T) {
			h, s, id := recoveryHarness(t)
			kube := &recoveryTerminationKube{fakePoolKube: h.kube, absent: true}
			h.operator.kube = func(int64) trinoPoolKube { return kube }
			h.kube.observed.CoordinatorPods = []trinoPoolCoordinatorPod{{UID: "replacement-pod", RunningContainerID: "containerd://replacement"}}
			h.operator.identity = func(context.Context, string) (string, error) {
				t.Fatal("a replacement process must not authorize the old process's recovery")
				return "", nil
			}
			obligations := h.gateway.obligations[id]
			obligations.PendingRequests = 2
			obligations.OpenTransactions = 1
			h.gateway.obligations[id] = obligations
			h.gateway.loseResponse = map[string]bool{lostReply: true}
			for range 16 {
				if err := h.operator.reconcileOnce(context.Background()); err != nil {
					successor := newOperatorHarness(t).operator
					successor.config = h.operator.config
					successor.store = s
					successor.gateway = h.gateway
					successor.kube = func(int64) trinoPoolKube { return kube }
					successor.owner = "successor"
					h.operator = successor
				}
			}
			if got := h.store.instances[id].Phase; got != string(trinopool.PhaseFailureRetired) {
				t.Fatalf("proven-dead recovery stuck at %s", got)
			}
			if h.gateway.members[id].RetirementKind != "FAILED" || h.gateway.obligations[id].PendingRequests != 2 || h.gateway.obligations[id].OpenTransactions != 1 {
				t.Fatal("recovery must retain the failed work accounting")
			}
			if len(h.kube.deleted) != 1 || !h.kube.deleted[h.store.instances[id].ServiceName] {
				t.Fatal("recovery deleted another instance")
			}
		})
	}
}

func TestTrinoPoolRecoveryAbsentEvidenceFailsClosed(t *testing.T) {
	for _, scenario := range []string{"pod-present", "lookup-error", "capacity", "wrong-identity", "wrong-generation"} {
		t.Run(scenario, func(t *testing.T) {
			h, s, id := recoveryHarness(t)
			kube := &recoveryTerminationKube{fakePoolKube: h.kube, absent: true}
			h.operator.kube = func(int64) trinoPoolKube { return kube }
			h.kube.observed.CoordinatorPods = nil
			switch scenario {
			case "pod-present":
				kube.absent = false
			case "lookup-error":
				kube.err = errors.New("pod inventory unavailable")
			case "capacity":
				h.gateway.members[h.store.order[1]].Phase = "SUSPECT"
			case "wrong-identity":
				s.requests[0].PodUID = "different-pod"
			case "wrong-generation":
				s.requests[0].ExpectedGeneration++
			}
			h.tickTolerant(4)
			if h.gateway.members[id].Phase != "DRAINING" || len(h.kube.deleted) != 0 {
				t.Fatal("ambiguous evidence or failed guard authorized retirement")
			}
		})
	}
}
