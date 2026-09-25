//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"testing"

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
}

func (k *recoveryTerminationKube) CoordinatorPodAbsent(context.Context, trinoPoolInventory, string) (bool, error) {
	return k.absent, k.err
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
