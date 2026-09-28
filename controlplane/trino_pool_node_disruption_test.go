//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func attachNodeFixture(t *testing.T, h *operatorHarness, instance *configstore.TrinoPoolInstance) (*trinoPoolNodeGuard, trinoPoolInventory, *trinoNodeClaim) {
	t.Helper()
	g, _, inventory, claim := nodeGuardFixture(t)
	instance.ServiceName = inventory.ServiceName
	instance.CoordinatorDeploymentName = inventory.CoordinatorDeploymentName
	instance.CoordinatorDeploymentUID = inventory.CoordinatorDeploymentUID
	instance.WorkerDeploymentName = inventory.WorkerDeploymentName
	instance.WorkerDeploymentUID = inventory.WorkerDeploymentUID
	blueprint, _, _ := testPoolObjects(t, 7)
	instance.BlueprintSnapshot, _ = blueprint.MarshalSnapshot()
	h.operator.nodeGuard = g
	h.operator.lease.Epoch = 7
	h.store.epoch = 7
	return g, inventory, claim
}

func TestTrinoNodePreparingCandidateRejectsDefinitivePlacement(t *testing.T) {
	for _, scenario := range []string{"drift", "unschedulable", "missing-claim", "mismatched-claim", "api-error", "scheduling"} {
		t.Run(scenario, func(t *testing.T) {
			h, instance := validatingNodeFixture(t)
			instance.Phase = string(trinopool.PhasePreparing)
			g, inventory, claim := attachNodeFixture(t, h, instance)
			switch scenario {
			case "drift":
				claim.Status.Conditions = []metav1.Condition{{Type: "Drifted", Status: metav1.ConditionTrue}}
			case "unschedulable":
				node, _ := g.client.CoreV1().Nodes().Get(context.Background(), "compute-node", metav1.GetOptions{})
				node.Spec.Unschedulable = true
				if _, err := g.client.CoreV1().Nodes().Update(context.Background(), node, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "missing-claim":
				g.listClaims = func(context.Context) ([]trinoNodeClaim, error) { return nil, nil }
			case "mismatched-claim":
				claim.Status.ProviderID = "different"
			case "api-error":
				g.listClaims = func(context.Context) ([]trinoNodeClaim, error) { return nil, errors.New("API unavailable") }
			case "scheduling":
				pod, _ := g.client.CoreV1().Pods(inventory.Namespace).Get(context.Background(), inventory.WorkerDeploymentName+"-0", metav1.GetOptions{})
				pod.Spec.NodeName = ""
				if _, err := g.client.CoreV1().Pods(inventory.Namespace).Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			progressed, err := h.operator.validateCandidate(context.Background(), *instance)
			switch scenario {
			case "api-error":
				if err == nil || progressed || instance.Phase != string(trinopool.PhasePreparing) {
					t.Fatal("API error retired or admitted candidate")
				}
			case "scheduling":
				if err != nil || progressed || instance.Phase != string(trinopool.PhasePreparing) {
					t.Fatalf("scheduling was treated as failure: %v", err)
				}
			default:
				if err != nil || !progressed || instance.Phase != string(trinopool.PhaseFailedPreparing) {
					t.Fatalf("invalid candidate retained preparation slot: phase=%s err=%v", instance.Phase, err)
				}
			}
		})
	}
}

func TestTrinoNodeProtectionTreatsCandidateSchedulingAsProgress(t *testing.T) {
	for _, phase := range []trinopool.Phase{trinopool.PhaseCreating, trinopool.PhasePreparing, trinopool.PhaseServing} {
		t.Run(string(phase), func(t *testing.T) {
			h, instance := validatingNodeFixture(t)
			instance.Phase = string(phase)
			g, inventory, _ := attachNodeFixture(t, h, instance)
			if err := g.client.CoreV1().Pods(inventory.Namespace).Delete(context.Background(), inventory.WorkerDeploymentName+"-0", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			err := h.operator.protectPoolNodes(context.Background(), []configstore.TrinoPoolInstance{*instance})
			if phase == trinopool.PhaseServing {
				if err == nil {
					t.Fatal("incomplete serving inventory was ignored")
				}
			} else if err != nil || len(h.operator.nodeProtectionErrors) != 0 {
				t.Fatalf("normal scheduling reported reconcile failure: %v", err)
			}
		})
	}
}

func TestTrinoNodeValidatingCandidateRetiresOnlyDefinitivePlacement(t *testing.T) {
	for _, scenario := range []string{"mismatched-claim", "finite-grace", "api-error"} {
		t.Run(scenario, func(t *testing.T) {
			h, instance := validatingNodeFixture(t)
			g, _, claim := attachNodeFixture(t, h, instance)
			switch scenario {
			case "mismatched-claim":
				claim.Status.ProviderID = "different"
			case "finite-grace":
				duration := "1h"
				claim.Spec.TerminationGracePeriod = &duration
			case "api-error":
				g.listClaims = func(context.Context) ([]trinoNodeClaim, error) { return nil, errors.New("API unavailable") }
			}
			err := h.operator.admitCandidate(context.Background(), *instance)
			if scenario == "api-error" {
				if err == nil || instance.Phase != string(trinopool.PhaseValidating) || countCalls(h.gateway.calls, "retire:") != 0 {
					t.Fatal("transient API failure authorized candidate retirement")
				}
			} else if err != nil || instance.Phase != string(trinopool.PhaseFailedPreparing) || h.gateway.members[instance.InstanceID].Phase != "RETIRING" {
				t.Fatalf("invalid placement did not obtain retirement claim: phase=%s err=%v", instance.Phase, err)
			}
			if len(h.kube.deleted) != 0 {
				t.Fatal("candidate deleted before retirement cleanup")
			}
		})
	}
}

func TestTrinoNodeObservedReplacementPersistsAndUsesNormalRollout(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(map[bool]string{false: "drift", true: "deletion"}[deleting], func(t *testing.T) {
			h := newOperatorHarness(t)
			h.tick(t, 20)
			instance := h.store.instances[h.store.order[0]]
			g, _, claim := attachNodeFixture(t, h, instance)
			claim.Status.Conditions = []metav1.Condition{{Type: "Drifted", Status: metav1.ConditionTrue}}
			if deleting {
				stamp := metav1.Now()
				claim.DeletionTimestamp = &stamp
				claim.Status.Conditions = nil
			}
			if err := h.operator.protectPoolNodes(context.Background(), []configstore.TrinoPoolInstance{*instance}); err != nil {
				t.Fatal(err)
			}
			if instance.NodeReplacementEvidence == nil {
				t.Fatal("observation did not persist evidence")
			}
			var evidence trinopool.NodeReplacementEvidence
			if err := json.Unmarshal([]byte(*instance.NodeReplacementEvidence), &evidence); err != nil {
				t.Fatal(err)
			}
			if evidence.NodeUID != "node-uid" || evidence.NodeClaimUID != string(claim.UID) || evidence.Reason != map[bool]string{false: "Drifted", true: "Deleting"}[deleting] {
				t.Fatalf("wrong evidence: %+v", evidence)
			}
			node, _ := g.client.CoreV1().Nodes().Get(context.Background(), "compute-node", metav1.GetOptions{})
			if !node.Spec.Unschedulable {
				t.Fatal("durable request did not exclude old node")
			}
			instances, _ := h.store.ListTrinoPoolInstances(context.Background(), h.operator.config.PoolID)
			before := len(instances)
			if err := h.operator.applyPlan(context.Background(), h.store.pool, instances); err != nil {
				t.Fatal(err)
			}
			if len(h.store.order) != before+1 || instance.Phase != string(trinopool.PhaseServing) {
				t.Fatal("drained before normal replacement existed")
			}
			replacement := h.store.instances[h.store.order[len(h.store.order)-1]]
			if replacement.Repair || replacement.RepairFor != "" {
				t.Fatal("voluntary replacement consumed repair allowance")
			}
			// Drive replacement admission on both sides before the original may drain.
			h.operator.nodeGuard = nil
			for range 8 {
				if replacement.Phase == string(trinopool.PhaseServing) {
					break
				}
				if _, err := h.operator.progressInstance(context.Background(), *replacement); err != nil {
					t.Fatal(err)
				}
			}
			h.operator.nodeGuard = g
			if replacement.Phase != string(trinopool.PhaseServing) || h.gateway.members[replacement.InstanceID].Phase != "ACTIVE" {
				t.Fatal("replacement was not admitted before drain")
			}
			instances, _ = h.store.ListTrinoPoolInstances(context.Background(), h.operator.config.PoolID)
			if err := h.operator.applyPlan(context.Background(), h.store.pool, instances); err != nil {
				t.Fatal(err)
			}
			if instance.Phase != string(trinopool.PhaseDraining) {
				t.Fatalf("serving replacement did not release drain: %s", instance.Phase)
			}
		})
	}
}

type trinoNodeAdmissionRaceGateway struct {
	*fakePoolGateway
	beforeRetire func()
}

func (g *trinoNodeAdmissionRaceGateway) RetireMember(ctx context.Context, pool, id string, request trinogateway.MemberStepRequest) (trinogateway.Member, error) {
	if g.beforeRetire != nil {
		call := g.beforeRetire
		g.beforeRetire = nil
		call()
	}
	return g.fakePoolGateway.RetireMember(ctx, pool, id, request)
}

func TestTrinoNodeAdmissionWinningRetirementRaceKeepsAcceptedWork(t *testing.T) {
	h, instance := validatingNodeFixture(t)
	value := `{"node_name":"node","node_uid":"uid","nodeclaim_name":"claim","nodeclaim_uid":"claim-uid","reason":"Drifted"}`
	instance.NodeReplacementEvidence = &value
	h.operator.nodeGuard = newTrinoPoolNodeGuard(fake.NewClientset())
	validation, err := unmarshalValidationReceipt(instance.ValidationReceipt)
	if err != nil {
		t.Fatal(err)
	}
	request := trinogateway.AdmitMemberRequest{Step: h.operator.step(instance.InstanceID, "admit"), ExpectedGeneration: instance.GatewayGeneration, Receipt: trinogateway.ValidationReceipt{
		CertificateHash: validation.CertificateHash, ConfigRevision: instance.ReleaseID, AuthRevision: validation.AuthRevision,
		PodUID: instance.CoordinatorPodUID, BootID: validation.ProcessID, NodeID: validation.NodeID, CoordinatorID: validation.CoordinatorID, ReadyWorkers: validation.ReadyWorkers, Checks: validation.Checks,
	}}
	race := &trinoNodeAdmissionRaceGateway{fakePoolGateway: h.gateway}
	race.beforeRetire = func() {
		if _, err := h.gateway.AdmitMember(context.Background(), h.operator.config.RoutingGroup, instance.InstanceID, request); err != nil {
			t.Fatal(err)
		}
	}
	h.operator.gateway = race
	if err := h.operator.admitCandidate(context.Background(), *instance); err == nil {
		t.Fatal("retirement did not lose its generation CAS")
	}
	if instance.Phase != string(trinopool.PhaseValidating) || h.gateway.members[instance.InstanceID].Phase != "ACTIVE" || len(h.kube.deleted) != 0 {
		t.Fatal("retirement race discarded admitted work")
	}
	if err := h.operator.admitCandidate(context.Background(), *instance); err != nil {
		t.Fatal(err)
	}
	if instance.Phase != string(trinopool.PhaseAdmitted) || len(h.kube.deleted) != 0 {
		t.Fatal("lost admission did not resume safely")
	}
}

func (f *fakePoolStore) RecordTrinoPoolNodeReplacement(_ context.Context, lease configstore.TrinoPoolLease, id string, evidence trinopool.NodeReplacementEvidence) error {
	if lease.Epoch != f.epoch {
		return configstore.ErrTrinoPoolConflict
	}
	if err := evidence.Validate(); err != nil {
		return err
	}
	if f.instances[id].NodeReplacementEvidence == nil {
		encoded, _ := json.Marshal(evidence)
		value := string(encoded)
		f.instances[id].NodeReplacementEvidence = &value
	}
	return nil
}

func TestTrinoNodeProtectionFailureDoesNotBlockHealthOrRecovery(t *testing.T) {
	t.Run("health", func(t *testing.T) {
		h := newOperatorHarness(t)
		h.tick(t, 20)
		instance := h.store.instances[h.store.order[0]]
		instance.PhaseChangedAt = time.Now().Add(-time.Hour)
		h.kube.observed.CoordinatorReady = false
		h.operator.nodeGuard = newTrinoPoolNodeGuard(fake.NewClientset())
		h.tickTolerant(1)
		if instance.Phase != string(trinopool.PhaseSuspect) {
			t.Fatalf("protection lookup error blocked health: %s", instance.Phase)
		}
	})
	t.Run("authorized recovery", func(t *testing.T) {
		h, _, id := recoveryHarness(t)
		h.operator.nodeGuard = newTrinoPoolNodeGuard(fake.NewClientset())
		h.tickTolerant(14)
		if h.store.instances[id].Phase != string(trinopool.PhaseFailureRetired) {
			t.Fatalf("protection lookup error blocked authorized recovery: %s", h.store.instances[id].Phase)
		}
	})
}

func validatingNodeFixture(t *testing.T) (*operatorHarness, *configstore.TrinoPoolInstance) {
	t.Helper()
	h := newOperatorHarness(t)
	for range 12 {
		h.tick(t, 1)
		for _, instance := range h.store.instances {
			if instance.Phase == string(trinopool.PhaseValidating) {
				return h, instance
			}
		}
	}
	t.Fatal("candidate did not reach validating")
	return nil, nil
}

func TestTrinoNodeAdmissionLostReplyStillReplaysExactIntent(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "matching intent", true: "changed certificate"}[changed], func(t *testing.T) {
			h, instance := validatingNodeFixture(t)
			h.gateway.loseResponse = map[string]bool{"admit": true}
			if err := h.operator.admitCandidate(context.Background(), *instance); err == nil {
				t.Fatal("admission response was not lost")
			}
			if h.gateway.members[instance.InstanceID].Phase != "ACTIVE" || instance.Phase != string(trinopool.PhaseValidating) {
				t.Fatal("fixture did not retain ambiguous admission")
			}
			h.operator.nodeGuard = newTrinoPoolNodeGuard(fake.NewClientset())
			if changed {
				validation, err := unmarshalValidationReceipt(instance.ValidationReceipt)
				if err != nil {
					t.Fatal(err)
				}
				validation.CertificateHash = "changed-certificate"
				instance.ValidationReceipt, err = marshalValidationReceipt(validation)
				if err != nil {
					t.Fatal(err)
				}
			}
			err := h.operator.admitCandidate(context.Background(), *instance)
			if changed {
				if err == nil || instance.Phase != string(trinopool.PhaseValidating) {
					t.Fatal("adopted ACTIVE member from a different admission intent")
				}
			} else if err != nil || instance.Phase != string(trinopool.PhaseAdmitted) {
				t.Fatalf("matching admission replay blocked on new node observation: %v", err)
			}
		})
	}
}

func TestTrinoNodeAdmissionRetiresOnlyNeverAdmittedCandidate(t *testing.T) {
	h, instance := validatingNodeFixture(t)
	value := `{"node_name":"node","node_uid":"uid","nodeclaim_name":"claim","nodeclaim_uid":"claim-uid","reason":"Drifted"}`
	instance.NodeReplacementEvidence = &value
	h.operator.nodeGuard = newTrinoPoolNodeGuard(fake.NewClientset())
	if err := h.operator.admitCandidate(context.Background(), *instance); err != nil {
		t.Fatal(err)
	}
	if instance.Phase != string(trinopool.PhaseFailedPreparing) || h.gateway.members[instance.InstanceID].Phase != "RETIRING" {
		t.Fatal("drifted candidate was not guardedly retired before admission")
	}
	if len(h.kube.deleted) != 0 {
		t.Fatal("retirement claim deleted candidate before cleanup")
	}
}

func TestTrinoNodeReplacementRemainsAfterDriftClears(t *testing.T) {
	g, _, inventory, claim := nodeGuardFixture(t)
	evidence := trinopool.NodeReplacementEvidence{NodeName: "compute-node", NodeUID: "node-uid", NodeClaimName: claim.Name, NodeClaimUID: string(claim.UID), Reason: "Drifted"}
	if err := g.cordon(context.Background(), evidence, "pool", "instance"); err != nil {
		t.Fatal(err)
	}
	h := newOperatorHarness(t)
	h.tick(t, 20)
	id := h.store.order[0]
	i := h.store.instances[id]
	i.ServiceName = inventory.ServiceName
	i.CoordinatorDeploymentName = inventory.CoordinatorDeploymentName
	i.CoordinatorDeploymentUID = inventory.CoordinatorDeploymentUID
	i.WorkerDeploymentName = inventory.WorkerDeploymentName
	i.WorkerDeploymentUID = inventory.WorkerDeploymentUID
	blueprint, _, _ := testPoolObjects(t, 7)
	encoded, _ := blueprint.MarshalSnapshot()
	i.BlueprintSnapshot = encoded
	proof, _ := json.Marshal(evidence)
	value := string(proof)
	i.NodeReplacementEvidence = &value
	h.operator.nodeGuard = g
	h.operator.lease.Epoch = 7
	h.store.epoch = 7
	if err := h.operator.protectPoolNodes(context.Background(), []configstore.TrinoPoolInstance{*i}); err != nil {
		t.Fatalf("cleared drift blocked recorded replacement: %v", err)
	}
	if h.operator.nodeProtectionErrors[id] != nil {
		t.Fatal("recorded replacement was blocked")
	}
	if err := h.operator.candidateNodeCheck(context.Background(), *i); !errors.Is(err, errTrinoCandidateNodeReplacement) {
		t.Fatalf("candidate bypassed durable replacement: %v", err)
	}
}

func TestTrinoNodeDurableReplacementSurvivesProtectionFailure(t *testing.T) {
	for _, evidenceState := range []string{"valid", "absent", "invalid"} {
		t.Run(evidenceState, func(t *testing.T) {
			h := newOperatorHarness(t)
			h.tick(t, 20)
			instance := h.store.instances[h.store.order[0]]
			value := `{"node_name":"node","node_uid":"uid","nodeclaim_name":"claim","nodeclaim_uid":"claim-uid","reason":"Drifted"}`
			if evidenceState == "invalid" {
				value = `{}`
			}
			if evidenceState != "absent" {
				instance.NodeReplacementEvidence = &value
			}
			h.operator.nodeProtectionErrors = map[string]error{instance.InstanceID: errors.New("node API unavailable")}
			instances, _ := h.store.ListTrinoPoolInstances(context.Background(), h.operator.config.PoolID)
			before := len(instances)
			err := h.operator.applyPlan(context.Background(), h.store.pool, instances)
			if evidenceState != "valid" {
				if len(h.store.order) != before {
					t.Fatal("unproven replacement bypassed protection failure")
				}
				return
			}
			if err != nil || len(h.store.order) != before+1 {
				t.Fatalf("durable replacement blocked: %v", err)
			}
			replacement := h.store.instances[h.store.order[len(h.store.order)-1]]
			for range 8 {
				if replacement.Phase == string(trinopool.PhaseServing) {
					break
				}
				if _, err := h.operator.progressInstance(context.Background(), *replacement); err != nil {
					t.Fatal(err)
				}
			}
			if replacement.Phase != string(trinopool.PhaseServing) || h.gateway.members[replacement.InstanceID].Phase != "ACTIVE" {
				t.Fatal("replacement not admitted")
			}
			instances, _ = h.store.ListTrinoPoolInstances(context.Background(), h.operator.config.PoolID)
			if err := h.operator.applyPlan(context.Background(), h.store.pool, instances); err != nil {
				t.Fatal(err)
			}
			if instance.Phase != string(trinopool.PhaseDraining) {
				t.Fatal("accepted replacement could not drain")
			}
		})
	}
}

func TestTrinoNodePartialInventoryPreservesEvidenceWithoutCordon(t *testing.T) {
	for _, phase := range []trinopool.Phase{trinopool.PhaseServing, trinopool.PhasePreparing} {
		t.Run(string(phase), func(t *testing.T) {
			h := newOperatorHarness(t)
			h.tick(t, 20)
			instance := h.store.instances[h.store.order[0]]
			instance.Phase = string(phase)
			g, inventory, claim := attachNodeFixture(t, h, instance)
			claim.Status.Conditions = []metav1.Condition{{Type: "Drifted", Status: metav1.ConditionTrue}}
			ctx := context.Background()
			if err := g.client.CoreV1().Pods(inventory.Namespace).Delete(ctx, inventory.WorkerDeploymentName+"-0", metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			err := h.operator.protectPoolNodes(ctx, []configstore.TrinoPoolInstance{*instance})
			if phase == trinopool.PhaseServing && !errors.Is(err, errTrinoPodProtectionIncomplete) {
				t.Fatalf("lost inventory error: %v", err)
			}
			if phase == trinopool.PhasePreparing && err != nil {
				t.Fatalf("scheduling became failure: %v", err)
			}
			if instance.NodeReplacementEvidence == nil {
				t.Fatal("proven drift was discarded")
			}
			node, _ := g.client.CoreV1().Nodes().Get(ctx, "compute-node", metav1.GetOptions{})
			if node.Spec.Unschedulable {
				t.Fatal("incomplete protection authorized cordon")
			}
		})
	}
}

func TestTrinoNodeCompoundProtectionErrorsRetainEvidenceAndFaults(t *testing.T) {
	h := newOperatorHarness(t)
	h.tick(t, 20)
	instance := h.store.instances[h.store.order[0]]
	g, inventory, claim := attachNodeFixture(t, h, instance)
	claim.Status.Conditions = []metav1.Condition{{Type: "Drifted", Status: metav1.ConditionTrue}}
	ctx := context.Background()
	other := *claim
	other.Name, other.UID = "other-claim", "other-uid"
	other.Status.NodeName, other.Status.ProviderID, other.Status.Conditions = "a-cordoned", "test://other", nil
	g.listClaims = func(context.Context) ([]trinoNodeClaim, error) { return []trinoNodeClaim{*claim, other}, nil }
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "a-cordoned", UID: "other-node-uid", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: "karpenter.sh/v1", Kind: "NodeClaim", Name: other.Name, UID: other.UID}}}, Spec: corev1.NodeSpec{ProviderID: other.Status.ProviderID, Unschedulable: true}}
	if _, err := g.client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for suffix, nodeName := range map[string]string{"-0": "a-cordoned", "-1": "b-unavailable", "-2": ""} {
		pod, err := g.client.CoreV1().Pods(inventory.Namespace).Get(ctx, inventory.WorkerDeploymentName+suffix, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		pod.Spec.NodeName = nodeName
		if _, err := g.client.CoreV1().Pods(inventory.Namespace).Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	apiFailure := errors.New("node API unavailable")
	g.client.(*fake.Clientset).PrependReactor("get", "nodes", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.(ktesting.GetAction).GetName() == "b-unavailable" {
			return true, nil, apiFailure
		}
		return false, nil, nil
	})
	// Repeat after persistence to exercise the existing-evidence suppression path.
	for range 2 {
		err := h.operator.protectPoolNodes(ctx, []configstore.TrinoPoolInstance{*instance})
		if !errors.Is(err, apiFailure) || !errors.Is(err, errTrinoUnschedulableNode) || !errors.Is(err, errTrinoPodProtectionIncomplete) {
			t.Fatalf("compound error was lost: %v", err)
		}
		if instance.NodeReplacementEvidence == nil {
			t.Fatal("exact drift discarded beside unrelated fault")
		}
		drifted, _ := g.client.CoreV1().Nodes().Get(ctx, "compute-node", metav1.GetOptions{})
		if drifted.Spec.Unschedulable {
			t.Fatal("compound fault authorized cordon")
		}
	}
}

func TestTrinoNodeCandidateUsesFreshNamedClaimsWithoutRelisting(t *testing.T) {
	h, instance := validatingNodeFixture(t)
	g, _, claim := attachNodeFixture(t, h, instance)
	listCalls, getCalls := 0, 0
	g.listClaims = func(context.Context) ([]trinoNodeClaim, error) { listCalls++; return []trinoNodeClaim{*claim}, nil }
	g.getClaim = func(ctx context.Context, name string) (trinoNodeClaim, error) {
		getCalls++
		if name != claim.Name {
			t.Fatalf("wrong named claim: %s", name)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("claim read has no deadline")
		}
		return *claim, nil
	}
	ctx := context.Background()
	if err := h.operator.protectPoolNodes(ctx, []configstore.TrinoPoolInstance{*instance}); err != nil {
		t.Fatal(err)
	}
	if err := h.operator.candidateNodeCheck(ctx, *instance); err != nil {
		t.Fatal(err)
	}
	claim.Status.Conditions = []metav1.Condition{{Type: "Drifted", Status: metav1.ConditionTrue}}
	if err := h.operator.candidateNodeCheck(ctx, *instance); !errors.Is(err, errTrinoCandidateNodeReplacement) {
		t.Fatalf("cached claim admitted new drift: %v", err)
	}
	claim.Status.Conditions = nil
	node, _ := g.client.CoreV1().Nodes().Get(ctx, "compute-node", metav1.GetOptions{})
	node.Spec.Unschedulable = true
	if _, err := g.client.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := h.operator.candidateNodeCheck(ctx, *instance); !errors.Is(err, errTrinoUnschedulableNode) {
		t.Fatalf("cached node admitted new cordon: %v", err)
	}
	if listCalls != 1 || getCalls != 3 {
		t.Fatalf("inventory=%d named reads=%d", listCalls, getCalls)
	}
	if g.namedClaims || g.loaded || len(g.claims) != 0 || len(g.nodes) != 0 {
		t.Fatal("candidate snapshot escaped its check")
	}
}

func TestTrinoNodeCandidateDoesNotHideSchedulingAndAPIFailure(t *testing.T) {
	h, instance := validatingNodeFixture(t)
	instance.Phase = string(trinopool.PhasePreparing)
	g, inventory, _ := attachNodeFixture(t, h, instance)
	ctx := context.Background()
	if err := g.client.CoreV1().Pods(inventory.Namespace).Delete(ctx, inventory.WorkerDeploymentName+"-0", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	apiFailure := errors.New("claim API unavailable")
	g.getClaim = func(context.Context, string) (trinoNodeClaim, error) { return trinoNodeClaim{}, apiFailure }
	progressed, err := h.operator.validateCandidate(ctx, *instance)
	if progressed || !errors.Is(err, apiFailure) || !errors.Is(err, errTrinoPodProtectionIncomplete) || instance.Phase != string(trinopool.PhasePreparing) {
		t.Fatalf("candidate hid API fault or retired: progressed=%v phase=%s err=%v", progressed, instance.Phase, err)
	}
}
