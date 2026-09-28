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
	"k8s.io/client-go/kubernetes/fake"
)

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
