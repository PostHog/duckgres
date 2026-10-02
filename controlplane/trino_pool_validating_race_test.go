//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
	"k8s.io/client-go/kubernetes/fake"
)

func supersededValidatingFixture(t *testing.T) (*operatorHarness, *configstore.TrinoPoolInstance, trinogateway.AdmitMemberRequest) {
	t.Helper()
	h, instance := supersededCandidateHarness(t)
	h.tick(t, 1)
	current := changedCandidateConfig(t, h.operator.config, true)
	h.operator.config = current
	h.store.pool.DesiredReleaseID = current.Spec.DesiredReleaseID
	h.store.pool.DesiredBlueprintDigest = current.Spec.DesiredBlueprintDigest
	validation, err := unmarshalValidationReceipt(instance.ValidationReceipt)
	if err != nil {
		t.Fatal(err)
	}
	request := trinogateway.AdmitMemberRequest{
		Step: h.operator.step(instance.InstanceID, "admit"), ExpectedGeneration: instance.GatewayGeneration,
		Receipt: trinogateway.ValidationReceipt{
			CertificateHash: validation.CertificateHash, ConfigRevision: instance.ReleaseID, AuthRevision: validation.AuthRevision,
			PodUID: instance.CoordinatorPodUID, BootID: validation.ProcessID, NodeID: validation.NodeID,
			CoordinatorID: validation.CoordinatorID, ReadyWorkers: validation.ReadyWorkers, Checks: validation.Checks,
		},
	}
	return h, instance, request
}

func TestSupersededValidatingAdmissionWinsRetirementRace(t *testing.T) {
	h, instance, request := supersededValidatingFixture(t)
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
		t.Fatal("committed admission was not adopted after retirement lost")
	}
}

func TestSupersededValidatingRetirementWinsDelayedAdmission(t *testing.T) {
	h, instance, request := supersededValidatingFixture(t)
	if err := h.operator.admitCandidate(context.Background(), *instance); err != nil {
		t.Fatal(err)
	}
	if instance.Phase != string(trinopool.PhaseFailedPreparing) || h.gateway.members[instance.InstanceID].Phase != "RETIRING" {
		t.Fatal("superseded candidate did not claim retirement")
	}
	if _, err := h.gateway.AdmitMember(context.Background(), h.operator.config.RoutingGroup, instance.InstanceID, request); err == nil {
		t.Fatal("delayed admission succeeded after retirement won")
	}
	if h.gateway.members[instance.InstanceID].Phase != "RETIRING" || len(h.kube.deleted) != 0 {
		t.Fatal("delayed admission changed retirement or deleted resources")
	}
}

func TestSupersededValidatingIdentityMismatchPreservesResources(t *testing.T) {
	for _, field := range []string{"incarnation", "pod", "boot"} {
		t.Run(field, func(t *testing.T) {
			h, instance, _ := supersededValidatingFixture(t)
			member := h.gateway.members[instance.InstanceID]
			switch field {
			case "incarnation":
				member.Incarnation += "-other"
			case "pod":
				member.PodUID += "-other"
			case "boot":
				member.BootID += "-other"
			}
			if err := h.operator.admitCandidate(context.Background(), *instance); err == nil {
				t.Fatal("changed identity was accepted")
			}
			if instance.Phase != string(trinopool.PhaseValidating) || member.Phase != "PREPARING" || len(h.kube.deleted) != 0 {
				t.Fatal("changed identity authorized lifecycle progress")
			}
		})
	}
}

type supersededRetirementFencedGateway struct {
	*fakePoolGateway
}

func (g *supersededRetirementFencedGateway) RetireMember(context.Context, string, string, trinogateway.MemberStepRequest) (trinogateway.Member, error) {
	return trinogateway.Member{}, trinogateway.ErrStaleEpoch
}

func TestSupersededValidatingRetirementAuthorityLost(t *testing.T) {
	h, instance, _ := supersededValidatingFixture(t)
	h.operator.gateway = &supersededRetirementFencedGateway{h.gateway}
	err := h.operator.admitCandidate(context.Background(), *instance)
	if !errors.Is(err, trinogateway.ErrStaleEpoch) || !h.operator.fenced {
		t.Fatalf("lost retirement authority did not fence the operator: %v", err)
	}
	if instance.Phase != string(trinopool.PhaseValidating) || h.gateway.members[instance.InstanceID].Phase != "PREPARING" || len(h.kube.deleted) != 0 {
		t.Fatal("lost authority authorized cleanup")
	}
}

func TestSupersededValidatingRetirementRecoversFailedCheckpoint(t *testing.T) {
	h, instance, _ := supersededValidatingFixture(t)
	h.store.failAdvance = true
	if err := h.operator.admitCandidate(context.Background(), *instance); !errors.Is(err, configstore.ErrTrinoPoolConflict) {
		t.Fatalf("expected failed phase checkpoint: %v", err)
	}
	if instance.Phase != string(trinopool.PhaseValidating) || h.gateway.members[instance.InstanceID].Phase != "RETIRING" || len(h.kube.deleted) != 0 {
		t.Fatal("failed checkpoint lost the guarded retirement state")
	}
	h.store.failAdvance = false
	h.operator.fenced = false
	previousEpoch := h.store.epoch
	h.tick(t, 1)
	if h.operator.lease.Epoch <= previousEpoch {
		t.Fatal("new leadership term did not reacquire authority")
	}
	if instance.Phase != string(trinopool.PhaseFailedPreparing) || len(h.kube.deleted) != 0 {
		t.Fatal("retirement read-back did not recover the failed checkpoint")
	}
}

func TestSupersededValidatingRetirementRecoversAfterConfigRollback(t *testing.T) {
	h, instance, _ := supersededValidatingFixture(t)
	h.operator.nodeGuard = newTrinoPoolNodeGuard(fake.NewClientset())
	h.gateway.loseResponse = map[string]bool{"retire": true}
	if err := h.operator.admitCandidate(context.Background(), *instance); err == nil {
		t.Fatal("retirement response was not lost")
	}
	if instance.Phase != string(trinopool.PhaseValidating) || h.gateway.members[instance.InstanceID].Phase != "RETIRING" || len(h.kube.deleted) != 0 {
		t.Fatal("lost retirement response did not retain the candidate for read-back")
	}
	blueprint, err := trinopool.ParseBlueprint([]byte(instance.BlueprintSnapshot))
	if err != nil {
		t.Fatal(err)
	}
	h.operator.config.Blueprint = blueprint
	h.operator.config.Spec.DesiredReleaseID = instance.ReleaseID
	h.operator.config.Spec.DesiredBlueprintDigest = blueprint.Digest()
	if err := h.store.UpsertTrinoPoolSpec(context.Background(), h.operator.lease, h.operator.config.Spec); err != nil {
		t.Fatal(err)
	}
	if err := h.operator.admitCandidate(context.Background(), *instance); err != nil {
		t.Fatal(err)
	}
	if instance.Phase != string(trinopool.PhaseFailedPreparing) || h.gateway.members[instance.InstanceID].Phase != "RETIRING" || len(h.kube.deleted) != 0 {
		t.Fatal("configuration rollback bypassed committed retirement read-back")
	}
}
