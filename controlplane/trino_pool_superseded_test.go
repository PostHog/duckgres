//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

func TestSupersededCreatingCandidateRecoversBeforeReadiness(t *testing.T) {
	for _, test := range []struct {
		name        string
		coordinator bool
		workers     int
		newRelease  bool
	}{
		{"unready coordinator", false, 4, true},
		{"unready workers", true, 0, true},
		{"changed configuration", false, 4, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newOperatorHarness(t)
			h.operator.config.Spec.DesiredInstances, h.operator.config.Spec.MinServing = 1, 1
			h.tick(t, 2)
			instance := h.store.instances[h.store.order[0]]
			original := *instance
			h.kube.observed.CoordinatorReady, h.kube.observed.ReadyWorkers = test.coordinator, test.workers
			h.tick(t, 3)
			if instance.Phase != string(trinopool.PhaseCreating) || len(h.store.order) != 1 {
				t.Fatal("unchanged unready candidate did not retain its capacity")
			}
			current := changedCandidateConfig(t, h.operator.config, test.newRelease)
			h.operator.resolveConfig = func() (trinoPoolConfig, error) { return current, nil }
			h.tick(t, 1)
			if instance.Phase != string(trinopool.PhaseFailedPreparing) {
				t.Fatalf("superseded unready candidate phase = %s, want FAILED_PREPARING", instance.Phase)
			}
			if instance.BlueprintSnapshot != original.BlueprintSnapshot || instance.ReleaseID != original.ReleaseID || instance.SpecDigest != original.SpecDigest {
				t.Fatal("supersession mutated the immutable candidate specification")
			}
			if h.kube.deleted[instance.InstanceID] || len(h.gateway.members) != 0 {
				t.Fatal("supersession deleted resources immediately or registered an unready member")
			}
			h.tick(t, 1)
			if !h.kube.deleted[instance.InstanceID] || !trinopool.Phase(instance.Phase).OccupiesCapacity() || len(h.store.order) != 1 {
				t.Fatal("cleanup freed capacity before confirmed resource absence")
			}
			h.kube.absent = true
			h.tick(t, 1)
			if instance.Phase != string(trinopool.PhaseFailureRetired) {
				t.Fatal("unregistered candidate did not finish cleanup after resource absence")
			}
			h.kube.observed.CoordinatorReady, h.kube.observed.ReadyWorkers = true, 4
			h.tick(t, 6)
			replacement := h.store.instances[h.store.order[1]]
			if replacement.Phase != string(trinopool.PhaseServing) || replacement.ReleaseID != current.Spec.DesiredReleaseID {
				t.Fatal("corrected candidate did not restore serving capacity")
			}
			if len(h.operator.registrationAttempts) != 0 {
				t.Fatal("completed registration retained its term-local uncertainty")
			}
		})
	}
}

func TestSupersededCreatingCandidateResolvesLostRegistrationBeforeCleanup(t *testing.T) {
	h := newOperatorHarness(t)
	h.operator.config.Spec.DesiredInstances, h.operator.config.Spec.MinServing = 1, 1
	h.tick(t, 2)
	instance := h.store.instances[h.store.order[0]]
	h.gateway.loseResponse = map[string]bool{"register": true}
	h.tickTolerant(1)
	member := h.gateway.members[instance.InstanceID]
	if instance.Phase != string(trinopool.PhaseCreating) || member == nil {
		t.Fatal("fixture did not lose a committed registration response")
	}
	h.kube.observed.CoordinatorReady = false
	h.operator.config = changedCandidateConfig(t, h.operator.config, true)
	h.tick(t, 1)
	if instance.Phase != string(trinopool.PhasePreparing) || instance.GatewayIncarnation != member.Incarnation || h.kube.deleted[instance.InstanceID] {
		t.Fatal("supersession bypassed committed registration read-back")
	}
	h.tick(t, 2)
	if member.Phase != "RETIRING" || h.kube.deleted[instance.InstanceID] {
		t.Fatal("registered candidate cleanup bypassed Gateway retirement")
	}
}

type unreadableCreatingMemberGateway struct{ *fakePoolGateway }

type delayedCreatingRegistrationGateway struct {
	*fakePoolGateway
	pending       *trinogateway.RegisterMemberRequest
	failConfigure bool
}

func (g *delayedCreatingRegistrationGateway) RegisterMember(_ context.Context, _ string, request trinogateway.RegisterMemberRequest) (trinogateway.Member, error) {
	g.pending = &request
	return trinogateway.Member{}, errors.New("registration response timed out before the Gateway committed")
}

func (g *delayedCreatingRegistrationGateway) ConfigurePool(ctx context.Context, pool string, request trinogateway.ConfigurePoolRequest) (trinogateway.PoolState, error) {
	if g.failConfigure {
		return trinogateway.PoolState{}, errors.New("Gateway fence unavailable")
	}
	return g.fakePoolGateway.ConfigurePool(ctx, pool, request)
}

func (g *delayedCreatingRegistrationGateway) complete(ctx context.Context, pool string) error {
	if g.pending.ControllerEpoch != g.configured.ControllerEpoch {
		return trinogateway.ErrStaleEpoch
	}
	_, err := g.fakePoolGateway.RegisterMember(ctx, pool, *g.pending)
	return err
}

func TestSupersededCreatingCandidateWaitsForSameTermRegistration(t *testing.T) {
	for _, takeover := range []bool{false, true} {
		t.Run(fmt.Sprint("takeover=", takeover), func(t *testing.T) {
			h := newOperatorHarness(t)
			h.tick(t, 2)
			instance := h.store.instances[h.store.order[0]]
			gateway := &delayedCreatingRegistrationGateway{fakePoolGateway: h.gateway}
			h.operator.gateway = gateway
			h.tickTolerant(1)
			if gateway.pending == nil || len(h.gateway.members) != 0 {
				t.Fatal("fixture did not leave registration executing before its commit")
			}
			h.kube.observed.CoordinatorReady = false
			h.operator.config = changedCandidateConfig(t, h.operator.config, true)
			h.tickTolerant(2)
			if instance.Phase != string(trinopool.PhaseCreating) || h.kube.deleted[instance.InstanceID] {
				t.Fatal("Gateway 404 was mistaken for proof that a same-term registration cannot commit")
			}
			if takeover {
				h.operator.lease = configstore.TrinoPoolLease{}
				h.operator.owner = "replacement-controller"
				gateway.failConfigure = true
				h.tickTolerant(1)
				if instance.Phase != string(trinopool.PhaseCreating) || h.kube.deleted[instance.InstanceID] {
					t.Fatal("an unconfirmed new Gateway fence authorized cleanup")
				}
				if !h.operator.registrationAttempts[instance.InstanceID] {
					t.Fatal("failed Gateway fencing cleared unresolved registration evidence")
				}
				gateway.failConfigure = false
				h.tick(t, 1)
				if instance.Phase != string(trinopool.PhaseFailedPreparing) {
					t.Fatal("a confirmed higher fence did not release the obsolete registration uncertainty")
				}
				if len(h.operator.registrationAttempts) != 0 {
					t.Fatal("confirmed new term retained its predecessor's registration attempts")
				}
				if err := gateway.complete(context.Background(), h.operator.config.RoutingGroup); !errors.Is(err, trinogateway.ErrStaleEpoch) {
					t.Fatalf("old registration was not fenced: %v", err)
				}
			} else {
				if err := gateway.complete(context.Background(), h.operator.config.RoutingGroup); err != nil {
					t.Fatal(err)
				}
				h.tick(t, 1)
				if instance.Phase != string(trinopool.PhasePreparing) || instance.GatewayIncarnation == "" || h.kube.deleted[instance.InstanceID] {
					t.Fatal("late committed registration was not adopted before candidate cleanup")
				}
				if len(h.operator.registrationAttempts) != 0 {
					t.Fatal("durable adoption retained registration uncertainty")
				}
				h.tick(t, 2)
				if h.gateway.members[instance.InstanceID].Phase != "RETIRING" || h.kube.deleted[instance.InstanceID] {
					t.Fatal("late registered candidate bypassed guarded retirement")
				}
			}
		})
	}
}

func (g *unreadableCreatingMemberGateway) GetMember(context.Context, string, string) (trinogateway.Member, error) {
	return trinogateway.Member{}, errors.New("Gateway read-back unavailable")
}

func TestSupersededCreatingCandidatePreservesUnknownRegistration(t *testing.T) {
	h := newOperatorHarness(t)
	h.tick(t, 2)
	instance := h.store.instances[h.store.order[0]]
	h.operator.config = changedCandidateConfig(t, h.operator.config, true)
	h.operator.gateway = &unreadableCreatingMemberGateway{h.gateway}
	h.tickTolerant(2)
	if instance.Phase != string(trinopool.PhaseCreating) || h.kube.deleted[instance.InstanceID] {
		t.Fatal("an unknown registration outcome authorized candidate cleanup")
	}
}

func supersededCandidateHarness(t *testing.T) (*operatorHarness, *configstore.TrinoPoolInstance) {
	t.Helper()
	h := newOperatorHarness(t)
	h.operator.config.Spec.DesiredInstances, h.operator.config.Spec.MinServing = 1, 1
	h.tick(t, 3)
	return h, h.store.instances[h.store.order[0]]
}

func changedCandidateConfig(t *testing.T, config trinoPoolConfig, newRelease bool) trinoPoolConfig {
	t.Helper()
	raw, err := json.Marshal(config.Blueprint)
	if err != nil {
		t.Fatal(err)
	}
	blueprint, err := trinopool.ParseBlueprint(raw)
	if err != nil {
		t.Fatal(err)
	}
	blueprint.ConfigFiles["coordinator"]["config.properties"] += "\ncatalog.sync.enabled=true\n"
	if newRelease {
		blueprint.ReleaseID += "-next"
	}
	config.Blueprint = blueprint
	config.Spec.DesiredReleaseID = blueprint.ReleaseID
	config.Spec.DesiredBlueprintDigest = blueprint.Digest()
	return config
}

func TestSupersededPreparingCandidateRecovers(t *testing.T) {
	for _, newRelease := range []bool{false, true} {
		name := "same release with changed configuration"
		if newRelease {
			name = "new release"
		}
		t.Run(name, func(t *testing.T) {
			h, instance := supersededCandidateHarness(t)
			original := *instance
			valid := h.operator.validate
			h.operator.validate = func(context.Context, string, trinoPoolObservation, trinoPoolExpectation) (trinoPoolValidation, error) {
				return trinoPoolValidation{}, errors.New("catalog synchronization is disabled")
			}
			h.tick(t, 2)
			if instance.Phase != string(trinopool.PhasePreparing) {
				t.Fatalf("unchanged candidate phase = %s", instance.Phase)
			}
			current := changedCandidateConfig(t, h.operator.config, newRelease)
			h.operator.resolveConfig = func() (trinoPoolConfig, error) { return current, nil }
			h.tick(t, 1)
			if instance.Phase != string(trinopool.PhaseFailedPreparing) {
				t.Fatalf("superseded candidate phase = %s, want FAILED_PREPARING", instance.Phase)
			}
			if instance.ReleaseID != original.ReleaseID || instance.SpecDigest != original.SpecDigest || instance.BlueprintSnapshot != original.BlueprintSnapshot {
				t.Fatal("supersession mutated the immutable candidate snapshot")
			}
			if h.kube.deleted[instance.InstanceID] {
				t.Fatal("resources deleted before the Gateway retirement claim")
			}
			h.gateway.loseResponse = map[string]bool{"retire": true}
			h.tickTolerant(1)
			if h.gateway.members[instance.InstanceID].Phase != "RETIRING" || h.kube.deleted[instance.InstanceID] {
				t.Fatal("a lost retirement response bypassed the retirement claim")
			}
			h.tick(t, 1)
			if !h.kube.deleted[instance.InstanceID] || !trinopool.Phase(instance.Phase).OccupiesCapacity() {
				t.Fatal("candidate did not retain its capacity while resources remained")
			}
			h.kube.absent = true
			h.tick(t, 1)
			if instance.Phase != string(trinopool.PhaseFailureRetired) || h.gateway.members[instance.InstanceID].Phase != "RETIRED" {
				t.Fatal("candidate retirement did not converge after verified absence")
			}
			retireCalls := 0
			for _, call := range h.gateway.calls {
				if call == "retire:"+instance.InstanceID {
					retireCalls++
				}
				if call == "lost:"+instance.InstanceID {
					t.Fatal("never-admitted cleanup fabricated a loss claim")
				}
			}
			if retireCalls != 1 {
				t.Fatalf("retirement claim repeated %d times", retireCalls)
			}
			h.operator.validate = valid
			h.tick(t, 6)
			replacement := h.store.instances[h.store.order[1]]
			if replacement.Phase != string(trinopool.PhaseServing) || replacement.ReleaseID != current.Spec.DesiredReleaseID || replacement.SpecDigest != current.Blueprint.SpecDigest(h.operator.identityFor(replacement.InstanceID)) {
				t.Fatalf("replacement did not serve the current immutable blueprint: %+v", replacement)
			}
			if !strings.Contains(replacement.BlueprintSnapshot, "catalog.sync.enabled=true") {
				t.Fatal("replacement did not receive the corrected configuration")
			}
		})
	}
}

func TestSupersededCandidatePreservesAdmissionAmbiguity(t *testing.T) {
	for _, phase := range []trinopool.Phase{trinopool.PhaseValidating, trinopool.PhaseAdmitted, trinopool.PhaseServing} {
		t.Run(string(phase), func(t *testing.T) {
			h, instance := supersededCandidateHarness(t)
			h.tick(t, 1)
			h.gateway.loseResponse = map[string]bool{"admit": true}
			h.tickTolerant(1)
			if phase != trinopool.PhaseValidating {
				h.tick(t, 1)
			}
			if phase == trinopool.PhaseServing {
				h.tick(t, 1)
			}
			if instance.Phase != string(phase) {
				t.Fatalf("setup phase = %s, want %s", instance.Phase, phase)
			}
			h.operator.config = changedCandidateConfig(t, h.operator.config, false)
			h.tick(t, 1)
			if instance.Phase == string(trinopool.PhaseFailedPreparing) || h.kube.deleted[instance.InstanceID] {
				t.Fatal("supersession treated a possibly admitted member as never admitted")
			}
		})
	}
}

func TestSupersededCandidateRetirementRequiresAuthority(t *testing.T) {
	for _, missing := range []string{"pool", "release", "digest", "snapshot"} {
		t.Run("missing "+missing, func(t *testing.T) {
			h, instance := supersededCandidateHarness(t)
			switch missing {
			case "pool":
				h.operator.pool = nil
			case "release":
				h.operator.pool.DesiredReleaseID = ""
			case "digest":
				h.operator.pool.DesiredBlueprintDigest = ""
			case "snapshot":
				instance.BlueprintSnapshot = "invalid"
			}
			if _, err := h.operator.progressInstance(context.Background(), *instance); err == nil {
				t.Fatal("incomplete supersession evidence did not report an error")
			}
			if instance.Phase != string(trinopool.PhasePreparing) || h.kube.deleted[instance.InstanceID] {
				t.Fatal("incomplete supersession evidence changed the candidate lifecycle")
			}
		})
	}
	t.Run("Gateway refuses an active member", func(t *testing.T) {
		h, instance := supersededCandidateHarness(t)
		h.operator.config = changedCandidateConfig(t, h.operator.config, false)
		h.gateway.members[instance.InstanceID].Phase = "ACTIVE"
		h.tick(t, 1)
		h.tickTolerant(2)
		if instance.Phase != string(trinopool.PhaseFailedPreparing) || h.kube.deleted[instance.InstanceID] {
			t.Fatal("Gateway retirement refusal did not preserve the candidate resources")
		}
	})
	t.Run("unreadable desired configuration freezes", func(t *testing.T) {
		h, instance := supersededCandidateHarness(t)
		h.operator.config = changedCandidateConfig(t, h.operator.config, false)
		h.operator.resolveConfig = func() (trinoPoolConfig, error) { return trinoPoolConfig{}, errors.New("unreadable") }
		h.tick(t, 1)
		if instance.Phase != string(trinopool.PhasePreparing) || !h.store.pool.Frozen || h.kube.deleted[instance.InstanceID] {
			t.Fatal("unreadable desired state changed the candidate lifecycle")
		}
	})
	t.Run("lost authority refuses transition", func(t *testing.T) {
		h, instance := supersededCandidateHarness(t)
		h.operator.config = changedCandidateConfig(t, h.operator.config, false)
		h.store.failAdvance = true
		h.tickTolerant(1)
		if !h.operator.fenced || instance.Phase != string(trinopool.PhasePreparing) || h.kube.deleted[instance.InstanceID] {
			t.Fatal("lost authority did not stop superseded candidate cleanup")
		}
	})
}

func TestSupersededValidatingCandidateRecoversAfterRejection(t *testing.T) {
	for _, newRelease := range []bool{false, true} {
		t.Run(fmt.Sprint("new release=", newRelease), func(t *testing.T) {
			h, instance := supersededCandidateHarness(t)
			h.tick(t, 1)
			original := *instance
			h.gateway.admitErr = trinogateway.ErrNotCertified
			h.tickTolerant(2)
			if instance.Phase != string(trinopool.PhaseValidating) {
				t.Fatal("unchanged rejected candidate must retain its slot")
			}
			current := changedCandidateConfig(t, h.operator.config, newRelease)
			h.operator.resolveConfig = func() (trinoPoolConfig, error) { return current, nil }
			h.gateway.loseResponse = map[string]bool{"retire": true}
			h.tickTolerant(1)
			if h.gateway.members[instance.InstanceID].Phase != "RETIRING" || instance.Phase != string(trinopool.PhaseValidating) || h.kube.deleted[instance.InstanceID] {
				t.Fatal("lost retirement response must retain the candidate until read-back")
			}
			h.gateway.admitErr = nil
			h.tick(t, 1)
			if instance.Phase != string(trinopool.PhaseFailedPreparing) || h.kube.deleted[instance.InstanceID] {
				t.Fatal("superseded rejected candidate did not enter guarded cleanup")
			}
			if instance.ValidationReceipt != original.ValidationReceipt || instance.BlueprintSnapshot != original.BlueprintSnapshot || instance.ReleaseID != original.ReleaseID || instance.SpecDigest != original.SpecDigest {
				t.Fatal("supersession rewrote immutable admission intent")
			}
			h.tick(t, 1)
			if !h.kube.deleted[instance.InstanceID] || !trinopool.Phase(instance.Phase).OccupiesCapacity() || len(h.store.order) != 1 {
				t.Fatal("candidate capacity released before resource absence")
			}
			h.kube.absent = true
			h.tick(t, 1)
			if instance.Phase != string(trinopool.PhaseFailureRetired) || h.gateway.members[instance.InstanceID].Phase != "RETIRED" {
				t.Fatal("candidate cleanup did not finish")
			}
			h.gateway.admitErr = nil
			h.tick(t, 6)
			replacement := h.store.instances[h.store.order[1]]
			if replacement.Phase != string(trinopool.PhaseServing) || replacement.ReleaseID != current.Spec.DesiredReleaseID || replacement.SpecDigest != current.Blueprint.SpecDigest(h.operator.identityFor(replacement.InstanceID)) {
				t.Fatal("replacement did not serve the desired blueprint")
			}
		})
	}
}

func TestSupersededValidatingCandidatePreservesUnreadableAdmission(t *testing.T) {
	h, instance := supersededCandidateHarness(t)
	h.tick(t, 1)
	h.operator.config = changedCandidateConfig(t, h.operator.config, true)
	h.operator.gateway = &unreadableCreatingMemberGateway{h.gateway}
	h.tickTolerant(2)
	if instance.Phase != string(trinopool.PhaseValidating) || h.kube.deleted[instance.InstanceID] || h.gateway.members[instance.InstanceID].Phase != "PREPARING" {
		t.Fatal("unreadable admission outcome authorized cleanup")
	}
}
