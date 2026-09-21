//go:build kubernetes

package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

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
