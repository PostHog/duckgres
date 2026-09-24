//go:build kubernetes

package controlplane

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestTrinoServingBlueprintChangesRollOutWithoutAnImageChange(t *testing.T) {
	changes := map[string]func(*trinopool.Blueprint){
		"worker replicas": func(b *trinopool.Blueprint) { b.Worker.Replicas++ },
		"worker resources": func(b *trinopool.Blueprint) {
			b.Worker.PodTemplate.Spec.Containers[0].Resources = corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
			}
		},
		"coordinator configuration": func(b *trinopool.Blueprint) {
			b.ConfigFiles["coordinator"]["config.properties"] += "\nquery.max-history=200\n"
		},
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			h := newOperatorHarness(t)
			h.tick(t, 20)
			oldIDs := append([]string(nil), h.store.order...)
			old := *h.store.instances[oldIDs[0]]
			current := h.operator.config
			raw, err := json.Marshal(current.Blueprint)
			if err != nil {
				t.Fatal(err)
			}
			current.Blueprint, err = trinopool.ParseBlueprint(raw)
			if err != nil {
				t.Fatal(err)
			}
			change(current.Blueprint)
			current.Spec.DesiredBlueprintDigest = current.Blueprint.Digest()
			h.operator.resolveConfig = func() (trinoPoolConfig, error) { return current, nil }
			h.tick(t, 1)
			if len(h.store.order) != 4 {
				t.Fatalf("configuration-only rollout created %d instances, want three serving plus one surge", len(h.store.order))
			}
			if h.store.instances[old.InstanceID].Phase != string(trinopool.PhaseServing) {
				t.Fatal("old capacity drained before its replacement served")
			}
			for range 10 {
				h.tick(t, 1)
				if h.store.instances[old.InstanceID].Phase == string(trinopool.PhaseDraining) {
					break
				}
			}
			if h.store.instances[old.InstanceID].Phase != string(trinopool.PhaseDraining) {
				t.Fatal("old configuration was not drained after replacement admission")
			}
			member := h.gateway.members[old.InstanceID]
			h.gateway.obligations[old.InstanceID] = trinogateway.Obligations{Generation: member.Generation, OpenTransactions: 1}
			h.tick(t, 3)
			if len(h.store.order) != 4 || h.kube.deleted[old.InstanceID] || h.store.instances[old.InstanceID].Phase != string(trinopool.PhaseDraining) {
				t.Fatal("open transaction did not retain the old instance and surge slot")
			}
			h.gateway.obligations[old.InstanceID] = trinogateway.Obligations{Generation: member.Generation}
			h.kube.absent = true
			for range 50 {
				h.tick(t, 1)
				serving, live := 0, 0
				for _, instance := range h.store.instances {
					phase := trinopool.Phase(instance.Phase)
					if phase.Serving() {
						serving++
					}
					if phase.OccupiesCapacity() {
						live++
					}
				}
				if serving < 3 || live > 4 {
					t.Fatalf("rollout capacity serving=%d live=%d", serving, live)
				}
			}
			if len(h.store.order) != 6 {
				t.Fatalf("instance count = %d, want exactly three replacements", len(h.store.order))
			}
			for _, id := range oldIDs {
				if h.store.instances[id].Phase != string(trinopool.PhaseRetired) {
					t.Fatalf("old instance %s did not retire", id)
				}
			}
			for _, id := range h.store.order[3:] {
				instance := h.store.instances[id]
				if instance.Phase != string(trinopool.PhaseServing) || instance.Repair || instance.ReleaseID != old.ReleaseID || instance.SpecDigest != current.Blueprint.SpecDigest(h.operator.identityFor(id)) {
					t.Fatalf("replacement %s did not converge on desired configuration", id)
				}
			}
			if h.store.instances[old.InstanceID].BlueprintSnapshot != old.BlueprintSnapshot || h.store.instances[old.InstanceID].SpecDigest != old.SpecDigest {
				t.Fatal("rollout rewrote immutable instance configuration")
			}
		})
	}
}

func TestTrinoServingBlueprintUnchangedAfterAuthorityChange(t *testing.T) {
	h := newOperatorHarness(t)
	h.tick(t, 20)
	previousEpoch := h.operator.lease.Epoch
	h.operator.lease.Epoch++
	for _, instance := range h.store.instances {
		if instance.SpecDigest != h.operator.config.Blueprint.SpecDigest(h.operator.identityFor(instance.InstanceID)) {
			t.Fatal("authority epoch changed the desired execution digest")
		}
	}
	h.operator.lease.Epoch = 0
	h.tick(t, 20)
	if h.operator.lease.Epoch <= previousEpoch {
		t.Fatal("test did not acquire a new authority epoch")
	}
	if len(h.store.order) != 3 || len(h.kube.deleted) != 0 {
		t.Fatal("unchanged configuration caused replacement churn")
	}
}

func TestTrinoServingBlueprintInvalidStoredDigestUsesGuardedReplacement(t *testing.T) {
	for _, digest := range []string{"", "invalid", strings.Repeat("z", 64)} {
		t.Run(digest, func(t *testing.T) {
			h := newOperatorHarness(t)
			h.tick(t, 20)
			h.store.instances[h.store.order[0]].SpecDigest = digest
			h.tick(t, 1)
			if len(h.store.order) != 4 || len(h.kube.deleted) != 0 {
				t.Fatal("invalid digest did not create one guarded replacement")
			}
			oldID := h.store.order[0]
			if h.store.instances[oldID].Phase != string(trinopool.PhaseServing) {
				t.Fatal("invalid digest bypassed the serving floor")
			}
			h.kube.absent = true
			h.tick(t, 25)
			if h.store.instances[oldID].Phase != string(trinopool.PhaseRetired) || len(h.store.order) != 4 {
				t.Fatal("invalid digest replacement did not converge")
			}
			if h.store.instances[oldID].SpecDigest != digest {
				t.Fatal("replacement rewrote the stored ownership digest")
			}
		})
	}
}

func TestTrinoInvalidServingDigestDoesNotBlockCapacityRestoration(t *testing.T) {
	for _, action := range []string{"scale", "repair"} {
		t.Run(action, func(t *testing.T) {
			h := newOperatorHarness(t)
			h.tick(t, 20)
			h.store.instances[h.store.order[0]].SpecDigest = "invalid"
			if action == "scale" {
				h.operator.config.Spec.DesiredInstances++
			} else {
				h.placeInstance(t, h.store.order[1], trinopool.PhaseSuspect, "SUSPECT")
			}
			h.tick(t, 1)
			if len(h.store.order) != 4 {
				t.Fatal("malformed serving digest blocked capacity restoration")
			}
			replacement := h.store.instances[h.store.order[3]]
			if replacement.Repair != (action == "repair") {
				t.Fatal("replacement used the wrong budget")
			}
			if action == "repair" && replacement.RepairFor != h.store.order[1] {
				t.Fatal("repair targeted the wrong instance")
			}
			if countCalls(h.gateway.calls, "drain:") != 0 {
				t.Fatal("configuration rollout outranked capacity restoration")
			}
		})
	}
}

func TestTrinoServingBlueprintComparisonDoesNotDependOnStoredDigestEncoding(t *testing.T) {
	for _, encoding := range []string{"uppercase", "previous encoder", "explicit zero blueprint field"} {
		t.Run(encoding, func(t *testing.T) {
			h := newOperatorHarness(t)
			h.tick(t, 20)
			instance := h.store.instances[h.store.order[0]]
			if encoding == "explicit zero blueprint field" {
				var snapshot map[string]any
				if err := json.Unmarshal([]byte(instance.BlueprintSnapshot), &snapshot); err != nil {
					t.Fatal(err)
				}
				if h.operator.config.Blueprint.Generation != 0 {
					t.Fatal("fixture generation must default to zero")
				}
				snapshot["generation"] = 0
				raw, err := json.MarshalIndent(snapshot, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				instance.BlueprintSnapshot = string(raw)
			} else if encoding == "uppercase" {
				instance.SpecDigest = strings.ToUpper(instance.SpecDigest)
			} else {
				// Model an older encoder omitting a zero-valued identity field.
				identity := h.operator.identityFor(instance.InstanceID)
				identity.AuthorityEpoch = 0
				raw, err := json.Marshal(map[string]any{"blueprint": h.operator.config.Blueprint.Digest(), "identity": identity})
				if err != nil {
					t.Fatal(err)
				}
				var prior map[string]any
				if err := json.Unmarshal(raw, &prior); err != nil {
					t.Fatal(err)
				}
				delete(prior["identity"].(map[string]any), "AuthorityEpoch")
				raw, err = json.Marshal(prior)
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(raw)
				instance.SpecDigest = hex.EncodeToString(digest[:])
			}
			if encoding != "explicit zero blueprint field" && instance.SpecDigest == h.operator.config.Blueprint.SpecDigest(h.operator.identityFor(instance.InstanceID)) {
				t.Fatal("fixture did not change digest encoding")
			}
			storedDigest := instance.SpecDigest
			h.tick(t, 20)
			if len(h.store.order) != 3 || countCalls(h.gateway.calls, "drain:") != 0 {
				t.Fatal("unchanged blueprint rolled because of stored digest encoding")
			}
			if instance.SpecDigest != storedDigest {
				t.Fatal("rollout comparison rewrote the ownership digest")
			}
		})
	}
}

func TestTrinoUnreadableServingBlueprintDoesNotBlockOtherInstances(t *testing.T) {
	for _, snapshot := range []string{"", "invalid", `{}`} {
		for _, action := range []string{"hold", "scale", "repair", "replace sibling"} {
			t.Run(action+snapshot, func(t *testing.T) {
				h := newOperatorHarness(t)
				h.tick(t, 20)
				oldID := h.store.order[0]
				h.store.instances[oldID].BlueprintSnapshot = snapshot
				if action == "scale" {
					h.operator.config.Spec.DesiredInstances++
				}
				if action == "repair" {
					h.placeInstance(t, h.store.order[1], trinopool.PhaseSuspect, "SUSPECT")
				}
				if action == "replace sibling" {
					current := changedCandidateConfig(t, h.operator.config, false)
					h.operator.resolveConfig = func() (trinoPoolConfig, error) { return current, nil }
				}
				// A malformed snapshot cannot establish a safe namespace for retirement.
				if err := h.operator.reconcileOnce(t.Context()); err == nil {
					t.Fatal("unreadable snapshot was not reported")
				}
				want := 4
				if action == "hold" {
					want = 3
				}
				if len(h.store.order) != want {
					t.Fatalf("instance count = %d, want %d", len(h.store.order), want)
				}
				if h.store.instances[oldID].Phase != string(trinopool.PhaseServing) || len(h.kube.deleted) != 0 {
					t.Fatal("unreadable snapshot triggered unsafe retirement")
				}
				if action == "replace sibling" {
					for range 10 {
						h.tickTolerant(1)
						if h.store.instances[h.store.order[1]].Phase == string(trinopool.PhaseDraining) {
							break
						}
					}
					if h.store.instances[h.store.order[1]].Phase != string(trinopool.PhaseDraining) || h.store.instances[oldID].Phase != string(trinopool.PhaseServing) {
						t.Fatal("unreadable snapshot blocked a healthy sibling rollout")
					}
				}
			})
		}
	}
}
