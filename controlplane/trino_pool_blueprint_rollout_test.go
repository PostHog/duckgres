//go:build kubernetes

package controlplane

import (
	"context"
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

func TestTrinoServingBlueprintInvalidStoredDigestFailsClosed(t *testing.T) {
	for _, digest := range []string{"", "invalid", strings.Repeat("z", 64)} {
		t.Run(digest, func(t *testing.T) {
			h := newOperatorHarness(t)
			h.tick(t, 20)
			h.store.instances[h.store.order[0]].SpecDigest = digest
			if err := h.operator.reconcileOnce(context.Background()); err == nil {
				t.Fatal("invalid immutable digest did not report an error")
			}
			if len(h.store.order) != 3 || len(h.kube.deleted) != 0 {
				t.Fatal("invalid digest triggered a rollout")
			}
		})
	}
}
