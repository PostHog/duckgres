//go:build linux || darwin

package configstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	cpconfigstore "github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

func TestTrinoPoolNodeReplacementFirstEvidenceIsFencedAndImmutable(t *testing.T) {
	ctx := context.Background()
	store := newPoolStore(t)
	lease := claimPool(t, store, "first")
	if err := store.CreateTrinoPoolInstance(ctx, lease, newInstance("drifted-instance", trinopool.PhaseServing)); err != nil {
		t.Fatal(err)
	}
	evidence := trinopool.NodeReplacementEvidence{NodeName: "node-a", NodeUID: "node-uid-a", NodeClaimName: "claim-a", NodeClaimUID: "claim-uid-a", Reason: "Drifted"}
	if err := store.RecordTrinoPoolNodeReplacement(ctx, lease, "drifted-instance", evidence); err != nil {
		t.Fatal(err)
	}
	changed := evidence
	changed.NodeUID = "node-uid-b"
	if err := store.RecordTrinoPoolNodeReplacement(ctx, lease, "drifted-instance", changed); err != nil {
		t.Fatal(err)
	}
	instance, err := store.GetTrinoPoolInstance(ctx, "drifted-instance")
	if err != nil {
		t.Fatal(err)
	}
	var recorded trinopool.NodeReplacementEvidence
	if instance.NodeReplacementEvidence == nil {
		t.Fatal("missing durable evidence")
	}
	if err := json.Unmarshal([]byte(*instance.NodeReplacementEvidence), &recorded); err != nil {
		t.Fatal(err)
	}
	if recorded != evidence {
		t.Fatalf("first evidence rewritten: %+v", recorded)
	}
	_ = claimPool(t, store, "successor")
	if err := store.RecordTrinoPoolNodeReplacement(ctx, lease, "drifted-instance", evidence); !errors.Is(err, cpconfigstore.ErrTrinoPoolConflict) {
		t.Fatalf("stale leader reused evidence authority: %v", err)
	}
}
