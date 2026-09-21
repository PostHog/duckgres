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

func drainingCandidateHarness(t *testing.T) (*operatorHarness, *configstore.TrinoPoolInstance) {
	t.Helper()
	h := newOperatorHarness(t)
	h.tick(t, 20)
	id := h.store.order[0]
	return h, h.placeInstance(t, id, trinopool.PhaseDraining, "DRAINING")
}

func TestPoolSealUsesGatewayReadinessContract(t *testing.T) {
	h, instance := drainingCandidateHarness(t)
	before, err := h.gateway.GetObligations(context.Background(), h.operator.config.RoutingGroup, instance.InstanceID)
	if err != nil || !before.ReadyToSeal || before.Drained || before.Outstanding() != 0 {
		t.Fatalf("fixture must report DRAINING, readyToSeal=true, drained=false: %+v, %v", before, err)
	}
	h.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseSealed) || countCalls(h.gateway.calls, "seal:") != 1 {
		t.Fatalf("ready member was not sealed: phase=%s, calls=%v", instance.Phase, h.gateway.calls)
	}
	if h.kube.deleted[instance.InstanceID] {
		t.Fatal("sealing deleted resources before the retirement claim")
	}
}

func TestPoolSealWaitsForEveryObligationKind(t *testing.T) {
	for name, obligations := range map[string]trinogateway.Obligations{
		"pending request":  {PendingRequests: 1},
		"open transaction": {OpenTransactions: 1},
		"retained query":   {ActiveQueries: 1},
	} {
		t.Run(name, func(t *testing.T) {
			h, instance := drainingCandidateHarness(t)
			h.gateway.obligations[instance.InstanceID] = obligations
			h.tick(t, 3)
			if instance.Phase != string(trinopool.PhaseDraining) || countCalls(h.gateway.calls, "seal:") != 0 || h.kube.deleted[instance.InstanceID] {
				t.Fatal("an outstanding obligation did not block sealing and deletion")
			}
			h.gateway.obligations[instance.InstanceID] = trinogateway.Obligations{}
			h.tick(t, 1)
			if instance.Phase != string(trinopool.PhaseSealed) {
				t.Fatalf("resolved obligation still blocked sealing: %s", instance.Phase)
			}
		})
	}
}

func TestPoolSealRefusalPreservesMemberAndRetries(t *testing.T) {
	h, instance := drainingCandidateHarness(t)
	generation := instance.GatewayGeneration
	h.gateway.sealErr = trinogateway.ErrNotDrained
	if err := h.operator.reconcileOnce(context.Background()); !errors.Is(err, trinogateway.ErrNotDrained) {
		t.Fatalf("Gateway refusal was not surfaced: %v", err)
	}
	if instance.Phase != string(trinopool.PhaseDraining) || instance.GatewayGeneration != generation || h.kube.deleted[instance.InstanceID] {
		t.Fatal("Gateway refusal changed the member or deleted its resources")
	}
	h.gateway.sealErr = nil
	h.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseSealed) || countCalls(h.gateway.calls, "seal:") != 2 {
		t.Fatal("a later successful seal did not advance the same member")
	}
}

func TestPoolSealReplaysLostResponseAfterGatewayIsSealed(t *testing.T) {
	h, instance := drainingCandidateHarness(t)
	originalGeneration := instance.GatewayGeneration
	h.gateway.loseResponse = map[string]bool{"seal": true}
	if err := h.operator.reconcileOnce(context.Background()); err == nil {
		t.Fatal("fixture did not lose the first seal response")
	}
	if instance.Phase != string(trinopool.PhaseDraining) || instance.GatewayGeneration != originalGeneration {
		t.Fatal("unknown seal result advanced the local checkpoint")
	}
	current, err := h.gateway.GetObligations(context.Background(), h.operator.config.RoutingGroup, instance.InstanceID)
	if err != nil || current.Phase != "SEALED" || current.ReadyToSeal || !current.Drained || current.Generation != originalGeneration+1 {
		t.Fatalf("Gateway did not commit the seal before losing its response: %+v, %v", current, err)
	}
	h.tick(t, 1)
	if instance.Phase != string(trinopool.PhaseSealed) || instance.GatewayGeneration != current.Generation || countCalls(h.gateway.calls, "seal:") != 2 {
		t.Fatal("lost response was not resolved by replaying the original seal intent")
	}
	if h.gateway.members[instance.InstanceID].Generation != current.Generation || h.kube.deleted[instance.InstanceID] {
		t.Fatal("replay repeated the seal effect or deleted resources before retirement")
	}
}
