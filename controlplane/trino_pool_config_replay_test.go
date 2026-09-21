//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/posthog/duckgres/controlplane/trinogateway"
)

type configureReplayEntry struct {
	payload string
	state   trinogateway.PoolState
}

type configureReplayGateway struct {
	*fakePoolGateway
	state        trinogateway.PoolState
	journal      map[string]configureReplayEntry
	requests     []trinogateway.ConfigurePoolRequest
	loseResponse bool
	reject       bool
	delayRequest bool
	corruptReply bool
	staleEpoch   bool
	requestError error
}

func (g *configureReplayGateway) ConfigurePool(_ context.Context, poolID string, request trinogateway.ConfigurePoolRequest) (trinogateway.PoolState, error) {
	g.requests = append(g.requests, request)
	if g.requestError != nil {
		return trinogateway.PoolState{}, g.requestError
	}
	if g.staleEpoch {
		return trinogateway.PoolState{}, trinogateway.ErrStaleEpoch
	}
	if g.delayRequest {
		g.delayRequest = false
		return trinogateway.PoolState{}, errors.New("request timed out before commit")
	}
	if g.reject {
		return trinogateway.PoolState{}, &trinogateway.Error{Status: 400, Code: "POOL_VALIDATION"}
	}
	key := request.OperationID + "/" + request.StepID
	payload := fakeRequestPayload(request)
	if entry, ok := g.journal[key]; ok {
		if entry.payload != payload || entry.state.ControllerEpoch != request.ControllerEpoch {
			return trinogateway.PoolState{}, fmt.Errorf("%w: configure payload or epoch changed", trinogateway.ErrIntentChanged)
		}
		// The Gateway returns the recorded result without applying it again.
		state := entry.state
		state.Replayed = true
		return state, nil
	}
	g.state = trinogateway.PoolState{
		PoolID: poolID, ControllerEpoch: request.ControllerEpoch,
		APIMode: request.APIMode, MinServing: request.MinServing,
		DesiredMembers: request.DesiredMembers, MaxSurge: request.MaxSurge,
		MaxRepair: request.MaxRepair, DesiredRevision: request.DesiredRevision,
		TenantAdmissionEnabled: request.TenantAdmissionEnabled,
	}
	g.journal[key] = configureReplayEntry{payload: payload, state: g.state}
	if g.corruptReply {
		return trinogateway.PoolState{}, nil
	}
	if g.loseResponse {
		g.loseResponse = false
		return trinogateway.PoolState{}, errors.New("response lost after commit")
	}
	return g.state, nil
}

func TestPoolConfigureRollbackAppliesOriginalReleaseAgain(t *testing.T) {
	harness := newOperatorHarness(t)
	gateway := &configureReplayGateway{
		fakePoolGateway: harness.gateway,
		journal:         map[string]configureReplayEntry{},
	}
	harness.operator.gateway = gateway
	ctx := context.Background()
	if err := harness.operator.ensureAuthority(ctx); err != nil {
		t.Fatal(err)
	}
	initialEpoch := harness.operator.lease.Epoch
	for index, release := range []string{"release-a", "release-b", "release-a", "release-b", "release-a"} {
		harness.operator.config.Blueprint.ReleaseID = release
		harness.operator.config.Spec.DesiredReleaseID = release
		harness.operator.config.Spec.DesiredBlueprintDigest = harness.operator.config.Blueprint.Digest()
		if err := harness.operator.configureGatewayPool(ctx); err != nil {
			t.Fatalf("transition %d: %v", index, err)
		}
		if gateway.state.DesiredRevision != release {
			t.Fatalf("transition %d: Gateway still wants %q, expected %q", index, gateway.state.DesiredRevision, release)
		}
		if harness.operator.lease.Epoch != initialEpoch {
			t.Fatalf("transition %d changed the authority epoch", index)
		}
	}
	if len(gateway.journal) != 5 {
		t.Fatalf("recorded %d transitions, want 5", len(gateway.journal))
	}
}

func configureReplayHarness(t *testing.T) (*operatorHarness, *configureReplayGateway) {
	t.Helper()
	harness := newOperatorHarness(t)
	gateway := &configureReplayGateway{fakePoolGateway: harness.gateway, journal: map[string]configureReplayEntry{}}
	harness.operator.gateway = gateway
	if err := harness.operator.ensureAuthority(context.Background()); err != nil {
		t.Fatal(err)
	}
	return harness, gateway
}

func TestPoolConfigureReusesUnchangedRequest(t *testing.T) {
	harness, gateway := configureReplayHarness(t)
	for range 3 {
		if err := harness.operator.configureGatewayPool(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(gateway.journal) != 1 || gateway.requests[0] != gateway.requests[2] {
		t.Fatal("unchanged settings created a new operation")
	}
}

func TestPoolConfigureChangesAdmissionSetting(t *testing.T) {
	harness, gateway := configureReplayHarness(t)
	for _, enabled := range []bool{false, true, false} {
		harness.operator.config.Pool.TenantAdmission = enabled
		if err := harness.operator.configureGatewayPool(context.Background()); err != nil {
			t.Fatal(err)
		}
		if gateway.state.TenantAdmissionEnabled != enabled {
			t.Fatalf("tenant admission = %t, want %t", gateway.state.TenantAdmissionEnabled, enabled)
		}
	}
}

func TestPoolConfigureSettlesUnknownBeforeApplyingNewDesired(t *testing.T) {
	harness, gateway := configureReplayHarness(t)
	gateway.loseResponse = true
	if err := harness.operator.configureGatewayPool(context.Background()); err == nil {
		t.Fatal("lost response must stop reconciliation")
	}
	original := gateway.requests[0]
	harness.operator.config.Spec.DesiredReleaseID = "release-next"
	harness.operator.config.Spec.DesiredBlueprintDigest = "next-digest"
	if err := harness.operator.configureGatewayPool(context.Background()); !errors.Is(err, errTrinoPoolBackoff) {
		t.Fatalf("settling obsolete configuration must defer lifecycle work: %v", err)
	}
	if gateway.requests[1] != original {
		t.Fatal("unknown operation was not retried with its exact original payload")
	}
	if err := harness.operator.configureGatewayPool(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gateway.state.DesiredRevision != "release-next" || len(gateway.journal) != 2 {
		t.Fatalf("new desired configuration was not applied: %+v", gateway.state)
	}
}

func TestPoolConfigureNewEpochDiscardsOldPendingAttempt(t *testing.T) {
	harness, gateway := configureReplayHarness(t)
	gateway.loseResponse = true
	if err := harness.operator.configureGatewayPool(context.Background()); err == nil {
		t.Fatal("lost response must stop reconciliation")
	}
	original := gateway.requests[0]
	harness.operator.lease.Epoch = 0
	if err := harness.operator.ensureAuthority(context.Background()); err != nil {
		t.Fatal(err)
	}
	harness.operator.config.Spec.DesiredReleaseID = "release-next"
	if err := harness.operator.configureGatewayPool(context.Background()); err != nil {
		t.Fatal(err)
	}
	latest := gateway.requests[1]
	if latest.ControllerEpoch <= original.ControllerEpoch || latest.StepID == original.StepID || latest.DesiredRevision != "release-next" {
		t.Fatalf("new authority reused old pending configuration: %+v", latest)
	}
}

func TestPoolConfigureCorrectedRejectedSettingsCanProceed(t *testing.T) {
	harness, gateway := configureReplayHarness(t)
	gateway.reject = true
	if err := harness.operator.configureGatewayPool(context.Background()); err == nil {
		t.Fatal("rejection must stop reconciliation")
	}
	gateway.reject = false
	harness.operator.config.Pool.TenantAdmission = true
	if err := harness.operator.configureGatewayPool(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !gateway.state.TenantAdmissionEnabled {
		t.Fatal("corrected configuration was not applied")
	}
}

func TestPoolConfigureDelayedRequestCannotOverwriteLaterSettings(t *testing.T) {
	harness, gateway := configureReplayHarness(t)
	gateway.delayRequest = true
	if err := harness.operator.configureGatewayPool(context.Background()); err == nil {
		t.Fatal("timeout must stop reconciliation")
	}
	delayed := gateway.requests[0]
	harness.operator.config.Spec.DesiredReleaseID = "release-next"
	if err := harness.operator.configureGatewayPool(context.Background()); !errors.Is(err, errTrinoPoolBackoff) {
		t.Fatalf("old request must settle before new configuration: %v", err)
	}
	if gateway.requests[1] != delayed {
		t.Fatal("delayed request was replaced instead of settled")
	}
	if err := harness.operator.configureGatewayPool(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.ConfigurePool(context.Background(), "cell-001", delayed); err != nil {
		t.Fatal(err)
	}
	if gateway.state.DesiredRevision != "release-next" {
		t.Fatal("late request overwrote the current configuration")
	}
}

func TestPoolConfigureRejectionAfterUnknownDoesNotReleasePendingAttempt(t *testing.T) {
	harness, gateway := configureReplayHarness(t)
	gateway.delayRequest = true
	if err := harness.operator.configureGatewayPool(context.Background()); err == nil {
		t.Fatal("timeout must stop reconciliation")
	}
	original := gateway.requests[0]
	harness.operator.config.Spec.DesiredReleaseID = "release-next"
	gateway.reject = true
	if err := harness.operator.configureGatewayPool(context.Background()); err == nil {
		t.Fatal("rejection must stop reconciliation")
	}
	gateway.reject = false
	if err := harness.operator.configureGatewayPool(context.Background()); !errors.Is(err, errTrinoPoolBackoff) {
		t.Fatalf("unknown old request must still settle first: %v", err)
	}
	if gateway.requests[2] != original {
		t.Fatal("a later refusal discarded the earlier unknown request")
	}
}

func TestPoolConfigureRejectsMismatchedSuccessResponse(t *testing.T) {
	harness, gateway := configureReplayHarness(t)
	gateway.corruptReply = true
	if err := harness.operator.configureGatewayPool(context.Background()); err == nil {
		t.Fatal("mismatched successful response must stop reconciliation")
	}
	original := gateway.requests[0]
	gateway.corruptReply = false
	if err := harness.operator.configureGatewayPool(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gateway.requests[1] != original {
		t.Fatal("invalid response changed the pending request")
	}
}

func TestPoolConfigureStaleEpochEndsAuthority(t *testing.T) {
	harness, gateway := configureReplayHarness(t)
	gateway.staleEpoch = true
	if err := harness.operator.configureGatewayPool(context.Background()); !errors.Is(err, trinogateway.ErrStaleEpoch) {
		t.Fatalf("stale epoch error = %v", err)
	}
	if !harness.operator.fenced || harness.operator.lease.Epoch != 0 {
		t.Fatal("stale configuration attempt did not end the authority term")
	}
}

func TestPoolConfigureHTTPTimeoutAndProxyErrorsRemainUnknown(t *testing.T) {
	for _, status := range []int{400, 403, 408, 429, 502, 504} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			harness, gateway := configureReplayHarness(t)
			gateway.requestError = &trinogateway.Error{Status: status}
			if err := harness.operator.configureGatewayPool(context.Background()); err == nil {
				t.Fatal("unclassified HTTP error must stop reconciliation")
			}
			original := gateway.requests[0]
			gateway.requestError = nil
			harness.operator.config.Spec.DesiredReleaseID = "release-next"
			if err := harness.operator.configureGatewayPool(context.Background()); !errors.Is(err, errTrinoPoolBackoff) {
				t.Fatalf("ambiguous HTTP outcome must settle first: %v", err)
			}
			if gateway.requests[1] != original {
				t.Fatal("unclassified HTTP error discarded the pending request")
			}
		})
	}
}

func TestPoolConfigureRoutingIdentityChangeDoesNotRedirectPendingRequest(t *testing.T) {
	harness, gateway := configureReplayHarness(t)
	gateway.delayRequest = true
	if err := harness.operator.configureGatewayPool(context.Background()); err == nil {
		t.Fatal("timeout must stop reconciliation")
	}
	original := gateway.requests[0]
	originalGroup := harness.operator.config.RoutingGroup
	harness.operator.config.RoutingGroup = "different-pool"
	if err := harness.operator.configureGatewayPool(context.Background()); err == nil {
		t.Fatal("routing identity change must stop reconciliation")
	}
	if len(gateway.requests) != 1 {
		t.Fatal("pending request was sent against changed routing identity")
	}
	harness.operator.config.RoutingGroup = originalGroup
	if err := harness.operator.configureGatewayPool(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gateway.requests[1] != original || gateway.state.PoolID != originalGroup {
		t.Fatal("restoring routing identity did not settle the original request")
	}
}
