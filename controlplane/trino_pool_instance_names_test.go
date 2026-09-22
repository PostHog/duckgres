//go:build kubernetes

package controlplane

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

func TestTrinoPoolInstanceNamesDoNotIncludeTheLogicalPool(t *testing.T) {
	for _, publicID := range []string{"cell-001", "cell-002", "analytics"} {
		t.Run(publicID, func(t *testing.T) {
			h := newOperatorHarness(t)
			h.operator.config.PublicID = publicID
			h.operator.config.PoolID = "registered:" + publicID
			h.operator.config.RoutingGroup = publicID
			h.operator.newInstanceID = func() string { return "a1b2c3d4" }
			if err := h.operator.createInstance(context.Background(), trinopool.Plan{}); err != nil {
				t.Fatal(err)
			}
			instance, ok := h.store.instances["cell-a1b2c3d4"]
			if !ok {
				t.Fatalf("instance names = %v, want cell-a1b2c3d4", h.store.order)
			}
			if instance.PoolID != "registered:"+publicID || h.operator.config.RoutingGroup != publicID {
				t.Fatal("shortening the instance name changed its logical pool")
			}
			objects, err := h.operator.config.Blueprint.Instantiate(h.operator.identityFor(instance.InstanceID))
			if err != nil {
				t.Fatal(err)
			}
			if objects.CoordinatorDeployment.Name != "cell-a1b2c3d4-coordinator" || objects.WorkerDeployment.Name != "cell-a1b2c3d4-worker" {
				t.Fatalf("unexpected deployment names: %s / %s", objects.CoordinatorDeployment.Name, objects.WorkerDeployment.Name)
			}
			if objects.Service.Name != instance.InstanceID || !strings.Contains(instance.EndpointURL, "//cell-a1b2c3d4.") {
				t.Fatal("service identity and endpoint do not match the short instance name")
			}
			if objects.CoordinatorDeployment.Labels["posthog.com/trino-pool"] != publicID {
				t.Fatal("the workload lost its logical pool label")
			}
		})
	}
}

func TestTrinoPoolExistingInstanceNamesRemainUnchanged(t *testing.T) {
	h := newOperatorHarness(t)
	const instanceID = "cell-001-a1b2c3d4"
	instance := configstore.TrinoPoolInstance{
		InstanceID:        instanceID,
		PoolID:            h.operator.config.PoolID,
		Phase:             string(trinopool.PhasePending),
		BlueprintSnapshot: h.operator.blueprintSnapshot(),
		EndpointURL:       h.operator.endpointFor(instanceID),
	}
	h.store.instances[instanceID] = &instance
	h.store.order = append(h.store.order, instanceID)
	if err := h.operator.createResources(context.Background(), instance); err != nil {
		t.Fatal(err)
	}
	if instance.CoordinatorDeploymentName != instanceID+"-coordinator" || instance.WorkerDeploymentName != instanceID+"-worker" {
		t.Fatalf("existing resource names changed: %+v", instance)
	}
	if instance.EndpointURL != h.operator.endpointFor(instanceID) {
		t.Fatal("existing endpoint changed")
	}
}

func TestTrinoPoolShortNameCollisionDoesNotOverwriteAnotherPool(t *testing.T) {
	h := newOperatorHarness(t)
	const instanceID = "cell-a1b2c3d4"
	existing := &configstore.TrinoPoolInstance{InstanceID: instanceID, PoolID: "registered:other", Phase: string(trinopool.PhaseRetired)}
	h.store.instances[instanceID] = existing
	h.store.order = append(h.store.order, instanceID)
	h.operator.newInstanceID = func() string { return "a1b2c3d4" }
	if err := h.operator.createInstance(context.Background(), trinopool.Plan{}); err == nil {
		t.Fatal("a cross-pool collision reused an existing identity")
	}
	if h.store.instances[instanceID] != existing || len(h.store.instances) != 1 {
		t.Fatal("the collision changed existing instances")
	}
	if len(h.kube.applied) != 0 {
		t.Fatal("the collision created Kubernetes resources")
	}
	h.operator.newInstanceID = func() string { return "b2c3d4e5" }
	if err := h.operator.createInstance(context.Background(), trinopool.Plan{}); err != nil {
		t.Fatalf("a fresh random suffix did not recover: %v", err)
	}
}

func TestTrinoPoolRandomSuffixRemainsEightHexDigits(t *testing.T) {
	if suffix := newTrinoPoolInstanceID(); !regexp.MustCompile(`^[0-9a-f]{8}$`).MatchString(suffix) {
		t.Fatalf("random suffix = %q", suffix)
	}
}
