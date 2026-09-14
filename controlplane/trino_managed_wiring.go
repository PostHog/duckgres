//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/controlplane/provisioner/opa"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func buildTrinoManagedFleet(fleet trinoFleet, store *configstore.ConfigStore, readiness *trinoRolloutReadinessHandler) (*trinoRolloutProvisioningHandler, error) {
	managed := make(map[string]trinoRolloutProvisioningCell)
	for _, wire := range fleet {
		if wire.Cell.CatalogManagement == "gateway-shared" {
			managed[wire.Cell.RoutingGroup] = trinoRolloutProvisioningCell{StoredCellID: wire.Cell.ID, BlueBackend: wire.Cell.RoutingGroup + "-blue", GreenBackend: wire.Cell.RoutingGroup + "-green"}
		}
	}
	if len(managed) == 0 {
		return nil, nil
	}
	if readiness == nil {
		return nil, errors.New("managed catalog mode requires registered rollout readiness and canaries")
	}
	gateway, err := newTrinoManagedGateway(strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_MANAGED_GATEWAY_URL")), strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_MANAGED_GATEWAY_SERVER_NAME")), strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_MANAGED_GATEWAY_USERNAME")), readiness.token)
	if err != nil {
		return nil, err
	}
	for _, wire := range fleet {
		if wire.Cell.CatalogManagement != "gateway-shared" {
			continue
		}
		clients := make(map[string]provisioner.TrinoCatalogClient)
		var credentials []provisioner.TrinoCatalogClient
		for _, backend := range wire.Cell.Backends {
			client, err := provisioner.NewTrinoSharedCatalogHTTPClient(backend.CoordinatorURL, opa.AdminPrincipal, "", backend.TLSServerName)
			if err != nil {
				return nil, err
			}
			clients[wire.Cell.RoutingGroup+"-"+backend.ID] = client
			credentials = append(credentials, client)
		}
		opts := managedCatalogOptions(store, gateway, wire.Cell.RoutingGroup, clients, readiness.process)
		opts.CatalogClients = credentials
		if err := wire.Provisioner.ConfigureManagedCatalogs(opts); err != nil {
			return nil, err
		}
	}
	return newTrinoRolloutProvisioningHandler(readiness.token, managed, store, gateway, readiness.process)
}

func managedCatalogOptions(store provisioner.TrinoCellLifecycleStore, gateway trinoManagedGatewayReader, group string, clients map[string]provisioner.TrinoCatalogClient, process func(context.Context, string, string) (string, string, error)) *provisioner.TrinoManagedCatalogOpts {
	return &provisioner.TrinoManagedCatalogOpts{
		Store: store,
		Active: func(ctx context.Context) (*provisioner.TrinoManagedBackend, error) {
			observation, err := gateway.Observe(ctx, group)
			if err != nil {
				return nil, err
			}
			client := clients[observation.Route.BackendName]
			if client == nil {
				return nil, errors.New("active Gateway route is outside the registered cell")
			}
			backend, err := gateway.Backend(ctx, observation.Route.BackendName)
			if err != nil || backend == nil || backend.State != "ACTIVE" || backend.Incarnation != observation.Route.BackendIncarnation {
				return nil, errors.New("active Gateway backend is not available for provisioning")
			}
			return &provisioner.TrinoManagedBackend{Name: observation.Route.BackendName, Catalog: client}, nil
		},
		Target: func(ctx context.Context, freeze *configstore.TrinoCellFreeze) (*provisioner.TrinoManagedBackend, error) {
			observation, err := gateway.Observe(ctx, group)
			if err != nil {
				return nil, err
			}
			op := observation.Rollout
			if op == nil || op.OperationID != freeze.OperationID || op.Plan.PlanHash != freeze.PlanHash || op.Plan.TargetBackend != freeze.TargetBackend {
				return nil, errors.New("frozen cell differs from Gateway rollout")
			}
			if op.Phase == "CLAIMED" {
				return nil, nil
			}
			if op.Phase != "WARMED" && op.Phase != "VERIFIED" {
				return nil, errors.New("Gateway rollout cannot prepare a target in this phase")
			}
			if observation.Route.Generation != op.Plan.ExpectedRouteGeneration || observation.Route.BackendName != op.Plan.SourceBackend || observation.Route.BackendIncarnation != op.Plan.SourceIncarnation || clients[freeze.TargetBackend] == nil {
				return nil, errors.New("Gateway source changed before target certification")
			}
			return &provisioner.TrinoManagedBackend{Name: freeze.TargetBackend, Catalog: clients[freeze.TargetBackend]}, nil
		},
		TargetProcess: func(ctx context.Context, backend string) (string, string, error) { return process(ctx, group, backend) },
	}
}

// process binds live coordinator identity to the registered pod inventory and canary.
func (h *trinoRolloutReadinessHandler) process(ctx context.Context, group, backend string) (string, string, error) {
	color, ok := strings.CutPrefix(backend, group+"-")
	slot, exists := h.slots[group+"/"+color]
	if !ok || !exists || slot.backendName != backend {
		return "", "", errors.New("unknown registered rollout target")
	}
	select {
	case h.limit <- struct{}{}:
		defer func() { <-h.limit }()
	default:
		return "", "", errors.New("rollout readiness busy")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	selector := labels.Set{"posthog.com/trino-cell": slot.cell, "posthog.com/trino-color": slot.color}.String()
	pods, err := h.kube.CoreV1().Pods(slot.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 1001})
	if err != nil || pods == nil || pods.Continue != "" || len(pods.Items) > 1000 {
		return "", "", errors.New("rollout pod inventory unavailable")
	}
	inventory, err := rolloutPodInventory(pods.Items)
	if err != nil || inventory.Terminating != 0 || inventory.Coordinators != 1 || inventory.ReadyCoordinators != 1 || inventory.Workers == 0 || inventory.ReadyWorkers != inventory.Workers {
		return "", "", errors.New("rollout target pods not ready")
	}
	facts, err := h.probe(ctx, slot)
	if err != nil || facts == nil || facts.RegisteredWorkers != inventory.Workers || !rolloutMembersMatchPods(facts.members, pods.Items) || ctx.Err() != nil {
		return "", "", errors.New("rollout target process not ready")
	}
	return facts.NodeID, facts.CoordinatorID, nil
}
