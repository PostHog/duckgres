//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
	"github.com/posthog/duckgres/controlplane/trinopool"
	"k8s.io/client-go/kubernetes"
)

// Startup wiring for the shared Trino pool.
//
// The operator is constructed at startup and attached to the EXISTING janitor
// leader lease, exactly like the reshard reconciler and the usage collectors.
// With the feature disabled this returns no operators and nothing changes; with
// the pool enabled but the operator disabled, the loops run in a read-only mode
// that keeps the durable desired state in sync and touches nothing external.
//
// Wiring failures are fatal, for the same reason the existing Trino branch is
// fatal: silently skipping would leave an operator believing a pool is being
// reconciled while nothing is.

// buildTrinoPoolOperators constructs one operator per shared-pool cell.
func buildTrinoPoolOperators(
	store *configstore.ConfigStore,
	clientset kubernetes.Interface,
	observerCredential func() (string, string),
	controlPlaneID string,
) ([]*trinoPoolOperator, error) {
	configs, err := resolveTrinoPoolConfigs()
	if err != nil {
		return nil, err
	}
	if len(configs) == 0 {
		return nil, nil
	}
	if clientset == nil {
		return nil, fmt.Errorf("shared Trino pools are configured but no Kubernetes client is available")
	}
	if observerCredential == nil {
		return nil, fmt.Errorf("shared Trino pools are configured but no operational credential is available")
	}

	operatorEnabled := trinoPoolOperatorEnabled()
	gateway, err := buildTrinoPoolGateway()
	if err != nil {
		return nil, err
	}
	if operatorEnabled && gateway == nil {
		// Without a Gateway the operator could create compute but never admit,
		// drain or retire it. Creating unadmittable instances is worse than
		// refusing to start.
		return nil, fmt.Errorf("%s is enabled but %s is not configured", envTrinoPoolOperatorEnabled, envTrinoPoolGatewayURL)
	}

	operators := make([]*trinoPoolOperator, 0, len(configs))
	for _, config := range configs {
		operator := &trinoPoolOperator{
			config:          config,
			store:           store,
			gateway:         gateway,
			owner:           controlPlaneID,
			operatorEnabled: operatorEnabled,
			newInstanceID:   newTrinoPoolInstanceID,
		}
		shared := trinopool.BlueprintSharedResources{}
		if config.Blueprint != nil {
			shared = config.Blueprint.SharedResources
		}
		namespace := config.Namespace
		operator.kube = func(epoch int64) trinoPoolKube {
			return newTrinoPoolEffects(clientset, namespace, shared, epoch)
		}
		// The probe dials the instance's in-cluster Service while verifying the
		// pool's public certificate name. Those differ by design, so the server
		// name is explicit; it is never disabled.
		client := newRolloutHTTPClient(config.TLSServerName)
		operator.validate = func(ctx context.Context, endpoint string, observedWorkers int) (trinoPoolValidation, error) {
			return validateTrinoPoolCandidate(ctx, client, endpoint, observerCredential, observedWorkers, 0)
		}
		operators = append(operators, operator)

		slog.Info("Shared Trino pool configured.",
			"pool", config.PublicID, "namespace", config.Namespace,
			"desired", config.Spec.DesiredInstances, "minServing", config.Spec.MinServing,
			"operator", operatorEnabled, "frozen", config.Frozen, "reason", config.FrozenReason)
	}
	return operators, nil
}

// buildTrinoPoolGateway builds the pooled-lifecycle client from the existing
// managed-gateway configuration. It returns (nil, nil) when no Gateway is
// configured, which is valid while the operator is disabled.
func buildTrinoPoolGateway() (trinoPoolGateway, error) {
	endpoint := strings.TrimSpace(os.Getenv(envTrinoPoolGatewayURL))
	if endpoint == "" {
		endpoint = strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_MANAGED_GATEWAY_URL"))
	}
	if endpoint == "" {
		return nil, nil
	}
	token, err := readTrinoPoolGatewayToken()
	if err != nil {
		return nil, err
	}
	client, err := trinogateway.NewClient(trinogateway.Config{
		BaseURL:       endpoint,
		AdminToken:    token,
		TLSServerName: strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_MANAGED_GATEWAY_SERVER_NAME")),
	})
	if err != nil {
		return nil, fmt.Errorf("configure shared-pool Gateway client: %w", err)
	}
	return client, nil
}

// readTrinoPoolGatewayToken reuses the existing rollout capability token file.
// The pooled protocol authenticates with the Gateway's existing admin
// credential and introduces no new secret.
func readTrinoPoolGatewayToken() (string, error) {
	path := strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE"))
	if path == "" {
		return "", fmt.Errorf("a shared-pool Gateway is configured but DUCKGRES_TRINO_ROLLOUT_TOKEN_FILE is unset")
	}
	token, err := readRolloutSecretFile(path, 4096)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(token)), nil
}

// attachTrinoPoolOperators registers the reconcile loops under the janitor
// leader lease. One control plane reconciles a pool at a time; the authority
// epoch is what makes that safe rather than merely likely.
func attachTrinoPoolOperators(leader *JanitorLeaderManager, operators []*trinoPoolOperator) {
	if leader == nil {
		return
	}
	for _, operator := range operators {
		leader.AttachLeaderLoop(operator.Run)
	}
}

// trinoPoolClientset reuses the Kubernetes client the Trino fleet already
// built. A pooled cell is a Trino cell, so if the fleet has no client the pool
// cannot have one either, and buildTrinoPoolOperators refuses rather than
// running blind.
func trinoPoolClientset(fleet trinoFleet) kubernetes.Interface {
	for _, wire := range fleet {
		if wire.Kubernetes != nil {
			return wire.Kubernetes
		}
	}
	return nil
}

// trinoPoolObserverCredential returns the cluster's existing read-only operator
// credential. Candidate validation uses THIS, not a canary: the credential
// already exists, is already rotated by the provisioner, and carries no tenant
// data access.
func trinoPoolObserverCredential(fleet trinoFleet) func() (string, string) {
	for _, wire := range fleet {
		if wire.Provisioner != nil {
			return wire.Provisioner.ObserverCredential
		}
	}
	return nil
}
