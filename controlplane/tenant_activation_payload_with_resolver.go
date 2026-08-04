//go:build kubernetes

package controlplane

import (
	"context"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"k8s.io/client-go/kubernetes"
)

// BuildTenantActivationPayloadWithResolver builds a tenant activation payload
// when the caller already owns the Kubernetes and runtime resolver dependencies.
func BuildTenantActivationPayloadWithResolver(
	ctx context.Context,
	clientset kubernetes.Interface,
	defaultNamespace string,
	org *configstore.OrgConfig,
	stsBroker *STSBroker,
	defaultSpecVersion string,
	resolveDucklingStatus func(context.Context, string) (*provisioner.DucklingStatus, error),
) (TenantActivationPayload, error) {
	activator := &SharedWorkerActivator{
		clientset:             clientset,
		defaultNamespace:      defaultNamespace,
		defaultSpecVersion:    defaultSpecVersion,
		stsBroker:             stsBroker,
		resolveDucklingStatus: resolveDucklingStatus,
	}
	assignment := &WorkerAssignment{
		OrgID: orgName(org),
	}
	return activator.BuildActivationRequest(ctx, org, assignment)
}
