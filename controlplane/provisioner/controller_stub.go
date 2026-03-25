//go:build !kubernetes

package provisioner

import (
	"context"
	"errors"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// Controller is a stub for non-Kubernetes builds.
type Controller struct{}

// NewController returns an error on non-Kubernetes builds since it requires K8s API access.
func NewController(_ *configstore.ConfigStore, _ time.Duration) (*Controller, error) {
	return nil, errors.New("provisioning controller requires kubernetes build tag")
}

// Run is a no-op stub.
func (c *Controller) Run(_ context.Context) {}
