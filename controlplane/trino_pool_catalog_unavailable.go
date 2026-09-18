//go:build kubernetes

package controlplane

import (
	"context"
	"fmt"

	"github.com/posthog/duckgres/controlplane/provisioner"
)

// unavailableTrinoCatalogClient is what a shared-pool cell publishes through
// before its operator has won the pool authority.
//
// It refuses every call rather than silently succeeding. A no-op would be worse
// than an error: the provisioner would record the org as reconciled while
// nothing was published, and the next coordinator to start would serve a
// catalog set missing that tenant. Refusing keeps the org visibly not-ready
// until a control plane actually holds the fence.
type unavailableTrinoCatalogClient struct{ cellID string }

func newUnavailableTrinoCatalogClient(cellID string) provisioner.TrinoCatalogClient {
	return unavailableTrinoCatalogClient{cellID: cellID}
}

// err wraps the provisioner's "not this replica" sentinel. The distinction is
// load-bearing: a refusal here means this control plane does not own the write
// path, NOT that the warehouse is broken, and the reconcile must leave every
// org's Trino state row alone rather than marking the whole pool Failed on every
// replica that is not the leader.
func (c unavailableTrinoCatalogClient) err() error {
	return fmt.Errorf("shared Trino pool %s has no catalog writer on this control plane: %w",
		c.cellID, provisioner.ErrTrinoCatalogNotThisReplica)
}

func (c unavailableTrinoCatalogClient) ListCatalogs(context.Context) ([]string, error) {
	return nil, c.err()
}

func (c unavailableTrinoCatalogClient) ListNodes(context.Context) ([]provisioner.TrinoNode, error) {
	return nil, c.err()
}

func (c unavailableTrinoCatalogClient) CreateCatalog(context.Context, string, map[string]string) error {
	return c.err()
}

func (c unavailableTrinoCatalogClient) AlterCatalog(context.Context, string, map[string]string) error {
	return c.err()
}

func (c unavailableTrinoCatalogClient) DropCatalog(context.Context, string) error {
	return c.err()
}
