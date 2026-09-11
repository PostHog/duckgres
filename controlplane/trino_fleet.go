//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
	"github.com/posthog/duckgres/controlplane/provisioning"
	"k8s.io/client-go/kubernetes"
)

type trinoFleet []*trinoWiring

func buildTrinoFleetWiring(store trinoWiringStore, kc kubernetes.Interface, ducklings provisioner.TrinoDucklingResolver) (trinoFleet, error) {
	if !trinoProvisionerEnabled() {
		return nil, nil
	}
	cells, err := resolveTrinoCells()
	if err != nil {
		return nil, err
	}
	fleet := make(trinoFleet, 0, len(cells))
	for _, cell := range cells {
		wire, err := buildTrinoCellWiring(store, kc, ducklings, cell)
		if err != nil {
			return nil, fmt.Errorf("wire Trino cell %s: %w", cell.consoleCell().ID, err)
		}
		fleet = append(fleet, wire)
	}
	return fleet, nil
}

// enablementCheck requires explicit placement when no legacy provisioner exists.
func (f trinoFleet) enablementCheck(store interface {
	GetManagedWarehouseTrino(string) (*configstore.ManagedWarehouseTrino, error)
}) func(string) error {
	if len(f) == 0 {
		return nil
	}
	owners := make(map[string]bool, len(f))
	for _, wire := range f {
		if wire.Cell.PublicID == "" {
			return nil
		}
		owners[wire.Cell.ID] = true
	}
	return func(orgID string) error {
		row, err := store.GetManagedWarehouseTrino(orgID)
		if err != nil {
			return err
		}
		if row == nil || row.TrinoCellID == "" {
			return provisioning.ErrTrinoCellSelectionRequired
		}
		if !owners[row.TrinoCellID] {
			return provisioning.ErrTrinoCellNotConfigured
		}
		return nil
	}
}

// Reconcile isolates cell failures and bounds each cell's external API work.
// Config-store methods retain their existing database timeout behavior.
func (f trinoFleet) Reconcile(ctx context.Context) error {
	errs := make([]error, len(f))
	var wg sync.WaitGroup
	for i, wire := range f {
		wg.Go(func() {
			cellCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			if err := wire.Provisioner.Reconcile(cellCtx); err != nil {
				errs[i] = fmt.Errorf("cell %s: %w", wire.Cell.consoleCell().ID, err)
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (f trinoFleet) adminAPI(orgs admin.TrinoOrgStore, audit *admin.AuditStore) *admin.TrinoAPI {
	if len(f) == 0 {
		return nil
	}
	cells := make([]admin.TrinoCell, 0, len(f))
	clients := make([]admin.TrinoCoordinatorClient, 0, len(f))
	for _, wire := range f {
		cells = append(cells, wire.Console.Cell)
		clients = append(clients, wire.Console.Observer)
	}
	return admin.NewTrinoFleetAPI(cells, clients, orgs, audit)
}

func (w *trinoWiring) bundlePath() string {
	if w.Cell.PublicID == "" {
		return "/bundles/trino"
	}
	return "/bundles/trino/" + w.Cell.PublicID
}
