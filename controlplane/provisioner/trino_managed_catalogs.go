//go:build kubernetes

package provisioner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/posthog/duckgres/controlplane/configstore"
)

type TrinoCellLifecycleStore interface {
	BeginTrinoCellReconcile(context.Context, string, string) (*configstore.TrinoCellLease, bool, error)
	GetTrinoCellLifecycle(context.Context, string) (*configstore.TrinoCellLifecycleStatus, error)
	SetTrinoCellIntent(context.Context, configstore.TrinoCellLease, configstore.TrinoCatalogIntent) error
	ClearTrinoCellIntent(context.Context, configstore.TrinoCellLease, string) error
	FinishTrinoCellReconcile(context.Context, configstore.TrinoCellLease) error
	CertifyTrinoCellTarget(context.Context, configstore.TrinoCellLease, string, configstore.TrinoCellCertificate) error
	UpdateManagedTrinoState(context.Context, configstore.TrinoCellLease, string, configstore.TrinoStateUpdate) (bool, error)
	ListAdmittedTrinoOrgs(context.Context, string) ([]configstore.TrinoEnabledOrg, error)
}

type TrinoManagedBackend struct {
	Name    string
	Catalog TrinoCatalogClient
}

type TrinoManagedCatalogOpts struct {
	Paused         bool
	Store          TrinoCellLifecycleStore
	CatalogClients []TrinoCatalogClient
	Active         func(context.Context) (*TrinoManagedBackend, error)
	Target         func(context.Context, *configstore.TrinoCellFreeze) (*TrinoManagedBackend, error)
	TargetProcess  func(context.Context, string) (nodeID, coordinatorID string, err error)
}

// ConfigureManagedCatalogs runs before the provisioner loop starts.
func (p *TrinoProvisioner) ConfigureManagedCatalogs(opts *TrinoManagedCatalogOpts) error {
	if opts == nil || !p.explicitAssignmentOnly || !strings.HasPrefix(p.cellID, "registered:") || len(p.additionalCatalogs) != 0 {
		return errors.New("managed catalogs require an explicitly assigned registered cell without static extra writers")
	}
	if !opts.Paused {
		if opts.Store == nil || opts.Active == nil || opts.Target == nil || opts.TargetProcess == nil || len(opts.CatalogClients) != 2 {
			return errors.New("managed catalogs require lifecycle, Gateway, target readiness and both strict clients")
		}
		for _, client := range opts.CatalogClients {
			if client == nil {
				return errors.New("managed catalog client missing")
			}
		}
	}
	p.managed = opts
	return nil
}

type trinoManagedCatalogClient struct {
	TrinoCatalogClient
	store    TrinoCellLifecycleStore
	lease    configstore.TrinoCellLease
	backend  string
	sequence int64
}

func (c *trinoManagedCatalogClient) mutate(ctx context.Context, action, name string, submit func() error) error {
	intent := configstore.TrinoCatalogIntent{ID: uuid.NewString(), Sequence: c.sequence + 1, Backend: c.backend, Action: action, Catalog: name}
	if err := c.store.SetTrinoCellIntent(ctx, c.lease, intent); err != nil {
		return err
	}
	c.sequence = intent.Sequence
	err := submit()
	if !TrinoCatalogOutcomeTerminal(err) {
		return err
	}
	clearCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if clearErr := c.store.ClearTrinoCellIntent(clearCtx, c.lease, intent.ID); clearErr != nil {
		return errors.Join(err, clearErr)
	}
	return err
}

func (c *trinoManagedCatalogClient) CreateCatalog(ctx context.Context, name string, props map[string]string) error {
	return c.mutate(ctx, "create", name, func() error { return c.TrinoCatalogClient.CreateCatalog(ctx, name, props) })
}

func (c *trinoManagedCatalogClient) DropCatalog(ctx context.Context, name string) error {
	return c.mutate(ctx, "drop", name, func() error { return c.TrinoCatalogClient.DropCatalog(ctx, name) })
}

func (*trinoManagedCatalogClient) AlterCatalog(context.Context, string, map[string]string) error {
	return errors.New("managed catalog alteration requires an explicit migration")
}

func (p *TrinoProvisioner) managedCatalogs(ctx context.Context, lease configstore.TrinoCellLease, orgs []configstore.TrinoEnabledOrg, tenants tenantSecretProjection) (map[string]catalogOutcome, error) {
	state, err := p.managed.Store.GetTrinoCellLifecycle(ctx, p.cellID)
	if err != nil {
		return nil, err
	}
	if state.Freeze != nil {
		return nil, p.prepareManagedTarget(ctx, lease, state.Freeze, tenants)
	}
	backend, err := p.managed.Active(ctx)
	if err != nil {
		return nil, err
	}
	if backend == nil || backend.Catalog == nil || backend.Name == "" {
		return nil, errors.New("managed active backend unavailable")
	}
	pending, err := p.reconcileBackendReadiness(ctx, backend.Catalog, tenants.data)
	if err != nil {
		return nil, err
	}
	if len(pending) != 0 {
		outcomes := make(map[string]catalogOutcome, len(orgs))
		for _, org := range orgs {
			outcomes[org.OrgID] = catalogOutcome{Pending: true, PendingReason: "waiting for mounted credentials before catalog mutation"}
		}
		return outcomes, nil
	}
	client := &trinoManagedCatalogClient{TrinoCatalogClient: backend.Catalog, store: p.managed.Store, lease: lease, backend: backend.Name, sequence: lease.IntentSequence}
	return p.reconcileBoundedBackend(ctx, orgs, tenants, client)
}

func (p *TrinoProvisioner) prepareManagedTarget(ctx context.Context, lease configstore.TrinoCellLease, freeze *configstore.TrinoCellFreeze, tenants tenantSecretProjection) error {
	if !freeze.Stable || freeze.Certificate != nil {
		return nil
	}
	orgs, err := p.managed.Store.ListAdmittedTrinoOrgs(ctx, p.cellID)
	if err != nil {
		return err
	}
	backend, err := p.managed.Target(ctx, freeze)
	if err != nil {
		return err
	}
	if backend == nil {
		return nil
	}
	if backend.Name != freeze.TargetBackend {
		return errors.New("managed target differs from frozen target")
	}
	inventory, ok := backend.Catalog.(interface {
		CatalogStates(context.Context) (map[string]string, error)
	})
	if !ok {
		return errors.New("managed target lacks strict catalog inventory")
	}
	node, coordinator, err := p.managed.TargetProcess(ctx, backend.Name)
	if err != nil {
		return err
	}
	states, err := inventory.CatalogStates(ctx)
	if err != nil {
		return err
	}
	var roster []string
	expected := make(map[string][]byte)
	for _, org := range orgs {
		name := TrinoCatalogName(org.TrinoPrincipal())
		if org.TrinoPrincipal() == "" || states[name] != "OPERATIONAL" || !tenants.projected[org.OrgID] || (!isManagedHoglake(org) && len(tenants.data[org.OrgID]) == 0) {
			return errors.New("managed target is missing an admitted catalog or credential")
		}
		if isManagedHoglake(org) {
			if err := verifyHoglakeConnector(ctx, backend.Catalog, name); err != nil {
				return err
			}
			if p.managedHoglake == nil {
				return errors.New("managed Hoglake is not configured")
			}
			warehouse, err := p.warehouses.GetManagedWarehouseForTrino(org.OrgID)
			if err != nil {
				return err
			}
			if warehouse == nil {
				return errors.New("managed Hoglake warehouse identity unavailable")
			}
			if err := p.managedHoglake.ensure(ctx, org.OrgID, warehouse.DucklingName); err != nil {
				return err
			}
		}
		roster = append(roster, org.OrgID+"\x00"+org.TrinoPrincipal()+"\x00"+name)
		expected[org.OrgID] = tenants.data[org.OrgID]
	}
	pending, err := p.reconcileBackendReadiness(ctx, backend.Catalog, expected)
	if err != nil || len(pending) != 0 {
		return errors.New("managed target credential projection is not ready")
	}
	afterNode, afterCoordinator, err := p.managed.TargetProcess(ctx, backend.Name)
	if err != nil || node != afterNode || coordinator != afterCoordinator {
		return errors.New("managed target process changed during certification")
	}
	sort.Strings(roster)
	encoded, err := json.Marshal(roster)
	if err != nil {
		return fmt.Errorf("encode admitted catalog roster: %w", err)
	}
	hash := sha256.Sum256(encoded)
	return p.managed.Store.CertifyTrinoCellTarget(ctx, lease, freeze.OperationID, configstore.TrinoCellCertificate{TargetBackend: backend.Name, NodeID: node, CoordinatorID: coordinator, RosterHash: hex.EncodeToString(hash[:]), AdmittedCount: len(roster)})
}
