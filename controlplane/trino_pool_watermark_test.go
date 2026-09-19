//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinopool"
)

// A catalog that committed while its revision checkpoint failed must not let a
// tenant be admitted against the older revision.
//
// The failure is specific and silent: the catalog publish commits, the write of
// its revision onto the pool row fails, and NOTHING republishes it - the
// catalog already exists, so no later mutation carries the number forward. The
// admission gate then certifies members against a revision that predates this
// tenant, which is how a warehouse is admitted, and reported ready, without its
// catalog. The tenant loop reads the enabled orgs independently of the
// provisioner's outcome, so it never sees that anything failed.
func TestAdmissionStaysClosedUntilTheCatalogWatermarkIsKnown(t *testing.T) {
	t.Run("an unreadable watermark admits nothing", func(t *testing.T) {
		harness := newOperatorHarness(t)
		harness.operator.config.Pool.TenantAdmission = true
		harness.servingPool(t)
		// The tenant arrives while the catalog store cannot be asked what it
		// has published.
		harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
		harness.operator.catalogWatermark = func(context.Context) (int64, error) {
			return 0, errors.New("catalog store is unreachable")
		}

		harness.tickTolerant(10)

		if admitted := harness.gateway.admitted["org-a"]; admitted != "" {
			t.Fatalf("a tenant was admitted at %q while the published catalog revision was unknown", admitted)
		}
		if row := harness.publications.rows["org-a"]; row != nil && row.State == configstore.TrinoPublicationAdmitted {
			t.Fatal("a tenant was recorded admitted against an unconfirmed catalog revision")
		}
	})

	t.Run("a committed-but-unrecorded revision is recovered", func(t *testing.T) {
		harness := newOperatorHarness(t)
		harness.operator.config.Pool.TenantAdmission = true
		harness.servingPool(t)
		harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
		// The store holds a catalog at revision 7; the pool row never got it.
		harness.operator.catalogWatermark = func(context.Context) (int64, error) { return 7, nil }
		harness.store.pool.PublicationRevision = 3

		harness.tick(t, 10)

		if harness.store.pool.PublicationRevision != 7 {
			t.Fatalf("publication revision = %d, want the store's 7 to be checkpointed",
				harness.store.pool.PublicationRevision)
		}
		if harness.gateway.admitted["org-a"] == "" {
			t.Fatalf("the tenant was never admitted after recovery; calls: %v", harness.gateway.calls)
		}
	})

	t.Run("a watermark that cannot be checkpointed admits nothing", func(t *testing.T) {
		harness := newOperatorHarness(t)
		harness.operator.config.Pool.TenantAdmission = true
		harness.servingPool(t)
		harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
		harness.operator.catalogWatermark = func(context.Context) (int64, error) { return 9, nil }
		harness.store.failRevisionCheckpoint = true

		harness.tickTolerant(10)

		if admitted := harness.gateway.admitted["org-a"]; admitted != "" {
			t.Fatalf("a tenant was admitted at %q while the watermark could not be checkpointed", admitted)
		}
	})

	// Holding admissions must not hold the compute lifecycle: a pool still has
	// to repair and drain while its catalog watermark is in doubt.
	t.Run("the instance lifecycle keeps running", func(t *testing.T) {
		harness := newOperatorHarness(t)
		harness.operator.config.Pool.TenantAdmission = true
		harness.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
		harness.operator.catalogWatermark = func(context.Context) (int64, error) {
			return 0, errors.New("catalog store is unreachable")
		}

		harness.tickTolerant(20)

		serving := 0
		for _, instance := range harness.store.instances {
			if instance.Phase == string(trinopool.PhaseServing) {
				serving++
			}
		}
		if serving == 0 {
			t.Fatalf("no instance reached serving while admissions were held: %v", harness.phases())
		}
		if len(harness.gateway.calls) == 0 {
			t.Fatal("the instance lifecycle made no progress at all")
		}
	})
}

// A checkpoint failure on the publish path is REPORTED, not logged and dropped:
// the catalog is committed, nothing will republish it, and a silent failure
// leaves the gate certifying members against a revision that predates the
// tenant.
func TestPublishReportsAFailedRevisionCheckpoint(t *testing.T) {
	lease := configstore.TrinoPoolLease{PoolID: "registered:cell-001", Owner: "cp-test", Epoch: 4}
	writer := &trinoPoolCatalogWriter{
		cellID:    "registered:cell-001",
		store:     refusingRevisionStore{},
		authority: func() (configstore.TrinoPoolLease, bool) { return lease, true },
	}
	// No database handle: the publish path fails before the store write, which
	// is enough to pin that the checkpoint error is not swallowed - the record
	// call itself is exercised through the operator recovery test above.
	err := writer.checkpoint(context.Background(), 12)
	if err == nil || !strings.Contains(err.Error(), "checkpoint published catalog revision 12") {
		t.Fatalf("checkpoint error = %v, want the failure to be surfaced", err)
	}
}

type refusingRevisionStore struct{}

func (refusingRevisionStore) RecordTrinoPoolPublicationRevision(context.Context, configstore.TrinoPoolLease, string, int64) error {
	return errors.New("refused")
}
