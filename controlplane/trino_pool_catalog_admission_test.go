//go:build kubernetes

package controlplane

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoTenantAdmissionWaitsForItsPublishedCatalog(t *testing.T) {
	h := newOperatorHarness(t)
	h.operator.config.Pool.TenantAdmission = true
	h.servingPool(t)
	org := poolOrg("analyst")
	h.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{org}}
	published, applied, included := int64(1), int64(1), false
	h.operator.catalogWatermark = func(context.Context) (int64, error) { return published, nil }
	h.operator.catalogPublicationRevision = func(_ context.Context, catalog string) (int64, bool, error) {
		if catalog != configstore.TrinoCatalogName(org.DatabaseName) {
			t.Fatalf("catalog = %q, want the tenant's catalog", catalog)
		}
		if !included {
			return 0, false, nil
		}
		return published, true, nil
	}
	h.operator.acknowledgement = func(_ context.Context, _ string, _ trinoPoolProjectionRevisions, required int64) (trinoPoolAcknowledgement, error) {
		if required != 2 {
			t.Fatalf("admission requires revision %d, want the revision containing the tenant (2)", required)
		}
		if applied < required {
			return trinoPoolAcknowledgement{}, fmt.Errorf("catalog revision %d is not applied", required)
		}
		return trinoPoolAcknowledgement{ProcessID: "process-1", AppliedRevision: applied, ProjectionCurrent: true}, nil
	}

	h.tick(t, 12)
	if row := h.publications.rows[org.OrgID]; row == nil || row.PublicationID != "" || h.gateway.admitted[org.OrgID] != "" {
		t.Fatalf("tenant without a published catalog opened admission: %+v", row)
	}
	included, published = true, 2
	h.tick(t, 12)
	if h.gateway.admitted[org.OrgID] != "" {
		t.Fatal("tenant admitted before members applied its catalog")
	}
	applied = 2
	h.admitAll(t, 1)
	admitted := h.gateway.admitted[org.OrgID]
	opens := countGatewayCalls(h.gateway.calls, "open:")
	published = 3
	h.tick(t, 12)
	if h.gateway.admitted[org.OrgID] != admitted || countGatewayCalls(h.gateway.calls, "open:") != opens {
		t.Fatal("an unrelated catalog publication re-admitted the tenant")
	}
}

func TestTrinoTenantAdmissionFailsClosedOnCatalogRead(t *testing.T) {
	h := newOperatorHarness(t)
	h.operator.config.Pool.TenantAdmission = true
	h.servingPool(t)
	h.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	h.operator.catalogPublicationRevision = func(context.Context, string) (int64, bool, error) {
		return 0, false, errors.New("catalog store unavailable")
	}
	h.tickTolerant(12)
	if h.gateway.admitted["org-a"] != "" || countGatewayCalls(h.gateway.calls, "open:") != 0 {
		t.Fatal("unreadable tenant catalog allowed admission")
	}
	h.operator.catalogPublicationRevision = nil
	h.tickTolerant(12)
	if h.gateway.admitted["org-a"] != "" || countGatewayCalls(h.gateway.calls, "open:") != 0 {
		t.Fatal("missing tenant catalog reader allowed admission")
	}
}

func TestTrinoUnpublishedTenantDoesNotBlockAnotherTenant(t *testing.T) {
	h := newOperatorHarness(t)
	h.operator.config.Pool.TenantAdmission = true
	h.servingPool(t)
	absent, ready := poolOrg("analyst"), poolOrg("engineer")
	ready.OrgID, ready.DatabaseName = "org-b", "warehouse_b"
	h.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{absent, ready}}
	h.operator.catalogPublicationRevision = func(_ context.Context, catalog string) (int64, bool, error) {
		if catalog == configstore.TrinoCatalogName(ready.DatabaseName) {
			return 2, true, nil
		}
		return 0, false, nil
	}
	h.tick(t, 30)
	if h.gateway.admitted[absent.OrgID] != "" || h.gateway.admitted[ready.OrgID] == "" {
		t.Fatalf("admissions = %v, want only the tenant with a published catalog", h.gateway.admitted)
	}
}

func TestTrinoAdmissionRechecksCatalogAfterLeaderRestart(t *testing.T) {
	for _, opened := range []bool{false, true} {
		t.Run(fmt.Sprintf("gateway_open=%t", opened), func(t *testing.T) {
			h := newOperatorHarness(t)
			h.operator.config.Pool.TenantAdmission = true
			h.servingPool(t)
			h.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
			for tick := 0; tick < 10 && countGatewayCalls(h.gateway.calls, "open:") == 0; tick++ {
				h.tick(t, 1)
			}
			row := h.publications.rows["org-a"]
			if row == nil || row.PublicationID == "" {
				t.Fatal("expected an open admission attempt")
			}
			if !opened {
				delete(h.gateway.publications, row.PublicationID)
			}
			successor := newOperatorHarness(t)
			successor.store, successor.publications, successor.gateway = h.store, h.publications, h.gateway
			successor.operator.store, successor.operator.publications, successor.operator.gateway = h.store, h.publications, h.gateway
			successor.operator.tenants = h.operator.tenants
			successor.operator.config.Pool.TenantAdmission = true
			available := false
			successor.operator.catalogPublicationRevision = func(context.Context, string) (int64, bool, error) {
				if available {
					return 2, true, nil
				}
				return 0, false, nil
			}
			successor.tick(t, 12)
			if successor.gateway.admitted["org-a"] != "" || successor.publications.rows["org-a"].PublicationID != "" {
				t.Fatal("successor reused admission evidence without a published tenant catalog")
			}
			available = true
			successor.admitAll(t, 1)
		})
	}
}

func TestTrinoAdmissionUsesCatalogSnapshotNewerThanWatermark(t *testing.T) {
	h := newOperatorHarness(t)
	h.operator.config.Pool.TenantAdmission = true
	h.servingPool(t)
	h.operator.tenants = &fakeTenantStore{orgs: []configstore.TrinoEnabledOrg{poolOrg("analyst")}}
	h.operator.catalogWatermark = func(context.Context) (int64, error) { return 1, nil }
	h.operator.catalogPublicationRevision = func(context.Context, string) (int64, bool, error) { return 2, true, nil }
	h.operator.acknowledgement = func(_ context.Context, _ string, _ trinoPoolProjectionRevisions, revision int64) (trinoPoolAcknowledgement, error) {
		if revision != 2 {
			t.Fatalf("required revision = %d, want the snapshot containing the tenant", revision)
		}
		return trinoPoolAcknowledgement{ProcessID: "process-1", AppliedRevision: 2, ProjectionCurrent: true}, nil
	}
	h.admitAll(t, 1)
}
