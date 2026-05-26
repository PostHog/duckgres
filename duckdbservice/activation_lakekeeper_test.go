package duckdbservice

import (
	"testing"

	"github.com/posthog/duckgres/server"
)

// TestSameTenantActivationRuntime_LakekeeperFields locks in the cross-PR fix
// that extends the same-tenant equality check to include the Lakekeeper-side
// identity fields. Without these in the check, a hot-idle worker activated
// before Lakekeeper provisioning completed would be reclaimed for the same
// org without forcing a fresh ATTACH — the worker would keep running with no
// iceberg catalog attached even though the new payload now carries the
// provisioned endpoint.
func TestSameTenantActivationRuntime_LakekeeperFields(t *testing.T) {
	base := ActivationPayload{
		OrgID: "acme",
		Iceberg: server.IcebergConfig{
			Enabled: true,
			Backend: server.IcebergConfig{}.Backend, // empty
		},
	}

	cases := []struct {
		name        string
		mutate      func(p *ActivationPayload)
		wantSameRun bool
	}{
		{
			name:        "identical payload is same-tenant",
			mutate:      func(p *ActivationPayload) {},
			wantSameRun: true,
		},
		{
			name: "Backend differs → not same",
			mutate: func(p *ActivationPayload) {
				p.Iceberg.Backend = "lakekeeper"
			},
			wantSameRun: false,
		},
		{
			name: "LakekeeperEndpoint added → not same (provisioning completed mid-flight)",
			mutate: func(p *ActivationPayload) {
				p.Iceberg.LakekeeperEndpoint = "http://lk-acme.lakekeeper.svc:8181/catalog"
			},
			wantSameRun: false,
		},
		{
			name: "LakekeeperWarehouse differs → not same",
			mutate: func(p *ActivationPayload) {
				p.Iceberg.LakekeeperWarehouse = "org-acme"
			},
			wantSameRun: false,
		},
		{
			name: "LakekeeperClientID differs → not same",
			mutate: func(p *ActivationPayload) {
				p.Iceberg.LakekeeperClientID = "duckling-acme"
			},
			wantSameRun: false,
		},
		{
			name: "LakekeeperOAuth2ServerURI differs → not same (OIDC flip)",
			mutate: func(p *ActivationPayload) {
				p.Iceberg.LakekeeperOAuth2ServerURI = "http://127.0.0.1:9876/token"
			},
			wantSameRun: false,
		},
		{
			name: "LakekeeperMetadataDSN differs → not same",
			mutate: func(p *ActivationPayload) {
				p.Iceberg.LakekeeperMetadataDSN = "postgres://lakekeeper_acme:pw@metadata/lakekeeper_acme"
			},
			wantSameRun: false,
		},
		{
			name: "LakekeeperClientSecret intentionally NOT compared",
			mutate: func(p *ActivationPayload) {
				// The client_secret is rotated only via re-provisioning,
				// which already changes other fields. Including it would
				// also force a useless reactivation if the activator
				// re-resolves the same SecretRef across two activations.
				p.Iceberg.LakekeeperClientSecret = "different-but-shouldn't-matter"
			},
			wantSameRun: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			next := base
			c.mutate(&next)
			got := sameTenantActivationRuntime(base, next)
			if got != c.wantSameRun {
				t.Errorf("sameTenantActivationRuntime() = %v, want %v\nbase iceberg: %+v\nnext iceberg: %+v",
					got, c.wantSameRun, base.Iceberg, next.Iceberg)
			}
		})
	}
}
