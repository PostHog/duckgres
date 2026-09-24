//go:build kubernetes

package controlplane

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioning"
)

func TestTrinoServiceAuthSecretConfiguration(t *testing.T) {
	t.Setenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE", "")
	adminSecret := strings.Repeat("a", 32)
	adminTokens := admin.NewTokenSet(adminSecret, nil)
	readOnlyTokens := admin.NewTokenSet(strings.Repeat("d", 32), nil)
	fleet := trinoFleet{&trinoWiring{Cell: trinoCell{ID: "registered:cell-a"}}, &trinoWiring{Cell: trinoCell{ID: "legacy-stored-id"}}}
	if got, err := fleet.loadTrinoServiceAuthCells(adminTokens, readOnlyTokens); err != nil || len(got) != 0 {
		t.Fatalf("disabled: %v %v", got, err)
	}
	path := filepath.Join(t.TempDir(), "tokens.json")
	t.Setenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE", path)
	good := strings.Repeat("b", 32)
	cell := func(id string, tokens ...string) provisioning.TrinoServiceAuthCell {
		return provisioning.TrinoServiceAuthCell{CellID: id, Tokens: tokens}
	}
	for _, tc := range []struct {
		name  string
		cells []provisioning.TrinoServiceAuthCell
		valid bool
	}{
		{"rotation", []provisioning.TrinoServiceAuthCell{cell("registered:cell-a", good, strings.Repeat("c", 32)), cell("legacy-stored-id", strings.Repeat("e", 32))}, true},
		{"empty", nil, false},
		{"short", []provisioning.TrinoServiceAuthCell{cell("registered:cell-a", "short")}, false},
		{"no tokens", []provisioning.TrinoServiceAuthCell{cell("registered:cell-a")}, false},
		{"admin", []provisioning.TrinoServiceAuthCell{cell("registered:cell-a", adminSecret)}, false},
		{"discovery", []provisioning.TrinoServiceAuthCell{cell("registered:cell-a", strings.Repeat("d", 32))}, false},
		{"unknown cell", []provisioning.TrinoServiceAuthCell{cell("unknown", good)}, false},
		{"public legacy alias", []provisioning.TrinoServiceAuthCell{cell("legacy", good)}, false},
		{"empty cell", []provisioning.TrinoServiceAuthCell{cell("", good)}, false},
		{"duplicate cells", []provisioning.TrinoServiceAuthCell{cell("registered:cell-a", good), cell("registered:cell-a", strings.Repeat("e", 32))}, false},
		{"shared token", []provisioning.TrinoServiceAuthCell{cell("registered:cell-a", good), cell("legacy-stored-id", good)}, false},
		{"too many rotations", []provisioning.TrinoServiceAuthCell{cell("registered:cell-a", good, good, good, good, good)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := json.Marshal(struct {
				Cells []provisioning.TrinoServiceAuthCell `json:"cells"`
			}{tc.cells})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, value, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = fleet.loadTrinoServiceAuthCells(adminTokens, readOnlyTokens)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
	for _, invalid := range []string{strings.Repeat("x", 65537), `{"cells":[],"unknown":true}`, `{"cells":[]} {}`} {
		if err := os.WriteFile(path, []byte(invalid), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := fleet.loadTrinoServiceAuthCells(adminTokens, readOnlyTokens); err == nil {
			t.Fatal("invalid token file accepted")
		}
	}
}

type serviceCredentialTargetStore struct {
	row *configstore.ManagedWarehouseTrino
	org *configstore.Org
}

func (s serviceCredentialTargetStore) GetManagedWarehouseTrino(string) (*configstore.ManagedWarehouseTrino, error) {
	return s.row, nil
}
func (s serviceCredentialTargetStore) GetOrg(string) (*configstore.Org, error) { return s.org, nil }

func TestTrinoServiceCredentialTargetUsesOwningCell(t *testing.T) {
	const grant = "svc_0123456789abcdef01234567"
	store := serviceCredentialTargetStore{row: &configstore.ManagedWarehouseTrino{Enabled: true, State: configstore.ManagedWarehouseStateReady, TrinoCellID: "cell-a"}, org: &configstore.Org{Name: "org-a", DatabaseName: "acme"}}
	fleet := trinoFleet{&trinoWiring{Cell: trinoCell{ID: "cell-a"}, Console: &trinoConsoleWiring{Cell: admin.TrinoCell{ID: "cell-a", ClientURL: "https://{database_name}.warehouse.example.com"}}}}
	connect := fleet.serviceCredentialConnect(store, []provisioning.TrinoServiceAuthCell{{CellID: "cell-a"}})("org-a", grant)
	if connect == nil || connect.Host != "acme.warehouse.example.com" || connect.Username != grant || connect.Catalog != "org_acme" || connect.Port != 443 || connect.HTTPScheme != "https" {
		t.Fatalf("unexpected connect: %+v", connect)
	}
	fleet[0].Console.Cell.ClientURL = "https://shared.example.com:8443"
	connect = fleet.serviceCredentialConnect(store, []provisioning.TrinoServiceAuthCell{{CellID: "cell-a"}})("org-a", grant)
	if connect == nil || connect.Username != "acme."+grant || connect.Port != 8443 {
		t.Fatalf("unexpected shared connect: %+v", connect)
	}
	store.org.DatabaseName = "root"
	connect = fleet.serviceCredentialConnect(store, []provisioning.TrinoServiceAuthCell{{CellID: "cell-a"}})("org-a", grant)
	if connect == nil || connect.Username != "root."+grant {
		t.Fatalf("shared host mistaken for tenant host: %+v", connect)
	}
	if got := fleet.serviceCredentialConnect(store, nil)("org-a", grant); got != nil {
		t.Fatal("cell without configured service auth advertised a target")
	}
	for _, state := range []configstore.ManagedWarehouseProvisioningState{configstore.ManagedWarehouseStatePending, configstore.ManagedWarehouseStateFailed} {
		store.row.State = state
		if got := fleet.serviceCredentialConnect(store, []provisioning.TrinoServiceAuthCell{{CellID: "cell-a"}})("org-a", grant); got != nil {
			t.Fatalf("unready target: %+v", got)
		}
	}
}
