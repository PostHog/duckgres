//go:build kubernetes

package controlplane

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

func TestTrinoServiceAuthSecretConfiguration(t *testing.T) {
	t.Setenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE", "")
	adminSecret := strings.Repeat("a", 32)
	adminTokens := admin.NewTokenSet(adminSecret, nil)
	readOnlyTokens := admin.NewTokenSet(strings.Repeat("d", 32), nil)
	if got, err := loadTrinoServiceAuthSecret(adminTokens, readOnlyTokens); err != nil || got != "" {
		t.Fatalf("disabled: %q %v", got, err)
	}
	path := filepath.Join(t.TempDir(), "token")
	t.Setenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE", path)
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"", false}, {"short", false}, {adminSecret, false}, {strings.Repeat("d", 32), false},
		{strings.Repeat("b", 32) + "\n" + strings.Repeat("c", 32) + "\n", true},
	} {
		if err := os.WriteFile(path, []byte(tc.value), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := loadTrinoServiceAuthSecret(adminTokens, readOnlyTokens)
		if (err == nil) != tc.valid {
			t.Fatalf("valid=%v error=%v", tc.valid, err)
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
	connect := fleet.serviceCredentialConnect(store)("org-a", grant)
	if connect == nil || connect.Host != "acme.warehouse.example.com" || connect.Username != grant || connect.Catalog != "org_acme" || connect.Port != 443 || connect.HTTPScheme != "https" {
		t.Fatalf("unexpected connect: %+v", connect)
	}
	fleet[0].Console.Cell.ClientURL = "https://shared.example.com:8443"
	connect = fleet.serviceCredentialConnect(store)("org-a", grant)
	if connect == nil || connect.Username != "acme."+grant || connect.Port != 8443 {
		t.Fatalf("unexpected shared connect: %+v", connect)
	}
	store.org.DatabaseName = "root"
	connect = fleet.serviceCredentialConnect(store)("org-a", grant)
	if connect == nil || connect.Username != "root."+grant {
		t.Fatalf("shared host mistaken for tenant host: %+v", connect)
	}
	for _, state := range []configstore.ManagedWarehouseProvisioningState{configstore.ManagedWarehouseStatePending, configstore.ManagedWarehouseStateFailed} {
		store.row.State = state
		if got := fleet.serviceCredentialConnect(store)("org-a", grant); got != nil {
			t.Fatalf("unready target: %+v", got)
		}
	}
}
