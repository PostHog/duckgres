//go:build kubernetes

package controlplane

import (
	"errors"
	"os"
	"strings"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioning"
)

// A mounted token file lets deployments keep this validation-only credential
// separate from the fleet-admin secret. Blank configuration disables the route.
func loadTrinoServiceAuthSecret(adminTokens, readOnlyTokens admin.TokenSet) (string, error) {
	path := strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE"))
	if path == "" {
		return "", nil
	}
	value, err := os.ReadFile(path)
	if err != nil {
		return "", errors.New("cannot read Trino service auth secret file")
	}
	tokens := strings.Fields(string(value))
	if len(tokens) == 0 {
		return "", errors.New("Trino service auth secret file is empty")
	}
	if len(value) > 16*1024 || len(tokens) > 4 {
		return "", errors.New("Trino service auth secret file exceeds supported token rotation bounds")
	}
	for _, token := range tokens {
		if len(token) < 32 {
			return "", errors.New("Trino service auth secret must be at least 32 bytes")
		}
		if adminTokens.Valid(token) || readOnlyTokens.Valid(token) {
			return "", errors.New("Trino service auth secret must differ from admin and discovery secrets")
		}
	}
	return strings.Join(tokens, "\n"), nil
}

func (f trinoFleet) serviceCredentialConnect(store interface {
	GetManagedWarehouseTrino(string) (*configstore.ManagedWarehouseTrino, error)
	GetOrg(string) (*configstore.Org, error)
}) func(string, string) *provisioning.TrinoServiceCredentialConnect {
	return func(orgID, credentialID string) *provisioning.TrinoServiceCredentialConnect {
		row, err := store.GetManagedWarehouseTrino(orgID)
		if err != nil || row == nil || !row.Enabled || row.State != configstore.ManagedWarehouseStateReady {
			return nil
		}
		wire := f.byStoredID(row.TrinoCellID)
		if wire == nil || wire.Console == nil {
			return nil
		}
		org, err := store.GetOrg(orgID)
		if err != nil || org == nil || configstore.ValidateDatabaseName(org.DatabaseName) != nil {
			return nil
		}
		connection := wire.Console.Cell.ServiceCredentialConnection(org.DatabaseName, credentialID)
		if connection == nil {
			return nil
		}
		return &provisioning.TrinoServiceCredentialConnect{
			Host: connection.Host, Port: connection.Port, Username: connection.Username,
			Catalog: configstore.TrinoCatalogName(org.DatabaseName), HTTPScheme: "https",
		}
	}
}
