//go:build kubernetes

package controlplane

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioning"
)

// A mounted token file lets deployments keep this validation-only credential
// separate from the fleet-admin secret. Blank configuration disables the route.
func (f trinoFleet) loadTrinoServiceAuthCells(adminTokens, readOnlyTokens admin.TokenSet) ([]provisioning.TrinoServiceAuthCell, error) {
	path := strings.TrimSpace(os.Getenv("DUCKGRES_TRINO_SERVICE_AUTH_SECRET_FILE"))
	if path == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot read Trino service auth secret file")
	}
	defer func() { _ = file.Close() }()
	const maxFileBytes = 64 * 1024
	value, err := io.ReadAll(io.LimitReader(file, maxFileBytes+1))
	if err != nil || len(value) > maxFileBytes {
		return nil, errors.New("cannot read Trino service auth secret file within size limit")
	}
	var config struct {
		Cells []provisioning.TrinoServiceAuthCell `json:"cells"`
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, errors.New("invalid Trino service auth cell configuration")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF || len(config.Cells) == 0 {
		return nil, errors.New("invalid Trino service auth cell configuration")
	}
	seenCells := make(map[string]bool)
	seenTokens := make(map[string]bool)
	for _, cell := range config.Cells {
		if cell.CellID == "" || f.byStoredID(cell.CellID) == nil || seenCells[cell.CellID] {
			return nil, errors.New("Trino service auth requires unique configured stored cell IDs")
		}
		seenCells[cell.CellID] = true
		if len(cell.Tokens) == 0 || len(cell.Tokens) > 4 {
			return nil, errors.New("Trino service auth requires one to four tokens per cell")
		}
		for _, token := range cell.Tokens {
			if len(token) < 32 || strings.ContainsAny(token, " \t\n\r") {
				return nil, errors.New("Trino service auth tokens require at least 32 bytes without whitespace")
			}
			if adminTokens.Valid(token) || readOnlyTokens.Valid(token) {
				return nil, errors.New("Trino service auth secret must differ from admin and discovery secrets")
			}
			if seenTokens[token] {
				return nil, errors.New("Trino service auth tokens must be unique")
			}
			seenTokens[token] = true
		}
	}
	return config.Cells, nil
}

func (f trinoFleet) serviceCredentialConnect(store interface {
	GetManagedWarehouseTrino(string) (*configstore.ManagedWarehouseTrino, error)
	GetOrg(string) (*configstore.Org, error)
}, cells []provisioning.TrinoServiceAuthCell) func(string, string) *provisioning.TrinoServiceCredentialConnect {
	enabledCells := make(map[string]bool, len(cells))
	for _, cell := range cells {
		enabledCells[cell.CellID] = true
	}
	return func(orgID, credentialID string) *provisioning.TrinoServiceCredentialConnect {
		row, err := store.GetManagedWarehouseTrino(orgID)
		if err != nil || row == nil || !row.Enabled || row.State != configstore.ManagedWarehouseStateReady || !enabledCells[row.TrinoCellID] {
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
