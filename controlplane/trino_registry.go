//go:build kubernetes

package controlplane

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/posthog/duckgres/controlplane/admin"
	"k8s.io/apimachinery/pkg/util/validation"
)

const envTrinoCellsFile = "DUCKGRES_TRINO_CELLS_FILE"

const registeredTrinoCellPrefix = "registered:"

// resolveTrinoCells loads the shared-pool registry and the initial placement default.
func resolveTrinoCells() ([]trinoCell, string, error) {
	path := strings.TrimSpace(os.Getenv(envTrinoCellsFile))
	if path == "" {
		return nil, "", fmt.Errorf("%s is required for Trino provisioning", envTrinoCellsFile)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("read Trino registry: %w", err)
	}
	registered, err := parseTrinoCellRegistry(data)
	if err != nil {
		return nil, "", err
	}
	cells := make([]trinoCell, 0, len(registered))
	for _, entry := range registered {
		cell := trinoCell{Mode: trinoPoolModeShared, ID: registeredTrinoCellPrefix + entry.ID, PublicID: entry.ID, RoutingGroup: entry.RoutingGroup, Namespace: entry.Namespace, ClientURL: entry.ClientURL}
		if entry.Pool != nil {
			cell.PoolCoordinatorPort = entry.Pool.CoordinatorServicePort
		}
		cells = append(cells, cell)
	}
	defaultCell, err := resolveTrinoDefaultCell(registered)
	return cells, defaultCell, err
}

type trinoRegisteredCell struct {
	// Mode must be shared-pool. Unknown topology fields are rejected.
	Mode         string               `json:"mode,omitempty"`
	Pool         *trinoRegisteredPool `json:"pool,omitempty"`
	ID           string               `json:"id"`
	Namespace    string               `json:"namespace"`
	ClientURL    string               `json:"client_url"`
	RoutingGroup string               `json:"routing_group"`
}

func parseTrinoCellRegistry(data []byte) ([]trinoRegisteredCell, error) {
	var registry struct {
		Cells []trinoRegisteredCell `json:"cells"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&registry); err != nil {
		return nil, fmt.Errorf("decode Trino cell registry: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("Trino cell registry must contain one JSON document")
	}
	if len(registry.Cells) == 0 {
		return nil, errors.New("Trino cell registry must contain at least one cell")
	}
	if len(registry.Cells) > 16 {
		return nil, errors.New("trino cell registry supports at most 16 cells")
	}
	identities, namespaces, groups := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, cell := range registry.Cells {
		if strings.TrimSpace(cell.Mode) != trinoPoolModeShared {
			return nil, errors.New("trino supports shared-pool cells only")
		}

		if cell.ID == "legacy" || len(validation.IsDNS1123Label(cell.ID)) != 0 {
			return nil, errors.New("Trino cell identity must be a DNS label other than legacy")
		}
		if len(validation.IsDNS1123Label(cell.Namespace)) != 0 {
			return nil, fmt.Errorf("Trino cell %s has an invalid namespace", cell.ID)
		}
		if len(validation.IsDNS1123Label(cell.RoutingGroup)) != 0 {
			return nil, fmt.Errorf("Trino cell %s has an invalid routing group", cell.ID)
		}
		if identities[cell.ID] || namespaces[cell.Namespace] || groups[cell.RoutingGroup] {
			return nil, errors.New("Trino cells must have distinct identities, namespaces and routing groups")
		}
		identities[cell.ID], namespaces[cell.Namespace], groups[cell.RoutingGroup] = true, true, true
		clientURL, _, ok := admin.ResolveTrinoClientURL(cell.ClientURL, "org")
		if !ok {
			return nil, fmt.Errorf("Trino cell %s client URL: %s may only be the leading host label", cell.ID, admin.TrinoClientHostPlaceholder)
		}
		if _, err := trinoEndpointKey(clientURL); err != nil {
			return nil, fmt.Errorf("Trino cell %s client URL: %w", cell.ID, err)
		}

	}
	return registry.Cells, nil
}

func trinoEndpointKey(raw string) (string, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint.Scheme != "https" || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return "", errors.New("expected an HTTPS endpoint without credentials, path, query or fragment")
	}
	port := endpoint.Port()
	if port == "" {
		port = "443"
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return "", errors.New("invalid HTTPS endpoint port")
	}
	return net.JoinHostPort(strings.TrimSuffix(strings.ToLower(endpoint.Hostname()), "."), strconv.Itoa(number)), nil
}
