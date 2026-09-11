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

	"github.com/posthog/duckgres/controlplane/provisioner"
	"k8s.io/apimachinery/pkg/util/validation"
)

const envTrinoCellsFile = "DUCKGRES_TRINO_CELLS_FILE"
const envTrinoRegistryOnly = "DUCKGRES_TRINO_REGISTRY_ONLY"

const registeredTrinoCellPrefix = "registered:"

// resolveTrinoCells preserves legacy unless registry-only mode explicitly excludes it.
func resolveTrinoCells() ([]trinoCell, error) {
	registryOnly := false
	if value := strings.TrimSpace(os.Getenv(envTrinoRegistryOnly)); value != "" {
		var err error
		registryOnly, err = strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("%s must be a boolean", envTrinoRegistryOnly)
		}
	}
	path := strings.TrimSpace(os.Getenv(envTrinoCellsFile))
	var legacy trinoCell
	var cells []trinoCell
	if registryOnly {
		if path == "" {
			return nil, fmt.Errorf("%s requires %s", envTrinoRegistryOnly, envTrinoCellsFile)
		}
		if strings.TrimSpace(os.Getenv(envTrinoCoordinatorURL)) != "" {
			return nil, fmt.Errorf("%s cannot be combined with %s", envTrinoRegistryOnly, envTrinoCoordinatorURL)
		}
	} else {
		var err error
		legacy, err = resolveTrinoCell()
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(legacy.ID, registeredTrinoCellPrefix) {
			return nil, errors.New("legacy Trino cell ID uses the reserved registered prefix")
		}
		cells = append(cells, legacy)
	}
	if path == "" {
		return cells, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read Trino registry: %w", err)
	}
	registered, err := parseTrinoCellRegistry(data)
	if err != nil {
		return nil, err
	}
	legacyNS := legacy.Namespace
	if legacyNS == "" {
		legacyNS = provisioner.TrinoCustomerNamespace
	}
	var legacyEndpoint string
	if !registryOnly {
		legacyEndpoint, err = trinoEndpointKey(legacy.CoordinatorURL)
		if err != nil {
			return nil, fmt.Errorf("legacy coordinator URL: %w", err)
		}
	}
	for _, entry := range registered {
		if !registryOnly && entry.Namespace == legacyNS {
			return nil, errors.New("registered cell must not share the legacy namespace")
		}
		cell := trinoCell{ID: registeredTrinoCellPrefix + entry.ID, PublicID: entry.ID, Namespace: entry.Namespace, ClientURL: entry.ClientURL, Backends: entry.Backends}
		for _, backend := range entry.Backends {
			endpoint, _ := trinoEndpointKey(backend.CoordinatorURL)
			if !registryOnly && endpoint == legacyEndpoint {
				return nil, errors.New("registered cell must not share a legacy coordinator")
			}
			if backend.RoutingActive {
				cell.CoordinatorURL, cell.TLSServerName = backend.CoordinatorURL, backend.TLSServerName
			}
		}
		cells = append(cells, cell)
	}
	return cells, nil
}

type trinoRegisteredCell struct {
	ID           string                   `json:"id"`
	Namespace    string                   `json:"namespace"`
	ClientURL    string                   `json:"client_url"`
	RoutingGroup string                   `json:"routing_group"`
	Backends     []trinoRegisteredBackend `json:"backends"`
}

type trinoRegisteredBackend struct {
	ID                 string `json:"id"`
	CoordinatorURL     string `json:"coordinator_url"`
	TLSServerName      string `json:"tls_server_name,omitempty"`
	Running            bool   `json:"running"`
	RoutingActive      bool   `json:"routing_active"`
	InternalSecretName string `json:"internal_secret_name"`
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
		return nil, errors.New("Trino cell registry supports at most 16 additional cells")
	}
	identities, namespaces, groups, endpoints := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, cell := range registry.Cells {
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
		if _, err := trinoEndpointKey(cell.ClientURL); err != nil {
			return nil, fmt.Errorf("Trino cell %s client URL: %w", cell.ID, err)
		}
		backendIDs, secrets := map[string]bool{}, map[string]bool{}
		if len(cell.Backends) > 2 {
			return nil, fmt.Errorf("Trino cell %s supports at most two backends", cell.ID)
		}
		active := 0
		for _, backend := range cell.Backends {
			if len(validation.IsDNS1123Label(backend.ID)) != 0 || backendIDs[backend.ID] {
				return nil, fmt.Errorf("Trino cell %s has an invalid or duplicate backend identity", cell.ID)
			}
			backendIDs[backend.ID] = true
			if len(validation.IsDNS1123Subdomain(backend.InternalSecretName)) != 0 || secrets[backend.InternalSecretName] {
				return nil, fmt.Errorf("Trino cell %s has an invalid or shared internal secret reference", cell.ID)
			}
			secrets[backend.InternalSecretName] = true
			endpoint, err := trinoEndpointKey(backend.CoordinatorURL)
			if err != nil {
				return nil, fmt.Errorf("Trino cell %s backend %s URL: %w", cell.ID, backend.ID, err)
			}
			if endpoints[endpoint] {
				return nil, errors.New("Trino backends must have distinct coordinator endpoints")
			}
			endpoints[endpoint] = true
			if backend.TLSServerName != "" && len(validation.IsDNS1123Subdomain(backend.TLSServerName)) != 0 {
				return nil, fmt.Errorf("Trino cell %s backend %s has an invalid TLS server name", cell.ID, backend.ID)
			}
			if backend.RoutingActive {
				if !backend.Running {
					return nil, fmt.Errorf("Trino cell %s routes to a stopped backend", cell.ID)
				}
				active++
			}
		}
		if active != 1 {
			return nil, fmt.Errorf("Trino cell %s must have exactly one routing-active backend", cell.ID)
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
