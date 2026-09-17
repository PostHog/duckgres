//go:build kubernetes

package provisioner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

// TrinoManagedHoglakeConfig is separate from the legacy benchmark switch.
// DataPath reserves a dedicated prefix; each catalog owns its warehouse Duckling-name child.
// Disabling Trino never deletes Hoglake metadata or data.
type TrinoManagedHoglakeConfig struct {
	URI       string
	DataPath  string
	Namespace string
}

var hoglakeIdentifier = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// The trusted, configured service origin is internal infrastructure traffic.
// Keep it off the environment's public egress proxy without changing NO_PROXY.
var managedHoglakeHTTPClient = func() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}()

func (c TrinoManagedHoglakeConfig) Validate() error {
	u, err := url.Parse(c.URI)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawPath != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("managed Hoglake URI must be an HTTP(S) origin without credentials")
	}
	p, err := url.Parse(c.DataPath)
	if err != nil || p.Scheme != "s3" || p.Hostname() == "" || p.Host != p.Hostname() || p.User != nil || p.RawQuery != "" || p.ForceQuery || p.Fragment != "" || p.RawPath != "" || strings.ContainsAny(c.DataPath, "%\t\r\n ") || !strings.HasSuffix(p.Path, "/") || p.Path == "/" || p.Path == "" || strings.Contains(p.Path, "//") {
		return errors.New("managed Hoglake data path must be a dedicated s3://bucket/prefix/ URI")
	}
	for _, part := range strings.Split(strings.Trim(p.Path, "/"), "/") {
		if part == "" || part == "." || part == ".." {
			return errors.New("managed Hoglake data path has an invalid segment")
		}
	}
	if !hoglakeIdentifier.MatchString(c.Namespace) {
		return errors.New("managed Hoglake namespace must contain only letters, digits, underscores or hyphens")
	}
	return nil
}

func (c TrinoManagedHoglakeConfig) catalogPath(orgID string) (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	if !hoglakeIdentifier.MatchString(orgID) {
		return "", errors.New("invalid managed Hoglake organization identifier")
	}
	return c.DataPath + orgID + "/", nil
}

// request never follows redirects, logs response bodies, or retries mutations.
// A following reconciliation resolves uncertain creates with GET.
func (c TrinoManagedHoglakeConfig) request(ctx context.Context, method, path string, body any, out any) (int, error) {
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return 0, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.URI, "/")+path, bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := managedHoglakeHTTPClient.Do(req)
	if err != nil {
		return 0, errors.New("Hoglake API request failed")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK && out != nil {
		b, readErr := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
		if readErr != nil || len(b) > 1<<20 || json.Unmarshal(b, out) != nil {
			return resp.StatusCode, errors.New("invalid Hoglake API response")
		}
	}
	return resp.StatusCode, nil
}

func (c TrinoManagedHoglakeConfig) ensure(ctx context.Context, orgID, storageKey string) error {
	if !hoglakeIdentifier.MatchString(orgID) {
		return errors.New("invalid managed Hoglake catalog name")
	}
	dataPath, err := c.catalogPath(storageKey)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	path := "/v1/catalogs/" + orgID
	var catalog struct {
		Name         string   `json:"name"`
		DataPath     string   `json:"data_path"`
		Capabilities []string `json:"capabilities"`
	}
	status, err := c.request(ctx, http.MethodGet, path, nil, &catalog)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		status, err = c.request(ctx, http.MethodPost, "/v1/catalogs", map[string]string{"name": orgID, "data_path": dataPath}, nil)
		if err != nil {
			return err
		}
		if status != http.StatusCreated && status != http.StatusConflict {
			return fmt.Errorf("Hoglake catalog create returned HTTP %d", status)
		}
		status, err = c.request(ctx, http.MethodGet, path, nil, &catalog)
		if err != nil {
			return err
		}
	}
	if status != http.StatusOK {
		return fmt.Errorf("Hoglake catalog read returned HTTP %d", status)
	}
	if catalog.Name != orgID || catalog.DataPath != dataPath {
		return errors.New("Hoglake catalog ownership or data path differs from configured tenant; explicit recovery required")
	}
	if !slices.Contains(catalog.Capabilities, "atomic-table-creation-v1") {
		return errors.New("Hoglake server does not support atomic table creation")
	}
	var namespace struct {
		Name string `json:"name"`
	}
	nsPath := path + "/namespaces/" + c.Namespace
	status, err = c.request(ctx, http.MethodGet, nsPath, nil, &namespace)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		status, err = c.request(ctx, http.MethodPost, path+"/namespaces", map[string]string{"name": c.Namespace}, nil)
		if err != nil {
			return err
		}
		if status != http.StatusCreated && status != http.StatusConflict {
			return fmt.Errorf("Hoglake namespace create returned HTTP %d", status)
		}
		status, err = c.request(ctx, http.MethodGet, nsPath, nil, &namespace)
		if err != nil {
			return err
		}
	}
	if status != http.StatusOK || namespace.Name != c.Namespace {
		return errors.New("Hoglake namespace is not ready")
	}
	return nil
}

func isManagedHoglake(org configstore.TrinoEnabledOrg) bool {
	return configstore.EffectiveTrinoBackend(org.Backend) == configstore.TrinoBackendHoglake
}

func (p *TrinoProvisioner) managedHoglakeProperties(orgID string, d *DucklingStatus) (map[string]string, error) {
	if p.managedHoglake == nil {
		return nil, errors.New("managed Hoglake is not configured")
	}
	if err := p.managedHoglake.Validate(); err != nil {
		return nil, err
	}
	if d == nil || d.IAMRoleARN == "" {
		return nil, errors.New("waiting for the tenant storage IAM role")
	}
	region := d.DataStore.S3Region
	if region == "" {
		region = p.awsRegion
	}
	if region == "" {
		return nil, errors.New("waiting for the tenant storage region")
	}
	return map[string]string{"connector.name": "hoglake", "fs.s3.enabled": "true", "hoglake.uri": p.managedHoglake.URI, "hoglake.catalog": orgID, "s3.region": region, "s3.auth-type": "IAM_ROLE", "s3.iam-role": d.IAMRoleARN, "s3.max-connections": strconv.Itoa(p.s3MaxConnections), "fs.cache.enabled": strconv.FormatBool(p.filesystemCacheEnabled)}, nil
}

// Optional inventory keeps existing catalog-client implementations compatible.
// Managed Hoglake refuses readiness if the client cannot inspect the connector.
type trinoConnectorInventory interface {
	CatalogConnectors(context.Context) (map[string]string, error)
}

func verifyHoglakeConnector(ctx context.Context, client TrinoCatalogClient, name string) error {
	inventory, ok := client.(trinoConnectorInventory)
	if !ok {
		return errors.New("catalog client cannot verify the Hoglake connector")
	}
	connectors, err := inventory.CatalogConnectors(ctx)
	if err != nil {
		return err
	}
	if connectors[name] != "hoglake" {
		return errors.New("existing Trino catalog is not an operational Hoglake catalog; explicit migration required")
	}
	return nil
}

func parseTrinoConnectors(rows [][]interface{}) (map[string]string, error) {
	result := make(map[string]string, len(rows))
	for _, row := range rows {
		if len(row) != 3 {
			return nil, errors.New("invalid catalog connector inventory")
		}
		name, nok := row[0].(string)
		connector, cok := row[1].(string)
		state, sok := row[2].(string)
		if !nok || !cok || !sok || name == "" || connector == "" || result[name] != "" || (state != "OPERATIONAL" && state != "FAILING") {
			return nil, errors.New("invalid catalog connector inventory")
		}
		if state == "OPERATIONAL" {
			result[name] = connector
		} else {
			result[name] = "FAILING"
		}
	}
	return result, nil
}

const trinoConnectorInventorySQL = "SELECT catalog_name, connector_name, state FROM system.metadata.catalogs"

func (c *trinoCatalogHTTPClient) CatalogConnectors(ctx context.Context) (map[string]string, error) {
	rows, err := c.runStatement(ctx, trinoConnectorInventorySQL)
	if err != nil {
		return nil, err
	}
	return parseTrinoConnectors(rows)
}
func (c *trinoSharedCatalogHTTPClient) CatalogConnectors(ctx context.Context) (map[string]string, error) {
	rows, err := c.runStatement(ctx, trinoConnectorInventorySQL)
	if err != nil {
		return nil, err
	}
	return parseTrinoConnectors(rows)
}
func (c *trinoManagedCatalogClient) CatalogConnectors(ctx context.Context) (map[string]string, error) {
	inventory, ok := c.TrinoCatalogClient.(trinoConnectorInventory)
	if !ok {
		return nil, errors.New("managed catalog client lacks connector inventory")
	}
	return inventory.CatalogConnectors(ctx)
}

func (p *TrinoProvisioner) reconcileHoglakeCatalog(ctx context.Context, client TrinoCatalogClient, name, orgID string, status *DucklingStatus, exists bool) error {
	props, err := p.managedHoglakeProperties(orgID, status)
	if err != nil {
		return err
	}
	// Check the existing connector before touching the remote metadata or IAM.
	if exists {
		if err = verifyHoglakeConnector(ctx, client, name); err != nil {
			return err
		}
	}
	warehouse, err := p.warehouses.GetManagedWarehouseForTrino(orgID)
	if err != nil {
		return err
	}
	if warehouse == nil || warehouse.DucklingName == "" {
		return errors.New("waiting for the managed warehouse storage identity")
	}
	if err = p.managedHoglake.ensure(ctx, orgID, warehouse.DucklingName); err != nil {
		return err
	}
	if !exists {
		if err = client.CreateCatalog(ctx, name, props); err != nil {
			return err
		}
	}
	return verifyHoglakeConnector(ctx, client, name)
}
