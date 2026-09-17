//go:build kubernetes

package provisioner

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	sharedTrinoMaxPageBytes  = 4 << 20
	sharedTrinoMaxTotalBytes = 32 << 20
	sharedTrinoMaxPages      = 256
	sharedTrinoMaxRows       = 100000
)

var sharedTrinoQueryID = regexp.MustCompile(`^[0-9]{8}_[0-9]{6}_[0-9]{5,}_[a-z0-9]{5}$`)
var sharedTrinoPageToken = regexp.MustCompile(`^[0-9]+$`)
var sharedTrinoSlug = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
var sharedTrinoConnectorName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

type trinoCatalogTerminalError struct{}

func (*trinoCatalogTerminalError) Error() string {
	return "trino catalog statement failed with confirmed terminal outcome"
}

// TrinoCatalogOutcomeTerminal permits clearing a durable DDL intent.
// All unclassified failures retain the intent and require explicit recovery.
func TrinoCatalogOutcomeTerminal(err error) bool {
	var terminal *trinoCatalogTerminalError
	return err == nil || errors.As(err, &terminal)
}

type trinoSharedCatalogHTTPClient struct {
	*trinoCatalogHTTPClient
	origin *url.URL
}

// NewTrinoSharedCatalogHTTPClient requires verified HTTPS and rejects redirects.
// This private coordinator path does not use the environment egress proxy.
func NewTrinoSharedCatalogHTTPClient(baseURL, username, password, tlsServerName string) (*trinoSharedCatalogHTTPClient, error) {
	origin, err := url.Parse(baseURL)
	if err != nil || origin.Scheme != "https" || origin.Hostname() == "" || origin.User != nil || origin.RawQuery != "" || origin.ForceQuery || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") {
		return nil, errors.New("invalid shared catalog coordinator URL")
	}
	legacy := NewTrinoCatalogHTTPClient(strings.TrimRight(baseURL, "/"), username, password, tlsServerName).(*trinoCatalogHTTPClient)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: tlsServerName}
	legacy.hc = &http.Client{
		Transport:     transport,
		Timeout:       30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &trinoSharedCatalogHTTPClient{trinoCatalogHTTPClient: legacy, origin: origin}, nil
}

func (c *trinoSharedCatalogHTTPClient) validContinuation(raw, queryID string) bool {
	next, err := url.Parse(raw)
	if err != nil || next.Scheme != c.origin.Scheme || !strings.EqualFold(next.Hostname(), c.origin.Hostname()) || sharedTrinoPort(next) != sharedTrinoPort(c.origin) || next.User != nil || next.RawQuery != "" || next.ForceQuery || next.Fragment != "" || next.RawPath != "" {
		return false
	}
	parts := strings.Split(next.Path, "/")
	return len(parts) == 7 && parts[1] == "v1" && parts[2] == "statement" && (parts[3] == "executing" || parts[3] == "queued") && parts[4] == queryID && len(parts[5]) <= 256 && sharedTrinoSlug.MatchString(parts[5]) && len(parts[6]) <= 20 && sharedTrinoPageToken.MatchString(parts[6])
}

func sharedTrinoPort(endpoint *url.URL) string {
	if port := endpoint.Port(); port != "" {
		return port
	}
	return "443"
}

func (c *trinoSharedCatalogHTTPClient) runStatement(ctx context.Context, statement string) ([][]interface{}, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	username, password := c.credentials()
	if username == "" || password == "" {
		return nil, &trinoCatalogTerminalError{}
	}
	method, target := http.MethodPost, c.baseURL+"/v1/statement"
	var queryID string
	var rows [][]interface{}
	totalBytes := 0
	for range sharedTrinoMaxPages {
		var body io.Reader
		if method == http.MethodPost {
			body = strings.NewReader(statement)
		}
		request, err := http.NewRequestWithContext(ctx, method, target, body)
		if err != nil {
			return nil, errors.New("cannot construct catalog statement request")
		}
		request.SetBasicAuth(username, password)
		request.Header.Set("X-Trino-User", username)
		request.Header.Set("X-Trino-Source", TrinoProvisionerSource)
		request.Header.Set("Content-Type", "text/plain")
		response, err := c.hc.Do(request)
		if err != nil {
			return nil, errors.New("catalog statement transport outcome unknown")
		}
		data, readErr := io.ReadAll(io.LimitReader(response.Body, sharedTrinoMaxPageBytes+1))
		closeErr := response.Body.Close()
		totalBytes += len(data)
		if readErr != nil || closeErr != nil || len(data) > sharedTrinoMaxPageBytes || totalBytes > sharedTrinoMaxTotalBytes || response.StatusCode != http.StatusOK {
			return nil, errors.New("catalog statement response outcome unknown")
		}
		var page trinoStatementResponse
		if json.Unmarshal(data, &page) != nil || len(page.ID) > 128 || !sharedTrinoQueryID.MatchString(page.ID) || (queryID != "" && queryID != page.ID) {
			return nil, errors.New("catalog statement identity or response invalid")
		}
		queryID = page.ID
		state, _ := page.Stats["state"].(string)
		switch state {
		case "QUEUED", "WAITING_FOR_RESOURCES", "DISPATCHING", "PLANNING", "STARTING", "RUNNING", "FINISHING", "FINISHED", "FAILED":
		default:
			return nil, errors.New("catalog statement state invalid")
		}
		if page.NextURI == "" {
			// FAILED can precede completion of a synchronous catalog mutation.
			// Only FINISHED proves that the mutation task returned successfully.
			if state != "FINISHED" || page.Error != nil {
				return nil, errors.New("catalog statement terminal outcome unknown")
			}
		} else if page.Error != nil || state == "FAILED" || !c.validContinuation(page.NextURI, queryID) {
			return nil, errors.New("catalog statement continuation invalid")
		}
		if len(rows)+len(page.Data) > sharedTrinoMaxRows {
			return nil, errors.New("catalog statement row limit exceeded")
		}
		rows = append(rows, page.Data...)
		if page.NextURI == "" {
			return rows, nil
		}
		method, target = http.MethodGet, page.NextURI
	}
	return nil, errors.New("catalog statement page limit exceeded")
}

// CatalogStates reads all catalog states with a bounded bulk query.
// A failed startup catalog is not equivalent to a usable catalog name.
func (c *trinoSharedCatalogHTTPClient) CatalogStates(ctx context.Context) (map[string]string, error) {
	rows, err := c.runStatement(ctx, "SELECT catalog_name, state FROM system.metadata.catalogs")
	if err != nil {
		return nil, err
	}
	states := make(map[string]string, len(rows))
	for _, row := range rows {
		if len(row) != 2 {
			return nil, errors.New("invalid catalog state inventory")
		}
		name, nameOK := row[0].(string)
		state, stateOK := row[1].(string)
		if !nameOK || name == "" || !stateOK || (state != "OPERATIONAL" && state != "FAILING") || states[name] != "" {
			return nil, errors.New("invalid catalog state inventory")
		}
		states[name] = state
	}
	return states, nil
}

func (c *trinoSharedCatalogHTTPClient) ListCatalogs(ctx context.Context) ([]string, error) {
	states, err := c.CatalogStates(ctx)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(states))
	for name, state := range states {
		if state != "OPERATIONAL" {
			return nil, errors.New("catalog startup failed")
		}
		names = append(names, name)
	}
	return names, nil
}

func (c *trinoSharedCatalogHTTPClient) ListNodes(ctx context.Context) ([]TrinoNode, error) {
	rows, err := c.runStatement(ctx, "SELECT node_id, http_uri, coordinator, state FROM system.runtime.nodes")
	if err != nil {
		return nil, err
	}
	return parseTrinoNodes(rows)
}

func (c *trinoSharedCatalogHTTPClient) CreateCatalog(ctx context.Context, name string, props map[string]string) error {
	connector := props["connector.name"]
	if !sharedTrinoConnectorName.MatchString(connector) {
		return &trinoCatalogTerminalError{}
	}
	withProps := make(map[string]string, len(props))
	for key, value := range props {
		if key != "connector.name" {
			withProps[key] = value
		}
	}
	// Trino treats a quoted connector identifier as a connector name containing quotes.
	// Validate its restricted grammar before emitting it without quotes.
	_, err := c.runStatement(ctx, fmt.Sprintf("CREATE CATALOG %s USING %s%s", quoteTrinoIdentifier(name), connector, renderWithClause(withProps)))
	return err
}

func (c *trinoSharedCatalogHTTPClient) DropCatalog(ctx context.Context, name string) error {
	_, err := c.runStatement(ctx, "DROP CATALOG "+quoteTrinoIdentifier(name))
	return err
}

func (c *trinoSharedCatalogHTTPClient) AlterCatalog(context.Context, string, map[string]string) error {
	return &trinoCatalogTerminalError{}
}
