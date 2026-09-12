//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

// TrinoNode identifies a node as reported by its coordinator. Readiness uses
// this inventory rather than inferring Trino membership from Kubernetes pods.
type TrinoNode struct {
	ID          string
	URI         string
	Coordinator bool
	State       string
}

// ListNodes reads the complete coordinator inventory. Invalid rows fail the
// whole read: silently omitting a worker could falsely acknowledge readiness.
func (c *trinoCatalogHTTPClient) ListNodes(ctx context.Context) ([]TrinoNode, error) {
	rows, err := c.runStatement(ctx, "SELECT node_id, http_uri, coordinator, state FROM system.runtime.nodes")
	if err != nil {
		return nil, fmt.Errorf("query Trino node inventory: %w", err)
	}
	nodes := make([]TrinoNode, 0, len(rows))
	ids := make(map[string]bool, len(rows))
	uris := make(map[string]bool, len(rows))
	for i, row := range rows {
		if len(row) != 4 {
			return nil, fmt.Errorf("trino node inventory row %d: expected 4 columns", i)
		}
		id, idOK := row[0].(string)
		uri, uriOK := row[1].(string)
		coordinator, coordinatorOK := row[2].(bool)
		state, stateOK := row[3].(string)
		if !idOK || strings.TrimSpace(id) == "" || !uriOK || !coordinatorOK || !stateOK {
			return nil, fmt.Errorf("trino node inventory row %d: invalid identity, URI, coordinator flag or state", i)
		}
		parsed, err := url.Parse(uri)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
			return nil, fmt.Errorf("trino node inventory row %d: invalid HTTP endpoint", i)
		}
		// NodeSystemTable serializes NodeState using lowercase names.
		switch state {
		case "active", "inactive", "draining", "drained", "shutting_down":
		default:
			return nil, fmt.Errorf("trino node inventory row %d: unrecognized lifecycle state", i)
		}
		if ids[id] || uris[uri] {
			return nil, fmt.Errorf("trino node inventory row %d: duplicate node identity or endpoint", i)
		}
		ids[id] = true
		uris[uri] = true
		nodes = append(nodes, TrinoNode{ID: id, URI: uri, Coordinator: coordinator, State: state})
	}
	return nodes, nil
}
