//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

var errTrinoManagedGateway = errors.New("managed Trino Gateway observation unavailable")
var managedGatewayName = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
var managedGatewayHash = regexp.MustCompile(`^[a-f0-9]{64}$`)

type trinoManagedGatewayRoute struct {
	RoutingGroup       string `json:"routingGroup"`
	Generation         int64  `json:"generation"`
	BackendName        string `json:"backendName"`
	BackendIncarnation string `json:"backendIncarnation"`
}

type trinoManagedGatewayPlan struct {
	PlanHash                string `json:"planHash"`
	ExpectedRouteGeneration int64  `json:"expectedRouteGeneration"`
	SourceBackend           string `json:"sourceBackend"`
	SourceIncarnation       string `json:"sourceIncarnation"`
	TargetBackend           string `json:"targetBackend"`
	TargetIncarnation       string `json:"targetIncarnation"`
}

type trinoManagedGatewayRollout struct {
	RoutingGroup string                  `json:"routingGroup"`
	OperationID  string                  `json:"operationId"`
	Plan         trinoManagedGatewayPlan `json:"plan"`
	Phase        string                  `json:"phase"`
	Version      int64                   `json:"version"`
}

type trinoManagedGatewayBackend struct {
	BackendName   string `json:"name"`
	Incarnation   string `json:"incarnation"`
	State         string `json:"state"`
	NodeID        string `json:"nodeId"`
	CoordinatorID string `json:"coordinatorId"`
}

type trinoManagedGatewayObservation struct {
	Route   trinoManagedGatewayRoute
	Rollout *trinoManagedGatewayRollout
}

type trinoManagedGatewayReader interface {
	Observe(context.Context, string) (*trinoManagedGatewayObservation, error)
	Backend(context.Context, string) (*trinoManagedGatewayBackend, error)
}

type trinoManagedGateway struct {
	baseURL  string
	username string
	token    string
	client   *http.Client
}

func newTrinoManagedGateway(endpoint, tlsName, username, token string) (*trinoManagedGateway, error) {
	address, err := url.Parse(endpoint)
	if err != nil || address.Scheme != "https" || address.Hostname() == "" || address.User != nil || address.RawQuery != "" || address.Fragment != "" || (address.Path != "" && address.Path != "/") || address.RawPath != "" {
		return nil, errors.New("managed Gateway requires a credential-free HTTPS origin")
	}
	if !managedGatewayValue(username, 255) || strings.Contains(username, ":") || len(token) < 32 || !managedGatewayValue(token, 4096) || (tlsName != "" && (!managedGatewayValue(tlsName, 255) || strings.ContainsAny(tlsName, "/:@"))) {
		return nil, errors.New("invalid managed Gateway client configuration")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: tlsName}
	transport.MaxIdleConnsPerHost = 4
	transport.ResponseHeaderTimeout = 5 * time.Second
	return &trinoManagedGateway{baseURL: strings.TrimSuffix(endpoint, "/"), username: username, token: token, client: &http.Client{
		Transport: transport, Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errTrinoManagedGateway },
	}}, nil
}

func managedGatewayValue(value string, limit int) bool {
	return value != "" && len(value) <= limit && strings.TrimSpace(value) == value && strings.IndexFunc(value, unicode.IsControl) == -1
}

func managedGatewayUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func managedGatewayPhase(phase string) int {
	for index, known := range []string{"CLAIMED", "WARMED", "VERIFIED", "CUTOVER", "DRAINING", "SEALED", "STOPPED", "COMPLETE"} {
		if phase == known {
			return index
		}
	}
	return -1
}

// Observe detects concurrent route or operation changes before returning a view.
// It does not replace the control-plane admission epoch or the Gateway operation guard.
func (g *trinoManagedGateway) Observe(ctx context.Context, group string) (*trinoManagedGatewayObservation, error) {
	if !managedGatewayName.MatchString(group) {
		return nil, errTrinoManagedGateway
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	first, err := g.observeOnce(ctx, group)
	if err != nil {
		return nil, err
	}
	second, err := g.observeOnce(ctx, group)
	if err != nil || first.Route != second.Route || (first.Rollout == nil) != (second.Rollout == nil) {
		return nil, errTrinoManagedGateway
	}
	if first.Rollout != nil && *first.Rollout != *second.Rollout {
		return nil, errTrinoManagedGateway
	}
	return second, nil
}

func (g *trinoManagedGateway) observeOnce(ctx context.Context, group string) (*trinoManagedGatewayObservation, error) {
	var route *trinoManagedGatewayRoute
	if _, err := g.get(ctx, "/routes/"+group, &route, false); err != nil {
		return nil, err
	}
	if route == nil || route.RoutingGroup != group || route.Generation < 1 || !managedGatewayName.MatchString(route.BackendName) || !managedGatewayUUID(route.BackendIncarnation) {
		return nil, errTrinoManagedGateway
	}
	var operation *trinoManagedGatewayRollout
	missing, err := g.get(ctx, "/rollouts/"+group, &operation, true)
	if err != nil {
		return nil, err
	}
	if !missing {
		if operation == nil || operation.RoutingGroup != group || !managedGatewayValue(operation.OperationID, 256) || operation.Version < 0 || managedGatewayPhase(operation.Phase) < 0 {
			return nil, errTrinoManagedGateway
		}
		plan := operation.Plan
		if !managedGatewayHash.MatchString(plan.PlanHash) || plan.ExpectedRouteGeneration < 1 || !managedGatewayName.MatchString(plan.SourceBackend) || !managedGatewayName.MatchString(plan.TargetBackend) || plan.SourceBackend == plan.TargetBackend || !managedGatewayUUID(plan.SourceIncarnation) || (plan.TargetIncarnation != "" && !managedGatewayUUID(plan.TargetIncarnation)) {
			return nil, errTrinoManagedGateway
		}
	}
	return &trinoManagedGatewayObservation{Route: *route, Rollout: operation}, nil
}

func (g *trinoManagedGateway) Backend(ctx context.Context, name string) (*trinoManagedGatewayBackend, error) {
	if !managedGatewayName.MatchString(name) {
		return nil, errTrinoManagedGateway
	}
	var backend *trinoManagedGatewayBackend
	if _, err := g.get(ctx, "/backends/"+name+"/drain", &backend, false); err != nil {
		return nil, err
	}
	if backend == nil || backend.BackendName != name || !managedGatewayUUID(backend.Incarnation) || (backend.State != "ACTIVE" && backend.State != "DRAINING" && backend.State != "SEALED") || (backend.NodeID != "" && !managedGatewayValue(backend.NodeID, 255)) || (backend.CoordinatorID != "" && !managedGatewayValue(backend.CoordinatorID, 255)) {
		return nil, errTrinoManagedGateway
	}
	return backend, nil
}

func (g *trinoManagedGateway) get(ctx context.Context, path string, target any, allowMissing bool) (bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, g.baseURL+"/gateway/transactions"+path, nil)
	if err != nil {
		return false, errTrinoManagedGateway
	}
	request.SetBasicAuth(g.username, g.token)
	request.Header.Set("X-Gateway-Transaction-Admin-Token", g.token)
	request.Header.Set("Accept", "application/json")
	response, err := g.client.Do(request)
	if err != nil {
		return false, errTrinoManagedGateway
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode == http.StatusNotFound && allowMissing {
		return true, nil
	}
	if response.StatusCode != http.StatusOK {
		return false, errTrinoManagedGateway
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 || json.Unmarshal(body, target) != nil {
		return false, errTrinoManagedGateway
	}
	return false, nil
}
