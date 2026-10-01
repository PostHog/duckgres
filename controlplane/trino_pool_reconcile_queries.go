//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/trinogateway"
)

const (
	trinoDrainQueryBatchSize = 10
	trinoDrainQueryBudget    = 5 * time.Second
)

type trinoQueryDrainStatus struct {
	NodeID        string `json:"nodeId"`
	CoordinatorID string `json:"coordinatorId"`
	Absent        bool   `json:"absent"`
}

func probeTrinoQueryDrainStatus(ctx context.Context, client *http.Client, endpoint string, credentials func() (string, string), queryID string) (trinoQueryDrainStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(endpoint, "/")+"/v1/query/"+url.PathEscape(queryID)+"/drain-status", nil)
	if err != nil {
		return trinoQueryDrainStatus{}, errors.New("invalid query drain proof endpoint")
	}
	username, password := credentials()
	request.SetBasicAuth(username, password)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Forwarded-Proto", forwardedScheme)
	request.Header.Set("X-Forwarded-Port", "443")
	response, err := client.Do(request)
	if err != nil {
		return trinoQueryDrainStatus{}, errors.New("query drain proof request failed")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return trinoQueryDrainStatus{}, fmt.Errorf("query drain proof returned HTTP %d", response.StatusCode)
	}
	var proof trinoQueryDrainStatus
	if err := json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&proof); err != nil {
		return trinoQueryDrainStatus{}, errors.New("invalid query drain proof response")
	}
	return proof, nil
}

func (o *trinoPoolOperator) reconcileDrainingQueries(ctx context.Context, instance configstore.TrinoPoolInstance, obligations trinogateway.Obligations) (bool, error) {
	if o.queryDrainStatus == nil || obligations.Phase != "DRAINING" || obligations.PendingRequests != 0 || obligations.ActiveQueries == 0 || instance.CoordinatorNodeID == "" || instance.CoordinatorID == "" {
		return false, nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, trinoDrainQueryBudget)
	defer cancel()
	if o.drainCursors == nil {
		o.drainCursors = map[string]string{}
	}
	candidates, err := o.gateway.GetDrainCandidates(probeCtx, o.config.RoutingGroup, instance.InstanceID, o.drainCursors[instance.InstanceID])
	if drainEndpointUnavailable(err) {
		return false, nil
	}
	if err != nil {
		return false, o.dropAuthority(fmt.Errorf("read drain candidates for %s: %w", instance.InstanceID, err))
	}
	if len(candidates) == 0 {
		delete(o.drainCursors, instance.InstanceID)
		return false, nil
	}
	request := trinogateway.ReconcileQueriesRequest{ExpectedGeneration: instance.GatewayGeneration, NodeID: instance.CoordinatorNodeID, CoordinatorID: instance.CoordinatorID}
	limit := min(len(candidates), trinoDrainQueryBatchSize)
	failures := make(map[string]int)
	for _, candidate := range candidates[:limit] {
		if probeCtx.Err() != nil {
			break
		}
		// A failing first page must not starve later query obligations. The cursor
		// is only scheduling state; all safety fences remain in Gateway storage.
		o.drainCursors[instance.InstanceID] = candidate.QueryID
		proof, probeErr := o.queryDrainStatus(probeCtx, instance.EndpointURL, candidate.QueryID)
		if probeErr != nil {
			failures[probeErr.Error()]++
			continue
		}
		if proof.NodeID != instance.CoordinatorNodeID || proof.CoordinatorID != instance.CoordinatorID {
			failures["coordinator identity changed or missing"]++
			continue
		}
		if proof.Absent {
			request.Queries = append(request.Queries, candidate)
		}
	}
	if limit == len(candidates) && probeCtx.Err() == nil {
		delete(o.drainCursors, instance.InstanceID)
	}
	if len(failures) > 0 {
		slog.DebugContext(ctx, "Trino drain proof unavailable", "instance_id", instance.InstanceID, "failures", failures)
	}
	if len(request.Queries) == 0 {
		return false, nil
	}
	// The same proofs replay the same step after a lost response. Any admission
	// change produces a new intent that the Gateway independently checks.
	payload, err := json.Marshal(request)
	if err != nil {
		return false, err
	}
	digest := sha256.Sum256(payload)
	request.Step = o.step(instance.InstanceID, "reconcile-queries:"+base64.RawURLEncoding.EncodeToString(digest[:]))
	result, err := o.gateway.ReconcileQueries(ctx, o.config.RoutingGroup, instance.InstanceID, request)
	if drainEndpointUnavailable(err) {
		return false, nil
	}
	if err != nil {
		return false, o.dropAuthority(fmt.Errorf("reconcile drain queries for %s: %w", instance.InstanceID, err))
	}
	if result.Reconciled > 0 {
		slog.InfoContext(ctx, "Trino drain query obligations reconciled", "instance_id", instance.InstanceID, "reconciled", result.Reconciled)
	}
	// Re-read obligations on a later tick; reconciliation does not authorize a
	// seal and terminal records still pin the member through retry retention.
	return result.Reconciled > 0, nil
}

// An old Gateway has no route or protocol error header. Its HTTP 404 cannot
// authorize any cleanup, but it must not turn a mixed rollout into a failure.
func drainEndpointUnavailable(err error) bool {
	var response *trinogateway.Error
	return errors.Is(err, trinogateway.ErrNotFound) || (errors.As(err, &response) && response.Status == http.StatusNotFound)
}
