//go:build kubernetes

package controlplane

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/configstore"
)

const trinoRolloutProvisioningPrefix = "/internal/trino/rollout-provisioning/"

type trinoRolloutProvisioningCell struct {
	StoredCellID string
	BlueBackend  string
	GreenBackend string
}

type trinoRolloutLifecycleStore interface {
	GetTrinoCellLifecycle(context.Context, string) (*configstore.TrinoCellLifecycleStatus, error)
	FreezeTrinoCellAdmissions(context.Context, string, string, string, string, int64) (*configstore.TrinoCellFreeze, error)
	ReleaseTrinoCellAdmissions(context.Context, string, string, int64) error
}

type trinoRolloutProvisioningResponse struct {
	OperationID    string `json:"operationId"`
	AdmissionEpoch int64  `json:"admissionEpoch"`
	Frozen         bool   `json:"frozen"`
	Stable         bool   `json:"stable"`
	Prepared       bool   `json:"prepared"`
	TargetBackend  string `json:"targetBackend"`
	NodeID         string `json:"nodeId"`
	CoordinatorID  string `json:"coordinatorId"`
	RosterHash     string `json:"rosterHash"`
	AdmittedCount  int    `json:"admittedCount"`
}

type trinoRolloutProvisioningHandler struct {
	token        string
	cells        map[string]trinoRolloutProvisioningCell
	store        trinoRolloutLifecycleStore
	gateway      trinoManagedGatewayReader
	processProbe func(context.Context, string, string) (string, string, error)
	limit        chan struct{}
	timeout      time.Duration
}

func newTrinoRolloutProvisioningHandler(token string, cells map[string]trinoRolloutProvisioningCell, store trinoRolloutLifecycleStore, gateway trinoManagedGatewayReader, processProbe func(context.Context, string, string) (string, string, error)) (*trinoRolloutProvisioningHandler, error) {
	if len(token) < 32 || !managedGatewayValue(token, 4096) || len(cells) == 0 || len(cells) > 16 || store == nil || gateway == nil || processProbe == nil {
		return nil, errors.New("invalid Trino rollout provisioning configuration")
	}
	configured := make(map[string]trinoRolloutProvisioningCell, len(cells))
	owners := make(map[string]bool, len(cells))
	for group, cell := range cells {
		logical, registered := strings.CutPrefix(cell.StoredCellID, "registered:")
		if !registered || !managedGatewayName.MatchString(logical) || !managedGatewayName.MatchString(group) || cell.BlueBackend != group+"-blue" || cell.GreenBackend != group+"-green" || owners[cell.StoredCellID] {
			return nil, errors.New("rollout provisioning requires distinct registered two-slot cells")
		}
		owners[cell.StoredCellID] = true
		configured[group] = cell
	}
	return &trinoRolloutProvisioningHandler{token: token, cells: configured, store: store, gateway: gateway, processProbe: processProbe, limit: make(chan struct{}, 4), timeout: 10 * time.Second}, nil
}

func (h *trinoRolloutProvisioningHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	tokens := r.Header.Values("X-Gateway-Transaction-Admin-Token")
	if h.token == "" || len(tokens) != 1 || subtle.ConstantTimeCompare([]byte(tokens[0]), []byte(h.token)) != 1 {
		trinoProvisioningError(w, http.StatusUnauthorized)
		return
	}
	path, ok := strings.CutPrefix(r.URL.Path, trinoRolloutProvisioningPrefix)
	parts := strings.Split(path, "/")
	if !ok || len(parts) < 1 || len(parts) > 2 || r.URL.RawQuery != "" || r.URL.RawPath != "" {
		trinoProvisioningError(w, http.StatusBadRequest)
		return
	}
	cell, ok := h.cells[parts[0]]
	if !ok {
		trinoProvisioningError(w, http.StatusNotFound)
		return
	}
	if (len(parts) == 1 && r.Method != http.MethodGet) || (len(parts) == 2 && (r.Method != http.MethodPost || (parts[1] != "freeze" && parts[1] != "release"))) {
		trinoProvisioningError(w, http.StatusMethodNotAllowed)
		return
	}
	select {
	case h.limit <- struct{}{}:
		defer func() { <-h.limit }()
	default:
		trinoProvisioningError(w, http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	var request trinoProvisioningMutation
	if len(parts) == 2 {
		controller := http.NewResponseController(w)
		if controller.SetReadDeadline(time.Now().Add(h.timeout)) != nil {
			r.Close = true
			w.Header().Set("Connection", "close")
			trinoProvisioningError(w, http.StatusServiceUnavailable)
			return
		}
		var err error
		request, err = readTrinoProvisioningMutation(r.Body, parts[1])
		if err != nil {
			r.Close = true
			w.Header().Set("Connection", "close")
			_ = r.Body.Close()
			trinoProvisioningError(w, http.StatusBadRequest)
			return
		}
		if controller.SetReadDeadline(time.Time{}) != nil {
			trinoProvisioningError(w, http.StatusServiceUnavailable)
			return
		}
	}
	state, err := h.store.GetTrinoCellLifecycle(ctx, cell.StoredCellID)
	if err != nil || state == nil || state.CellID != cell.StoredCellID || state.AdmissionEpoch < 0 || ctx.Err() != nil {
		trinoProvisioningError(w, http.StatusServiceUnavailable)
		return
	}
	if len(parts) == 1 {
		writeTrinoProvisioning(w, http.StatusOK, trinoProvisioningStatus(state))
		return
	}
	if parts[1] == "freeze" {
		h.freeze(ctx, w, parts[0], cell, state, request)
		return
	}
	h.release(ctx, w, parts[0], cell, state, request)
}

type trinoProvisioningMutation struct {
	operation, hash string
	epoch           int64
}

func readTrinoProvisioningMutation(body io.Reader, action string) (trinoProvisioningMutation, error) {
	invalid := errors.New("invalid rollout provisioning request")
	data, err := io.ReadAll(io.LimitReader(body, 8193))
	if err != nil || len(data) > 8192 {
		return trinoProvisioningMutation{}, invalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return trinoProvisioningMutation{}, invalid
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		key, err := decoder.Token()
		name, ok := key.(string)
		if err != nil || !ok || fields[name] != nil {
			return trinoProvisioningMutation{}, invalid
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return trinoProvisioningMutation{}, invalid
		}
		fields[name] = value
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || decoder.Decode(new(any)) != io.EOF || len(fields) != 3 {
		return trinoProvisioningMutation{}, invalid
	}
	epochField := "admissionEpoch"
	if action == "freeze" {
		epochField = "expectedAdmissionEpoch"
	}
	var operation, hash string
	var epoch *int64
	if json.Unmarshal(fields["operationId"], &operation) != nil || json.Unmarshal(fields["planHash"], &hash) != nil || json.Unmarshal(fields[epochField], &epoch) != nil || epoch == nil || *epoch < 0 || *epoch == math.MaxInt64 || !managedGatewayValue(operation, 256) || !managedGatewayHash.MatchString(hash) {
		return trinoProvisioningMutation{}, invalid
	}
	return trinoProvisioningMutation{operation: operation, hash: hash, epoch: *epoch}, nil
}

func (h *trinoRolloutProvisioningHandler) freeze(ctx context.Context, w http.ResponseWriter, group string, cell trinoRolloutProvisioningCell, state *configstore.TrinoCellLifecycleStatus, request trinoProvisioningMutation) {
	if (state.Freeze == nil && state.AdmissionEpoch != request.epoch) || (state.Freeze != nil && (state.Freeze.OperationID != request.operation || state.Freeze.PlanHash != request.hash || state.AdmissionEpoch != request.epoch+1)) {
		trinoProvisioningError(w, http.StatusConflict)
		return
	}
	observation, err := h.gateway.Observe(ctx, group)
	if err != nil {
		trinoProvisioningError(w, http.StatusServiceUnavailable)
		return
	}
	if !trinoProvisioningPlanMatches(observation, group, cell, request) || observation.Rollout.Phase != "CLAIMED" {
		trinoProvisioningError(w, http.StatusConflict)
		return
	}
	plan, route := observation.Rollout.Plan, observation.Route
	if route.Generation != plan.ExpectedRouteGeneration || route.BackendName != plan.SourceBackend || route.BackendIncarnation != plan.SourceIncarnation {
		trinoProvisioningError(w, http.StatusConflict)
		return
	}
	freeze, err := h.store.FreezeTrinoCellAdmissions(ctx, cell.StoredCellID, request.operation, request.hash, plan.TargetBackend, request.epoch)
	if err != nil {
		trinoProvisioningStoreError(w, err)
		return
	}
	if freeze == nil || freeze.OperationID != request.operation || freeze.PlanHash != request.hash || freeze.AdmissionEpoch != request.epoch+1 || freeze.TargetBackend != plan.TargetBackend {
		trinoProvisioningError(w, http.StatusConflict)
		return
	}
	status := http.StatusOK
	if !freeze.Stable {
		status = http.StatusAccepted
	}
	writeTrinoProvisioning(w, status, trinoProvisioningStatus(&configstore.TrinoCellLifecycleStatus{AdmissionEpoch: freeze.AdmissionEpoch, Freeze: freeze}))
}

// Only the guarded rollout workflow may change routes in managed mode.
// An arbitrary manual route flip bypasses the catalog freeze protocol.
func (h *trinoRolloutProvisioningHandler) release(ctx context.Context, w http.ResponseWriter, group string, cell trinoRolloutProvisioningCell, state *configstore.TrinoCellLifecycleStatus, request trinoProvisioningMutation) {
	observation, err := h.gateway.Observe(ctx, group)
	if err != nil {
		trinoProvisioningError(w, http.StatusServiceUnavailable)
		return
	}
	if !trinoProvisioningPlanMatches(observation, group, cell, request) || managedGatewayPhase(observation.Rollout.Phase) < managedGatewayPhase("CUTOVER") {
		trinoProvisioningError(w, http.StatusConflict)
		return
	}
	plan, route := observation.Rollout.Plan, observation.Route
	if plan.ExpectedRouteGeneration == math.MaxInt64 || route.Generation != plan.ExpectedRouteGeneration+1 || route.BackendName != plan.TargetBackend {
		trinoProvisioningError(w, http.StatusConflict)
		return
	}
	if state.Freeze == nil {
		if state.ReleasedOperationID != request.operation || state.ReleasedAdmissionEpoch != request.epoch || state.AdmissionEpoch != request.epoch+1 {
			trinoProvisioningError(w, http.StatusConflict)
			return
		}
		writeTrinoProvisioning(w, http.StatusOK, trinoRolloutProvisioningResponse{OperationID: request.operation, AdmissionEpoch: request.epoch + 1})
		return
	}
	freeze := state.Freeze
	if !freeze.Stable || freeze.OperationID != request.operation || freeze.PlanHash != request.hash || freeze.AdmissionEpoch != request.epoch || state.AdmissionEpoch != request.epoch || freeze.TargetBackend != plan.TargetBackend || freeze.Certificate == nil {
		trinoProvisioningError(w, http.StatusConflict)
		return
	}
	certificate := freeze.Certificate
	backend, err := h.gateway.Backend(ctx, plan.TargetBackend)
	if err != nil {
		trinoProvisioningError(w, http.StatusServiceUnavailable)
		return
	}
	if backend == nil || backend.BackendName != plan.TargetBackend || backend.State != "ACTIVE" || backend.Incarnation != route.BackendIncarnation || certificate.TargetBackend != plan.TargetBackend || certificate.NodeID == "" || certificate.CoordinatorID == "" || backend.NodeID != certificate.NodeID || backend.CoordinatorID != certificate.CoordinatorID {
		trinoProvisioningError(w, http.StatusConflict)
		return
	}
	nodeID, coordinatorID, err := h.processProbe(ctx, group, plan.TargetBackend)
	if err != nil || nodeID != certificate.NodeID || coordinatorID != certificate.CoordinatorID {
		trinoProvisioningError(w, http.StatusServiceUnavailable)
		return
	}
	final, err := h.gateway.Observe(ctx, group)
	if err != nil || final == nil || final.Rollout == nil || final.Route != route || *final.Rollout != *observation.Rollout || ctx.Err() != nil {
		trinoProvisioningError(w, http.StatusServiceUnavailable)
		return
	}
	if err := h.store.ReleaseTrinoCellAdmissions(ctx, cell.StoredCellID, request.operation, request.epoch); err != nil {
		trinoProvisioningStoreError(w, err)
		return
	}
	writeTrinoProvisioning(w, http.StatusOK, trinoRolloutProvisioningResponse{OperationID: request.operation, AdmissionEpoch: request.epoch + 1})
}

func trinoProvisioningPlanMatches(observation *trinoManagedGatewayObservation, group string, cell trinoRolloutProvisioningCell, request trinoProvisioningMutation) bool {
	if observation == nil || observation.Rollout == nil {
		return false
	}
	op := observation.Rollout
	plan := op.Plan
	return observation.Route.RoutingGroup == group && op.RoutingGroup == group && op.OperationID == request.operation && plan.PlanHash == request.hash && ((plan.SourceBackend == cell.BlueBackend && plan.TargetBackend == cell.GreenBackend) || (plan.SourceBackend == cell.GreenBackend && plan.TargetBackend == cell.BlueBackend))
}

func trinoProvisioningStatus(state *configstore.TrinoCellLifecycleStatus) trinoRolloutProvisioningResponse {
	response := trinoRolloutProvisioningResponse{AdmissionEpoch: state.AdmissionEpoch, OperationID: state.ReleasedOperationID}
	if freeze := state.Freeze; freeze != nil {
		response.OperationID, response.Frozen, response.Stable, response.TargetBackend = freeze.OperationID, true, freeze.Stable, freeze.TargetBackend
		if certificate := freeze.Certificate; certificate != nil {
			response.Prepared = true
			response.NodeID, response.CoordinatorID, response.RosterHash, response.AdmittedCount = certificate.NodeID, certificate.CoordinatorID, certificate.RosterHash, certificate.AdmittedCount
		}
	}
	return response
}

func writeTrinoProvisioning(w http.ResponseWriter, status int, response trinoRolloutProvisioningResponse) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}

func trinoProvisioningError(w http.ResponseWriter, status int) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": "rollout_provisioning_unavailable"})
}

func trinoProvisioningStoreError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	if errors.Is(err, configstore.ErrTrinoCellConflict) {
		status = http.StatusConflict
	}
	trinoProvisioningError(w, status)
}
