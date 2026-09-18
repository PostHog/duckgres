//go:build kubernetes

package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/trinogateway"
)

// Candidate validation: what duckgres checks about a PREPARING instance before
// asking the Gateway to admit it.
//
// There is no canary warehouse, no canary credential and no synthetic tenant.
// The checks reuse the operational credential the provisioner already holds and
// the coordinator's own authenticated endpoints. Two rules shape this file:
//
//   - The candidate is probed through ITS OWN Service, never through the pool's
//     load-balanced endpoint. A pooled probe can silently validate a different
//     cluster and admit an unready one.
//   - A check that was not performed is not reported. The receipt lists exactly
//     what was observed, because the Gateway records that list verbatim and an
//     operator will read it as evidence.
const (
	// A structural check, not a workload: one request budget, one pass budget.
	trinoPoolProbeRequestBudget = 10 * time.Second
	trinoPoolProbePassBudget    = 5 * time.Minute
	catalogSyncPath             = "/v1/catalog/sync"
)

var errTrinoPoolCandidateNotReady = errors.New("trino pool candidate is not ready")

// The checks duckgres reports on an admission receipt. They mirror the
// constants in the Gateway client and are listed verbatim in the Gateway's
// record, so each one must correspond to something actually observed above.
const (
	trinoPoolCheckImage                 = trinogateway.CheckImage
	trinoPoolCheckWorkers               = trinogateway.CheckWorkers
	trinoPoolCheckCatalogRevision       = trinogateway.CheckCatalogRevision
	trinoPoolCheckAuthRevision          = trinogateway.CheckAuthRevision
	trinoPoolCheckOperationalConnection = trinogateway.CheckOperationalConnection
)

// catalogSyncStatus is the coordinator's /v1/catalog/sync response.
//
// observedRevision and appliedRevision are null until the first successful
// snapshot, which is why they are pointers: zero would be indistinguishable
// from "revision 0 applied", and revision 0 is the legitimate empty state.
type catalogSyncStatus struct {
	NodeID            string              `json:"nodeId"`
	NodeVersion       string              `json:"nodeVersion"`
	ProcessID         string              `json:"processId"`
	CoordinatorID     string              `json:"coordinatorId"`
	Enabled           bool                `json:"enabled"`
	Ready             bool                `json:"ready"`
	ObservedRevision  *int64              `json:"observedRevision"`
	AppliedRevision   *int64              `json:"appliedRevision"`
	ActiveCatalogs    int                 `json:"activeCatalogs"`
	FailedCatalogs    int                 `json:"failedCatalogs"`
	LastFailure       string              `json:"lastFailure"`
	NotReadyReason    string              `json:"notReadyReason"`
	SecurityRevisions []componentRevision `json:"securityRevisions"`
}

// componentRevision is what one security component of the process has loaded.
// This is the coordinator's answer to "which credentials, groups and
// authorization data are actually in effect", which a catalog revision says
// nothing about.
type componentRevision struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Revision string `json:"revision"`
	Error    string `json:"error"`
}

// trinoPoolValidation is a process-bound validation result.
type trinoPoolValidation struct {
	NodeID          string
	ProcessID       string
	CoordinatorID   string
	AppliedRevision int64
	AuthRevision    string
	ReadyWorkers    int
	Checks          []string
	CertificateHash string
}

// validateTrinoPoolCandidate probes one candidate through its own endpoint.
//
// coordinatorURL must be the instance's own Service. readyWorkers comes from
// the Kubernetes observation, and the coordinator's own node inventory has to
// agree with it: a coordinator that reports fewer registered workers than the
// cluster has running pods is still warming up, and admitting it would send
// tenant queries to a cluster that cannot plan them.
func validateTrinoPoolCandidate(
	ctx context.Context,
	client *http.Client,
	coordinatorURL string,
	credential func() (string, string),
	observedWorkers int,
	requiredCatalogRevision int64,
) (trinoPoolValidation, error) {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolProbePassBudget)
	defer cancel()

	username, password := credential()
	sql := rolloutSQLClient{baseURL: coordinatorURL, client: client, username: username, password: password}

	before, err := sql.info(ctx)
	if err != nil {
		return trinoPoolValidation{}, fmt.Errorf("%w: %v", errTrinoPoolCandidateNotReady, err)
	}

	sync, err := fetchCatalogSync(ctx, client, coordinatorURL, username, password)
	if err != nil {
		return trinoPoolValidation{}, err
	}
	if !sync.Enabled {
		return trinoPoolValidation{}, fmt.Errorf("%w: catalog synchronization is disabled on the candidate", errTrinoPoolCandidateNotReady)
	}
	if !sync.Ready || sync.FailedCatalogs != 0 {
		return trinoPoolValidation{}, fmt.Errorf("%w: %s", errTrinoPoolCandidateNotReady, syncReason(sync))
	}
	if sync.AppliedRevision == nil || *sync.AppliedRevision < requiredCatalogRevision {
		// Admitting a member behind the published revision would serve a tenant
		// a catalog set that does not include them yet.
		return trinoPoolValidation{}, fmt.Errorf("%w: applied catalog revision %v is behind the published revision %d",
			errTrinoPoolCandidateNotReady, revisionText(sync.AppliedRevision), requiredCatalogRevision)
	}
	// The process identity has to be the one we started talking to. A restart
	// mid-validation invalidates everything observed before it.
	if sync.NodeID != before.NodeID || (sync.CoordinatorID != "" && sync.CoordinatorID != before.CoordinatorID) {
		return trinoPoolValidation{}, fmt.Errorf("%w: coordinator identity changed during validation", errTrinoPoolCandidateNotReady)
	}
	if sync.ProcessID == "" {
		return trinoPoolValidation{}, fmt.Errorf("%w: candidate reports no process identity", errTrinoPoolCandidateNotReady)
	}

	registered, err := registeredWorkerCount(ctx, sql, before.NodeID)
	if err != nil {
		return trinoPoolValidation{}, err
	}
	if observedWorkers == 0 || registered != observedWorkers {
		return trinoPoolValidation{}, fmt.Errorf("%w: %d workers registered, %d running",
			errTrinoPoolCandidateNotReady, registered, observedWorkers)
	}

	// Re-read the identity last: anything observed above is only valid if the
	// same process was serving throughout.
	after, err := sql.info(ctx)
	if err != nil || after.NodeID != before.NodeID || after.CoordinatorID != before.CoordinatorID {
		return trinoPoolValidation{}, fmt.Errorf("%w: coordinator changed during observation", errTrinoPoolCandidateNotReady)
	}

	validation := trinoPoolValidation{
		NodeID:          sync.NodeID,
		ProcessID:       sync.ProcessID,
		CoordinatorID:   before.CoordinatorID,
		AppliedRevision: *sync.AppliedRevision,
		AuthRevision:    authRevisionFingerprint(sync.SecurityRevisions),
		ReadyWorkers:    registered,
		Checks: []string{
			trinoPoolCheckImage,
			trinoPoolCheckWorkers,
			trinoPoolCheckCatalogRevision,
			trinoPoolCheckAuthRevision,
			trinoPoolCheckOperationalConnection,
		},
	}
	validation.CertificateHash = certificateHash(validation)
	return validation, nil
}

// authRevisionFingerprint condenses what the process's security components have
// loaded into one opaque value.
//
// It is a hash, not the revisions themselves: the value is sent to the Gateway
// and shown to operators, and component revisions can carry credential-derived
// data. A component reporting an ERROR is folded in, so a coordinator whose
// password provider failed to load produces a different fingerprint from one
// where it loaded cleanly — the two must never compare equal.
func authRevisionFingerprint(revisions []componentRevision) string {
	if len(revisions) == 0 {
		// Nothing to acknowledge. Reported as an explicit marker rather than an
		// empty string, which the Gateway rejects and which would read as "no
		// auth configured" instead of "this build reports nothing".
		return "none"
	}
	ordered := make([]string, 0, len(revisions))
	for _, revision := range revisions {
		ordered = append(ordered, strings.Join([]string{revision.Kind, revision.Name, revision.Revision, revision.Error}, "\x00"))
	}
	sort.Strings(ordered)
	digest := sha256.Sum256([]byte(strings.Join(ordered, "\x1e")))
	return hex.EncodeToString(digest[:])
}

// certificateHash binds the receipt to the exact facts it asserts, so a
// certificate cannot be reused for a different process or revision.
func certificateHash(validation trinoPoolValidation) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{
		validation.NodeID,
		validation.ProcessID,
		validation.CoordinatorID,
		fmt.Sprint(validation.AppliedRevision),
		validation.AuthRevision,
		fmt.Sprint(validation.ReadyWorkers),
		strings.Join(validation.Checks, ","),
	}, "\x00")))
	return hex.EncodeToString(digest[:])
}

func fetchCatalogSync(ctx context.Context, client *http.Client, coordinatorURL, username, password string) (catalogSyncStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolProbeRequestBudget)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, coordinatorURL+catalogSyncPath, nil)
	if err != nil {
		return catalogSyncStatus{}, fmt.Errorf("build catalog sync request: %w", err)
	}
	request.SetBasicAuth(username, password)
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return catalogSyncStatus{}, fmt.Errorf("%w: catalog sync request failed", errTrinoPoolCandidateNotReady)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return catalogSyncStatus{}, fmt.Errorf("%w: catalog sync returned HTTP %d", errTrinoPoolCandidateNotReady, response.StatusCode)
	}
	var status catalogSyncStatus
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		return catalogSyncStatus{}, fmt.Errorf("%w: catalog sync response is unreadable", errTrinoPoolCandidateNotReady)
	}
	return status, nil
}

// registeredWorkerCount reads the coordinator's own node inventory and counts
// the active workers that have registered with it.
func registeredWorkerCount(ctx context.Context, sql rolloutSQLClient, coordinatorNodeID string) (int, error) {
	rows, err := sql.statement(ctx, "SELECT node_id, coordinator, state FROM system.runtime.nodes")
	if err != nil {
		return 0, fmt.Errorf("%w: node inventory unavailable", errTrinoPoolCandidateNotReady)
	}
	workers, coordinators := 0, 0
	for _, row := range rows {
		if len(row) != 3 {
			return 0, fmt.Errorf("%w: invalid node inventory row", errTrinoPoolCandidateNotReady)
		}
		nodeID, idOK := row[0].(string)
		isCoordinator, coordinatorOK := row[1].(bool)
		state, stateOK := row[2].(string)
		if !idOK || !coordinatorOK || !stateOK || state != "active" {
			return 0, fmt.Errorf("%w: inactive or invalid node in the inventory", errTrinoPoolCandidateNotReady)
		}
		if isCoordinator {
			coordinators++
			if nodeID != coordinatorNodeID {
				return 0, fmt.Errorf("%w: the inventory names a different coordinator", errTrinoPoolCandidateNotReady)
			}
			continue
		}
		workers++
	}
	if coordinators != 1 {
		return 0, fmt.Errorf("%w: %d coordinators in the inventory", errTrinoPoolCandidateNotReady, coordinators)
	}
	return workers, nil
}

func syncReason(status catalogSyncStatus) string {
	switch {
	case status.NotReadyReason != "":
		return status.NotReadyReason
	case status.LastFailure != "":
		return status.LastFailure
	case status.FailedCatalogs != 0:
		return fmt.Sprintf("%d catalogs failed to apply", status.FailedCatalogs)
	default:
		return "candidate reports itself not ready"
	}
}

func revisionText(revision *int64) string {
	if revision == nil {
		return "none"
	}
	return fmt.Sprint(*revision)
}
