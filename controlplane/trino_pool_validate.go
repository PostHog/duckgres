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

// trinoPoolExpectation is what the pool requires of a candidate. Every field is
// compared; none of them is assumed.
type trinoPoolExpectation struct {
	// Image is the blueprint's digest-pinned release image.
	Image string
	// CatalogRevision is the pool's published catalog revision. A structurally
	// healthy coordinator sitting at an older revision is NOT certified: it
	// would serve a catalog set that does not include the newest tenant.
	CatalogRevision int64
	// PolicyRevision is the authorization projection this control plane
	// currently serves. The candidate's access control must report deciding
	// with exactly this value before the auth-revision check may be claimed.
	// Empty means the projection is unknown here, and nothing may be claimed on
	// its behalf - which fails admission closed at the Gateway.
	PolicyRevision string
	// PasswordRevision and GroupRevision are the fingerprints of the
	// authentication files this control plane has projected.
	//
	// They are checked for the same reason as the policy revision, and they are
	// NOT implied by it: the OPA bundle and the auth Secret reach a coordinator
	// by different paths and at different times, so a candidate can be deciding
	// with the current authorization data while its password store still
	// predates the tenant that is about to be admitted. That candidate passes an
	// authorization-only check and then rejects that tenant's very first
	// request.
	PasswordRevision string
	GroupRevision    string
	// InternalHTTP marks a pooled coordinator reached on its in-cluster
	// Service, where TLS terminates at the Gateway.
	InternalHTTP bool
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
	// Unacknowledged names the security components that did NOT report a
	// loaded revision. It is recorded and surfaced rather than being folded
	// into a pass: a component that cannot say what it loaded has not
	// acknowledged anything, and treating its silence as agreement is exactly
	// the false readiness this validation exists to prevent.
	Unacknowledged []string
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
	observed trinoPoolObservation,
	expected trinoPoolExpectation,
) (trinoPoolValidation, error) {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolProbePassBudget)
	defer cancel()

	// The image is checked against what the cluster is RUNNING, not against
	// what the spec asked for. Claiming the check without comparing anything
	// put a false acknowledgement into the Gateway's durable evidence - the
	// same defect the auth-revision handling exists to avoid.
	if expected.Image == "" {
		return trinoPoolValidation{}, fmt.Errorf("%w: no expected image to verify against", errTrinoPoolCandidateNotReady)
	}
	if observed.CoordinatorImage != expected.Image {
		return trinoPoolValidation{}, fmt.Errorf("%w: coordinator runs image %q, the release pins %q",
			errTrinoPoolCandidateNotReady, observed.CoordinatorImage, expected.Image)
	}
	if observed.WorkerImage != "" && observed.WorkerImage != expected.Image {
		return trinoPoolValidation{}, fmt.Errorf("%w: workers run image %q, the release pins %q",
			errTrinoPoolCandidateNotReady, observed.WorkerImage, expected.Image)
	}

	observedWorkers := observed.ReadyWorkers
	requiredCatalogRevision := expected.CatalogRevision

	username, password := credential()
	sql := rolloutSQLClient{
		baseURL: coordinatorURL, client: client, username: username, password: password,
		internalHTTP: expected.InternalHTTP,
	}

	before, err := sql.info(ctx)
	if err != nil {
		return trinoPoolValidation{}, fmt.Errorf("%w: %v", errTrinoPoolCandidateNotReady, err)
	}

	sync, err := fetchCatalogSync(ctx, client, coordinatorURL, username, password, expected.InternalHTTP)
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

	acknowledged, unacknowledged := splitSecurityRevisions(sync.SecurityRevisions)
	checks := []string{
		trinoPoolCheckImage,
		trinoPoolCheckWorkers,
		trinoPoolCheckCatalogRevision,
		trinoPoolCheckOperationalConnection,
	}
	// The auth-revision check is claimed ONLY when both of these hold:
	//
	//   - every security component the coordinator exposes reported what it
	//     loaded, and
	//   - the authorization data one of them reports is the projection THIS
	//     control plane is serving right now.
	//
	// The second condition is the one that makes the check mean anything. "The
	// components answered" proves a coordinator can describe itself, not that
	// it decides with current data: a pooled coordinator whose OPA still serves
	// the bundle from before a tenant was provisioned answers perfectly and
	// authorizes against a policy that has never heard of that tenant. The
	// Gateway records this list verbatim and an operator reads it as evidence,
	// so an unverifiable claim must be absent rather than optimistic.
	//
	// It requires `opa.policy.revision-uri` on a pooled coordinator, pointed at
	// the document the bundle publishes. Without it the access control reports
	// nothing, the check is absent, and admission fails closed.
	// Every component whose data this control plane projects must report having
	// loaded exactly what is being served: the authorization bundle, the
	// password file and the group file. They travel by different paths and
	// settle at different times, so one being current says nothing about the
	// others - a coordinator with the newest bundle and a password file from
	// before the tenant existed refuses that tenant's first request while
	// looking perfectly healthy.
	expectations := map[string]string{
		trinoAccessControlKind:      expected.PolicyRevision,
		trinoPasswordAuthenticator:  expected.PasswordRevision,
		trinoGroupProviderComponent: expected.GroupRevision,
	}
	projectionAcknowledged := true
	for kind, revision := range expectations {
		if revision == "" || !reportsRevision(sync.SecurityRevisions, kind, revision) {
			projectionAcknowledged = false
			break
		}
	}
	if len(acknowledged) > 0 && len(unacknowledged) == 0 && projectionAcknowledged {
		checks = append(checks, trinoPoolCheckAuthRevision)
	}

	validation := trinoPoolValidation{
		NodeID:          sync.NodeID,
		ProcessID:       sync.ProcessID,
		CoordinatorID:   before.CoordinatorID,
		AppliedRevision: *sync.AppliedRevision,
		AuthRevision:    authRevisionFingerprint(sync.SecurityRevisions),
		ReadyWorkers:    registered,
		Checks:          checks,
		Unacknowledged:  unacknowledged,
	}
	validation.CertificateHash = certificateHash(validation)
	return validation, nil
}

// trinoAccessControlKind is how the coordinator names an authorization
// component in its readiness report. Matching on the KIND rather than on the
// configured implementation name keeps this working for a deployment that names
// its access control something other than "opa".
const (
	trinoAccessControlKind      = "system-access-control"
	trinoPasswordAuthenticator  = "password-authenticator"
	trinoGroupProviderComponent = "group-provider"
)

// reportsRevision reports whether this kind of component is present AND every
// instance of it acknowledged exactly this revision.
//
// Equality, not ordering: the question is whether the coordinator decides with
// the data being served, and a coordinator carrying a LATER revision than this
// replica knows about is equally uncertifiable here.
//
// EVERY instance has to match, not merely one. A coordinator configured with a
// second password authenticator - a file this control plane does not write -
// can authenticate principals outside the projection, and admitting it on the
// strength of the one component that agrees would put that file inside the
// pool's trust boundary without anybody stating it.
func reportsRevision(revisions []componentRevision, kind, revision string) bool {
	found := false
	for _, reported := range revisions {
		if reported.Kind != kind {
			continue
		}
		if reported.Error != "" || reported.Revision != revision {
			return false
		}
		found = true
	}
	return found
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

func fetchCatalogSync(ctx context.Context, client *http.Client, coordinatorURL, username, password string, internalHTTP bool) (catalogSyncStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolProbeRequestBudget)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, coordinatorURL+catalogSyncPath, nil)
	if err != nil {
		return catalogSyncStatus{}, fmt.Errorf("build catalog sync request: %w", err)
	}
	request.SetBasicAuth(username, password)
	request.Header.Set("Accept", "application/json")
	if internalHTTP {
		// Same forwarded-HTTPS declaration the statement client sends: the
		// readiness endpoint is authenticated too, and a management probe must
		// not be the one path that quietly drops that requirement.
		request.Header.Set("X-Forwarded-Proto", forwardedScheme)
		request.Header.Set("X-Forwarded-Port", "443")
	}
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

// splitSecurityRevisions separates components that reported a loaded revision
// from those that did not.
//
// A component reports an error when it cannot describe its own loaded state -
// for the OPA access control that is the normal case today, because the plugin
// does not implement the interface that would let it say which bundle revision
// its decisions are being made against. That silence is a fact about the
// system, not a validation failure to retry, so it is carried forward rather
// than swallowed.
func splitSecurityRevisions(revisions []componentRevision) (acknowledged, unacknowledged []string) {
	for _, revision := range revisions {
		name := revision.Kind + "/" + revision.Name
		if revision.Error != "" || strings.TrimSpace(revision.Revision) == "" {
			unacknowledged = append(unacknowledged, name)
			continue
		}
		acknowledged = append(acknowledged, name)
	}
	sort.Strings(acknowledged)
	sort.Strings(unacknowledged)
	return acknowledged, unacknowledged
}

// probeProcessIdentity reads ONLY the coordinator's process identity.
//
// It runs before member registration because the Gateway binds podUid and
// bootId at registration and then requires the admission receipt to carry the
// identical pair. Registering the pod UID as the boot id and admitting with the
// Trino processId made every admission fail POOL_NOT_CERTIFIED: there is one
// authoritative boot identity, and it is the coordinator's processId, which
// changes on every JVM start exactly as a boot identity must.
func probeProcessIdentity(ctx context.Context, client *http.Client, coordinatorURL string, credential func() (string, string), internalHTTP bool) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, trinoPoolProbeRequestBudget)
	defer cancel()

	username, password := credential()
	status, err := fetchCatalogSync(ctx, client, coordinatorURL, username, password, internalHTTP)
	if err != nil {
		return "", err
	}
	if status.ProcessID == "" {
		return "", fmt.Errorf("%w: candidate reports no process identity", errTrinoPoolCandidateNotReady)
	}
	return status.ProcessID, nil
}

// trinoPoolProjectionRevisions is what this control plane currently serves to
// coordinators: the authorization bundle's revision and the fingerprints of the
// password and group files.
//
// They are carried together because they are checked together. Any one of them
// being current is not evidence about the others: the bundle is pulled over
// HTTP on OPA's schedule, while the files arrive as a mounted Secret the
// kubelet refreshes on its own.
type trinoPoolProjectionRevisions struct {
	Policy   string
	Password string
	Group    string
}
