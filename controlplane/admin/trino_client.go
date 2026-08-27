//go:build kubernetes

package admin

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/posthog/duckgres/controlplane/provisioner/opa"
	"github.com/posthog/duckgres/server/usersecrets"
)

// TrinoAdminSource is the `X-Trino-Source` the admin console stamps on its
// own coordinator reads. Trino records it verbatim as
// system.runtime.queries.source, so tagging lets an operator (and this
// console's own live view) tell console traffic apart from tenant SQL.
// Mirrors provisioner.TrinoProvisionerSource for the reconcile loop.
const TrinoAdminSource = "duckgres-admin"

// trinoObserverUser is the Trino principal the console authenticates as.
// Aliased here so this file reads without chasing the opa package, but it
// is the SAME constant the policy and the password.db projection use — a
// second spelling would authenticate fine and then be denied everything.
const trinoObserverUser = opa.ObserverPrincipal

// Coordinator call budget. The console polls, so a wedged coordinator must
// cost one slow request and not a pile-up: every call is bounded here and
// again by the caller's request context.
const (
	trinoCoordinatorTimeout = 10 * time.Second
	// trinoKillTimeout is shorter: a kill is an interactive action behind a
	// button, and a slow one should report failure rather than hang an
	// operator who is watching a runaway query.
	trinoKillTimeout = 5 * time.Second
)

// errTrinoNotFound is returned for a query the coordinator no longer knows
// about. Trino answers 410 Gone once a finished query ages out of memory,
// which is an expected outcome of opening a stale link, not an outage.
var errTrinoNotFound = errors.New("trino: query not found")

func isTrinoNotFound(err error) bool { return errors.Is(err, errTrinoNotFound) }

// errTrinoEndpointUnavailable is returned when the coordinator answers but
// does not serve the route at all. That is a statement about how the cell is
// built, not about its health, and it is not the same incident as a
// coordinator that never answered.
//
// /v1/node is the case this exists for. Trino binds NodeResource only under
// discovery.type=AIRLIFT_DISCOVERY; the default is ANNOUNCE, which these
// cells use, so the route is absent and the coordinator correctly answers
// 404. Reporting that as a dead cell hides the real state of a healthy one.
var errTrinoEndpointUnavailable = errors.New("trino: endpoint not served by this coordinator")

func isTrinoEndpointUnavailable(err error) bool {
	return errors.Is(err, errTrinoEndpointUnavailable)
}

// queryRouteNotFound re-reads a 404 for the per-query routes as a missing
// query rather than a missing route.
//
// /v1/query/{id} is always served, so a 404 there is about the id, not the
// route: JAX-RS answers 404 when it cannot convert the path parameter into a
// QueryId. Only the aged-out case answers 410, so without this an operator
// following a malformed link would be told the coordinator does not serve
// query lookups at all.
func queryRouteNotFound(err error) error {
	if isTrinoEndpointUnavailable(err) {
		return fmt.Errorf("%w", errTrinoNotFound)
	}
	return err
}

// TrinoQuery is one query as the console shows it: identity, lifecycle and
// the cost counters an operator triages on.
//
// Query is REDACTED (usersecrets.RedactForLog) at decode — see
// trinoCoordinatorHTTPClient.Queries. Raw tenant SQL never populates this
// struct, so no caller can leak it by forgetting.
type TrinoQuery struct {
	QueryID string `json:"query_id"`
	State   string `json:"state"`
	// Org is the duckgres org id, resolved from Principal by the handler.
	// Empty when the principal is not a known tenant — the two operational
	// principals, or a tenant whose row was removed mid-flight.
	Org string `json:"org"`
	// Principal is the Trino username the query authenticated as. For a
	// tenant this is its database_name (TrinoEnabledOrg.TrinoPrincipal).
	Principal string `json:"principal"`
	// Source is the client's X-Trino-Source, verbatim.
	Source string `json:"source"`
	// ResourceGroup is the dotted resource-group path the query was
	// admitted under, e.g. "global.tier_free". Empty while queued for
	// admission.
	ResourceGroup string `json:"resource_group"`
	// Query is the redacted SQL text.
	Query string `json:"query"`

	Created   time.Time `json:"created,omitempty"`
	ElapsedMS int64     `json:"elapsed_ms"`
	QueuedMS  int64     `json:"queued_ms"`
	CPUMS     int64     `json:"cpu_ms"`

	PhysicalInputBytes   int64 `json:"physical_input_bytes"`
	InternalNetworkBytes int64 `json:"internal_network_bytes"`
	PeakMemoryBytes      int64 `json:"peak_memory_bytes"`
	SpilledBytes         int64 `json:"spilled_bytes"`
	ProcessedInputRows   int64 `json:"processed_input_rows"`

	TotalDrivers     int `json:"total_drivers"`
	QueuedDrivers    int `json:"queued_drivers"`
	RunningDrivers   int `json:"running_drivers"`
	CompletedDrivers int `json:"completed_drivers"`

	// FullyBlocked means every driver is blocked — the signature of a query
	// waiting on the metadata store or on S3 rather than doing work.
	FullyBlocked bool `json:"fully_blocked"`
	// ProgressPercentage is nil when Trino cannot estimate it (queued
	// queries, and any query whose splits aren't all known yet). Nil is
	// meaningfully different from 0 and must stay nullable.
	ProgressPercentage *float64 `json:"progress_percentage"`

	ErrorType string `json:"error_type,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
}

// TrinoServerInfo is the coordinator's own `/v1/info`.
type TrinoServerInfo struct {
	Version     string `json:"version"`
	Environment string `json:"environment"`
	Coordinator bool   `json:"coordinator"`
	// Starting is true while the coordinator is still coming up. Queries
	// are rejected in that window, so it explains an otherwise baffling
	// "the cell is up but everything fails".
	Starting bool  `json:"starting"`
	UptimeMS int64 `json:"uptime_ms"`
}

// TrinoNode is one node of the cell as its coordinator describes it.
//
// Neither inventory Trino can serve carries a node id or a version, so
// worker version skew is not observable here. The console derives that from
// the K8s pod projection (/cluster/pods) instead, which reports each pod's
// running image.
//
// Only the failure-detector inventory fills the heartbeat fields; under
// ANNOUNCE every field but URI is a zero value that means "not reported",
// NOT "reported as zero". Read TrinoNodeInventory.Source before showing
// them — a rendered 0.0 failure ratio sourced from ANNOUNCE is a
// fabricated health claim.
type TrinoNode struct {
	URI                string  `json:"uri"`
	AgeMS              int64   `json:"age_ms"`
	RecentFailures     float64 `json:"recent_failures"`
	RecentSuccesses    float64 `json:"recent_successes"`
	RecentFailureRatio float64 `json:"recent_failure_ratio"`
	LastResponseTime   string  `json:"last_response_time,omitempty"`
	// Failed mirrors the coordinator's own verdict from /v1/node/failed
	// rather than re-deriving a threshold here; the failure detector is
	// what actually removes a node from scheduling.
	Failed bool `json:"failed"`

	// The remaining fields come only from system.runtime.nodes. NodeID and
	// Version are not on either REST route, so this is the one place worker
	// version skew is observable from the coordinator.
	NodeID string `json:"node_id,omitempty"`
	// Version is the worker's Trino version. During a rollout a cell runs
	// two of these at once, which is exactly what an operator wants to see.
	Version     string `json:"version,omitempty"`
	Coordinator bool   `json:"coordinator,omitempty"`
	// State is Trino's own NodeState: ACTIVE, INACTIVE, DRAINING,
	// DRAINED or SHUTTING_DOWN.
	State string `json:"state,omitempty"`
}

// Which of Trino's two node inventories a TrinoNodeInventory came from.
// They differ in detail, not just in route, so the source travels with the
// data rather than being re-derived by each consumer.
const (
	// TrinoNodeSourceFailureDetector is `/v1/node`: per-node heartbeat
	// health, plus the coordinator's own failed verdict. Bound only under
	// discovery.type=AIRLIFT_DISCOVERY.
	TrinoNodeSourceFailureDetector = "failure_detector"
	// TrinoNodeSourceSystemTable is `system.runtime.nodes` over SQL: node
	// id, uri, VERSION, coordinator flag and lifecycle state. Served by any
	// cell regardless of discovery.type, because the system connector does
	// not depend on it. Richer than either REST route on everything except
	// heartbeat ratios, and the only source carrying version.
	TrinoNodeSourceSystemTable = "system_table"
	// TrinoNodeSourceAnnounce is `/v1/announce`: the set of node URIs that
	// have announced themselves. Bound under the ANNOUNCE and DNS
	// inventories — ANNOUNCE being Trino's default and what these cells
	// run. It answers "who is in the fleet" and nothing about their health.
	TrinoNodeSourceAnnounce = "announce"
)

// TrinoNodeInventory is the fleet as this cell is able to describe it.
//
// Trino binds exactly one node-listing route depending on discovery.type,
// and the two carry different detail. Returning the source alongside the
// nodes is what lets the console show membership from a cell that cannot
// report health, instead of showing nothing (which reads as an empty
// cluster) or showing zeros (which reads as a perfectly healthy one).
type TrinoNodeInventory struct {
	Source string      `json:"source"`
	Nodes  []TrinoNode `json:"nodes"`
}

// HasHealth reports whether the entries carry the failure detector's
// heartbeat fields (age, recent failure ratio, the failed verdict).
func (i TrinoNodeInventory) HasHealth() bool {
	return i.Source == TrinoNodeSourceFailureDetector
}

// HasNodeDetail reports whether the entries carry node id, version,
// coordinator flag and lifecycle state.
func (i TrinoNodeInventory) HasNodeDetail() bool {
	return i.Source == TrinoNodeSourceSystemTable
}

// TrinoCoordinatorClient is the narrow read surface the console needs.
// An interface so handler tests can drive them without an HTTP server and
// so a coordinator outage is a substitutable condition.
type TrinoCoordinatorClient interface {
	Queries(ctx context.Context) ([]TrinoQuery, error)
	Query(ctx context.Context, queryID string) (*TrinoQuery, error)
	KillQuery(ctx context.Context, queryID, message string) error
	Nodes(ctx context.Context) (TrinoNodeInventory, error)
	ServerInfo(ctx context.Context) (*TrinoServerInfo, error)
}

// TrinoCredentialSource hands out the observer password on each call
// rather than capturing it once. The provisioner regenerates the pair if
// it goes missing, so a captured copy would 401 forever after a
// self-heal; reading through recovers on the next tick.
type TrinoCredentialSource func() (username, password string)

// trinoCoordinatorHTTPClient talks to one cell's coordinator as the
// observer principal.
type trinoCoordinatorHTTPClient struct {
	baseURL string
	hc      *http.Client
	creds   TrinoCredentialSource
}

// newTrinoCoordinatorClient builds a client with a fixed password. Used by
// tests; production goes through newTrinoCoordinatorClientFromSource so a
// regenerated credential is picked up without a restart.
func newTrinoCoordinatorClient(baseURL, password, tlsServerName string) *trinoCoordinatorHTTPClient {
	return NewTrinoCoordinatorClient(baseURL, tlsServerName,
		func() (string, string) { return trinoObserverUser, password }).(*trinoCoordinatorHTTPClient)
}

// NewTrinoCoordinatorClient builds the production client. creds is called
// per request rather than captured so a regenerated observer password is
// picked up without a control-plane restart.
//
// tlsServerName pins TLS verification to the coordinator certificate's
// hostname while dialling the in-cluster Service address — the same
// asymmetry the provisioner's catalog client handles, and for the same
// reason (cert-manager issues for the external name). This is NOT
// InsecureSkipVerify: the chain is still validated.
func NewTrinoCoordinatorClient(baseURL, tlsServerName string, creds TrinoCredentialSource) TrinoCoordinatorClient {
	hc := &http.Client{Timeout: trinoCoordinatorTimeout}
	if tlsServerName != "" {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{
			ServerName: tlsServerName,
			MinVersion: tls.VersionTLS12,
		}
		hc.Transport = transport
	}
	return &trinoCoordinatorHTTPClient{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		hc:      hc,
		creds:   creds,
	}
}

// do issues one authenticated request and returns the body. Non-2xx is an
// error carrying the status, except two cases that are not cell failures:
//
//	410 Gone      the query is no longer held -> errTrinoNotFound, so a
//	              stale query link reads as "gone" rather than as a broken
//	              cell. QueryResource throws GoneException for this, and
//	              only for this.
//	404 Not Found the coordinator does not serve the route at all ->
//	              errTrinoEndpointUnavailable, so an endpoint this cell was
//	              never built with reads as unavailable rather than as
//	              silence from the coordinator.
func (c *trinoCoordinatorHTTPClient) do(ctx context.Context, method, path string, body io.Reader) ([]byte, error) {
	return c.doURL(ctx, method, c.baseURL+path, path, body)
}

// doURL is do() against an absolute URL. The /v1/statement protocol hands
// back absolute nextUri links, so the drain cannot go through the
// baseURL-relative helper. `label` keeps the error text talking about the
// route rather than echoing a full internal coordinator URL.
func (c *trinoCoordinatorHTTPClient) doURL(ctx context.Context, method, rawURL, label string, body io.Reader) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, fmt.Errorf("build %s %s: %w", method, label, err)
	}
	user, password := c.creds()
	req.Header.Set("X-Trino-User", user)
	req.Header.Set("X-Trino-Source", TrinoAdminSource)
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(user, password)

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, label, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, readErr := io.ReadAll(resp.Body)
	switch {
	case resp.StatusCode == http.StatusGone:
		return nil, fmt.Errorf("%s %s: %w", method, label, errTrinoNotFound)
	case resp.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("%s %s: %w", method, label, errTrinoEndpointUnavailable)
	case resp.StatusCode == http.StatusForbidden:
		// Worth its own message: this is what a missing observer grant or
		// an un-rolled-out OPA bundle looks like, and it is fixed in a
		// completely different place from a 5xx.
		return nil, fmt.Errorf("%s %s: 403 forbidden — the %s principal is not authorized by the cell's OPA bundle",
			method, label, trinoObserverUser)
	case resp.StatusCode/100 != 2:
		return nil, fmt.Errorf("%s %s: status %d: %s", method, label, resp.StatusCode, truncateForError(string(raw)))
	}
	if readErr != nil {
		return nil, fmt.Errorf("%s %s: read body: %w", method, label, readErr)
	}
	return raw, nil
}

// truncateForError bounds a coordinator error body so a stray HTML error
// page doesn't become the whole log line.
func truncateForError(s string) string {
	const max = 256
	s = strings.TrimSpace(s)
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}

// --------------------------------------------------------------------------
// Wire shapes. These mirror io.trino.server.BasicQueryInfo / BasicQueryStats
// as Jackson emits them. The airlift value types are STRINGS on the wire
// (see parseAirliftDurationMS / parseAirliftDataSizeBytes), which is why
// every duration and size below is typed string, not a number.
// --------------------------------------------------------------------------

type trinoBasicQueryInfo struct {
	QueryID         string   `json:"queryId"`
	State           string   `json:"state"`
	Query           string   `json:"query"`
	ResourceGroupID []string `json:"resourceGroupId"`
	Session         struct {
		User   string `json:"user"`
		Source string `json:"source"`
	} `json:"session"`
	QueryStats trinoBasicQueryStats `json:"queryStats"`
	ErrorType  string               `json:"errorType"`
	ErrorCode  struct {
		Name string `json:"name"`
	} `json:"errorCode"`
}

type trinoBasicQueryStats struct {
	CreateTime                   time.Time `json:"createTime"`
	ElapsedTime                  string    `json:"elapsedTime"`
	QueuedTime                   string    `json:"queuedTime"`
	TotalCPUTime                 string    `json:"totalCpuTime"`
	PhysicalInputDataSize        string    `json:"physicalInputDataSize"`
	InternalNetworkInputDataSize string    `json:"internalNetworkInputDataSize"`
	PeakTotalMemoryReservation   string    `json:"peakTotalMemoryReservation"`
	SpilledDataSize              string    `json:"spilledDataSize"`
	ProcessedInputPositions      int64     `json:"processedInputPositions"`
	TotalDrivers                 int       `json:"totalDrivers"`
	QueuedDrivers                int       `json:"queuedDrivers"`
	RunningDrivers               int       `json:"runningDrivers"`
	CompletedDrivers             int       `json:"completedDrivers"`
	FullyBlocked                 bool      `json:"fullyBlocked"`
	ProgressPercentage           *float64  `json:"progressPercentage"`
}

// toTrinoQuery projects a coordinator payload onto the console shape,
// redacting the SQL on the way through.
func (w trinoBasicQueryInfo) toTrinoQuery() TrinoQuery {
	return TrinoQuery{
		QueryID:   w.QueryID,
		State:     w.State,
		Principal: w.Session.User,
		Source:    w.Session.Source,
		// Trino models the resource group as a path; the dotted form is
		// what its own config and web UI use.
		ResourceGroup: strings.Join(w.ResourceGroupID, "."),
		// Redact HERE, at the single decode point, so no downstream caller
		// can forget. A tenant's SQL embeds table names, filter literals
		// and customer identifiers, and a failed CREATE SECRET embeds a
		// credential.
		Query: usersecrets.RedactForLog(w.Query),

		Created:   w.QueryStats.CreateTime,
		ElapsedMS: int64(parseAirliftDurationMS(w.QueryStats.ElapsedTime)),
		QueuedMS:  int64(parseAirliftDurationMS(w.QueryStats.QueuedTime)),
		CPUMS:     int64(parseAirliftDurationMS(w.QueryStats.TotalCPUTime)),

		PhysicalInputBytes:   parseAirliftDataSizeBytes(w.QueryStats.PhysicalInputDataSize),
		InternalNetworkBytes: parseAirliftDataSizeBytes(w.QueryStats.InternalNetworkInputDataSize),
		PeakMemoryBytes:      parseAirliftDataSizeBytes(w.QueryStats.PeakTotalMemoryReservation),
		SpilledBytes:         parseAirliftDataSizeBytes(w.QueryStats.SpilledDataSize),
		ProcessedInputRows:   w.QueryStats.ProcessedInputPositions,

		TotalDrivers:     w.QueryStats.TotalDrivers,
		QueuedDrivers:    w.QueryStats.QueuedDrivers,
		RunningDrivers:   w.QueryStats.RunningDrivers,
		CompletedDrivers: w.QueryStats.CompletedDrivers,

		FullyBlocked:       w.QueryStats.FullyBlocked,
		ProgressPercentage: w.QueryStats.ProgressPercentage,

		ErrorType: w.ErrorType,
		ErrorCode: w.ErrorCode.Name,
	}
}

// Queries lists every query the coordinator still holds — running, queued,
// and recently finished. What comes back is already filtered by Trino
// through FilterViewQueryOwnedBy, so this is exactly the set the observer
// principal is authorized to see.
func (c *trinoCoordinatorHTTPClient) Queries(ctx context.Context) ([]TrinoQuery, error) {
	raw, err := c.do(ctx, http.MethodGet, "/v1/query", nil)
	if err != nil {
		return nil, err
	}
	var wire []trinoBasicQueryInfo
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, fmt.Errorf("decode query list: %w", err)
	}
	out := make([]TrinoQuery, 0, len(wire))
	for _, w := range wire {
		out = append(out, w.toTrinoQuery())
	}
	return out, nil
}

// Query fetches one query. It reads the same BasicQueryInfo shape as the
// list rather than the full QueryInfo: the full form carries the entire
// stage/operator tree and the unredacted plan, which is both large and a
// second copy of tenant data with no redactor in front of it.
func (c *trinoCoordinatorHTTPClient) Query(ctx context.Context, queryID string) (*TrinoQuery, error) {
	raw, err := c.do(ctx, http.MethodGet, "/v1/query/"+url.PathEscape(queryID)+"?pruned=true", nil)
	if err != nil {
		return nil, queryRouteNotFound(err)
	}
	var wire trinoBasicQueryInfo
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, fmt.Errorf("decode query %s: %w", queryID, err)
	}
	q := wire.toTrinoQuery()
	return &q, nil
}

// KillQuery fails a running query with an explanation.
//
// PUT .../killed rather than DELETE .../{id}: the DELETE path cancels
// silently, while the kill path fails the query with a message the TENANT
// sees in their client. An operator killing someone's query should leave
// that trace, not a mystery cancellation.
func (c *trinoCoordinatorHTTPClient) KillQuery(ctx context.Context, queryID, message string) error {
	ctx, cancel := context.WithTimeout(ctx, trinoKillTimeout)
	defer cancel()
	_, err := c.do(ctx, http.MethodPut, "/v1/query/"+url.PathEscape(queryID)+"/killed", strings.NewReader(message))
	return queryRouteNotFound(err)
}

type trinoNodeStats struct {
	URI                string  `json:"uri"`
	Age                string  `json:"age"`
	RecentFailures     float64 `json:"recentFailures"`
	RecentSuccesses    float64 `json:"recentSuccesses"`
	RecentFailureRatio float64 `json:"recentFailureRatio"`
	LastResponseTime   string  `json:"lastResponseTime"`
}

// Nodes reports the coordinator's failure-detector view of the fleet,
// flagging the ones the coordinator has actually taken out of scheduling.
//
// The failed set comes from `/v1/node/failed` rather than from a threshold
// applied here: the failure detector's verdict is what determines whether
// a node receives splits, and a second opinion computed in the console
// would disagree with the engine exactly when it matters.
func (c *trinoCoordinatorHTTPClient) Nodes(ctx context.Context) (TrinoNodeInventory, error) {
	raw, err := c.do(ctx, http.MethodGet, "/v1/node", nil)
	if err != nil {
		// A cell on the default discovery.type does not bind /v1/node at
		// all. Fall back rather than report a fleet we asked for the wrong
		// way: system.runtime.nodes first, because it carries version and
		// lifecycle state and every cell serves it, then /v1/announce for
		// bare membership if the SQL path is unavailable too.
		if isTrinoEndpointUnavailable(err) {
			if inv, sqlErr := c.systemTableNodes(ctx); sqlErr == nil {
				return inv, nil
			}
			return c.announcedNodes(ctx)
		}
		return TrinoNodeInventory{}, err
	}
	var wire []trinoNodeStats
	if err := json.Unmarshal(raw, &wire); err != nil {
		return TrinoNodeInventory{}, fmt.Errorf("decode node list: %w", err)
	}

	failed := map[string]bool{}
	if failedRaw, failedErr := c.do(ctx, http.MethodGet, "/v1/node/failed", nil); failedErr == nil {
		var failedWire []trinoNodeStats
		if json.Unmarshal(failedRaw, &failedWire) == nil {
			for _, f := range failedWire {
				failed[f.URI] = true
			}
		}
	}

	out := make([]TrinoNode, 0, len(wire))
	for _, n := range wire {
		out = append(out, TrinoNode{
			URI:                n.URI,
			AgeMS:              int64(parseAirliftDurationMS(n.Age)),
			RecentFailures:     n.RecentFailures,
			RecentSuccesses:    n.RecentSuccesses,
			RecentFailureRatio: n.RecentFailureRatio,
			LastResponseTime:   n.LastResponseTime,
			Failed:             failed[n.URI],
		})
	}
	return TrinoNodeInventory{Source: TrinoNodeSourceFailureDetector, Nodes: out}, nil
}

// trinoNodesQuery is the console's ONE SQL statement. Columns are listed
// explicitly rather than SELECT *, so a future Trino adding a column cannot
// silently change the row shape this decodes positionally.
//
// The observer's OPA grant is pinned to exactly this table; see the
// "observer's ONE data read" section of policy.rego.
const trinoNodesQuery = `SELECT node_id, http_uri, node_version, coordinator, state FROM system.runtime.nodes`

// systemTableNodes reads the fleet from system.runtime.nodes over SQL.
//
// The system connector is served by every cell regardless of discovery.type,
// so this works where /v1/node does not, and it is the only source that
// carries the worker VERSION — i.e. the only way the console can show
// version skew mid-rollout.
//
// It costs the observer a catalog grant (AccessCatalog on `system`,
// SelectFromColumns on this one table) and a resource-group lane. Both are
// deliberately narrow; policy.rego documents exactly what stays denied.
func (c *trinoCoordinatorHTTPClient) systemTableNodes(ctx context.Context) (TrinoNodeInventory, error) {
	rows, err := c.runStatement(ctx, trinoNodesQuery)
	if err != nil {
		return TrinoNodeInventory{}, err
	}
	out := make([]TrinoNode, 0, len(rows))
	for _, r := range rows {
		// Positional decode against trinoNodesQuery's column list. A short
		// row is skipped rather than panicking a console request.
		if len(r) < 5 {
			continue
		}
		out = append(out, TrinoNode{
			NodeID:      stringCell(r[0]),
			URI:         stringCell(r[1]),
			Version:     stringCell(r[2]),
			Coordinator: r[3] == true,
			State:       stringCell(r[4]),
		})
	}
	// Coordinator first, then by URI: a stable order, and the node an
	// operator most often wants at the top.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Coordinator != out[j].Coordinator {
			return out[i].Coordinator
		}
		return out[i].URI < out[j].URI
	})
	return TrinoNodeInventory{Source: TrinoNodeSourceSystemTable, Nodes: out}, nil
}

// stringCell reads a Trino JSON result cell as a string, tolerating the
// null a nullable column can produce.
func stringCell(v interface{}) string {
	s, _ := v.(string)
	return s
}

// trinoStatementResponse is the subset of the /v1/statement response the
// console needs. The full payload is much larger.
type trinoStatementResponse struct {
	NextURI string          `json:"nextUri,omitempty"`
	Data    [][]interface{} `json:"data,omitempty"`
	Error   *struct {
		Message   string `json:"message"`
		ErrorName string `json:"errorName"`
		ErrorType string `json:"errorType"`
	} `json:"error,omitempty"`
}

// runStatement executes one statement and drains the nextUri chain.
//
// Bounded hops so a coordinator that never completes cannot hang a console
// request; the node query returns in a couple of hops in practice. Every hop
// honours ctx, so the handler's timeout aborts the drain promptly.
func (c *trinoCoordinatorHTTPClient) runStatement(ctx context.Context, sql string) ([][]interface{}, error) {
	const maxDrainHops = 50
	body, err := c.doURL(ctx, http.MethodPost, c.baseURL+"/v1/statement", "/v1/statement", strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	var all [][]interface{}
	for hop := 0; hop < maxDrainHops; hop++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("statement drain aborted: %w", err)
		}
		var r trinoStatementResponse
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, fmt.Errorf("parse statement response: %w", err)
		}
		if r.Error != nil {
			// A denied grant surfaces here rather than as an HTTP status:
			// the POST succeeds and the failure arrives inside the payload.
			return nil, fmt.Errorf("trino: %s (%s): %s", r.Error.ErrorName, r.Error.ErrorType, r.Error.Message)
		}
		all = append(all, r.Data...)
		if r.NextURI == "" {
			return all, nil
		}
		if body, err = c.doURL(ctx, http.MethodGet, r.NextURI, "/v1/statement", nil); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("statement drain exceeded %d hops without completing", maxDrainHops)
}

// announcedNodes reads the ANNOUNCE inventory: the set of node URIs that
// have announced themselves to this coordinator.
//
// `GET /v1/announce` is bound by AnnounceNodeInventoryModule and
// DnsNodeInventoryModule — i.e. for every discovery.type except the one
// that binds /v1/node — so between them the console can name the fleet of
// any cell. It carries membership only: no heartbeat, no age, no failed
// verdict, which is why the inventory records where it came from.
//
// It is declared @ResourceSecurity(MANAGEMENT_READ), the same access type
// as /v1/node, so the observer's existing ReadSystemInformation grant
// already covers it. This needs no change to policy.rego.
func (c *trinoCoordinatorHTTPClient) announcedNodes(ctx context.Context) (TrinoNodeInventory, error) {
	raw, err := c.do(ctx, http.MethodGet, "/v1/announce", nil)
	if err != nil {
		return TrinoNodeInventory{}, err
	}
	var uris []string
	if err := json.Unmarshal(raw, &uris); err != nil {
		return TrinoNodeInventory{}, fmt.Errorf("decode announced node list: %w", err)
	}
	// The wire type is a Set<URI>, so the order is whatever the coordinator's
	// hash iteration produced. Sort it: without this the console reshuffles
	// its node rows on every poll.
	sort.Strings(uris)
	out := make([]TrinoNode, 0, len(uris))
	for _, u := range uris {
		out = append(out, TrinoNode{URI: u})
	}
	return TrinoNodeInventory{Source: TrinoNodeSourceAnnounce, Nodes: out}, nil
}

type trinoServerInfoWire struct {
	NodeVersion struct {
		Version string `json:"version"`
	} `json:"nodeVersion"`
	Environment string `json:"environment"`
	Coordinator bool   `json:"coordinator"`
	Starting    bool   `json:"starting"`
	Uptime      string `json:"uptime"`
}

// ServerInfo reads the coordinator's `/v1/info`. Unlike everything else
// here it is a PUBLIC endpoint in Trino, so it still answers when the
// observer's authorization is broken — which makes it the useful probe for
// telling "the cell is down" apart from "the console cannot see it".
func (c *trinoCoordinatorHTTPClient) ServerInfo(ctx context.Context) (*TrinoServerInfo, error) {
	raw, err := c.do(ctx, http.MethodGet, "/v1/info", nil)
	if err != nil {
		return nil, err
	}
	var wire trinoServerInfoWire
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, fmt.Errorf("decode server info: %w", err)
	}
	return &TrinoServerInfo{
		Version:     wire.NodeVersion.Version,
		Environment: wire.Environment,
		Coordinator: wire.Coordinator,
		Starting:    wire.Starting,
		UptimeMS:    int64(parseAirliftDurationMS(wire.Uptime)),
	}, nil
}

// --------------------------------------------------------------------------
// Airlift unit parsing.
//
// io.airlift.units.Duration and DataSize are @JsonValue types: they arrive
// as STRINGS, not numbers. Decoding either into a numeric Go field yields a
// zero value and a plausible-looking page of zeroes, so both are parsed
// explicitly here.
//
// Both parsers return 0 for anything they cannot read. A coordinator
// upgrade that changes a unit spelling should cost one column, not the
// whole live view — and a hard failure here would take out the page an
// operator opened precisely because something is wrong.
// --------------------------------------------------------------------------

// durationUnitMS maps airlift's unit abbreviations (Duration.timeUnitToString)
// to milliseconds. Longest suffixes must be tried first — "ms" and "m" share
// a prefix, and matching "m" against "12.34ms" would read 12.34 MINUTES.
var durationUnitMS = []struct {
	suffix string
	ms     float64
}{
	{"ns", 1.0 / 1_000_000},
	{"us", 1.0 / 1_000},
	{"ms", 1},
	{"s", 1_000},
	{"m", 60_000},
	{"h", 3_600_000},
	{"d", 86_400_000},
}

// parseAirliftDurationMS converts an airlift Duration string ("12.34ms",
// "1.50s", "2.00m") to milliseconds.
func parseAirliftDurationMS(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	for _, u := range durationUnitMS {
		if !strings.HasSuffix(s, u.suffix) {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSuffix(s, u.suffix), 64)
		if err != nil {
			// Keep scanning: a value like "5s" has already matched "s",
			// but a malformed one should fall through to 0 rather than
			// match a shorter suffix by accident.
			return 0
		}
		return v * u.ms
	}
	return 0
}

// dataSizeUnitBytes maps DataSize.Unit's strings to byte multipliers.
// Ordered longest-first for the same prefix reason as durations ("kB" vs
// "B"). JSON always carries the "B" form (toBytesValueString), but
// DataSize.toString() emits scaled units and those show up in log lines and
// hand-written fixtures, so both are accepted.
var dataSizeUnitBytes = []struct {
	suffix string
	mult   float64
}{
	{"kB", 1 << 10},
	{"MB", 1 << 20},
	{"GB", 1 << 30},
	{"TB", 1 << 40},
	{"PB", 1 << 50},
	{"B", 1},
}

// parseAirliftDataSizeBytes converts an airlift DataSize string to bytes.
func parseAirliftDataSizeBytes(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	for _, u := range dataSizeUnitBytes {
		if !strings.HasSuffix(s, u.suffix) {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSuffix(s, u.suffix), 64)
		if err != nil {
			return 0
		}
		return int64(v * u.mult)
	}
	return 0
}
