//go:build kubernetes

package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

// --------------------------------------------------------------------------
// Airlift unit parsing.
//
// The coordinator's REST payloads are Jackson-serialized airlift value types,
// not plain numbers: io.airlift.units.Duration serializes via @JsonValue as
// "%.2f<unit>" ("12.34ms", "1.50s", "2.00m"), and io.airlift.units.DataSize
// serializes via toBytesValueString() as an exact byte count with a "B"
// suffix ("1234B") regardless of how the value was constructed. Decoding
// either as a JSON number silently yields zero for every stat on the page.
// --------------------------------------------------------------------------

func TestParseAirliftDuration(t *testing.T) {
	cases := []struct {
		in     string
		wantMS float64
	}{
		{"0.00ns", 0},
		{"1500.00ns", 0.0015},
		{"250.00us", 0.25},
		{"12.34ms", 12.34},
		{"1.50s", 1500},
		{"2.00m", 120000},
		{"1.25h", 4500000},
		{"1.00d", 86400000},
		// No-decimal and integer forms: Duration always emits two decimals,
		// but a hand-written or future payload should still parse.
		{"5s", 5000},
		{"", 0},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := parseAirliftDurationMS(tc.in)
			if diff := got - tc.wantMS; diff > 0.001 || diff < -0.001 {
				t.Errorf("parseAirliftDurationMS(%q) = %v, want %v", tc.in, got, tc.wantMS)
			}
		})
	}
}

// TestParseAirliftDurationRejectsGarbage: an unparseable value must read as
// zero rather than panicking or poisoning the whole payload. A coordinator
// upgrade that changes a unit spelling should degrade one number, not the
// operator's whole live view.
func TestParseAirliftDurationRejectsGarbage(t *testing.T) {
	for _, in := range []string{"abc", "12.34fortnights", "ms", "-", "12.34 ms"} {
		if got := parseAirliftDurationMS(in); got != 0 {
			t.Errorf("parseAirliftDurationMS(%q) = %v, want 0", in, got)
		}
	}
}

func TestParseAirliftDataSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"0B", 0},
		{"1234B", 1234},
		{"2748779069440B", 2748779069440},
		// DataSize's JSON form is always bytes, but toString() can emit
		// scaled units, so accept them rather than reading 1kB as 1.
		{"1kB", 1024},
		{"2MB", 2 << 20},
		{"1.50GB", 1610612736},
		{"1TB", 1 << 40},
		{"", 0},
		{"nonsense", 0},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := parseAirliftDataSizeBytes(tc.in); got != tc.want {
				t.Errorf("parseAirliftDataSizeBytes(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// --------------------------------------------------------------------------
// Coordinator REST client.
// --------------------------------------------------------------------------

// trinoTestCoordinator spins up a fake coordinator and returns a client
// pointed at it plus a recorder of the requests it received.
type trinoTestCoordinator struct {
	server   *httptest.Server
	requests []*http.Request
	// handler, when set, overrides the default routing.
	handler http.HandlerFunc
}

func newTrinoTestCoordinator(t *testing.T, h http.HandlerFunc) (*trinoCoordinatorHTTPClient, *trinoTestCoordinator) {
	t.Helper()
	rec := &trinoTestCoordinator{handler: h}
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.requests = append(rec.requests, r.Clone(r.Context()))
		rec.handler(w, r)
	}))
	t.Cleanup(rec.server.Close)
	c := newTrinoCoordinatorClient(rec.server.URL, "obs-password", "")
	return c, rec
}

func TestCoordinatorClientAuthenticatesAsTheObserver(t *testing.T) {
	c, rec := newTrinoTestCoordinator(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	})
	if _, err := c.Queries(context.Background()); err != nil {
		t.Fatalf("Queries: %v", err)
	}
	if len(rec.requests) != 1 {
		t.Fatalf("expected 1 request, got %d", len(rec.requests))
	}
	r := rec.requests[0]
	if got := r.URL.Path; got != "/v1/query" {
		t.Errorf("path = %q, want /v1/query", got)
	}
	if got := r.Header.Get("X-Trino-User"); got != trinoObserverUser {
		t.Errorf("X-Trino-User = %q, want %q", got, trinoObserverUser)
	}
	// Tagging the source is what keeps the console's own reads out of the
	// console's own live-query view.
	if got := r.Header.Get("X-Trino-Source"); got != TrinoAdminSource {
		t.Errorf("X-Trino-Source = %q, want %q", got, TrinoAdminSource)
	}
	if _, _, ok := r.BasicAuth(); !ok {
		t.Error("expected HTTP basic auth on the coordinator request")
	}
}

// TestCoordinatorClientDecodesQueryStats is the payload-shape regression
// guard: every stat the live view shows arrives as an airlift-encoded
// string, and decoding one as a number zeroes that column silently.
func TestCoordinatorClientDecodesQueryStats(t *testing.T) {
	payload := `[{
	  "queryId": "20260826_101112_00007_abcde",
	  "state": "RUNNING",
	  "query": "SELECT * FROM events",
	  "resourceGroupId": ["global", "tier_free"],
	  "session": {"user": "db42", "source": "trino-cli", "principal": "db42"},
	  "queryStats": {
	    "createTime": "2026-08-26T10:11:12.000Z",
	    "elapsedTime": "1.50s",
	    "queuedTime": "12.34ms",
	    "totalCpuTime": "2.00m",
	    "physicalInputDataSize": "1234B",
	    "internalNetworkInputDataSize": "10B",
	    "peakTotalMemoryReservation": "2097152B",
	    "spilledDataSize": "0B",
	    "processedInputPositions": 4200,
	    "totalDrivers": 10,
	    "queuedDrivers": 1,
	    "runningDrivers": 2,
	    "completedDrivers": 7,
	    "fullyBlocked": true,
	    "progressPercentage": 70.0
	  }
	}]`
	c, _ := newTrinoTestCoordinator(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	})

	got, err := c.Queries(context.Background())
	if err != nil {
		t.Fatalf("Queries: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 query, got %d", len(got))
	}
	q := got[0]
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"QueryID", q.QueryID, "20260826_101112_00007_abcde"},
		{"State", q.State, "RUNNING"},
		{"Principal", q.Principal, "db42"},
		{"Source", q.Source, "trino-cli"},
		{"ElapsedMS", q.ElapsedMS, int64(1500)},
		{"QueuedMS", q.QueuedMS, int64(12)},
		{"CPUMS", q.CPUMS, int64(120000)},
		{"PhysicalInputBytes", q.PhysicalInputBytes, int64(1234)},
		{"PeakMemoryBytes", q.PeakMemoryBytes, int64(2097152)},
		{"ProcessedInputRows", q.ProcessedInputRows, int64(4200)},
		{"TotalDrivers", q.TotalDrivers, 10},
		{"RunningDrivers", q.RunningDrivers, 2},
		{"QueuedDrivers", q.QueuedDrivers, 1},
		{"CompletedDrivers", q.CompletedDrivers, 7},
		{"FullyBlocked", q.FullyBlocked, true},
		{"ResourceGroup", q.ResourceGroup, "global.tier_free"},
	}
	for _, ch := range checks {
		if ch.got != ch.want {
			t.Errorf("%s = %v, want %v", ch.name, ch.got, ch.want)
		}
	}
	if q.ProgressPercentage == nil || *q.ProgressPercentage != 70 {
		t.Errorf("ProgressPercentage = %v, want 70", q.ProgressPercentage)
	}
	if q.Created.IsZero() {
		t.Error("Created was not decoded")
	}
}

// TestCoordinatorClientRedactsQueryText: SQL text is tenant data. The
// coordinator hands it over in full; nothing past this boundary should ever
// see the raw form, so the client redacts at the decode step rather than
// leaving it to each caller to remember.
func TestCoordinatorClientRedactsQueryText(t *testing.T) {
	const secretSQL = `CREATE SECRET s (TYPE s3, KEY_ID 'AKIAEXAMPLE', SECRET 'sup3rs3cret')`
	body, err := json.Marshal([]map[string]any{{
		"queryId":    "q1",
		"state":      "RUNNING",
		"query":      secretSQL,
		"session":    map[string]any{"user": "db42"},
		"queryStats": map[string]any{"elapsedTime": "1.00s"},
	}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	c, _ := newTrinoTestCoordinator(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	})

	got, err := c.Queries(context.Background())
	if err != nil {
		t.Fatalf("Queries: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 query, got %d", len(got))
	}
	if got[0].Query == secretSQL {
		t.Error("query text reached the client verbatim; it must be redacted at decode")
	}
	if got[0].Query == "" {
		t.Error("redaction dropped the query text entirely; operators still need the shape of the statement")
	}
}

func TestCoordinatorClientKillQuery(t *testing.T) {
	var gotMethod, gotPath, gotBody string
	c, _ := newTrinoTestCoordinator(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		buf := make([]byte, 256)
		n, _ := r.Body.Read(buf)
		gotBody = string(buf[:n])
		w.WriteHeader(http.StatusOK)
	})

	if err := c.KillQuery(context.Background(), "q1", "killed by operator"); err != nil {
		t.Fatalf("KillQuery: %v", err)
	}
	// PUT .../killed is the "fail the query with an explanation" path;
	// DELETE would cancel it silently and the operator loses the audit trail
	// Trino itself surfaces to the tenant.
	if gotMethod != http.MethodPut {
		t.Errorf("method = %s, want PUT", gotMethod)
	}
	if gotPath != "/v1/query/q1/killed" {
		t.Errorf("path = %q, want /v1/query/q1/killed", gotPath)
	}
	if gotBody != "killed by operator" {
		t.Errorf("body = %q, want the kill message", gotBody)
	}
}

// TestCoordinatorClientGoneQueryIsNotFound: Trino answers 410 Gone for a
// query that has aged out of the coordinator's memory. That is "no longer
// there", not an outage, and must not read as a broken cell.
func TestCoordinatorClientGoneQueryIsNotFound(t *testing.T) {
	c, _ := newTrinoTestCoordinator(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	})
	_, err := c.Query(context.Background(), "q1")
	if !isTrinoNotFound(err) {
		t.Errorf("410 Gone should map to a not-found error, got %v", err)
	}
	if isTrinoEndpointUnavailable(err) {
		t.Error("410 Gone is a missing query, not a missing endpoint")
	}
}

// TestCoordinatorClientMissingRouteIsEndpointUnavailable: a coordinator
// A cell that serves NEITHER node inventory still must not read as a cell
// that never answered. /v1/node is bound only under AIRLIFT_DISCOVERY and
// /v1/announce only under ANNOUNCE/DNS, so in practice one of them is always
// present; this pins the behaviour for the case where the fallback also
// 404s, which is a statement about how the cell is built, not its health.
func TestCoordinatorClientMissingRouteIsEndpointUnavailable(t *testing.T) {
	c, _ := newTrinoTestCoordinator(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	_, err := c.Nodes(context.Background())
	if !isTrinoEndpointUnavailable(err) {
		t.Errorf("404 should map to an endpoint-unavailable error, got %v", err)
	}
	if isTrinoNotFound(err) {
		t.Error("a missing route must not read as a missing query")
	}
}

// TestCoordinatorClientQueryRoute404StaysNotFound guards the per-query
// routes. /v1/query/{id} is always served, so a 404 there is about the id —
// JAX-RS answers 404 when it cannot parse one into a QueryId. An operator
// following a malformed link must be told the query is missing, not that the
// coordinator does not serve query lookups.
func TestCoordinatorClientQueryRoute404StaysNotFound(t *testing.T) {
	c, _ := newTrinoTestCoordinator(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	_, err := c.Query(context.Background(), "not a query id")
	if !isTrinoNotFound(err) {
		t.Errorf("404 on the query route should read as a missing query, got %v", err)
	}
	if isTrinoEndpointUnavailable(err) {
		t.Error("the query route exists; a 404 there is not a missing endpoint")
	}

	if killErr := c.KillQuery(context.Background(), "not a query id", "why"); !isTrinoNotFound(killErr) {
		t.Errorf("404 on kill should read as a missing query, got %v", killErr)
	}
}

// TestCoordinatorClientForbiddenIsSurfaced: a 403 means the observer's OPA
// grant is missing or the bundle has not rolled out. That is a distinct,
// actionable failure and must not be flattened into "no queries".
func TestCoordinatorClientForbiddenIsSurfaced(t *testing.T) {
	c, _ := newTrinoTestCoordinator(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})
	if _, err := c.Queries(context.Background()); err == nil {
		t.Error("a 403 from the coordinator must surface as an error, not an empty list")
	}
}

func TestCoordinatorClientServerInfo(t *testing.T) {
	c, _ := newTrinoTestCoordinator(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"nodeVersion":{"version":"484"},"environment":"production","coordinator":true,"starting":false,"uptime":"3.00h"}`))
	})
	info, err := c.ServerInfo(context.Background())
	if err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}
	if info.Version != "484" {
		t.Errorf("Version = %q, want 484", info.Version)
	}
	if info.Environment != "production" {
		t.Errorf("Environment = %q, want production", info.Environment)
	}
	if info.UptimeMS != int64(3*time.Hour/time.Millisecond) {
		t.Errorf("UptimeMS = %d, want %d", info.UptimeMS, int64(3*time.Hour/time.Millisecond))
	}
}

func TestCoordinatorClientNodes(t *testing.T) {
	c, _ := newTrinoTestCoordinator(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/node/failed" {
			_, _ = w.Write([]byte(`[{"uri":"http://10.0.0.2:8080","recentFailureRatio":1.0}]`))
			return
		}
		_, _ = w.Write([]byte(`[
		  {"uri":"http://10.0.0.1:8080","age":"1.00h","recentFailures":0.0,"recentSuccesses":120.0,"recentFailureRatio":0.0},
		  {"uri":"http://10.0.0.2:8080","age":"2.00m","recentFailures":5.0,"recentSuccesses":0.0,"recentFailureRatio":1.0}
		]`))
	})

	inv, err := c.Nodes(context.Background())
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if inv.Source != TrinoNodeSourceFailureDetector || !inv.HasHealth() {
		t.Fatalf("a cell serving /v1/node reports failure-detector health, got source %q", inv.Source)
	}
	if len(inv.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(inv.Nodes))
	}
	byURI := map[string]TrinoNode{}
	for _, n := range inv.Nodes {
		byURI[n.URI] = n
	}
	healthy := byURI["http://10.0.0.1:8080"]
	if healthy.Failed {
		t.Error("the healthy node must not be flagged failed")
	}
	if healthy.AgeMS != int64(time.Hour/time.Millisecond) {
		t.Errorf("AgeMS = %d, want %d", healthy.AgeMS, int64(time.Hour/time.Millisecond))
	}
	if !byURI["http://10.0.0.2:8080"].Failed {
		t.Error("the node listed under /v1/node/failed must be flagged failed")
	}
}

// The cells in production run Trino's default discovery.type=ANNOUNCE, which
// binds /v1/announce and not /v1/node. Listing the fleet from the inventory
// the cell actually serves is the difference between the console naming the
// workers and the console showing an operator nothing at all.
func TestNodesFallBackToTheAnnounceInventory(t *testing.T) {
	c, rec := newTrinoTestCoordinator(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/node" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// Set<URI> serializes unordered; return it worst-case out of order.
		_, _ = w.Write([]byte(`["http://10.0.0.9:8080","http://10.0.0.1:8080"]`))
	})

	inv, err := c.Nodes(context.Background())
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if inv.Source != TrinoNodeSourceAnnounce {
		t.Errorf("source = %q, want %q", inv.Source, TrinoNodeSourceAnnounce)
	}
	// The announce inventory carries membership only. Claiming health here
	// would render a 0.0 failure ratio the coordinator never measured.
	if inv.HasHealth() {
		t.Error("the announce inventory must not claim to carry health")
	}
	got := []string{}
	for _, n := range inv.Nodes {
		got = append(got, n.URI)
		if n.Failed || n.AgeMS != 0 || n.RecentFailureRatio != 0 {
			t.Errorf("announced node %s carries unmeasured health fields: %+v", n.URI, n)
		}
	}
	// Sorted, so the console does not reshuffle its rows between polls.
	want := []string{"http://10.0.0.1:8080", "http://10.0.0.9:8080"}
	if !slices.Equal(got, want) {
		t.Errorf("nodes = %v, want %v (sorted)", got, want)
	}

	var paths []string
	for _, r := range rec.requests {
		paths = append(paths, r.URL.Path)
	}
	if !slices.Contains(paths, "/v1/node") || !slices.Contains(paths, "/v1/announce") {
		t.Errorf("expected /v1/node to be tried before /v1/announce, got %v", paths)
	}
}

// A cell that serves /v1/node must not also be asked for /v1/announce: the
// fallback exists for the cells that lack the first route, and a per-poll
// extra request against every coordinator is a cost paid for nothing.
func TestNodesDoNotQueryAnnounceWhenTheFailureDetectorAnswers(t *testing.T) {
	_, rec := newTrinoTestCoordinator(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"uri":"http://10.0.0.1:8080","age":"1.00h"}]`))
	})
	c := newTrinoCoordinatorClient(rec.server.URL, "obs-password", "")
	if _, err := c.Nodes(context.Background()); err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	for _, r := range rec.requests {
		if r.URL.Path == "/v1/announce" {
			t.Error("/v1/announce must not be queried when /v1/node answers")
		}
	}
}

// On a cell with no /v1/node, system.runtime.nodes is preferred over
// /v1/announce: it is the only source carrying node_version, which is what
// makes version skew visible during a rollout.
func TestNodesPreferTheSystemTableOverAnnounce(t *testing.T) {
	c, rec := newTrinoTestCoordinator(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/node" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/statement" {
			_, _ = w.Write([]byte(`{"data":[
			  ["worker2","http://10.0.0.2:8080","477",false,"ACTIVE"],
			  ["coord","http://10.0.0.1:8080","476",true,"ACTIVE"],
			  ["worker1","http://10.0.0.3:8080","476",false,"SHUTTING_DOWN"]
			]}`))
			return
		}
		_, _ = w.Write([]byte(`["http://should-not-be-used:8080"]`))
	})

	inv, err := c.Nodes(context.Background())
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if inv.Source != TrinoNodeSourceSystemTable || !inv.HasNodeDetail() {
		t.Fatalf("source = %q, want %q", inv.Source, TrinoNodeSourceSystemTable)
	}
	if inv.HasHealth() {
		t.Error("the system table carries no heartbeat ratios and must not claim health")
	}
	if len(inv.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(inv.Nodes))
	}
	// Coordinator first, then by URI.
	if !inv.Nodes[0].Coordinator || inv.Nodes[0].NodeID != "coord" {
		t.Errorf("coordinator must sort first, got %+v", inv.Nodes[0])
	}
	if inv.Nodes[1].URI != "http://10.0.0.2:8080" || inv.Nodes[2].URI != "http://10.0.0.3:8080" {
		t.Errorf("workers must sort by URI, got %+v", inv.Nodes[1:])
	}
	// Version is the whole reason this source is preferred.
	if inv.Nodes[1].Version != "477" || inv.Nodes[2].Version != "476" {
		t.Errorf("node versions not decoded: %+v", inv.Nodes)
	}
	if inv.Nodes[2].State != "SHUTTING_DOWN" {
		t.Errorf("state not decoded: %+v", inv.Nodes[2])
	}
	for _, r := range rec.requests {
		if r.URL.Path == "/v1/announce" {
			t.Error("/v1/announce must not be queried when the system table answers")
		}
	}
}

// If the SQL path fails — the grant is not rolled out yet, the resource
// group rejects it — the console must still name the fleet rather than show
// nothing. /v1/announce is the last resort.
func TestNodesFallBackToAnnounceWhenTheSystemTableIsDenied(t *testing.T) {
	c, _ := newTrinoTestCoordinator(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/node" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/statement" {
			// Trino reports authorization failures inside a 200 payload.
			_, _ = w.Write([]byte(`{"error":{"message":"Access Denied: Cannot access catalog system","errorName":"PERMISSION_DENIED","errorType":"USER_ERROR"}}`))
			return
		}
		_, _ = w.Write([]byte(`["http://10.0.0.1:8080"]`))
	})

	inv, err := c.Nodes(context.Background())
	if err != nil {
		t.Fatalf("a denied system table must not fail the whole node read: %v", err)
	}
	if inv.Source != TrinoNodeSourceAnnounce {
		t.Errorf("source = %q, want the announce fallback", inv.Source)
	}
	if len(inv.Nodes) != 1 {
		t.Errorf("expected the announced node, got %+v", inv.Nodes)
	}
}

// The statement protocol pages through nextUri; rows arrive across hops.
func TestSystemTableNodesDrainsTheNextUriChain(t *testing.T) {
	var hop int
	var base string
	c, rec := newTrinoTestCoordinator(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/node" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		hop++
		if hop == 1 {
			_, _ = fmt.Fprintf(w, `{"nextUri":"%s/v1/statement/x/1","data":[["a","http://10.0.0.1:8080","476",true,"ACTIVE"]]}`, base)
			return
		}
		_, _ = w.Write([]byte(`{"data":[["b","http://10.0.0.2:8080","476",false,"ACTIVE"]]}`))
	})
	base = rec.server.URL

	inv, err := c.Nodes(context.Background())
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if len(inv.Nodes) != 2 {
		t.Fatalf("rows from every hop must be kept, got %+v", inv.Nodes)
	}
}
