//go:build kubernetes

package admin

import (
	"context"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/posthog/duckgres/controlplane/provisioner"
)

// Poll budget for the console's Trino views.
//
// The coordinator is a shared, single-writer scheduler for the whole cell:
// `/v1/query` walks every query it holds, and the console polls it from
// every open browser tab. Serving those out of one short-lived cache keeps
// the load flat in the number of operators watching, which is the property
// that matters — an incident is exactly when the most tabs are open and the
// coordinator can least afford the extra work.
//
// The TTLs are deliberately different: query state is what an operator
// watches change, while the node list and the cell's own version move on a
// deploy timescale.
const (
	trinoQueriesCacheTTL = 2 * time.Second
	trinoClusterCacheTTL = 15 * time.Second
)

// TrinoOrgStore is the config-store read surface the console needs. Narrow
// interface so handler tests need no database.
type TrinoOrgStore interface {
	ListTrinoEnabledOrgs() ([]configstore.TrinoEnabledOrg, error)
	GetManagedWarehouseTrino(orgID string) (*configstore.ManagedWarehouseTrino, error)
}

// TrinoCell identifies the cell this control plane observes. One cell today
// (see controlplane/trino_inputs.go); every payload carries the id so the
// SPA and its consumers are already cell-aware when a second one lands.
type TrinoCell struct {
	ID             string `json:"id"`
	CoordinatorURL string `json:"coordinator_url"`
}

// TrinoOrgStatus is one org's Trino provisioning state, joined with what
// the coordinator currently shows for it.
//
// These columns exist on duckgres_managed_warehouse_trino and were, until
// now, surfaced nowhere: a Trino provisioning failure was silent unless
// somebody read the table by hand.
type TrinoOrgStatus struct {
	Org string `json:"org"`
	// Principal is the org's Trino username (its database_name), and the
	// stem its catalog and groups are derived from.
	Principal string `json:"principal"`
	Catalog   string `json:"catalog"`
	Tier      string `json:"tier"`
	Cell      string `json:"cell"`
	// State is the most recent reconcile tick's outcome: pending /
	// provisioning / ready / failed.
	State         string     `json:"state"`
	StatusMessage string     `json:"status_message,omitempty"`
	ReadyAt       *time.Time `json:"ready_at,omitempty"`
	FailedAt      *time.Time `json:"failed_at,omitempty"`
	// RunningQueries / QueuedQueries are this org's share of the cell right
	// now. Zero when the coordinator is unreachable — the envelope's
	// `available` flag is what distinguishes that from a genuinely idle org.
	RunningQueries int `json:"running_queries"`
	QueuedQueries  int `json:"queued_queries"`
}

// TrinoStatus is the cluster overview: which cell, whether it answers, what
// it is running, and how the fleet of tenants is provisioned.
//
// Available is false whenever the coordinator could not be read. Every
// count then reads zero, and a consumer that ignores the flag would render
// a healthy-looking idle cell during an outage — so the SPA keys its
// "unavailable" banner on it.
type TrinoStatus struct {
	Cell      TrinoCell        `json:"cell"`
	Available bool             `json:"available"`
	Error     string           `json:"error,omitempty"`
	Server    *TrinoServerInfo `json:"server,omitempty"`

	// QueriesByState counts what the coordinator holds, keyed by Trino's
	// own state names (QUEUED, RUNNING, FINISHED, FAILED, ...).
	QueriesByState map[string]int `json:"queries_by_state"`
	// BlockedQueries counts running queries whose every driver is blocked —
	// the signature of a cell waiting on the metadata store or on S3 rather
	// than one that is busy.
	BlockedQueries int `json:"blocked_queries"`

	// NodeStats reports whether Nodes means anything: false when the cell
	// serves neither node inventory, so the console shows "not reported
	// here" rather than an authoritative zero.
	NodeStats bool `json:"node_stats"`
	// NodeSource names the inventory Nodes was counted from. Under
	// TrinoNodeSourceAnnounce the cell reports membership without health,
	// so FailedNodes is always 0 because nothing is measured — not because
	// the fleet is known to be well. The SPA must not render it as health.
	NodeSource  string `json:"node_source,omitempty"`
	Nodes       int    `json:"nodes"`
	FailedNodes int    `json:"failed_nodes"`

	// OrgsByState counts Trino-enabled orgs by provisioning state, so a
	// stuck tenant is visible without opening the org list.
	OrgsByState map[string]int `json:"orgs_by_state"`
	TotalOrgs   int            `json:"total_orgs"`
}

// trinoCache memoizes one coordinator read for a short TTL, collapsing the
// polls of every open console tab into one request per interval.
//
// A refresh in flight does NOT block readers: they get the previous value.
// That keeps a slow coordinator from turning N polling tabs into N stuck
// requests, which is the failure mode that would make the console useless
// exactly during the incident it exists for. The cost is that a value can
// be one TTL staler than it looks, which for a 2-second poll is not a cost.
type trinoCache[T any] struct {
	ttl time.Duration

	mu        sync.Mutex
	value     T
	err       error
	fetchedAt time.Time
	// refreshing guards against concurrent fetches; readers during a
	// refresh take the cached value rather than queueing behind it.
	refreshing bool
}

func newTrinoCache[T any](ttl time.Duration) *trinoCache[T] {
	return &trinoCache[T]{ttl: ttl}
}

// get returns the cached value, refreshing it synchronously when it is
// stale and no other caller is already doing so.
func (c *trinoCache[T]) get(ctx context.Context, fetch func(context.Context) (T, error)) (T, error) {
	c.mu.Lock()
	populated := !c.fetchedAt.IsZero()
	fresh := populated && time.Since(c.fetchedAt) < c.ttl
	// Serve the cached value when it is fresh, or when it is merely stale
	// but someone is already refreshing it. A never-populated cache falls
	// through and fetches even during a refresh — handing back a zero value
	// there would render as "the cell is empty" rather than "not known yet".
	if fresh || (c.refreshing && populated) {
		v, err := c.value, c.err
		c.mu.Unlock()
		return v, err
	}
	c.refreshing = true
	c.mu.Unlock()

	v, err := fetch(ctx)

	c.mu.Lock()
	c.value, c.err, c.fetchedAt, c.refreshing = v, err, time.Now(), false
	c.mu.Unlock()
	return v, err
}

// TrinoAPI is the console's Trino surface: a coordinator client, the config
// store, and the cell's identity.
type TrinoAPI struct {
	cell    TrinoCell
	client  TrinoCoordinatorClient
	orgs    TrinoOrgStore
	audit   *AuditStore
	queries *trinoCache[[]TrinoQuery]
	nodes   *trinoCache[TrinoNodeInventory]
	info    *trinoCache[*TrinoServerInfo]
}

// NewTrinoAPI builds the console's Trino surface. A nil client or store
// leaves every route unregistered — the same shape the rest of Extras uses
// for a capability the deployment does not have.
func NewTrinoAPI(cell TrinoCell, client TrinoCoordinatorClient, orgs TrinoOrgStore, audit *AuditStore) *TrinoAPI {
	if client == nil || orgs == nil {
		return nil
	}
	return &TrinoAPI{
		cell:    cell,
		client:  client,
		orgs:    orgs,
		audit:   audit,
		queries: newTrinoCache[[]TrinoQuery](trinoQueriesCacheTTL),
		nodes:   newTrinoCache[TrinoNodeInventory](trinoClusterCacheTTL),
		info:    newTrinoCache[*TrinoServerInfo](trinoClusterCacheTTL),
	}
}

// registerTrinoAPI wires the Trino routes onto the authenticated /api/v1
// group. RoleGate already admits viewers on GETs and requires admin on the
// kill; the kill handler re-checks and audits, matching impersonation.
func registerTrinoAPI(r *gin.RouterGroup, api *TrinoAPI) {
	if api == nil {
		return
	}
	r.GET("/trino/status", api.handleStatus)
	r.GET("/trino/queries", api.handleQueries)
	r.GET("/trino/queries/:id", api.handleQueryDetail)
	r.POST("/trino/queries/:id/kill", api.handleKillQuery)
	r.GET("/trino/nodes", api.handleNodes)
	r.GET("/trino/orgs", api.handleOrgs)
	r.GET("/orgs/:id/trino", api.handleOrgDetail)
}

// principalIndex maps Trino principals to org ids, and carries the org
// rows the handlers annotate with.
type principalIndex struct {
	orgByPrincipal map[string]string
	rows           []configstore.TrinoEnabledOrg
}

func (a *TrinoAPI) index() (principalIndex, error) {
	rows, err := a.orgs.ListTrinoEnabledOrgs()
	if err != nil {
		return principalIndex{}, err
	}
	idx := principalIndex{orgByPrincipal: make(map[string]string, len(rows)), rows: rows}
	for _, o := range rows {
		if p := o.TrinoPrincipal(); p != "" {
			idx.orgByPrincipal[p] = o.OrgID
		}
	}
	return idx, nil
}

// liveQueries fetches the cached query list and stamps each row with the
// duckgres org that owns it. Trino only knows the principal; the mapping
// back to an org id lives here, in the config store, which is why the
// annotation happens server-side rather than in the SPA.
// A handler that already holds an index passes it in; principalIndex's zero
// value means "fetch one". Handlers that need the org list anyway would
// otherwise read the config store twice per request, and /trino/status is
// polled every few seconds by every open tab.
func (a *TrinoAPI) liveQueries(ctx context.Context, known ...principalIndex) ([]TrinoQuery, principalIndex, error) {
	queries, err := a.queries.get(ctx, a.client.Queries)
	if err != nil {
		return nil, principalIndex{}, err
	}
	var idx principalIndex
	var idxErr error
	if len(known) > 0 && known[0].orgByPrincipal != nil {
		idx = known[0]
	} else {
		idx, idxErr = a.index()
	}
	if idxErr != nil {
		// The coordinator answered; a config-store blip should degrade the
		// org column, not the whole live view.
		slog.Warn("admin: trino org index unavailable, serving unannotated queries", "error", idxErr)
		return queries, principalIndex{orgByPrincipal: map[string]string{}}, nil
	}
	out := make([]TrinoQuery, len(queries))
	copy(out, queries)
	for i := range out {
		out[i].Org = idx.orgByPrincipal[out[i].Principal]
	}
	return out, idx, nil
}

func (a *TrinoAPI) handleStatus(c *gin.Context) {
	ctx := c.Request.Context()
	status := TrinoStatus{
		Cell:           a.cell,
		Available:      true,
		QueriesByState: map[string]int{},
		OrgsByState:    map[string]int{},
	}

	// Provisioning state comes from the config store, so it is reported
	// even when the coordinator is unreachable — "the cell is down" and
	// "these tenants never provisioned" are different incidents.
	idx, idxErr := a.index()
	if idxErr == nil {
		status.TotalOrgs = len(idx.rows)
		for _, o := range idx.rows {
			state := string(o.State)
			if state == "" {
				state = string(configstore.ManagedWarehouseStatePending)
			}
			status.OrgsByState[state]++
		}
	} else {
		slog.Warn("admin: trino org list unavailable for status", "error", idxErr)
	}

	// /v1/info is PUBLIC in Trino, so it answers even when the observer's
	// authorization is broken. Reading it first is what lets the console
	// distinguish "the cell is down" from "the console cannot see it".
	if info, err := a.info.get(ctx, a.client.ServerInfo); err == nil {
		status.Server = info
	} else {
		status.Available = false
		status.Error = err.Error()
	}

	queries, _, err := a.liveQueries(ctx, idx)
	if err != nil {
		status.Available = false
		if status.Error == "" {
			status.Error = err.Error()
		}
	} else {
		for _, q := range queries {
			status.QueriesByState[q.State]++
			if q.State == trinoStateRunning && q.FullyBlocked {
				status.BlockedQueries++
			}
		}
	}

	// Node stats are best-effort. The client already falls back from
	// /v1/node to /v1/announce, so reaching the unavailable branch means the
	// cell serves neither inventory. A healthy cell must not be reported as
	// down because an endpoint it never had is missing; any other error
	// still counts against the cell.
	if inv, nodeErr := a.nodes.get(ctx, a.client.Nodes); nodeErr == nil {
		status.NodeStats = true
		status.NodeSource = inv.Source
		status.Nodes = len(inv.Nodes)
		// Only the failure detector measures this. Under ANNOUNCE every
		// Failed is false because nothing was measured, and the SPA keys
		// off NodeSource rather than reading 0 as "all healthy".
		if inv.HasHealth() {
			for _, n := range inv.Nodes {
				if n.Failed {
					status.FailedNodes++
				}
			}
		}
	} else if isTrinoEndpointUnavailable(nodeErr) {
		status.NodeStats = false
	} else if status.Error == "" {
		status.Available = false
		status.Error = nodeErr.Error()
	}

	c.JSON(http.StatusOK, status)
}

// Trino's own query-state names, used where the console reasons about a
// specific state rather than just counting.
const (
	trinoStateQueued   = "QUEUED"
	trinoStateRunning  = "RUNNING"
	trinoStateFinished = "FINISHED"
	trinoStateFailed   = "FAILED"
)

// isActiveTrinoState reports whether a query is still in flight, mirroring
// QueryState.isDone(): FINISHED and FAILED are the only terminal states.
//
// Defining it as the COMPLEMENT of the terminal pair rather than as a list
// of interesting states is load-bearing. Trino has nine states, and a query
// sitting in WAITING_FOR_RESOURCES, DISPATCHING, PLANNING, STARTING or
// FINISHING is still running, still killable, and — for a cell backed by
// DuckLake, where planning talks to a per-tenant Postgres — is exactly the
// pathology an operator opens this page to find. An allowlist of
// {RUNNING, QUEUED} hides all five, and hides them precisely when they
// matter.
func isActiveTrinoState(state string) bool {
	return state != trinoStateFinished && state != trinoStateFailed
}

func (a *TrinoAPI) handleQueries(c *gin.Context) {
	queries, _, err := a.liveQueries(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{
			"error":     err.Error(),
			"cell":      a.cell,
			"available": false,
		})
		return
	}

	org := c.Query("org")
	state := strings.ToUpper(c.Query("state"))
	// active=1 is the live view's default: the states an operator can still
	// act on. Without it the list also carries recently-finished queries,
	// which is what the history view wants.
	activeOnly := c.Query("active") == "1"

	out := make([]TrinoQuery, 0, len(queries))
	for _, q := range queries {
		if org != "" && q.Org != org {
			continue
		}
		if state != "" && q.State != state {
			continue
		}
		if activeOnly && !isActiveTrinoState(q.State) {
			continue
		}
		out = append(out, q)
	}
	// Longest-running first: the queries an operator is looking for are the
	// ones that have been going the longest.
	sort.SliceStable(out, func(i, j int) bool { return out[i].ElapsedMS > out[j].ElapsedMS })

	c.JSON(http.StatusOK, gin.H{
		"cell":      a.cell,
		"available": true,
		"queries":   out,
	})
}

func (a *TrinoAPI) handleQueryDetail(c *gin.Context) {
	q, err := a.client.Query(c.Request.Context(), c.Param("id"))
	if err != nil {
		if isTrinoNotFound(err) {
			// The coordinator drops finished queries from memory, so a
			// stale link is an expected 404, not a cell problem.
			c.JSON(http.StatusNotFound, gin.H{"error": "query not found on the coordinator (it may have aged out)"})
			return
		}
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	if idx, idxErr := a.index(); idxErr == nil {
		q.Org = idx.orgByPrincipal[q.Principal]
	}
	c.JSON(http.StatusOK, q)
}

type trinoKillRequest struct {
	// Reason is recorded in the audit row AND delivered to the tenant as
	// the query's failure message, so they learn why their query died
	// instead of seeing an unexplained cancellation.
	Reason string `json:"reason"`
}

func (a *TrinoAPI) handleKillQuery(c *gin.Context) {
	id := IdentityFromContext(c)
	if id == nil || id.Role != RoleAdmin {
		c.JSON(http.StatusForbidden, gin.H{"error": "admin role required"})
		return
	}
	queryID := c.Param("id")

	var req trinoKillRequest
	// A body is optional; an absent or malformed one just means no reason.
	_ = c.ShouldBindJSON(&req)
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "killed by a duckgres operator"
	}

	// Resolve the owner BEFORE the kill so the audit row names the tenant
	// whose query was killed. After the kill the coordinator may already
	// have moved the query out of its running set.
	targetOrg := ""
	if q, err := a.client.Query(c.Request.Context(), queryID); err == nil {
		if idx, idxErr := a.index(); idxErr == nil {
			targetOrg = idx.orgByPrincipal[q.Principal]
		}
	}

	recordAudit := func(status int) {
		entry := &AdminAuditEntry{
			Action:     "trino.query.kill",
			Method:     c.Request.Method,
			Path:       c.FullPath(),
			Org:        targetOrg,
			TargetUser: queryID,
			RemoteAddr: c.ClientIP(),
			Status:     status,
		}
		if id != nil {
			entry.Actor, entry.Role, entry.Source = id.Email, string(id.Role), id.Source
		}
		if a.audit != nil {
			if err := a.audit.Record(entry); err != nil {
				slog.Error("admin: FAILED to audit trino query kill",
					"actor", entry.Actor, "query_id", queryID, "org", targetOrg, "error", err)
			}
		}
		c.Set(ctxAuditHandledKey, true)
	}

	if err := a.client.KillQuery(c.Request.Context(), queryID, reason); err != nil {
		if isTrinoNotFound(err) {
			recordAudit(http.StatusNotFound)
			c.JSON(http.StatusNotFound, gin.H{"error": "query not found on the coordinator"})
			return
		}
		recordAudit(http.StatusBadGateway)
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	recordAudit(http.StatusOK)
	c.JSON(http.StatusOK, gin.H{"killed": true, "query_id": queryID, "org": targetOrg})
}

func (a *TrinoAPI) handleNodes(c *gin.Context) {
	inv, err := a.nodes.get(c.Request.Context(), a.client.Nodes)
	if isTrinoEndpointUnavailable(err) {
		// Not a gateway failure: the coordinator answered, and it serves
		// neither node inventory. 501 says the console asked for something
		// this cell cannot provide, which is what an operator needs to know.
		c.JSON(http.StatusNotImplemented, gin.H{
			"cell":      a.cell,
			"available": false,
			"reason":    "this cell serves neither /v1/node nor /v1/announce, so its fleet cannot be listed",
		})
		return
	}
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error(), "available": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"cell":      a.cell,
		"available": true,
		"source":    inv.Source,
		"nodes":     inv.Nodes,
	})
}

func (a *TrinoAPI) handleOrgs(c *gin.Context) {
	idx, err := a.index()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Live counts are best-effort: the provisioning state is the point of
	// this view and must render even when the coordinator is unreachable.
	running, queued := map[string]int{}, map[string]int{}
	available := true
	if queries, _, qErr := a.liveQueries(c.Request.Context(), idx); qErr == nil {
		for _, q := range queries {
			switch q.State {
			case trinoStateRunning:
				running[q.Org]++
			case trinoStateQueued:
				queued[q.Org]++
			}
		}
	} else {
		available = false
	}

	out := make([]TrinoOrgStatus, 0, len(idx.rows))
	for _, o := range idx.rows {
		out = append(out, trinoOrgStatus(o, running[o.OrgID], queued[o.OrgID]))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Org < out[j].Org })

	c.JSON(http.StatusOK, gin.H{"cell": a.cell, "available": available, "orgs": out})
}

func (a *TrinoAPI) handleOrgDetail(c *gin.Context) {
	orgID := c.Param("id")
	row, err := a.orgs.GetManagedWarehouseTrino(orgID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if row == nil || !row.Enabled {
		// Not an error: most orgs are not Trino-enabled, and the org page
		// renders a "not enabled" state rather than a failure.
		c.JSON(http.StatusOK, gin.H{"cell": a.cell, "enabled": false})
		return
	}

	// The principal is on the join row, not the Trino row, so find it via
	// the same listing the rest of the surface uses.
	var enabled configstore.TrinoEnabledOrg
	if idx, idxErr := a.index(); idxErr == nil {
		for _, o := range idx.rows {
			if o.OrgID == orgID {
				enabled = o
				break
			}
		}
	}

	running, queued := 0, 0
	available := true
	if queries, _, qErr := a.liveQueries(c.Request.Context()); qErr == nil {
		for _, q := range queries {
			if q.Org != orgID {
				continue
			}
			switch q.State {
			case trinoStateRunning:
				running++
			case trinoStateQueued:
				queued++
			}
		}
	} else {
		available = false
	}

	status := trinoOrgStatus(enabled, running, queued)
	status.Org = orgID
	// The Trino row is authoritative for lifecycle detail; the listing
	// carries only the current state.
	status.State = string(row.State)
	status.StatusMessage = row.StatusMessage
	status.ReadyAt = row.ReadyAt
	status.FailedAt = row.FailedAt
	status.Tier = row.Tier
	status.Cell = row.TrinoCellID

	c.JSON(http.StatusOK, gin.H{
		"cell":      a.cell,
		"enabled":   true,
		"available": available,
		"status":    status,
	})
}

// trinoOrgStatus projects a listing row onto the console shape. Note what
// it does NOT copy: TrinoEnabledOrg carries the org's root bcrypt hash, and
// this is the boundary that stops it reaching a browser.
func trinoOrgStatus(o configstore.TrinoEnabledOrg, running, queued int) TrinoOrgStatus {
	principal := o.TrinoPrincipal()
	catalog := ""
	if principal != "" {
		catalog = provisioner.TrinoCatalogName(principal)
	}
	state := string(o.State)
	if state == "" {
		state = string(configstore.ManagedWarehouseStatePending)
	}
	return TrinoOrgStatus{
		Org:            o.OrgID,
		Principal:      principal,
		Catalog:        catalog,
		Tier:           o.Tier,
		Cell:           o.CellID,
		State:          state,
		RunningQueries: running,
		QueuedQueries:  queued,
	}
}
