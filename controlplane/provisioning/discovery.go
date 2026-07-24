package provisioning

import (
	"log/slog"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Discovery endpoints: the machine-facing "which tenants exist and where do
// I write" surface consumed by external writers (viaduck destination
// discovery, millpond's include-values source). Read-only projections of the
// config store — the CP is a source of DESIRED config here; consumers keep
// their own runtime state.
//
// Teams come from duckgres_org_teams (schema-per-team model): each row
// carries the team's schema_name — its tables live at <schema>.events /
// <schema>.persons — plus nullable grandfathered explicit table names for
// pre-convention teams, and an enabled flag.
// The enabled flag is duckgres's per-team QUERY-SERVING switch (migration
// 000024) — it says nothing about ingestion. Discovery therefore passes it
// through as information and derives NOTHING from it: disabled teams stay
// in both the full listing and the values projection, because inferring
// "stop ingesting" from "stop serving queries" would turn a serving hold
// into permanent event loss. The only ingestion-stop signal is row
// removal (team deleted / warehouse leaving the discoverable states).
//
// Metadata-store note: for Kind == "external" the connection block comes
// straight off the warehouse row (provision-time inputs). For Kind ==
// "cnpg-shard" the composition picks the shard and publishes the
// authoritative endpoint/database/user + credential Secret ref in the
// Duckling CR status; the provisioner MIRRORS that into the row on the
// ready-reconcile tick (provisioner.reconcileMetadataStoreRow), so this
// endpoint serves it without any Kubernetes dependency. A just-turned-ready
// warehouse may serve an empty connection block for up to one reconcile
// interval until the mirror lands.

// discoveryStates are the lifecycle states external writers must see.
// Resharding is INCLUDED, with writable=false: if a resharding warehouse
// vanished from the list instead, discovery-driven consumers would read
// that as tenant REMOVAL (drain/retire) rather than a temporary write
// fence. Pending/provisioning warehouses are excluded until they can
// actually be written to; deleting/deleted/failed are gone — that absence
// IS the removal signal, and consumers damp it on their side.
//
// The state enum is OPEN (models.go) — a state added without being
// classified below silently reads as fleet-wide tenant removal to every
// discovery consumer while tenants pass through it. TestDiscoveryStateClassification
// is the tripwire: adding a state constant without extending one of these
// two lists fails it.
var discoveryStates = []configstore.ManagedWarehouseProvisioningState{
	configstore.ManagedWarehouseStateReady,
	configstore.ManagedWarehouseStateResharding,
}

// discoveryExcludedStates documents the deliberate exclusions (see the
// classification tripwire above).
var discoveryExcludedStates = []configstore.ManagedWarehouseProvisioningState{
	configstore.ManagedWarehouseStatePending,
	configstore.ManagedWarehouseStateProvisioning,
	configstore.ManagedWarehouseStateFailed,
	configstore.ManagedWarehouseStateDeleting,
	configstore.ManagedWarehouseStateDeleted,
}

var discoveryBrokenTeamRows = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_discovery_broken_team_rows_total",
	Help: "Warehouses served with an empty teams array (no duckgres_org_teams rows) or carrying a cross-org team conflict — sustained increase means a live tenant is silently unroutable or ambiguously routed",
}, []string{"reason"})

type discoveryTeam struct {
	TeamID int64 `json:"team_id"`
	// SchemaName is the team's schema (consumers still need it for DDL on
	// new tables).
	SchemaName string `json:"schema_name"`
	// Enabled is duckgres's per-team QUERY-SERVING switch (not yet
	// enforced on the serve path), passed through as information — see the
	// module comment: nothing ingestion-shaped may be derived from it.
	// backfill_enabled is deliberately NOT served: it is not a routing
	// concern, and adding a field later is wire-compatible while removing
	// one is not.
	Enabled bool `json:"enabled"`
	// Resolved, always-populated locations — the derivation from
	// schema_name + grandfathered overrides happens HERE, once, so no
	// consumer reimplements (or mis-guesses the qualification of) the
	// rule. See resolveTeamTables.
	EventsTable       string `json:"events_table"`
	PersonsTable      string `json:"persons_table"`
	DataImportsSchema string `json:"data_imports_schema"`
}

// resolveTeamTables derives the team's fully-qualified table locations.
// Contract (pinned here, the single derivation site): a grandfathered
// events/persons override is a BARE TABLE NAME within the team's schema —
// the schema part always comes from schema_name (a break-glass schema
// repair repoints overrides too); absent overrides derive `events` /
// `persons`. The data-imports override is a SCHEMA name and replaces the
// `<schema>_data_imports` derivation wholesale.
func resolveTeamTables(t *configstore.OrgTeam) (events, persons, dataImports string) {
	eventsName := "events"
	if t.EventsTableName != nil && *t.EventsTableName != "" {
		eventsName = *t.EventsTableName
	}
	personsName := "persons"
	if t.PersonsTableName != nil && *t.PersonsTableName != "" {
		personsName = *t.PersonsTableName
	}
	dataImports = t.SchemaName + "_data_imports"
	if t.SchemaDataImportsName != nil && *t.SchemaDataImportsName != "" {
		dataImports = *t.SchemaDataImportsName
	}
	return t.SchemaName + "." + eventsName, t.SchemaName + "." + personsName, dataImports
}

type discoverySecretRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Key       string `json:"key"`
}

type discoveryMetadataStore struct {
	Kind     string `json:"kind"`
	Endpoint string `json:"endpoint"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	// PasswordSecretRef points at the k8s Secret holding the metadata-store
	// password. NO plaintext credentials in this payload — consumers resolve
	// the ref themselves (RBAC-scoped secret reads).
	PasswordSecretRef discoverySecretRef `json:"password_secret_ref"`
}

type discoveryWarehouse struct {
	OrgID        string                                        `json:"org_id"`
	DucklingName string                                        `json:"duckling_name"`
	State        configstore.ManagedWarehouseProvisioningState `json:"state"`
	// Writable is the external-writer fence: false while resharding. Until
	// the external-writer lease protocol exists, this polled flag is the
	// ONLY fence external writers have — a reshard must account for their
	// poll (and damping) lag before acting destructively; the lease barrier
	// is the future enforcement, not the current one.
	Writable bool `json:"writable"`
	// Teams may be empty when the warehouse has no duckgres_org_teams rows
	// (a data inconsistency worth surfacing rather than hiding the whole
	// warehouse — consumers route nothing for an empty teams array).
	Teams         []discoveryTeam        `json:"teams"`
	MetadataStore discoveryMetadataStore `json:"metadata_store"`
	Bucket        string                 `json:"bucket,omitempty"`
}

type discoveryResponse struct {
	// ConfigGeneration is an opaque change token: max updated_at across
	// ALL warehouse, org, and org-team rows regardless of state (unix
	// seconds), so transitions OUT of the discoverable set (deprovision,
	// team deletion — see DeleteOrgTeamTx's org-row touch) move it too.
	// Pollers may skip processing when the value is UNCHANGED and export
	// it to make replica skew observable. Compare for equality only:
	// sub-second changes can share a value and a deletion of the row
	// carrying the fleet max can move it backwards.
	ConfigGeneration int64                `json:"config_generation"`
	Warehouses       []discoveryWarehouse `json:"warehouses"`
}

// assembleDiscovery builds the discovery view both endpoints project from —
// ONE assembly so the values-only projection can never drift to weaker
// semantics (error handling, logging) than the full listing.
//
// Error contract: transient store failures fail the WHOLE assembly (the
// caller 500s; a polling consumer keeps its last-known-good state — the
// safe direction). Only genuinely-missing org data degrades, per-warehouse,
// to an empty teams array — with a log and a counter, because a sustained
// broken row is a live tenant silently unroutable.
func (h *handler) assembleDiscovery() (*discoveryResponse, error) {
	// Generation FIRST: read before the data so the stamp is never newer
	// than the rows it accompanies — a commit landing mid-assembly then
	// advances the NEXT poll's generation instead of hiding behind an
	// already-served one.
	generation, err := h.store.LatestConfigChange()
	if err != nil {
		return nil, err
	}

	warehouses, err := h.store.ListWarehousesByStates(discoveryStates)
	if err != nil {
		return nil, err
	}

	orgIDs := make([]string, 0, len(warehouses))
	for i := range warehouses {
		orgIDs = append(orgIDs, warehouses[i].OrgID)
	}
	teamRows, err := h.store.ListOrgTeamsByOrgIDs(orgIDs)
	if err != nil {
		// Transient failure: surface it. Treating it as "no teams" would
		// serve every tenant teamless — indistinguishable from mass removal
		// downstream.
		return nil, err
	}
	teamsByOrg := make(map[string][]configstore.OrgTeam, len(orgIDs))
	for i := range teamRows {
		teamsByOrg[teamRows[i].OrgID] = append(teamsByOrg[teamRows[i].OrgID], teamRows[i])
	}

	resp := &discoveryResponse{Warehouses: make([]discoveryWarehouse, 0, len(warehouses))}
	if !generation.IsZero() {
		resp.ConfigGeneration = generation.Unix()
	}

	teamOwners := make(map[int64]string, len(warehouses))
	for i := range warehouses {
		w := &warehouses[i]

		teams := []discoveryTeam{}
		rows, ok := teamsByOrg[w.OrgID]
		if !ok {
			// Every org gets its first team row at provision (migration 000024
			// backfilled the fleet); a live warehouse with zero rows is a data
			// inconsistency. Surface the warehouse with no teams instead of
			// hiding it (hiding reads as removal downstream).
			slog.Warn("discovery: warehouse has no team rows", "org_id", w.OrgID)
			discoveryBrokenTeamRows.WithLabelValues("teams_missing").Inc()
		}
		for ti := range rows {
			t := &rows[ti]
			events, persons, dataImports := resolveTeamTables(t)
			teams = append(teams, discoveryTeam{
				TeamID:            t.TeamID,
				SchemaName:        t.SchemaName,
				Enabled:           t.Enabled,
				EventsTable:       events,
				PersonsTable:      persons,
				DataImportsSchema: dataImports,
			})
			if owner, dup := teamOwners[t.TeamID]; dup {
				// One team claimed by two orgs is a routing ambiguity — both
				// warehouses carry the team in the full listing and the
				// values projection dedupes it, so neither consumer can see
				// the conflict without this signal.
				slog.Warn("discovery: team claimed by multiple orgs",
					"team_id", t.TeamID, "org_id", w.OrgID, "also_claimed_by", owner)
				discoveryBrokenTeamRows.WithLabelValues("team_conflict").Inc()
			} else {
				teamOwners[t.TeamID] = w.OrgID
			}
		}

		resp.Warehouses = append(resp.Warehouses, discoveryWarehouse{
			OrgID:        w.OrgID,
			DucklingName: w.DucklingName,
			State:        w.State,
			Writable:     w.State == configstore.ManagedWarehouseStateReady,
			Teams:        teams,
			MetadataStore: discoveryMetadataStore{
				Kind:     w.MetadataStore.Kind,
				Endpoint: w.MetadataStore.Endpoint,
				Port:     w.MetadataStore.Port,
				Database: w.MetadataStore.DatabaseName,
				Username: w.MetadataStore.Username,
				// Served from the DISCOVERY-ONLY mirror columns (see
				// ManagedWarehouse.MetadataStoreSecretRef) — NOT the
				// activation-validated MetadataStoreCredentials.
				PasswordSecretRef: discoverySecretRef{
					Namespace: w.MetadataStoreSecretRef.Namespace,
					Name:      w.MetadataStoreSecretRef.Name,
					Key:       w.MetadataStoreSecretRef.Key,
				},
			},
			Bucket: w.DataStore.BucketName,
		})
	}
	return resp, nil
}

// listWarehouses serves GET /warehouses — the full discovery view.
func (h *handler) listWarehouses(c *gin.Context) {
	resp, err := h.assembleDiscovery()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, resp)
}

// listWarehouseTeamIDs serves GET /warehouse-team-ids — a bare JSON array
// of team ids across discoverable warehouses, shaped for generic
// include-list pollers (millpond's HttpIncludeValues) that want values
// only, no envelope. A pure projection of the same assembly as the full
// listing. Resharding warehouses' teams are INCLUDED: upstream ingestion
// keeps running during a duckling reshard, and dropping the team here
// would (after the consumer's damping) silently stop ingesting it.
//
// (Not /warehouses/team-ids: org ids are free-form slugs, so a static
// child under /warehouses would squat the namespace a future per-org
// GET /warehouses/:org_id needs.)
func (h *handler) listWarehouseTeamIDs(c *gin.Context) {
	resp, err := h.assembleDiscovery()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	seen := make(map[int64]struct{})
	ids := make([]int64, 0, len(resp.Warehouses))
	for _, w := range resp.Warehouses {
		for _, t := range w.Teams {
			// ALL teams, including disabled ones: enabled is the query-
			// serving switch, not an ingestion signal (see the module
			// comment) — dropping a disabled team here would stop its
			// ingestion upstream, i.e. permanent event loss from a flag
			// that only means "don't serve queries."
			if _, dup := seen[t.TeamID]; !dup {
				seen[t.TeamID] = struct{}{}
				ids = append(ids, t.TeamID)
			}
		}
	}
	sort.Slice(ids, func(a, b int) bool { return ids[a] < ids[b] })
	c.JSON(http.StatusOK, ids)
}
