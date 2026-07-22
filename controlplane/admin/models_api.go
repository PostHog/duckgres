//go:build kubernetes

package admin

import (
	"net/http"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// modelsRowLimit caps how many rows a single model listing returns. The
// config-schema tables are tiny, but runtime tables (worker_records,
// org_connection_queue) can grow; this keeps a stray listing from streaming an
// unbounded result into the browser. A listing at the cap sets `truncated`.
const modelsRowLimit = 2000

// modelGroup buckets descriptors into the UI sidebar sections.
type modelGroup string

const (
	modelGroupTenants modelGroup = "Tenants"
	modelGroupRuntime modelGroup = "Runtime"
	modelGroupAdmin   modelGroup = "Admin"
	// Other holds AUTO-DISCOVERED tables: everything information_schema shows
	// in the config + runtime schemas that no typed descriptor covers. New
	// tables (migrations, AutoMigrated operational state) appear here with no
	// registry edit — the explorer is a database explorer, not a curated list.
	modelGroupOther modelGroup = "Other"
)

// modelDescriptor describes one browsable config-store table for the generic
// models explorer. A typed descriptor gives a table PRECISE redaction (fields
// tagged json:"-" vanish) plus a human label/group; every table WITHOUT a
// descriptor is still browsable via information_schema auto-discovery (group
// "Other"), where credential-shaped column values are redacted by name
// pattern. Add a descriptor when a table needs exact redaction or a curated
// spot in the sidebar — nothing breaks if you don't.
type modelDescriptor struct {
	// Key is the URL slug and stable identifier (e.g. "orgs").
	Key string
	// Label is the human-facing sidebar name.
	Label string
	// Group is the sidebar section this model lives under.
	Group modelGroup
	// Table is the bare table name (the model's TableName()). Runtime models
	// are schema-qualified at query time with the CP runtime schema.
	Table string
	// Runtime marks a table that lives in the control-plane runtime schema
	// (cp_instances, worker_records, …) rather than the snapshot-backed config
	// schema, so reads must be schema-qualified.
	Runtime bool
	// newSlice returns a pointer to a fresh, empty typed slice to scan into.
	// Scanning into the typed model (not a map[string]any) means the struct's
	// json tags apply on marshal, so json:"-" fields — OrgUser.Password,
	// OrgUserSecret.Ciphertext, DuckLakeConfig.S3SecretKey, the singleton IDs —
	// are dropped from the API response for free. Never swap this for a raw
	// map scan: that would leak those columns.
	newSlice func() any
	// elem is the element type behind newSlice, used to derive column order.
	elem reflect.Type
}

// modelDescriptors returns the ordered registry of typed models. Order is the
// sidebar order for the curated groups; tables not listed here surface
// automatically under "Other" via information_schema discovery.
func modelDescriptors() []modelDescriptor {
	mk := func(key, label string, group modelGroup, runtime bool, sample any) modelDescriptor {
		// table name from the model's TableName() via the gorm Tabler contract.
		table := ""
		if t, ok := sample.(interface{ TableName() string }); ok {
			table = t.TableName()
		}
		elem := reflect.TypeOf(sample)
		return modelDescriptor{
			Key:     key,
			Label:   label,
			Group:   group,
			Table:   table,
			Runtime: runtime,
			elem:    elem,
			newSlice: func() any {
				return reflect.New(reflect.SliceOf(elem)).Interface()
			},
		}
	}
	return []modelDescriptor{
		mk("orgs", "Orgs", modelGroupTenants, false, configstore.Org{}),
		mk("org-teams", "Org Teams", modelGroupTenants, false, configstore.OrgTeam{}),
		mk("org-users", "Org Users", modelGroupTenants, false, configstore.OrgUser{}),
		mk("org-user-secrets", "Org User Secrets", modelGroupTenants, false, configstore.OrgUserSecret{}),
		mk("managed-warehouses", "Managed Warehouses", modelGroupTenants, false, configstore.ManagedWarehouse{}),

		mk("cp-instances", "Control Plane Instances", modelGroupRuntime, true, configstore.ControlPlaneInstance{}),
		mk("worker-records", "Worker Records", modelGroupRuntime, true, configstore.WorkerRecord{}),
		mk("flight-session-records", "Flight Session Records", modelGroupRuntime, true, configstore.FlightSessionRecord{}),
		mk("org-connection-queue", "Org Connection Queue", modelGroupRuntime, true, configstore.OrgConnectionQueueEntry{}),
		mk("org-connection-leases", "Org Connection Leases", modelGroupRuntime, true, configstore.OrgConnectionLease{}),

		// Admin section after Runtime: operators is a config-schema table
		// (goose-migrated duckgres_operators, read without schema qualification)
		// holding the admin-console access list. It stays in this position so the
		// sidebar order remains Tenants → Runtime → Admin.
		mk("operators", "Operators", modelGroupAdmin, false, configstore.Operator{}),
	}
}

type modelsHandler struct {
	store *configstore.ConfigStore
}

// autoKeyPrefix namespaces the keys of auto-discovered tables ("auto:<schema>.<table>").
const autoKeyPrefix = "auto:"

// sensitiveColumnRe redacts VALUES of auto-discovered columns whose name looks
// credential-shaped. The typed descriptors above redact precisely via json
// tags; auto-discovered tables have no type to consult, so err toward
// redaction — a bcrypt hash or ciphertext leaking is fatal, an over-redacted
// secret NAME is a shrug. (Matches e.g. password, password_aws_secret,
// ciphertext, token, nonce, credential.)
var sensitiveColumnRe = regexp.MustCompile(`(?i)(password|secret|ciphertext|token|nonce|credential)`)

// autoTable is one information_schema-discovered table.
type autoTable struct {
	Schema string
	Name   string
}

func (a autoTable) key() string   { return autoKeyPrefix + a.Schema + "." + a.Name }
func (a autoTable) label() string { return a.Name }

// discoverTables lists every base table in the config schema (current_schema)
// and the CP runtime schema that is NOT already covered by a typed descriptor.
// Reading information_schema fresh per request keeps the explorer honest: a
// table created by a migration or AutoMigrate after boot appears immediately.
func (h *modelsHandler) discoverTables() ([]autoTable, error) {
	covered := map[string]struct{}{}
	for _, d := range modelDescriptors() {
		covered[d.Table] = struct{}{}
	}
	runtimeSchema := h.store.RuntimeSchema()

	type row struct {
		TableSchema string
		TableName   string
	}
	var rows []row
	err := h.store.DB().Raw(`
		SELECT table_schema, table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		  AND table_schema IN (current_schema(), ?)
		ORDER BY table_schema, table_name`, runtimeSchema).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	var out []autoTable
	for _, r := range rows {
		if _, ok := covered[r.TableName]; ok {
			continue
		}
		out = append(out, autoTable{Schema: r.TableSchema, Name: r.TableName})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out, nil
}

// resolveAutoTable validates an auto key against a FRESH discovery — the only
// table names ever interpolated into SQL are ones information_schema just
// returned, so a crafted key can't smuggle SQL.
func (h *modelsHandler) resolveAutoTable(key string) (autoTable, bool) {
	if !strings.HasPrefix(key, autoKeyPrefix) {
		return autoTable{}, false
	}
	tables, err := h.discoverTables()
	if err != nil {
		return autoTable{}, false
	}
	for _, t := range tables {
		if t.key() == key {
			return t, true
		}
	}
	return autoTable{}, false
}

// autoColumns returns the table's column names in ordinal order.
func (h *modelsHandler) autoColumns(t autoTable) ([]string, error) {
	var cols []string
	err := h.store.DB().Raw(`
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`, t.Schema, t.Name).Scan(&cols).Error
	return cols, err
}

// registerModelsAPI registers the read-only generic models explorer used by the
// admin dashboard's models browser.
func registerModelsAPI(r *gin.RouterGroup, store *configstore.ConfigStore) {
	h := &modelsHandler{store: store}
	r.GET("/models", h.listModels)
	r.GET("/models/:model", h.getModel)
}

// qualifiedTable returns the table name to read for a descriptor, schema-
// qualifying runtime tables with the CP runtime schema.
func (h *modelsHandler) qualifiedTable(d modelDescriptor) string {
	if d.Runtime {
		return h.store.RuntimeSchema() + "." + d.Table
	}
	return d.Table
}

// modelSummary is a sidebar entry: identity plus a live row count.
type modelSummary struct {
	Key   string `json:"key"`
	Label string `json:"label"`
	Group string `json:"group"`
	Count int64  `json:"count"`
}

func (h *modelsHandler) listModels(c *gin.Context) {
	db := h.store.DB()
	descriptors := modelDescriptors()
	out := make([]modelSummary, 0, len(descriptors))
	for _, d := range descriptors {
		var count int64
		// A count failure on one table must not blank the whole sidebar; report
		// -1 so the UI can show "?" while the rest of the list still renders.
		if err := db.Table(h.qualifiedTable(d)).Count(&count).Error; err != nil {
			count = -1
		}
		out = append(out, modelSummary{
			Key:   d.Key,
			Label: d.Label,
			Group: string(d.Group),
			Count: count,
		})
	}
	autoTables, err := h.discoverTables()
	if err == nil {
		for _, t := range autoTables {
			var count int64
			if err := db.Table(t.Schema + "." + t.Name).Count(&count).Error; err != nil {
				count = -1
			}
			out = append(out, modelSummary{
				Key:   t.key(),
				Label: t.label(),
				Group: string(modelGroupOther),
				Count: count,
			})
		}
	}
	c.JSON(http.StatusOK, gin.H{"models": out})
}

// modelListing is the response for one model's rows.
type modelListing struct {
	Key       string   `json:"key"`
	Label     string   `json:"label"`
	Group     string   `json:"group"`
	Table     string   `json:"table"`
	Columns   []string `json:"columns"`
	Count     int64    `json:"count"`
	Truncated bool     `json:"truncated"`
	Rows      any      `json:"rows"`
}

func (h *modelsHandler) getModel(c *gin.Context) {
	key := c.Param("model")
	var desc *modelDescriptor
	for _, d := range modelDescriptors() {
		if d.Key == key {
			d := d
			desc = &d
			break
		}
	}
	if desc == nil {
		if t, ok := h.resolveAutoTable(key); ok {
			h.getAutoTable(c, t)
			return
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "unknown model: " + key})
		return
	}

	db := h.store.DB()
	table := h.qualifiedTable(*desc)

	var total int64
	if err := db.Table(table).Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	rows := desc.newSlice()
	// Limit + 1 detects truncation without a second query.
	if err := db.Table(table).Limit(modelsRowLimit + 1).Find(rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	truncated := false
	rv := reflect.ValueOf(rows).Elem()
	if rv.Len() > modelsRowLimit {
		truncated = true
		rv.Set(rv.Slice(0, modelsRowLimit))
	}

	c.JSON(http.StatusOK, modelListing{
		Key:       desc.Key,
		Label:     desc.Label,
		Group:     string(desc.Group),
		Table:     table,
		Columns:   jsonFieldOrder(desc.elem),
		Count:     total,
		Truncated: truncated,
		Rows:      rows,
	})
}

// jsonFieldOrder returns the ordered top-level JSON keys for a struct type,
// skipping fields tagged json:"-". Embedded warehouse sub-structs are named
// fields (json:"s3", …) so they surface as a single nested-object column, which
// is what the detail view renders. This gives the table stable column headers
// even for an empty result set (the common case for runtime tables).
func jsonFieldOrder(t reflect.Type) []string {
	if t == nil || t.Kind() != reflect.Struct {
		return nil
	}
	cols := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue // unexported
		}
		tag := f.Tag.Get("json")
		name := f.Name
		if tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] == "-" {
				continue
			}
			if parts[0] != "" {
				name = parts[0]
			}
		}
		cols = append(cols, name)
	}
	return cols
}

// getAutoTable lists an auto-discovered table: columns from
// information_schema, rows as generic maps, values of credential-shaped
// columns redacted (see sensitiveColumnRe — auto tables have no typed model
// whose json tags could redact precisely).
func (h *modelsHandler) getAutoTable(c *gin.Context, t autoTable) {
	db := h.store.DB()
	table := t.Schema + "." + t.Name

	columns, err := h.autoColumns(t)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var total int64
	if err := db.Table(table).Count(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var rows []map[string]interface{}
	if err := db.Table(table).Limit(modelsRowLimit + 1).Find(&rows).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	truncated := false
	if len(rows) > modelsRowLimit {
		truncated = true
		rows = rows[:modelsRowLimit]
	}
	for _, row := range rows {
		for col, v := range row {
			if v != nil && sensitiveColumnRe.MatchString(col) {
				row[col] = "[redacted]"
			}
		}
	}
	if rows == nil {
		rows = []map[string]interface{}{}
	}

	c.JSON(http.StatusOK, modelListing{
		Key:       t.key(),
		Label:     t.label(),
		Group:     string(modelGroupOther),
		Table:     table,
		Columns:   columns,
		Count:     total,
		Truncated: truncated,
		Rows:      rows,
	})
}
