//go:build kubernetes

package admin

import (
	"net/http"
	"reflect"
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
	modelGroupConfig  modelGroup = "Config"
	modelGroupRuntime modelGroup = "Runtime"
)

// modelDescriptor describes one browsable config-store table for the generic
// models explorer. The descriptors are the single source of truth the sidebar,
// the listing endpoint, and the column derivation all read from — adding a new
// config-store model is one entry here.
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

// modelDescriptors returns the ordered registry of browsable models. Order is
// the sidebar order. Every persisted configstore model that an operator might
// want to inspect belongs here; if you add a model in configstore/models.go,
// add it here too (and assert it in models_api_test.go).
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
		mk("org-users", "Org Users", modelGroupTenants, false, configstore.OrgUser{}),
		mk("org-user-secrets", "Org User Secrets", modelGroupTenants, false, configstore.OrgUserSecret{}),
		mk("managed-warehouses", "Managed Warehouses", modelGroupTenants, false, configstore.ManagedWarehouse{}),

		mk("global-config", "Global Config", modelGroupConfig, false, configstore.GlobalConfig{}),
		mk("ducklake-config", "DuckLake Config", modelGroupConfig, false, configstore.DuckLakeConfig{}),
		mk("ratelimit-config", "Rate Limit Config", modelGroupConfig, false, configstore.RateLimitConfig{}),
		mk("querylog-config", "Query Log Config", modelGroupConfig, false, configstore.QueryLogConfig{}),

		mk("cp-instances", "Control Plane Instances", modelGroupRuntime, true, configstore.ControlPlaneInstance{}),
		mk("worker-records", "Worker Records", modelGroupRuntime, true, configstore.WorkerRecord{}),
		mk("flight-session-records", "Flight Session Records", modelGroupRuntime, true, configstore.FlightSessionRecord{}),
		mk("org-connection-queue", "Org Connection Queue", modelGroupRuntime, true, configstore.OrgConnectionQueueEntry{}),
		mk("org-connection-leases", "Org Connection Leases", modelGroupRuntime, true, configstore.OrgConnectionLease{}),
	}
}

type modelsHandler struct {
	store *configstore.ConfigStore
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
