//go:build kubernetes

package admin

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/posthog/duckgres/controlplane/configstore"
)

// DucklingChecker is the slice of the Duckling k8s client the drift finder
// needs. Declared here so the admin package doesn't import provisioner;
// *provisioner.DucklingClient satisfies it.
type DucklingChecker interface {
	// CRStatus takes the Duckling CR name — the warehouse row's duckling_name,
	// never a derived value.
	CRStatus(ctx context.Context, name string) (present, ready bool, err error)
	ListCRNames(ctx context.Context) ([]string, error)
}

// driftHandler finds drift between config-store warehouses and live Duckling
// CRs in the cluster.
type driftHandler struct {
	store   *configstore.ConfigStore
	checker DucklingChecker
}

// driftEntry is one anomaly in the drift report. Only anomalies are emitted —
// healthy warehouses are omitted.
type driftEntry struct {
	Org            string `json:"org"`
	DucklingName   string `json:"duckling_name"`
	WarehouseState string `json:"warehouse_state"`
	CRPresent      bool   `json:"cr_present"`
	CRReady        bool   `json:"cr_ready"`
	Issue          string `json:"issue"`
	Message        string `json:"message"`
}

// driftCheckTimeout bounds the whole handler: each warehouse triggers at least
// one k8s GET plus a namespace LIST, which can be slow on a large cluster.
const driftCheckTimeout = 15 * time.Second

// RegisterDucklingsDrift wires GET /ducklings/drift. Admin-only via a
// per-route RequireAdmin so the gate travels with the route (mirrors
// operators_api.go). checker may be nil (Duckling client unavailable) — the
// handler then degrades to {"available": false}.
func RegisterDucklingsDrift(r *gin.RouterGroup, store *configstore.ConfigStore, checker DucklingChecker) {
	h := &driftHandler{store: store, checker: checker}
	r.GET("/ducklings/drift", RequireAdmin(), h.findDrift)
}

func (h *driftHandler) findDrift(c *gin.Context) {
	// When the Duckling client is unavailable (out-of-cluster, RBAC, etc.)
	// degrade gracefully rather than 500 — the UI can show "unavailable".
	if h.checker == nil {
		c.JSON(http.StatusOK, gin.H{"available": false, "checked": 0, "entries": []driftEntry{}})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), driftCheckTimeout)
	defer cancel()

	warehouses, err := h.store.ListWarehouses()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	entries := make([]driftEntry, 0)
	// expected holds exactly the stored duckling_name of every warehouse row,
	// so the orphan pass below flags any CR not claimed by a row.
	expected := make(map[string]struct{}, len(warehouses))

	for i := range warehouses {
		wh := warehouses[i]
		orgID := wh.OrgID
		// The stored duckling_name is the authoritative CR name. The column is
		// NOT NULL and backfilled; the org-ID fallback only covers legacy rows
		// with an empty value (the CR name defaults to the org ID verbatim).
		name := wh.DucklingName
		if name == "" {
			name = orgID
		}
		expected[name] = struct{}{}

		present, ready, cerr := h.checker.CRStatus(ctx, name)
		if cerr != nil {
			entries = append(entries, driftEntry{
				Org:            orgID,
				DucklingName:   name,
				WarehouseState: string(wh.State),
				Issue:          "check_error",
				Message:        "failed to check Duckling CR: " + cerr.Error(),
			})
			continue
		}

		entry := driftEntry{
			Org:            orgID,
			DucklingName:   name,
			WarehouseState: string(wh.State),
			CRPresent:      present,
			CRReady:        ready,
		}
		switch {
		case !present:
			entry.Issue = "missing"
			entry.Message = "warehouse exists but Duckling CR not found"
		case present && !ready:
			entry.Issue = "not_ready"
			entry.Message = "Duckling CR present but not Ready"
		case present && ready && wh.State != configstore.ManagedWarehouseStateReady:
			entry.Issue = "state_mismatch"
			entry.Message = fmt.Sprintf("CR healthy but warehouse state is %s", wh.State)
		default:
			// Healthy: CR present, ready, and warehouse state Ready. Skip.
			continue
		}
		entries = append(entries, entry)
	}

	// Orphan pass: any live CR not mapped by some warehouse row.
	names, err := h.checker.ListCRNames(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	for _, crName := range names {
		if _, ok := expected[crName]; ok {
			continue
		}
		entries = append(entries, driftEntry{
			Org:          "",
			DucklingName: crName,
			CRPresent:    true,
			Issue:        "orphan",
			Message:      "Duckling CR with no warehouse row",
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"available": true,
		"checked":   len(warehouses),
		"entries":   entries,
	})
}
