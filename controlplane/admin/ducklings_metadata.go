//go:build kubernetes

package admin

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// DucklingMetadataStore mirrors provisioner.CRMetadataStore without importing
// provisioner (same decoupling as DucklingChecker); multitenant.go adapts
// between the two.
type DucklingMetadataStore struct {
	Kind     string // "cnpg-shard" | "external"
	Endpoint string // Postgres host from the CR status
}

// DucklingMetadataLister lists the live metadata-store assignment of every
// Duckling CR, keyed by CR name. Satisfied via an adapter over
// *provisioner.DucklingClient.
type DucklingMetadataLister interface {
	CRMetadataStores(ctx context.Context) (map[string]DucklingMetadataStore, error)
}

// ducklingMetadataEntry is the per-CR wire shape. CnpgShard is the parsed
// shard name (e.g. "shard-001") for cnpg-shard tenants, empty otherwise —
// the console shows the shard directly instead of making operators eyeball
// the service hostname.
type ducklingMetadataEntry struct {
	Kind      string `json:"kind"`
	Endpoint  string `json:"endpoint"`
	CnpgShard string `json:"cnpg_shard,omitempty"`
}

// ducklingMetadataTimeout bounds the single CR LIST behind the handler.
const ducklingMetadataTimeout = 15 * time.Second

// RegisterDucklingsMetadata wires GET /ducklings/metadata: the live
// metadata-store backend of every Duckling CR (notably the current cnpg
// shard), keyed by CR name. Read-only infra info — viewer-accessible like the
// other GETs. lister may be nil (Duckling client unavailable) — the handler
// then degrades to {"available": false} rather than 500ing.
func RegisterDucklingsMetadata(r *gin.RouterGroup, lister DucklingMetadataLister) {
	r.GET("/ducklings/metadata", func(c *gin.Context) {
		if lister == nil {
			c.JSON(http.StatusOK, gin.H{"available": false, "entries": map[string]ducklingMetadataEntry{}})
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), ducklingMetadataTimeout)
		defer cancel()
		stores, err := lister.CRMetadataStores(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		entries := make(map[string]ducklingMetadataEntry, len(stores))
		for name, ms := range stores {
			e := ducklingMetadataEntry{Kind: ms.Kind, Endpoint: ms.Endpoint}
			if ms.Kind == "cnpg-shard" {
				e.CnpgShard = cnpgShardFromEndpoint(ms.Endpoint)
			}
			entries[name] = e
		}
		c.JSON(http.StatusOK, gin.H{"available": true, "entries": entries})
	})
}

// cnpgShardFromEndpoint extracts the shard name from a cnpg metadata-store
// endpoint: the first DNS label minus the CloudNativePG service-role suffix,
// e.g. "shard-001-pooler.cnpg-shards.svc.cluster.local" → "shard-001".
// An unrecognized shape degrades to the bare host label — still a stable,
// compact per-shard identifier.
func cnpgShardFromEndpoint(endpoint string) string {
	host := endpoint
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	if i := strings.IndexByte(host, '.'); i >= 0 {
		host = host[:i]
	}
	for _, suf := range []string{"-pooler", "-rw", "-ro", "-r"} {
		if s, ok := strings.CutSuffix(host, suf); ok && s != "" {
			return s
		}
	}
	return host
}
