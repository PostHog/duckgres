package sessioncatalog

import "strings"

const (
	CatalogDuckLake = "ducklake"
	CatalogIceberg  = "iceberg"
)

// AttachedCatalogs records which physical DuckDB catalogs are available for a
// session. The control plane probes this from the worker after session creation.
type AttachedCatalogs struct {
	DuckLake bool
	Iceberg  bool
}

// Request is the PostgreSQL session identity and catalog-selection input.
// ClientDatabase is the database name exposed to PostgreSQL clients;
// RequestedCatalog/DefaultCatalog select where queries execute.
type Request struct {
	ClientDatabase   string
	RequestedCatalog string
	DefaultCatalog   string
	Attached         AttachedCatalogs
}

// Selection is the resolved session identity. ClientDatabase remains the
// PostgreSQL-visible database. PhysicalCatalog is the DuckDB catalog to USE and
// to target when rewriting logical catalog references.
type Selection struct {
	ClientDatabase  string
	PhysicalCatalog string
}

// ResolveSelection resolves a PostgreSQL session to a physical catalog without
// rewriting the client-visible database name.
func ResolveSelection(req Request) (Selection, bool) {
	requested := normalizeCatalog(req.RequestedCatalog)
	defaultCatalog := normalizeCatalog(req.DefaultCatalog)

	if requested != "" {
		if !isAttached(requested, req.Attached) {
			return Selection{}, false
		}
		return Selection{ClientDatabase: req.ClientDatabase, PhysicalCatalog: requested}, true
	}

	if defaultCatalog != "" {
		if !isAttached(defaultCatalog, req.Attached) {
			return Selection{}, false
		}
		return Selection{ClientDatabase: req.ClientDatabase, PhysicalCatalog: defaultCatalog}, true
	}

	switch {
	case req.Attached.DuckLake:
		return Selection{ClientDatabase: req.ClientDatabase, PhysicalCatalog: CatalogDuckLake}, true
	case req.Attached.Iceberg:
		return Selection{ClientDatabase: req.ClientDatabase, PhysicalCatalog: CatalogIceberg}, true
	default:
		return Selection{ClientDatabase: req.ClientDatabase, PhysicalCatalog: req.ClientDatabase}, true
	}
}

func normalizeCatalog(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case CatalogDuckLake:
		return CatalogDuckLake
	case CatalogIceberg:
		return CatalogIceberg
	default:
		return ""
	}
}

func isAttached(catalog string, attached AttachedCatalogs) bool {
	switch catalog {
	case CatalogDuckLake:
		return attached.DuckLake
	case CatalogIceberg:
		return attached.Iceberg
	default:
		return false
	}
}
