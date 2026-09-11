package configstore

import (
	"regexp"
	"strings"
)

// trinoCatalogIdentifier is the Trino catalog identifier grammar
// ([a-z0-9_]+). Anything outside this set in the principal is replaced with
// `_` before forming the catalog name.
var trinoCatalogIdentifier = regexp.MustCompile(`[^a-z0-9_]`)

// TrinoSanitize lowercases and replaces non-[a-z0-9_] runs with `_`.
// Pure function so callers can recover the sanitized name without holding a
// provisioner.
func TrinoSanitize(principal string) string {
	return trinoCatalogIdentifier.ReplaceAllString(strings.ToLower(principal), "_")
}

// TrinoCatalogName returns the catalog identifier for an org.
// Format: org_<sanitized>. The sanitization maps the org's TrinoPrincipal
// (its database_name) to Trino identifier rules ([a-z0-9_]); any other
// characters collapse to underscores.
//
// For principals that satisfy ValidateDatabaseName the mapping is injective
// — that grammar allows only lowercase alphanumerics and hyphens, so the
// hyphen is the only character rewritten and no valid principal contains the
// underscore it becomes — which, with database_name's global unique index,
// makes distinct orgs' catalog names distinct by construction. Grandfathered
// rows predate the validation and can still converge; the Trino provisioner's
// rejectPrincipalCollisions holds those orgs back rather than letting one read
// the other's catalog.
//
// The name carried an `_iceberg` suffix while the backing table format was
// Iceberg behind Lakekeeper. Warehouses are DuckLake now (migration 000014
// dropped every iceberg_* column), so the suffix went with it. The shape is
// pinned from three sides — this function, opa.ManagedCatalogPattern, and
// the regex literal inside policy.rego — and the pair of tests named in
// ManagedCatalogPattern's doc comment fails if any one of them moves alone.
//
// It lives here, not in the (kubernetes-tagged) Trino provisioner, because
// ResolvePostgresConnection needs it in every build: the same name is the
// logical catalog alias a pgwire session may connect with, so SQLMesh and
// friends see ONE catalog name across the Duckgres and Trino engines.
// provisioner.TrinoCatalogName delegates here.
func TrinoCatalogName(principal string) string {
	return "org_" + TrinoSanitize(principal)
}
