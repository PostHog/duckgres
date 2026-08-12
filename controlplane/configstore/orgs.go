package configstore

import (
	"errors"
	"fmt"
	"regexp"
)

// databaseNamePattern constrains an org's database_name to a single DNS label
// (RFC 1035: lowercase alphanumeric + hyphens, no leading/trailing hyphen,
// 1–63 chars — the same rule as a Kubernetes DNS-1123 label). The database
// name is not just a Postgres startup parameter: in multitenant deployments
// it is the tenant's public socket identity — the single hostname label of
// its managed SNI hostname (<database_name>.<managed-suffix>) and the
// dbname clients connect with. A value that isn't a valid single label —
// spaces, dots, underscores, uppercase — can be stored fine but the hostname
// it produces is unroutable: it fails the wildcard cert and is rejected by
// the SNI prefix extraction (sni_kubernetes.go drops multi-label prefixes),
// leaving the tenant reachable by no hostname at all. Rejecting at write time
// surfaces the typo as a 400 instead of a mysteriously unreachable tenant.
var databaseNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// maxDatabaseNameLength is the DNS label length limit (RFC 1035).
const maxDatabaseNameLength = 63

// ValidateDatabaseName rejects org database names that are not valid single
// DNS labels. Applied on every surface that writes duckgres_orgs.database_name
// (provisioning, admin create, admin update). Pre-existing grandfathered rows
// that predate the rule stay readable and editable through the admin update
// path — fixing them is the point of that surface.
func ValidateDatabaseName(name string) error {
	if name == "" {
		return errors.New("database_name is required")
	}
	if len(name) > maxDatabaseNameLength {
		return fmt.Errorf("database_name must be at most %d characters (DNS label limit)", maxDatabaseNameLength)
	}
	if !databaseNamePattern.MatchString(name) {
		return fmt.Errorf("database_name %q must be a valid DNS label: lowercase letters, digits and hyphens, starting and ending alphanumeric (no spaces, dots or underscores — it becomes the org's hostname label)", name)
	}
	return nil
}

// IsUniqueViolationErr reports whether err comes from a Postgres 23505
// unique-constraint violation. The pgx/jackc driver surfaces the SQLSTATE
// through a method on the returned error (mirrors the pattern in
// controlplane/provisioner/postgres_admin.go). HTTP handlers map it to a
// clear 409 ("your input conflicts with existing state") instead of a 500
// carrying the raw constraint text.
func IsUniqueViolationErr(err error) bool {
	type sqlStater interface{ SQLState() string }
	var s sqlStater
	return errors.As(err, &s) && s.SQLState() == "23505"
}
