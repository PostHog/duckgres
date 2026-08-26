// Client-side mirror of configstore.ValidateDatabaseName (Go): an org's
// database_name is the tenant's public socket identity — the single label of
// its managed hostname (<database_name>.<managed-suffix>) and the dbname
// clients connect with — so it must be a valid single DNS label (RFC 1035:
// lowercase alphanumeric + hyphens, no leading/trailing hyphen, 1–63 chars,
// i.e. a Kubernetes DNS-1123 label). A name with spaces, dots or underscores
// stores fine but produces an unroutable hostname: the wildcard cert doesn't
// cover it and SNI prefix extraction drops multi-label prefixes.
//
// Client-side symmetry only — the server is authoritative (400 on every write
// surface: provision, admin create, admin update).
const DATABASE_NAME_PATTERN = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/;
const MAX_DATABASE_NAME_LENGTH = 63;

// Returns a human-readable problem, or null when name is a valid database
// name. Empty input is "no opinion" (null) so callers decide whether the
// field is required.
export function databaseNameProblem(name: string): string | null {
  if (name === "") {
    return null;
  }
  if (name.length > MAX_DATABASE_NAME_LENGTH) {
    return `Must be at most ${MAX_DATABASE_NAME_LENGTH} characters (DNS label limit).`;
  }
  if (!DATABASE_NAME_PATTERN.test(name)) {
    return "Lowercase letters, digits and hyphens; must start and end alphanumeric (a single DNS label — no spaces, dots or underscores: it becomes the org's hostname).";
  }
  return null;
}
