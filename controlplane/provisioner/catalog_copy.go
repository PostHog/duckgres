//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Catalog copy for reshard operations: streams every ducklake_* table of one
// per-org DuckLake catalog database into another (freshly created, empty)
// catalog database, faithfully (schema from pg_catalog introspection, rows via
// raw binary COPY passthrough, then constraints and non-constraint indexes).
//
// Invariants:
//   - The source is read inside ONE REPEATABLE READ read-only transaction, so
//     the copy is a consistent snapshot across all tables.
//   - The target session holds pg_advisory_lock(reshardCopyLockKey) for the
//     whole copy — a second (zombie ex-runner) copier blocks/fails fast
//     instead of interleaving DDL with ours.
//   - No sequences: the DuckLake catalog stores next-ids as rows (verified),
//     so a table copy carries counters correctly.
//   - Indexes backing constraints (PKs) are excluded from the pg_indexes
//     replay — ADD CONSTRAINT already created them.

// CatalogEndpoint describes one side of the copy.
type CatalogEndpoint struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	// SSLMode: "disable" for cnpg poolers (in-cluster plaintext), "require"
	// for direct RDS.
	SSLMode string
}

// DSN renders a postgres:// URL for pgx.
func (e CatalogEndpoint) DSN() string {
	port := e.Port
	if port == 0 {
		port = 5432
	}
	sslMode := e.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(e.User, e.Password),
		Host:     net.JoinHostPort(e.Host, strconv.Itoa(port)),
		Path:     "/" + e.Database,
		RawQuery: "sslmode=" + url.QueryEscape(sslMode) + "&connect_timeout=10",
	}
	return u.String()
}

// Redacted returns a loggable description without the password.
func (e CatalogEndpoint) Redacted() string {
	port := e.Port
	if port == 0 {
		port = 5432
	}
	return fmt.Sprintf("%s:%d/%s (user %s, sslmode %s)", e.Host, port, e.Database, e.User, e.SSLMode)
}

// reshardCopyLockKey is the advisory-lock key held in the TARGET database for
// the duration of copy+verify (fences concurrent copiers). The target DB is
// per-org, so a constant key suffices.
const reshardCopyLockKey = 824460 // arbitrary, stable: "reshard" niche

// CatalogCopyResult is the copy report.
type CatalogCopyResult struct {
	Tables int64
	Rows   int64
	Bytes  int64
	// PerTableRows are the row counts observed INSIDE the source snapshot
	// transaction — the reference for both the target verify and the
	// post-copy source-stability recheck.
	PerTableRows map[string]int64
}

// CatalogCopier is the interface the reshard runner drives; *PGCatalogCopier
// is the real implementation, tests fake it.
type CatalogCopier interface {
	// Probe runs SELECT 1 against the endpoint.
	Probe(ctx context.Context, ep CatalogEndpoint) error
	// Copy streams the full ducklake_* catalog from source into target and
	// verifies per-table row counts against the snapshot. log receives
	// verbose operator-facing progress lines.
	Copy(ctx context.Context, source, target CatalogEndpoint, log func(level, msg string)) (CatalogCopyResult, error)
	// SnapshotCounts re-reads the current per-table row counts (no snapshot
	// tx) — the post-copy source-stability recheck.
	SnapshotCounts(ctx context.Context, ep CatalogEndpoint) (map[string]int64, error)
	// DropCatalogTables drops all ducklake_* tables (rollback cleanup of a
	// partially copied target).
	DropCatalogTables(ctx context.Context, ep CatalogEndpoint, log func(level, msg string)) error
	// DropDatabase connects to the maintenance database ("postgres") on the
	// endpoint and drops dbName WITH (FORCE) — the cnpg source cleanup fence.
	DropDatabase(ctx context.Context, ep CatalogEndpoint, dbName string) error
}

// PGCatalogCopier is the pgx-backed CatalogCopier.
type PGCatalogCopier struct{}

func (PGCatalogCopier) Probe(ctx context.Context, ep CatalogEndpoint) error {
	conn, err := pgx.Connect(ctx, ep.DSN())
	if err != nil {
		return fmt.Errorf("connect %s: %w", ep.Redacted(), err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	var one int
	if err := conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("probe %s: %w", ep.Redacted(), err)
	}
	return nil
}

const catalogTablesQuery = `
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public' AND table_name LIKE 'ducklake\_%' ESCAPE '\'
ORDER BY table_name`

func (PGCatalogCopier) Copy(ctx context.Context, source, target CatalogEndpoint, log func(level, msg string)) (CatalogCopyResult, error) {
	result := CatalogCopyResult{PerTableRows: map[string]int64{}}

	src, err := pgx.Connect(ctx, source.DSN())
	if err != nil {
		return result, fmt.Errorf("connect source %s: %w", source.Redacted(), err)
	}
	defer src.Close(context.WithoutCancel(ctx))
	dst, err := pgx.Connect(ctx, target.DSN())
	if err != nil {
		return result, fmt.Errorf("connect target %s: %w", target.Redacted(), err)
	}
	defer dst.Close(context.WithoutCancel(ctx))

	// Fence: a second copier (zombie ex-runner) must not interleave with us.
	var locked bool
	if err := dst.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", reshardCopyLockKey).Scan(&locked); err != nil {
		return result, fmt.Errorf("acquire target copy lock: %w", err)
	}
	if !locked {
		return result, fmt.Errorf("target catalog is locked by another copier — refusing to interleave")
	}

	// One consistent snapshot across every table.
	tx, err := src.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, fmt.Errorf("begin source snapshot tx: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	tables, err := queryStrings(ctx, tx, catalogTablesQuery)
	if err != nil {
		return result, fmt.Errorf("discover catalog tables: %w", err)
	}
	if len(tables) == 0 {
		return result, fmt.Errorf("source has no ducklake_%% tables — refusing to copy an empty catalog")
	}
	log("info", fmt.Sprintf("discovered %d catalog tables in source snapshot", len(tables)))

	for _, table := range tables {
		ddl, err := buildCreateTable(ctx, tx, table)
		if err != nil {
			return result, fmt.Errorf("introspect %s: %w", table, err)
		}
		if _, err := dst.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteIdent(table))); err != nil {
			return result, fmt.Errorf("drop target %s: %w", table, err)
		}
		if _, err := dst.Exec(ctx, ddl); err != nil {
			return result, fmt.Errorf("create target %s: %w", table, err)
		}

		rows, bytes, err := copyTableBinary(ctx, tx, dst, table)
		if err != nil {
			return result, fmt.Errorf("copy %s: %w", table, err)
		}
		result.Tables++
		result.Rows += rows
		result.Bytes += bytes
		result.PerTableRows[table] = rows
		log("info", fmt.Sprintf("copied %s: %d rows, %d bytes", table, rows, bytes))
	}

	// Constraints after data (bulk-load fast path), then non-constraint
	// indexes (constraint-backed PK indexes already exist via ADD CONSTRAINT).
	for _, table := range tables {
		if err := applyConstraints(ctx, tx, dst, table); err != nil {
			return result, err
		}
		if err := applyIndexes(ctx, tx, dst, table); err != nil {
			return result, err
		}
	}
	log("info", "constraints and indexes applied on target")

	// Verify inside the same snapshot: source snapshot counts vs live target.
	for _, table := range tables {
		var srcCount, dstCount int64
		if err := tx.QueryRow(ctx, "SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&srcCount); err != nil {
			return result, fmt.Errorf("count source %s: %w", table, err)
		}
		if err := dst.QueryRow(ctx, "SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&dstCount); err != nil {
			return result, fmt.Errorf("count target %s: %w", table, err)
		}
		if srcCount != dstCount {
			return result, fmt.Errorf("row count mismatch on %s: source %d, target %d", table, srcCount, dstCount)
		}
		result.PerTableRows[table] = srcCount
	}
	log("info", fmt.Sprintf("verified %d tables: target row counts match the source snapshot", len(tables)))

	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("close source snapshot tx: %w", err)
	}
	return result, nil
}

func (PGCatalogCopier) SnapshotCounts(ctx context.Context, ep CatalogEndpoint) (map[string]int64, error) {
	conn, err := pgx.Connect(ctx, ep.DSN())
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", ep.Redacted(), err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	tables, err := queryStrings(ctx, conn, catalogTablesQuery)
	if err != nil {
		return nil, fmt.Errorf("discover catalog tables: %w", err)
	}
	counts := make(map[string]int64, len(tables))
	for _, table := range tables {
		var c int64
		if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&c); err != nil {
			return nil, fmt.Errorf("count %s: %w", table, err)
		}
		counts[table] = c
	}
	return counts, nil
}

func (PGCatalogCopier) DropCatalogTables(ctx context.Context, ep CatalogEndpoint, log func(level, msg string)) error {
	conn, err := pgx.Connect(ctx, ep.DSN())
	if err != nil {
		return fmt.Errorf("connect %s: %w", ep.Redacted(), err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	tables, err := queryStrings(ctx, conn, catalogTablesQuery)
	if err != nil {
		return fmt.Errorf("discover catalog tables: %w", err)
	}
	for _, table := range tables {
		if _, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", quoteIdent(table))); err != nil {
			return fmt.Errorf("drop %s: %w", table, err)
		}
		log("info", "dropped "+table)
	}
	return nil
}

func (PGCatalogCopier) DropDatabase(ctx context.Context, ep CatalogEndpoint, dbName string) error {
	admin := ep
	admin.Database = "postgres"
	conn, err := pgx.Connect(ctx, admin.DSN())
	if err != nil {
		return fmt.Errorf("connect %s: %w", admin.Redacted(), err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	// FORCE: pgbouncer may hold idle server connections into the tenant DB
	// for a few minutes; terminating same-role backends is permitted for the
	// database owner.
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP DATABASE %s WITH (FORCE)", quoteIdent(dbName))); err != nil {
		return fmt.Errorf("drop database %s: %w", dbName, err)
	}
	return nil
}

// pgxQuerier is the subset shared by *pgx.Conn and pgx.Tx.
type pgxQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func queryStrings(ctx context.Context, q pgxQuerier, sql string, args ...any) ([]string, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// buildCreateTable reconstructs a faithful CREATE TABLE from pg_catalog:
// format_type keeps type modifiers (varchar lengths, numeric precision) that
// information_schema.data_type loses, plus NOT NULL and column defaults.
// Constraints and indexes are applied separately after the data load.
func buildCreateTable(ctx context.Context, q pgxQuerier, table string) (string, error) {
	rows, err := q.Query(ctx, `
SELECT a.attname,
       format_type(a.atttypid, a.atttypmod),
       a.attnotnull,
       COALESCE(pg_get_expr(d.adbin, d.adrelid), '')
FROM pg_attribute a
LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE a.attrelid = ('public.' || quote_ident($1))::regclass
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`, table)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var name, typ, def string
		var notNull bool
		if err := rows.Scan(&name, &typ, &notNull, &def); err != nil {
			return "", err
		}
		col := quoteIdent(name) + " " + typ
		if def != "" {
			col += " DEFAULT " + def
		}
		if notNull {
			col += " NOT NULL"
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(cols) == 0 {
		return "", fmt.Errorf("table %s has no columns", table)
	}
	return "CREATE TABLE " + quoteIdent(table) + " (\n  " + strings.Join(cols, ",\n  ") + "\n)", nil
}

// copyTableBinary streams the table source→target with raw binary COPY
// passthrough — byte-faithful for every column type, no per-value decode.
func copyTableBinary(ctx context.Context, srcTx pgx.Tx, dst *pgx.Conn, table string) (rows int64, bytes int64, err error) {
	pr, pw := io.Pipe()
	counter := &countingWriter{w: pw}

	copyOut := fmt.Sprintf("COPY %s TO STDOUT (FORMAT binary)", quoteIdent(table))
	copyIn := fmt.Sprintf("COPY %s FROM STDIN (FORMAT binary)", quoteIdent(table))

	errCh := make(chan error, 1)
	go func() {
		_, copyErr := srcTx.Conn().PgConn().CopyTo(ctx, counter, copyOut)
		_ = pw.CloseWithError(copyErr)
		errCh <- copyErr
	}()

	tag, inErr := dst.PgConn().CopyFrom(ctx, pr, copyIn)
	outErr := <-errCh
	_ = pr.Close()
	if outErr != nil {
		return 0, counter.n, fmt.Errorf("source COPY TO: %w", outErr)
	}
	if inErr != nil {
		return 0, counter.n, fmt.Errorf("target COPY FROM: %w", inErr)
	}
	return tag.RowsAffected(), counter.n, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// applyConstraints replays the source table's constraints (PK, unique, check,
// FK) via pg_get_constraintdef, sorted so PKs/uniques come before FKs.
// NOT NULL constraints (contype 'n' — catalogued rows on PG 18+ sources) are
// skipped: the CREATE TABLE already carried NOT NULL from pg_attribute, and
// their constraintdef renders as "NOT NULL <col>", whose ADD CONSTRAINT form
// only parses on PG 18+ — an older target (external RDS) rejects it with a
// syntax error at "NOT".
func applyConstraints(ctx context.Context, srcTx pgx.Tx, dst *pgx.Conn, table string) error {
	// contype is the internal "char" type (OID 18), which pgx cannot scan in
	// binary format into a string — cast it.
	rows, err := srcTx.Query(ctx, `
SELECT conname, contype::text, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = ('public.' || quote_ident($1))::regclass
ORDER BY conname`, table)
	if err != nil {
		return fmt.Errorf("introspect constraints of %s: %w", table, err)
	}
	type con struct {
		name, typ, def string
	}
	var cons []con
	for rows.Next() {
		var c con
		if err := rows.Scan(&c.name, &c.typ, &c.def); err != nil {
			rows.Close()
			return fmt.Errorf("scan constraint of %s: %w", table, err)
		}
		cons = append(cons, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("introspect constraints of %s: %w", table, err)
	}
	// Foreign keys last (they may reference other tables' PKs).
	sort.SliceStable(cons, func(i, j int) bool { return cons[i].typ != "f" && cons[j].typ == "f" })
	for _, c := range cons {
		if !shouldReplayConstraint(c.typ) {
			continue
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", quoteIdent(table), quoteIdent(c.name), c.def)
		if _, err := dst.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("apply constraint %s on %s: %w", c.name, table, err)
		}
	}
	return nil
}

// applyIndexes replays the source table's standalone indexes (pg_indexes),
// EXCLUDING indexes backing constraints — ADD CONSTRAINT already created
// those, and replaying them would collide on the index name.
func applyIndexes(ctx context.Context, srcTx pgx.Tx, dst *pgx.Conn, table string) error {
	defs, err := queryStrings(ctx, srcTx, `
SELECT i.indexdef
FROM pg_indexes i
WHERE i.schemaname = 'public' AND i.tablename = $1
  AND NOT EXISTS (
    SELECT 1 FROM pg_constraint con
    JOIN pg_class ic ON ic.oid = con.conindid
    WHERE con.conrelid = ('public.' || quote_ident($1))::regclass
      AND ic.relname = i.indexname
  )
ORDER BY i.indexname`, table)
	if err != nil {
		return fmt.Errorf("introspect indexes of %s: %w", table, err)
	}
	for _, def := range defs {
		if _, err := dst.Exec(ctx, def); err != nil {
			return fmt.Errorf("apply index on %s (%s): %w", table, def, err)
		}
	}
	return nil
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// shouldReplayConstraint reports whether a pg_constraint row of the given
// contype is replayed on the target. 'n' (catalogued NOT NULL, PG 18+) is
// redundant with the column-level NOT NULL the CREATE TABLE already applied
// and its ADD CONSTRAINT syntax doesn't parse on older targets.
func shouldReplayConstraint(contype string) bool {
	return contype != "n"
}
