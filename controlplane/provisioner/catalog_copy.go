//go:build kubernetes

package provisioner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
//   - The target database may be REUSED (it survives failed/rolled-back
//     attempts and an earlier residence of the same catalog, and a dev target
//     can even host another tenant's live catalog): the per-table
//     DROP TABLE IF EXISTS covers same-named tables, but index NAMES are
//     schema-global, so replay is idempotent — every CREATE INDEX (and every
//     index-backed ADD CONSTRAINT) drops a same-named stale index first, and
//     a create that STILL collides (a live worker's
//     ensureDuckLakeMetadataIndexes CREATE INDEX IF NOT EXISTS racing between
//     our drop and create) is re-dropped and retried a bounded number of
//     times.

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
	PerTableRows         map[string]int64
	PerTableFingerprints map[string]CatalogTableFingerprint
	// ReleaseSourceFence releases the source snapshot and its SHARE locks.
	// The runner keeps it until verification and destructive cleanup finish.
	ReleaseSourceFence func()
}

type CatalogTableFingerprint struct {
	Rows   int64
	Digest string
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
	// tx) — the post-copy source-stability recheck and the external-catalog
	// verify. log receives periodic progress lines (a ~20k-table catalog
	// takes minutes to count).
	SnapshotCounts(ctx context.Context, ep CatalogEndpoint, log func(level, msg string)) (map[string]int64, error)
	SnapshotFingerprints(ctx context.Context, ep CatalogEndpoint, log func(level, msg string)) (map[string]CatalogTableFingerprint, error)
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
	result := CatalogCopyResult{PerTableRows: map[string]int64{}, PerTableFingerprints: map[string]CatalogTableFingerprint{}}

	src, err := pgx.Connect(ctx, source.DSN())
	if err != nil {
		return result, fmt.Errorf("connect source %s: %w", source.Redacted(), err)
	}
	keepSourceFence := false
	defer func() {
		if !keepSourceFence {
			src.Close(context.WithoutCancel(ctx))
		}
	}()
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

	log("info", "opening a consistent source snapshot and discovering catalog tables…")

	// One consistent snapshot across every table.
	tx, err := src.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return result, fmt.Errorf("begin source snapshot tx: %w", err)
	}
	defer func() {
		if !keepSourceFence {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()

	tables, err := queryStrings(ctx, tx, catalogTablesQuery)
	if err != nil {
		return result, fmt.Errorf("discover catalog tables: %w", err)
	}
	if len(tables) == 0 {
		return result, fmt.Errorf("source has no ducklake_%% tables — refusing to copy an empty catalog")
	}
	lockNames := make([]string, len(tables))
	for i, table := range tables {
		lockNames[i] = quoteIdent(table)
	}
	log("info", fmt.Sprintf("acquiring source write fence across %d catalog tables…", len(tables)))
	if _, err := tx.Exec(ctx, "LOCK TABLE "+strings.Join(lockNames, ", ")+" IN SHARE MODE"); err != nil {
		return result, fmt.Errorf("acquire source catalog SHARE locks: %w", err)
	}
	lockedTables, err := queryStrings(ctx, tx, catalogTablesQuery)
	if err != nil {
		return result, fmt.Errorf("re-discover catalog tables after source fence: %w", err)
	}
	if strings.Join(lockedTables, "\x00") != strings.Join(tables, "\x00") {
		return result, fmt.Errorf("source catalog table set changed while acquiring the write fence")
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
	if err := applyConstraintsAndIndexes(tables, func(table string) error {
		if err := applyConstraints(ctx, tx, dst, table); err != nil {
			return err
		}
		return applyIndexes(ctx, tx, dst, table)
	}, log); err != nil {
		return result, err
	}

	// Verify inside the same snapshot: source snapshot counts vs live target.
	verified, err := verifyCopiedRowCounts(tables,
		func(table string) (int64, error) {
			var c int64
			err := tx.QueryRow(ctx, "SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&c)
			return c, err
		},
		func(table string) (int64, error) {
			var c int64
			err := dst.QueryRow(ctx, "SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&c)
			return c, err
		},
		log)
	if err != nil {
		return result, err
	}
	for table, c := range verified {
		result.PerTableRows[table] = c
	}
	log("info", fmt.Sprintf("fingerprinting source and target contents across %d tables…", len(tables)))
	for _, table := range tables {
		sourceFingerprint, err := fingerprintTable(ctx, tx, table)
		if err != nil {
			return result, fmt.Errorf("fingerprint source %s: %w", table, err)
		}
		targetFingerprint, err := fingerprintTable(ctx, dst, table)
		if err != nil {
			return result, fmt.Errorf("fingerprint target %s: %w", table, err)
		}
		if sourceFingerprint != targetFingerprint {
			return result, fmt.Errorf("content fingerprint mismatch on %s: source %s, target %s", table, sourceFingerprint.Digest, targetFingerprint.Digest)
		}
		result.PerTableFingerprints[table] = sourceFingerprint
	}

	keepSourceFence = true
	released := false
	result.ReleaseSourceFence = func() {
		if released {
			return
		}
		released = true
		_ = tx.Rollback(context.Background())
		src.Close(context.Background())
	}
	return result, nil
}

type fingerprintQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

// fingerprintTable computes a streaming, order-independent multiset digest.
// Each canonical jsonb row is SHA-256 hashed; the hashes are added modulo
// 2^256, so row order does not matter while duplicates remain significant.
func fingerprintTable(ctx context.Context, q fingerprintQuerier, table string) (CatalogTableFingerprint, error) {
	rows, err := q.Query(ctx, "SELECT to_jsonb(t)::text FROM "+quoteIdent(table)+" AS t")
	if err != nil {
		return CatalogTableFingerprint{}, err
	}
	defer rows.Close()
	var out CatalogTableFingerprint
	var sum [sha256.Size]byte
	for rows.Next() {
		var canonical string
		if err := rows.Scan(&canonical); err != nil {
			return CatalogTableFingerprint{}, err
		}
		addRowFingerprint(&sum, canonical)
		out.Rows++
	}
	if err := rows.Err(); err != nil {
		return CatalogTableFingerprint{}, err
	}
	out.Digest = hex.EncodeToString(sum[:])
	return out, nil
}

func addRowFingerprint(sum *[sha256.Size]byte, canonical string) {
	h := sha256.Sum256([]byte(canonical))
	carry := uint16(0)
	for i := len(sum) - 1; i >= 0; i-- {
		total := uint16(sum[i]) + uint16(h[i]) + carry
		sum[i] = byte(total)
		carry = total >> 8
	}
}

func (PGCatalogCopier) SnapshotCounts(ctx context.Context, ep CatalogEndpoint, log func(level, msg string)) (map[string]int64, error) {
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
	for i, table := range tables {
		var c int64
		if err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&c); err != nil {
			return nil, fmt.Errorf("count %s: %w", table, err)
		}
		counts[table] = c
		maybeLogProgress(log, "counted", i+1, len(tables))
	}
	return counts, nil
}

func (PGCatalogCopier) SnapshotFingerprints(ctx context.Context, ep CatalogEndpoint, log func(level, msg string)) (map[string]CatalogTableFingerprint, error) {
	conn, err := pgx.Connect(ctx, ep.DSN())
	if err != nil {
		return nil, fmt.Errorf("connect %s: %w", ep.Redacted(), err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	tables, err := queryStrings(ctx, conn, catalogTablesQuery)
	if err != nil {
		return nil, fmt.Errorf("discover catalog tables: %w", err)
	}
	result := make(map[string]CatalogTableFingerprint, len(tables))
	for i, table := range tables {
		fingerprint, err := fingerprintTable(ctx, conn, table)
		if err != nil {
			return nil, fmt.Errorf("fingerprint %s: %w", table, err)
		}
		result[table] = fingerprint
		maybeLogProgress(log, "fingerprinted", i+1, len(tables))
	}
	return result, nil
}

// verifyProgressEvery is the periodic-progress cadence of the loops that visit
// every catalog table (row-count verification and recounts): one
// "<verb> N/M tables…" line per this many tables — ~8 lines on a ~20k-table
// catalog, never per-table — so the op log does not go silent for minutes
// while the loop is healthy.
const verifyProgressEvery = 2500

// maybeLogProgress emits the periodic progress line every verifyProgressEvery
// items. The final item is skipped — the caller logs its own completion line.
func maybeLogProgress(log func(level, msg string), verb string, done, total int) {
	if done > 0 && done < total && done%verifyProgressEvery == 0 {
		log("info", fmt.Sprintf("%s %d/%d tables…", verb, done, total))
	}
}

// applyConstraintsAndIndexes drives the constraint/index replay across all
// tables. On a large catalog (~20k tables) this runs tens of seconds, so the
// phase announces itself before the first ALTER instead of logging only on
// completion.
func applyConstraintsAndIndexes(tables []string, apply func(table string) error, log func(level, msg string)) error {
	log("info", fmt.Sprintf("applying constraints and indexes on the target (%d tables)…", len(tables)))
	for _, table := range tables {
		if err := apply(table); err != nil {
			return err
		}
	}
	log("info", "constraints and indexes applied on target")
	return nil
}

// verifyCopiedRowCounts compares the source-snapshot row count of every table
// against the live target, announcing the phase up front and emitting periodic
// progress. Returns the per-table source counts on success.
func verifyCopiedRowCounts(tables []string, srcCount, dstCount func(table string) (int64, error), log func(level, msg string)) (map[string]int64, error) {
	log("info", fmt.Sprintf("verifying row counts across %d tables… (may take a few minutes)", len(tables)))
	counts := make(map[string]int64, len(tables))
	for i, table := range tables {
		src, err := srcCount(table)
		if err != nil {
			return nil, fmt.Errorf("count source %s: %w", table, err)
		}
		dst, err := dstCount(table)
		if err != nil {
			return nil, fmt.Errorf("count target %s: %w", table, err)
		}
		if src != dst {
			return nil, fmt.Errorf("row count mismatch on %s: source %d, target %d", table, src, dst)
		}
		counts[table] = src
		maybeLogProgress(log, "verified", i+1, len(tables))
	}
	log("info", fmt.Sprintf("verified %d tables: target row counts match the source snapshot", len(tables)))
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
		if constraintCreatesIndex(c.typ) {
			// The backing index is named after the constraint and index names
			// are schema-global: on a reused target a stale index (attached to
			// a table outside the source's table set, so it survived the
			// per-table drop) can squat the name and fail the ADD CONSTRAINT
			// with 42P07.
			if _, err := dst.Exec(ctx, "DROP INDEX IF EXISTS public."+quoteIdent(c.name)); err != nil {
				return fmt.Errorf("drop stale index %s on target (squats the constraint name for %s): %w", c.name, table, err)
			}
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
	rows, err := srcTx.Query(ctx, `
SELECT i.indexname, i.indexdef
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
	var indexes []indexReplay
	for rows.Next() {
		var ix indexReplay
		if err := rows.Scan(&ix.name, &ix.def); err != nil {
			rows.Close()
			return fmt.Errorf("scan index of %s: %w", table, err)
		}
		indexes = append(indexes, ix)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("introspect indexes of %s: %w", table, err)
	}
	return replayIndexes(table, indexes, func(sql string) error {
		_, err := dst.Exec(ctx, sql)
		return err
	}, isDuplicateRelationError)
}

// indexReplay is one standalone index to recreate on the target.
type indexReplay struct {
	name string // pg_indexes.indexname — index names are schema-global
	def  string // pg_indexes.indexdef — a complete CREATE INDEX statement
}

// maxIndexReplayAttempts bounds the drop+create retry of one index replay when
// the create keeps colliding with a concurrently recreated same-named index.
const maxIndexReplayAttempts = 3

// replayIndexes recreates a table's standalone indexes on the target
// idempotently. The target may be a reused database where a same-named index
// already exists, in two flavors (both observed on the real mw-dev target):
//
//   - stale: left over from a prior residence/attempt, attached to a table
//     outside the source's table set, so the per-table DROP TABLE missed it;
//   - concurrent: a live worker attached to the same (dev, shared) database
//     re-created a duckgres metadata performance index via
//     ensureDuckLakeMetadataIndexes' CREATE INDEX IF NOT EXISTS on a table
//     this copy had just recreated — mid-copy, before this replay ran.
//
// So each index is dropped BY NAME first, and a create that still collides
// (SQLSTATE 42P07 — the concurrent ensure raced between our drop and our
// create) is re-dropped and retried, bounded by maxIndexReplayAttempts.
// IF NOT EXISTS instead would silently keep a possibly-wrong stale index; the
// replayed definition must win.
func replayIndexes(table string, indexes []indexReplay, exec func(sql string) error, isDuplicate func(error) bool) error {
	for _, ix := range indexes {
		drop := "DROP INDEX IF EXISTS public." + quoteIdent(ix.name)
		var lastCreateErr error
		created := false
		for attempt := 0; attempt < maxIndexReplayAttempts && !created; attempt++ {
			if err := exec(drop); err != nil {
				return fmt.Errorf("drop stale index %s on target (before replaying it for %s): %w", ix.name, table, err)
			}
			err := exec(ix.def)
			switch {
			case err == nil:
				created = true
			case isDuplicate(err):
				lastCreateErr = err // concurrently recreated — drop and retry
			default:
				return fmt.Errorf("apply index on %s (%s): %w", table, ix.def, err)
			}
		}
		if !created {
			return fmt.Errorf("apply index on %s (%s): still colliding after %d drop+create attempts: %w", table, ix.def, maxIndexReplayAttempts, lastCreateErr)
		}
	}
	return nil
}

// isDuplicateRelationError reports whether err is PostgreSQL "relation ...
// already exists" (SQLSTATE 42P07).
func isDuplicateRelationError(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P07"
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

// constraintCreatesIndex reports whether replaying a pg_constraint row of the
// given contype creates a backing index named after the constraint (primary
// key, unique, exclusion). Those names are schema-global on the target, so a
// stale same-named index must be dropped before the ADD CONSTRAINT.
func constraintCreatesIndex(contype string) bool {
	return contype == "p" || contype == "u" || contype == "x"
}
