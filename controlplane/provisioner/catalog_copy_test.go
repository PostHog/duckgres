//go:build kubernetes

package provisioner

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestContentFingerprintDetectsSameCountMutationAndIgnoresRowOrder(t *testing.T) {
	fingerprint := func(rows ...string) [sha256.Size]byte {
		var sum [sha256.Size]byte
		for _, row := range rows {
			addRowFingerprint(&sum, row)
		}
		return sum
	}
	original := fingerprint(`{"id":1,"value":"a"}`, `{"id":2,"value":"b"}`)
	reordered := fingerprint(`{"id":2,"value":"b"}`, `{"id":1,"value":"a"}`)
	mutated := fingerprint(`{"id":1,"value":"changed"}`, `{"id":2,"value":"b"}`)
	if original != reordered {
		t.Fatal("multiset fingerprint changed with row order")
	}
	if original == mutated {
		t.Fatal("same-row-count UPDATE was not detected by content fingerprint")
	}
}

// TestShouldReplayConstraint pins the PG-18 NOT NULL skip: catalogued
// not-null constraints (contype 'n') must never be replayed — the CREATE
// TABLE already carries column-level NOT NULL, and the ADD CONSTRAINT form
// is PG-18-only syntax that an older external-RDS target rejects
// ("syntax error at or near \"NOT\"", observed in a real cnpg→ext reshard).
func TestShouldReplayConstraint(t *testing.T) {
	replayed := map[string]bool{
		"p": true, "u": true, "f": true, "c": true, "x": true,
		"n": false,
	}
	for contype, want := range replayed {
		if got := shouldReplayConstraint(contype); got != want {
			t.Errorf("shouldReplayConstraint(%q) = %t, want %t", contype, got, want)
		}
	}
}

// logRecorder collects the operator-facing log lines the copy helpers emit.
type logRecorder struct{ lines []string }

func (l *logRecorder) log(level, msg string) { l.lines = append(l.lines, level+": "+msg) }

func (l *logRecorder) count(substr string) int {
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

func fakeTables(n int) []string {
	tables := make([]string, n)
	for i := range tables {
		tables[i] = fmt.Sprintf("ducklake_t%05d", i)
	}
	return tables
}

// TestVerifyCopiedRowCountsAnnouncesAndEmitsProgress pins the op-log
// liveness contract of the copy-verify loop: it announces itself with the
// table total BEFORE the first count, emits a periodic progress line every
// verifyProgressEvery tables (never per-table — a 20k-table catalog must
// produce ~8 lines, not 20k), and closes with the completion line.
func TestVerifyCopiedRowCountsAnnouncesAndEmitsProgress(t *testing.T) {
	tables := fakeTables(2*verifyProgressEvery + 1000) // 6000: progress at 2500 and 5000
	rec := &logRecorder{}
	one := func(string) (int64, error) { return 1, nil }

	counts, err := verifyCopiedRowCounts(tables, one, one, rec.log)
	if err != nil {
		t.Fatalf("verifyCopiedRowCounts: %v", err)
	}
	if len(counts) != len(tables) {
		t.Fatalf("returned %d counts, want %d", len(counts), len(tables))
	}
	if len(rec.lines) == 0 || !strings.Contains(rec.lines[0], "verifying row counts across 6000 tables") {
		t.Fatalf("first line must be the announce with the table total, got %v", rec.lines)
	}
	if rec.count("verified 2500/6000 tables…") != 1 || rec.count("verified 5000/6000 tables…") != 1 {
		t.Fatalf("periodic progress lines missing: %v", rec.lines)
	}
	if !strings.Contains(rec.lines[len(rec.lines)-1], "verified 6000 tables: target row counts match the source snapshot") {
		t.Fatalf("last line must be the completion line, got %v", rec.lines)
	}
	// Log volume is bounded: announce + 2 progress + completion. Never per-table.
	if len(rec.lines) != 4 {
		t.Fatalf("emitted %d lines, want exactly 4 (announce, 2 progress, completion): %v", len(rec.lines), rec.lines)
	}
}

// TestVerifyCopiedRowCountsSmallCatalogNoProgress pins the no-spam rule: a
// small catalog gets the announce and the completion line only.
func TestVerifyCopiedRowCountsSmallCatalogNoProgress(t *testing.T) {
	rec := &logRecorder{}
	one := func(string) (int64, error) { return 1, nil }
	if _, err := verifyCopiedRowCounts(fakeTables(3), one, one, rec.log); err != nil {
		t.Fatalf("verifyCopiedRowCounts: %v", err)
	}
	if len(rec.lines) != 2 {
		t.Fatalf("emitted %d lines, want exactly 2 (announce + completion): %v", len(rec.lines), rec.lines)
	}
}

// TestVerifyCopiedRowCountsMismatch pins the mismatch error text (asserted by
// operators reading the op log).
func TestVerifyCopiedRowCountsMismatch(t *testing.T) {
	rec := &logRecorder{}
	src := func(string) (int64, error) { return 2, nil }
	dst := func(string) (int64, error) { return 1, nil }
	_, err := verifyCopiedRowCounts([]string{"ducklake_metadata"}, src, dst, rec.log)
	if err == nil || !strings.Contains(err.Error(), "row count mismatch on ducklake_metadata: source 2, target 1") {
		t.Fatalf("err = %v, want the row-count-mismatch error", err)
	}
}

// TestApplyConstraintsAndIndexesAnnounces pins that the constraint/index
// replay ANNOUNCES itself before the first table (it used to log only on
// completion — a multi-ten-second silent gap on a big catalog) and still logs
// the completion line; an apply error propagates without the completion line.
func TestApplyConstraintsAndIndexesAnnounces(t *testing.T) {
	rec := &logRecorder{}
	var applied []string
	err := applyConstraintsAndIndexes([]string{"ducklake_metadata", "ducklake_snapshot"}, func(table string) error {
		if len(rec.lines) == 0 {
			t.Fatal("apply ran before the phase announced itself")
		}
		applied = append(applied, table)
		return nil
	}, rec.log)
	if err != nil {
		t.Fatalf("applyConstraintsAndIndexes: %v", err)
	}
	if len(applied) != 2 {
		t.Fatalf("applied %v, want both tables", applied)
	}
	if !strings.Contains(rec.lines[0], "applying constraints and indexes on the target (2 tables)") {
		t.Fatalf("announce missing/wrong: %v", rec.lines)
	}
	if rec.count("constraints and indexes applied on target") != 1 {
		t.Fatalf("completion line missing: %v", rec.lines)
	}

	failRec := &logRecorder{}
	failErr := fmt.Errorf("boom")
	if err := applyConstraintsAndIndexes([]string{"ducklake_metadata"}, func(string) error { return failErr }, failRec.log); err != failErr {
		t.Fatalf("err = %v, want the apply error", err)
	}
	if failRec.count("constraints and indexes applied on target") != 0 {
		t.Fatalf("completion line logged despite failure: %v", failRec.lines)
	}
}

// statementRecorder fakes the target-side exec of replayIndexes, recording
// every statement and optionally failing specific create attempts.
type statementRecorder struct {
	statements []string
	// failCreates maps a CREATE statement to a queue of errors returned on
	// successive attempts (nil entry = success).
	failCreates map[string][]error
}

func (r *statementRecorder) exec(sql string) error {
	r.statements = append(r.statements, sql)
	if q, ok := r.failCreates[sql]; ok && len(q) > 0 {
		err := q[0]
		r.failCreates[sql] = q[1:]
		return err
	}
	return nil
}

func (r *statementRecorder) count(stmt string) int {
	n := 0
	for _, s := range r.statements {
		if s == stmt {
			n++
		}
	}
	return n
}

var errDup = fmt.Errorf("ERROR: relation already exists (fake 42P07)")

func isFakeDup(err error) bool { return err == errDup }

// TestReplayIndexesDropsByNameBeforeEachCreate pins the reused-target
// idempotency contract (the 2026-07 mw-dev cnpg→ext reshard failure: a stale /
// concurrently recreated idx_ducklake_column_tbl_snap on the shared external
// target 42P07'd the plain CREATE INDEX): every replayed index is dropped BY
// NAME (schema-qualified) immediately before its CREATE, and each definition
// executes exactly once on the happy path.
func TestReplayIndexesDropsByNameBeforeEachCreate(t *testing.T) {
	indexes := []indexReplay{
		{name: "idx_ducklake_column_tbl_snap", def: "CREATE INDEX idx_ducklake_column_tbl_snap ON public.ducklake_column USING btree (table_id)"},
		{name: `weird"name`, def: `CREATE INDEX "weird""name" ON public.ducklake_tag USING btree (object_id)`},
	}
	rec := &statementRecorder{}
	if err := replayIndexes("ducklake_column", indexes, rec.exec, isFakeDup); err != nil {
		t.Fatalf("replayIndexes: %v", err)
	}
	want := []string{
		`DROP INDEX IF EXISTS public."idx_ducklake_column_tbl_snap"`,
		indexes[0].def,
		`DROP INDEX IF EXISTS public."weird""name"`,
		indexes[1].def,
	}
	if len(rec.statements) != len(want) {
		t.Fatalf("statements = %v, want %v", rec.statements, want)
	}
	for i := range want {
		if rec.statements[i] != want[i] {
			t.Fatalf("statement[%d] = %q, want %q", i, rec.statements[i], want[i])
		}
	}
}

// TestReplayIndexesRetriesConcurrentDuplicate pins the race recovery: a
// CREATE that collides (a live worker's CREATE INDEX IF NOT EXISTS landed
// between our drop and create) is re-dropped and retried, and succeeds.
func TestReplayIndexesRetriesConcurrentDuplicate(t *testing.T) {
	def := "CREATE INDEX idx_ducklake_column_tbl_snap ON public.ducklake_column USING btree (table_id)"
	rec := &statementRecorder{failCreates: map[string][]error{def: {errDup, nil}}}
	err := replayIndexes("ducklake_column", []indexReplay{{name: "idx_ducklake_column_tbl_snap", def: def}}, rec.exec, isFakeDup)
	if err != nil {
		t.Fatalf("replayIndexes: %v", err)
	}
	drop := `DROP INDEX IF EXISTS public."idx_ducklake_column_tbl_snap"`
	if rec.count(drop) != 2 || rec.count(def) != 2 {
		t.Fatalf("want drop+create retried once (2 each), got drops=%d creates=%d: %v", rec.count(drop), rec.count(def), rec.statements)
	}
	// Ordering: drop always precedes its create.
	if rec.statements[0] != drop || rec.statements[1] != def || rec.statements[2] != drop || rec.statements[3] != def {
		t.Fatalf("wrong ordering: %v", rec.statements)
	}
}

// TestReplayIndexesGivesUpAfterBoundedAttempts pins that a persistent 42P07
// does not loop forever: after maxIndexReplayAttempts drop+create rounds the
// replay fails with the offending definition in the error.
func TestReplayIndexesGivesUpAfterBoundedAttempts(t *testing.T) {
	def := "CREATE INDEX idx_x ON public.ducklake_column USING btree (table_id)"
	fails := make([]error, maxIndexReplayAttempts)
	for i := range fails {
		fails[i] = errDup
	}
	rec := &statementRecorder{failCreates: map[string][]error{def: fails}}
	err := replayIndexes("ducklake_column", []indexReplay{{name: "idx_x", def: def}}, rec.exec, isFakeDup)
	if err == nil || !strings.Contains(err.Error(), def) || !strings.Contains(err.Error(), "still colliding") {
		t.Fatalf("err = %v, want a still-colliding error naming the definition", err)
	}
	if got := rec.count(def); got != maxIndexReplayAttempts {
		t.Fatalf("create attempted %d times, want %d", got, maxIndexReplayAttempts)
	}
}

// TestReplayIndexesNonDuplicateErrorFailsFast pins that only 42P07 is
// retried — any other create error propagates immediately.
func TestReplayIndexesNonDuplicateErrorFailsFast(t *testing.T) {
	def := "CREATE INDEX idx_x ON public.ducklake_column USING btree (table_id)"
	boom := fmt.Errorf("boom")
	rec := &statementRecorder{failCreates: map[string][]error{def: {boom}}}
	err := replayIndexes("ducklake_column", []indexReplay{{name: "idx_x", def: def}}, rec.exec, isFakeDup)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want the create error", err)
	}
	if rec.count(def) != 1 {
		t.Fatalf("create attempted %d times, want exactly 1 (no retry on non-42P07)", rec.count(def))
	}
}

// TestReplayIndexesDropFailureSurfaces pins that a failing pre-drop is a hard
// error (a name we cannot free means the create is doomed anyway).
func TestReplayIndexesDropFailureSurfaces(t *testing.T) {
	drop := `DROP INDEX IF EXISTS public."idx_x"`
	rec := &statementRecorder{failCreates: map[string][]error{drop: {fmt.Errorf("cannot drop")}}}
	err := replayIndexes("ducklake_column", []indexReplay{{name: "idx_x", def: "CREATE INDEX idx_x ON public.t (c)"}}, rec.exec, isFakeDup)
	if err == nil || !strings.Contains(err.Error(), "drop stale index idx_x") {
		t.Fatalf("err = %v, want the drop error", err)
	}
	if len(rec.statements) != 1 {
		t.Fatalf("statements = %v, want only the failed drop", rec.statements)
	}
}

// TestIsDuplicateRelationError pins the SQLSTATE classification driving the
// retry: 42P07 (possibly wrapped) is a duplicate, everything else is not.
func TestIsDuplicateRelationError(t *testing.T) {
	dup := &pgconn.PgError{Code: "42P07", Message: `relation "idx_ducklake_column_tbl_snap" already exists`}
	if !isDuplicateRelationError(dup) {
		t.Error("bare 42P07 PgError must classify as duplicate")
	}
	if !isDuplicateRelationError(fmt.Errorf("apply index: %w", dup)) {
		t.Error("wrapped 42P07 PgError must classify as duplicate")
	}
	if isDuplicateRelationError(&pgconn.PgError{Code: "42P01"}) {
		t.Error("a different SQLSTATE must not classify as duplicate")
	}
	if isDuplicateRelationError(fmt.Errorf("plain error")) {
		t.Error("a non-PgError must not classify as duplicate")
	}
}

// TestConstraintCreatesIndex pins which constraint types get the stale-index
// pre-drop: exactly those whose ADD CONSTRAINT creates a backing index named
// after the constraint.
func TestConstraintCreatesIndex(t *testing.T) {
	creates := map[string]bool{
		"p": true, "u": true, "x": true,
		"f": false, "c": false, "n": false,
	}
	for contype, want := range creates {
		if got := constraintCreatesIndex(contype); got != want {
			t.Errorf("constraintCreatesIndex(%q) = %t, want %t", contype, got, want)
		}
	}
}

// TestMaybeLogProgressSkipsFinalItem pins that the final item never emits a
// progress line even when it lands exactly on the cadence — the caller's
// completion line covers it.
func TestMaybeLogProgressSkipsFinalItem(t *testing.T) {
	rec := &logRecorder{}
	total := 2 * verifyProgressEvery
	for i := 1; i <= total; i++ {
		maybeLogProgress(rec.log, "counted", i, total)
	}
	if len(rec.lines) != 1 || !strings.Contains(rec.lines[0], fmt.Sprintf("counted %d/%d tables…", verifyProgressEvery, total)) {
		t.Fatalf("lines = %v, want exactly one mid-loop progress line (the final on-cadence item is skipped)", rec.lines)
	}
}
