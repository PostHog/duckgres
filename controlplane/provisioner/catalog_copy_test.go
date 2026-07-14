//go:build kubernetes

package provisioner

import (
	"fmt"
	"strings"
	"testing"
)

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
