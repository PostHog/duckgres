package integration

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Cursor happy-path coverage (docs/postgres-compatibility.md §6 Wire Protocol).
//
// Server-side cursors are emulated in server/conn_cursor.go with two distinct
// entry points:
//   - Simple query protocol (lib/pq sends parameter-less statements this way):
//     handleDeclareCursor / handleFetchCursor / handleCloseCursor.
//   - Extended query protocol (pgx.Conn.Query always goes through
//     Parse/Bind/Describe/Execute): the cursorOp* detection in
//     conn_extended_query.go plus handle*CursorExtended.
//
// The shared flows also run against the comparison PostgreSQL 16 instance in
// TestCursorPostgresParity so the expected row sets and error shapes stay
// honest. Flows run inside a transaction block because real PostgreSQL
// requires one for cursor use (Duckgres accepts cursors outside transactions
// too, which the extended-protocol tests exercise).

// cursorRow is one (id, name) row from the users fixture.
type cursorRow struct {
	id   int
	name string
}

// cursorUsers returns the users fixture rows (fixtures/data.sql) with
// firstID <= id <= lastID, ordered by id.
func cursorUsers(firstID, lastID int) []cursorRow {
	all := []cursorRow{
		{1, "Alice"}, {2, "Bob"}, {3, "Charlie"}, {4, "Diana"}, {5, "Eve"},
		{6, "Frank"}, {7, "Grace"}, {8, "Henry"}, {9, "Ivy"}, {10, "Jack"},
	}
	var out []cursorRow
	for _, u := range all {
		if u.id >= firstID && u.id <= lastID {
			out = append(out, u)
		}
	}
	return out
}

func assertCursorRows(t *testing.T, label string, got, want []cursorRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d rows %v, want %d rows %v", label, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: row %d = %+v, want %+v", label, i, got[i], want[i])
		}
	}
}

// fetchCursorRows runs a FETCH/MOVE statement on the transaction's connection
// (lib/pq sends it via the simple query protocol) and collects any rows it
// returned. Statements that return no result set yield an empty slice.
func fetchCursorRows(t *testing.T, tx *sql.Tx, stmt string) []cursorRow {
	t.Helper()
	rows, err := tx.Query(stmt)
	if err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
	defer func() { _ = rows.Close() }()
	var out []cursorRow
	for rows.Next() {
		var r cursorRow
		if err := rows.Scan(&r.id, &r.name); err != nil {
			t.Fatalf("%s: scan: %v", stmt, err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: rows: %v", stmt, err)
	}
	return out
}

func mustExecTx(t *testing.T, tx *sql.Tx, stmt string) {
	t.Helper()
	if _, err := tx.Exec(stmt); err != nil {
		t.Fatalf("%s: %v", stmt, err)
	}
}

func beginCursorTx(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

// cursorLifecycleFlow covers DECLARE → FETCH n → FETCH n (position advances) →
// FETCH ALL (drains the rest) → FETCH on exhausted cursor (zero rows) →
// CLOSE → FETCH after CLOSE (clean missing-cursor error).
func cursorLifecycleFlow(t *testing.T, db *sql.DB) {
	tx := beginCursorTx(t, db)

	mustExecTx(t, tx, "DECLARE cur_lifecycle CURSOR FOR SELECT id, name FROM users ORDER BY id")

	assertCursorRows(t, "FETCH 3", fetchCursorRows(t, tx, "FETCH 3 FROM cur_lifecycle"), cursorUsers(1, 3))
	assertCursorRows(t, "FETCH 3 (resumes)", fetchCursorRows(t, tx, "FETCH 3 FROM cur_lifecycle"), cursorUsers(4, 6))
	assertCursorRows(t, "FETCH ALL", fetchCursorRows(t, tx, "FETCH ALL FROM cur_lifecycle"), cursorUsers(7, 10))
	// An exhausted cursor returns zero rows, not an error.
	assertCursorRows(t, "FETCH 3 (exhausted)", fetchCursorRows(t, tx, "FETCH 3 FROM cur_lifecycle"), nil)

	mustExecTx(t, tx, "CLOSE cur_lifecycle")

	rows, err := tx.Query("FETCH 3 FROM cur_lifecycle")
	if err == nil {
		_ = rows.Close()
		t.Fatal("FETCH after CLOSE succeeded, want missing-cursor error")
	}
	if !strings.Contains(err.Error(), `cursor "cur_lifecycle" does not exist`) {
		t.Errorf("FETCH after CLOSE: error = %q, want it to mention the missing cursor", err)
	}
}

// cursorMoveFlow covers MOVE n advancing the position without returning rows.
func cursorMoveFlow(t *testing.T, db *sql.DB) {
	tx := beginCursorTx(t, db)

	mustExecTx(t, tx, "DECLARE cur_move CURSOR FOR SELECT id, name FROM users ORDER BY id")

	moved := fetchCursorRows(t, tx, "MOVE 4 FROM cur_move")
	if len(moved) != 0 {
		t.Errorf("MOVE 4 returned %d rows %v, want none", len(moved), moved)
	}
	assertCursorRows(t, "FETCH ALL after MOVE", fetchCursorRows(t, tx, "FETCH ALL FROM cur_move"), cursorUsers(5, 10))

	mustExecTx(t, tx, "CLOSE cur_move")
}

// cursorBackwardFlow covers the forward-only restriction: backward fetch
// fails with a clean error. NO SCROLL pins real PostgreSQL to the same
// forward-only behavior Duckgres always has (without it, PostgreSQL can
// satisfy backward fetches from materialized plans like sorts).
func cursorBackwardFlow(t *testing.T, db *sql.DB) {
	tx := beginCursorTx(t, db)

	mustExecTx(t, tx, "DECLARE cur_backward NO SCROLL CURSOR FOR SELECT id, name FROM users ORDER BY id")
	assertCursorRows(t, "FETCH 2", fetchCursorRows(t, tx, "FETCH 2 FROM cur_backward"), cursorUsers(1, 2))

	rows, err := tx.Query("FETCH BACKWARD 1 FROM cur_backward")
	if err == nil {
		_ = rows.Close()
		t.Fatal("FETCH BACKWARD succeeded, want forward-only error")
	}
	if !strings.Contains(err.Error(), "cursor can only scan forward") {
		t.Errorf("FETCH BACKWARD: error = %q, want forward-only error", err)
	}
}

// cursorMissingFlow covers FETCH/CLOSE on a cursor that was never declared.
// Each statement gets its own transaction because the error aborts the
// transaction on real PostgreSQL.
func cursorMissingFlow(t *testing.T, db *sql.DB) {
	tx := beginCursorTx(t, db)
	rows, err := tx.Query("FETCH 1 FROM cur_never_declared")
	if err == nil {
		_ = rows.Close()
		t.Fatal("FETCH from undeclared cursor succeeded, want error")
	}
	if !strings.Contains(err.Error(), `cursor "cur_never_declared" does not exist`) {
		t.Errorf("FETCH from undeclared cursor: error = %q", err)
	}
	_ = tx.Rollback()

	tx2 := beginCursorTx(t, db)
	if _, err := tx2.Exec("CLOSE cur_never_declared"); err == nil {
		t.Fatal("CLOSE of undeclared cursor succeeded, want error")
	} else if !strings.Contains(err.Error(), `cursor "cur_never_declared" does not exist`) {
		t.Errorf("CLOSE of undeclared cursor: error = %q", err)
	}
}

// TestCursorSimpleQuery exercises the simple-query-protocol cursor path
// against Duckgres.
func TestCursorSimpleQuery(t *testing.T) {
	db := testHarness.DuckgresDB

	t.Run("lifecycle_fetch_close", func(t *testing.T) { cursorLifecycleFlow(t, db) })
	t.Run("move_advances_position", func(t *testing.T) { cursorMoveFlow(t, db) })
	t.Run("backward_fetch_rejected", func(t *testing.T) { cursorBackwardFlow(t, db) })
	t.Run("missing_cursor_errors", func(t *testing.T) { cursorMissingFlow(t, db) })

	t.Run("redeclare_replaces_cursor", func(t *testing.T) {
		tx := beginCursorTx(t, db)

		mustExecTx(t, tx, "DECLARE cur_redeclare CURSOR FOR SELECT id, name FROM users ORDER BY id")
		assertCursorRows(t, "FETCH 2", fetchCursorRows(t, tx, "FETCH 2 FROM cur_redeclare"), cursorUsers(1, 2))

		// Re-DECLARE with the same name replaces the cursor: the next FETCH
		// reads the new query from the start. Deliberate divergence: real
		// PostgreSQL raises 42P03 duplicate_cursor here.
		mustExecTx(t, tx, "DECLARE cur_redeclare CURSOR FOR SELECT id, name FROM users WHERE id >= 9 ORDER BY id")
		assertCursorRows(t, "FETCH ALL after re-DECLARE", fetchCursorRows(t, tx, "FETCH ALL FROM cur_redeclare"), cursorUsers(9, 10))

		mustExecTx(t, tx, "CLOSE cur_redeclare")
	})
}

// TestCursorPostgresParity runs the shared cursor flows against the
// comparison PostgreSQL 16 instance, keeping the expectations in the flows
// differential. redeclare_replaces_cursor is deliberately absent: PostgreSQL
// raises 42P03 duplicate_cursor where Duckgres replaces the cursor.
func TestCursorPostgresParity(t *testing.T) {
	if skipPostgresCompare || testHarness.PostgresDB == nil {
		t.Skip("PostgreSQL comparison not available")
	}
	db := testHarness.PostgresDB

	t.Run("lifecycle_fetch_close", func(t *testing.T) { cursorLifecycleFlow(t, db) })
	t.Run("move_advances_position", func(t *testing.T) { cursorMoveFlow(t, db) })
	t.Run("backward_fetch_rejected", func(t *testing.T) { cursorBackwardFlow(t, db) })
	t.Run("missing_cursor_errors", func(t *testing.T) { cursorMissingFlow(t, db) })
}

// pgxCursorRows issues stmt via the extended query protocol and returns the
// rows, command tag, and error. pgx.Conn.Query always goes through
// Parse/Bind/Describe/Execute; Exec with no arguments would silently
// downgrade to the simple protocol, which is why it is not used here.
func pgxCursorRows(ctx context.Context, conn *pgx.Conn, stmt string) ([]cursorRow, pgconn.CommandTag, error) {
	rows, err := conn.Query(ctx, stmt)
	if err != nil {
		return nil, pgconn.CommandTag{}, err
	}
	defer rows.Close()
	var out []cursorRow
	for rows.Next() {
		var r cursorRow
		if err := rows.Scan(&r.id, &r.name); err != nil {
			return out, pgconn.CommandTag{}, err
		}
		out = append(out, r)
	}
	rows.Close()
	return out, rows.CommandTag(), rows.Err()
}

// TestCursorExtendedQuery exercises the extended-query-protocol cursor path
// (cursorOpDeclare/cursorOpFetch/cursorOpClose) with pgx.
func TestCursorExtendedQuery(t *testing.T) {
	ctx := context.Background()

	t.Run("declare_fetch_close", func(t *testing.T) {
		conn := pgxConnect(t)
		defer func() { _ = conn.Close(ctx) }()

		_, tag, err := pgxCursorRows(ctx, conn, "DECLARE cur_ext CURSOR FOR SELECT id, name FROM users ORDER BY id")
		if err != nil {
			t.Fatalf("DECLARE: %v", err)
		}
		if tag.String() != "DECLARE CURSOR" {
			t.Errorf("DECLARE tag = %q, want %q", tag, "DECLARE CURSOR")
		}

		got, tag, err := pgxCursorRows(ctx, conn, "FETCH 4 FROM cur_ext")
		if err != nil {
			t.Fatalf("FETCH 4: %v", err)
		}
		assertCursorRows(t, "FETCH 4", got, cursorUsers(1, 4))
		if tag.String() != "FETCH 4" {
			t.Errorf("FETCH 4 tag = %q, want %q", tag, "FETCH 4")
		}

		got, tag, err = pgxCursorRows(ctx, conn, "FETCH ALL FROM cur_ext")
		if err != nil {
			t.Fatalf("FETCH ALL: %v", err)
		}
		assertCursorRows(t, "FETCH ALL", got, cursorUsers(5, 10))
		if tag.String() != "FETCH 6" {
			t.Errorf("FETCH ALL tag = %q, want %q", tag, "FETCH 6")
		}

		_, tag, err = pgxCursorRows(ctx, conn, "CLOSE cur_ext")
		if err != nil {
			t.Fatalf("CLOSE: %v", err)
		}
		if tag.String() != "CLOSE CURSOR" {
			t.Errorf("CLOSE tag = %q, want %q", tag, "CLOSE CURSOR")
		}

		// FETCH after CLOSE: clean SQLSTATE 34000 missing-cursor error.
		_, _, err = pgxCursorRows(ctx, conn, "FETCH 4 FROM cur_ext")
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			t.Fatalf("FETCH after CLOSE: error = %v, want *pgconn.PgError", err)
		}
		if pgErr.Code != "34000" || !strings.Contains(pgErr.Message, `cursor "cur_ext" does not exist`) {
			t.Errorf("FETCH after CLOSE: code=%s message=%q, want 34000 missing-cursor", pgErr.Code, pgErr.Message)
		}
	})

	t.Run("move_advances_position", func(t *testing.T) {
		conn := pgxConnect(t)
		defer func() { _ = conn.Close(ctx) }()

		_, _, err := pgxCursorRows(ctx, conn, "DECLARE cur_ext_move CURSOR FOR SELECT id, name FROM users ORDER BY id")
		if err != nil {
			t.Fatalf("DECLARE: %v", err)
		}

		moved, tag, err := pgxCursorRows(ctx, conn, "MOVE 4 FROM cur_ext_move")
		if err != nil {
			t.Fatalf("MOVE 4: %v", err)
		}
		if len(moved) != 0 {
			t.Errorf("MOVE 4 returned %d rows %v, want none", len(moved), moved)
		}
		if tag.String() != "MOVE 4" {
			t.Errorf("MOVE 4 tag = %q, want %q", tag, "MOVE 4")
		}

		got, _, err := pgxCursorRows(ctx, conn, "FETCH ALL FROM cur_ext_move")
		if err != nil {
			t.Fatalf("FETCH ALL: %v", err)
		}
		assertCursorRows(t, "FETCH ALL after MOVE", got, cursorUsers(5, 10))

		if _, _, err := pgxCursorRows(ctx, conn, "CLOSE cur_ext_move"); err != nil {
			t.Fatalf("CLOSE: %v", err)
		}
	})

	t.Run("backward_fetch_rejected", func(t *testing.T) {
		conn := pgxConnect(t)
		defer func() { _ = conn.Close(ctx) }()

		_, _, err := pgxCursorRows(ctx, conn, "DECLARE cur_ext_back NO SCROLL CURSOR FOR SELECT id, name FROM users ORDER BY id")
		if err != nil {
			t.Fatalf("DECLARE: %v", err)
		}

		_, _, err = pgxCursorRows(ctx, conn, "FETCH BACKWARD 1 FROM cur_ext_back")
		if err == nil {
			t.Fatal("FETCH BACKWARD succeeded, want forward-only error")
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "0A000" || !strings.Contains(pgErr.Message, "cursor can only scan forward") {
			t.Errorf("FETCH BACKWARD: error = %v, want 0A000 forward-only error", err)
		}
	})
}
