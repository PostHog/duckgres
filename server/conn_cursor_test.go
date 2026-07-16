package server

import "testing"

// closeCursorsAtTxEnd must release open cursors BEFORE a COMMIT/ROLLBACK
// executes: an open cursor rowset holds the session's only DuckDB connection
// (openBaseDB caps the pool at 1), so the transaction-end statement would
// otherwise block forever on the connection the cursor holds. The end-to-end
// regression for the deadlock is tests/integration/cursor_test.go
// (cursorBackwardFlow rolls back with a partially-read cursor open).
func TestCloseCursorsAtTxEnd(t *testing.T) {
	for _, tc := range []struct {
		command string
		closed  bool
	}{
		{"COMMIT", true},
		{"ROLLBACK", true},
		{"ROLLBACK TO savepoint_name", false},
		{"BEGIN", false},
		{"SELECT", false},
		{"FETCH", false},
		{"INSERT", false},
	} {
		released := false
		c := &clientConn{cursors: map[string]*cursorState{
			"cur_pending": {query: "SELECT 1"},
			"cur_open":    {query: "SELECT 2", cleanup: func() { released = true }},
		}}

		c.closeCursorsAtTxEnd(transactionControlForQuery(tc.command))

		if gotClosed := len(c.cursors) == 0; gotClosed != tc.closed {
			t.Errorf("closeCursorsAtTxEnd(%q): cursors closed = %v, want %v", tc.command, gotClosed, tc.closed)
		}
		if released != tc.closed {
			t.Errorf("closeCursorsAtTxEnd(%q): cursor query context released = %v, want %v", tc.command, released, tc.closed)
		}
	}
}
