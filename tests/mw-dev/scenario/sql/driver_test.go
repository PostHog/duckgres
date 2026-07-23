package sql

import (
	"errors"
	"fmt"
	"testing"
)

func TestConsumeQueryRowsDrainsEveryResultSet(t *testing.T) {
	rows := &fakeQueryRows{rowsPerResultSet: []int{1, 2}}

	count, err := consumeQueryRows(rows)
	if err != nil {
		t.Fatalf("consumeQueryRows returned error: %v", err)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3", count)
	}
	if !rows.closed {
		t.Fatal("expected rows to be closed")
	}
}

func TestConsumeQueryRowsReturnsErrorAfterLastResultSet(t *testing.T) {
	wantErr := errors.New("later statement failed")
	rows := &fakeQueryRows{
		rowsPerResultSet: []int{1},
		terminalErr:      wantErr,
	}

	_, err := consumeQueryRows(rows)
	if !errors.Is(err, wantErr) {
		t.Fatalf("consumeQueryRows error = %v, want %v", err, wantErr)
	}
	if !rows.closed {
		t.Fatal("expected rows to be closed")
	}
}

type fakeQueryRows struct {
	rowsPerResultSet []int
	resultSet        int
	row              int
	exhausted        bool
	terminalErr      error
	closed           bool
}

func (r *fakeQueryRows) Columns() ([]string, error) {
	return []string{"value"}, nil
}

func (r *fakeQueryRows) Next() bool {
	if r.row >= r.rowsPerResultSet[r.resultSet] {
		return false
	}
	r.row++
	return true
}

func (r *fakeQueryRows) Scan(dest ...any) error {
	if len(dest) != 1 {
		return fmt.Errorf("scan destinations = %d, want 1", len(dest))
	}
	value, ok := dest[0].(*any)
	if !ok {
		return fmt.Errorf("scan destination has type %T, want *any", dest[0])
	}
	*value = r.row
	return nil
}

func (r *fakeQueryRows) Err() error {
	if r.exhausted {
		return r.terminalErr
	}
	return nil
}

func (r *fakeQueryRows) NextResultSet() bool {
	if r.resultSet+1 < len(r.rowsPerResultSet) {
		r.resultSet++
		r.row = 0
		return true
	}
	r.exhausted = true
	return false
}

func (r *fakeQueryRows) Close() error {
	r.closed = true
	return nil
}
