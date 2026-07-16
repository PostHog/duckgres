package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type extendedBindResult struct {
	fields []pgproto3.FieldDescription
	rows   int
	err    error
}

// execBindWithoutDescribe sends the extended-protocol sequence used to test
// Bind directly. DML RETURNING has intentionally never supported Describe in
// Duckgres because probing its schema used to require executing a mutation.
// Keeping Describe out of this sequence exercises the legal Parse/Bind/Execute
// flow while ensuring the result-format vector is handled by Bind itself.
func execBindWithoutDescribe(t testing.TB, conn *pgx.Conn, query string, values [][]byte, formats []int16) extendedBindResult {
	t.Helper()

	frontend := conn.PgConn().Frontend()
	frontend.SendParse(&pgproto3.Parse{Query: query})
	frontend.SendBind(&pgproto3.Bind{
		Parameters:        values,
		ResultFormatCodes: formats,
	})
	frontend.SendExecute(&pgproto3.Execute{})
	frontend.SendSync(&pgproto3.Sync{})
	if err := frontend.Flush(); err != nil {
		return extendedBindResult{err: fmt.Errorf("flush extended Bind: %w", err)}
	}

	var result extendedBindResult
	for {
		message, err := conn.PgConn().ReceiveMessage(context.Background())
		if err != nil {
			result.err = err
			return result
		}
		switch message := message.(type) {
		case *pgproto3.RowDescription:
			result.fields = message.Fields
		case *pgproto3.DataRow:
			result.rows++
		case *pgproto3.ErrorResponse:
			result.err = pgconn.ErrorResponseToPgError(message)
		case *pgproto3.ReadyForQuery:
			return result
		}
	}
}

func mustExecOnPgConn(t testing.TB, conn *pgx.Conn, query string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func requireProtocolViolation(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected Bind result-format mismatch to fail")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("Bind mismatch error = %T %v, want pgconn.PgError", err, err)
	}
	if pgErr.Code != "08P01" {
		t.Fatalf("Bind mismatch SQLSTATE = %q, want 08P01", pgErr.Code)
	}
}

func requireTwoColumnResultFormats(t testing.TB, result extendedBindResult, want []int16) {
	t.Helper()
	if result.err != nil {
		t.Fatalf("extended query failed: %v", result.err)
	}
	if result.rows != 1 || len(result.fields) != len(want) {
		t.Fatalf("result rows/fields = %d/%d, want 1/%d", result.rows, len(result.fields), len(want))
	}
	for i, format := range want {
		if got := result.fields[i].Format; got != format {
			t.Fatalf("result format %d = %d, want %d", i, got, format)
		}
	}
}

// TestBindPerColumnResultFormatsForMutationOutputs exercises the wire-level
// Bind vector shape directly. In particular, the server must know the actual
// output arity before it begins a write, rather than discovering it after a
// RETURNING mutation or writable-CTE setup has already run.
func TestBindPerColumnResultFormatsForMutationOutputs(t *testing.T) {
	ctx := context.Background()
	conn := pgxConnect(t)
	defer func() { _ = conn.Close(ctx) }()

	t.Run("RETURNING star", func(t *testing.T) {
		mustExecOnPgConn(t, conn, "CREATE TEMP TABLE bind_result_formats_returning (id INTEGER, name TEXT)")
		t.Cleanup(func() { mustExecOnPgConn(t, conn, "DROP TABLE bind_result_formats_returning") })

		query := "INSERT INTO bind_result_formats_returning (id, name) VALUES ($1, $2) RETURNING *"
		for _, test := range []struct {
			name    string
			formats []int16
			want    []int16
		}{
			{name: "zero format codes", want: []int16{0, 0}},
			{name: "one binary format code", formats: []int16{1}, want: []int16{1, 1}},
			{name: "one code per output column", formats: []int16{1, 0}, want: []int16{1, 0}},
		} {
			t.Run(test.name, func(t *testing.T) {
				result := execBindWithoutDescribe(t, conn, query, [][]byte{[]byte(fmt.Sprintf("%d", len(test.name))), []byte(test.name)}, test.formats)
				requireTwoColumnResultFormats(t, result, test.want)
			})
		}

		mismatch := execBindWithoutDescribe(t, conn, query, [][]byte{[]byte("4"), []byte("mismatch")}, []int16{0, 1, 0})
		requireProtocolViolation(t, mismatch.err)

		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM bind_result_formats_returning").Scan(&count); err != nil {
			t.Fatalf("count after mismatched Bind: %v", err)
		}
		if count != 3 {
			t.Fatalf("RETURNING mutations = %d, want 3", count)
		}
	})

	t.Run("explicit RETURNING list", func(t *testing.T) {
		mustExecOnPgConn(t, conn, "CREATE TEMP TABLE bind_result_formats_explicit (id INTEGER, name TEXT, ignored TEXT)")
		t.Cleanup(func() { mustExecOnPgConn(t, conn, "DROP TABLE bind_result_formats_explicit") })

		query := "INSERT INTO bind_result_formats_explicit (id, name, ignored) VALUES ($1, $2, $3) RETURNING id, name"
		for _, test := range []struct {
			name    string
			formats []int16
			want    []int16
		}{
			{name: "zero format codes", want: []int16{0, 0}},
			{name: "one binary format code", formats: []int16{1}, want: []int16{1, 1}},
			{name: "one code per output column", formats: []int16{1, 0}, want: []int16{1, 0}},
		} {
			t.Run(test.name, func(t *testing.T) {
				result := execBindWithoutDescribe(t, conn, query, [][]byte{[]byte(fmt.Sprintf("%d", len(test.name))), []byte(test.name), []byte("ignored")}, test.formats)
				requireTwoColumnResultFormats(t, result, test.want)
			})
		}

		mismatch := execBindWithoutDescribe(t, conn, query, [][]byte{[]byte("4"), []byte("mismatch"), []byte("ignored")}, []int16{0, 1, 0})
		requireProtocolViolation(t, mismatch.err)

		var count int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM bind_result_formats_explicit").Scan(&count); err != nil {
			t.Fatalf("count after mismatched Bind: %v", err)
		}
		if count != 3 {
			t.Fatalf("explicit RETURNING mutations = %d, want 3", count)
		}
	})

	t.Run("writable CTE", func(t *testing.T) {
		mustExecOnPgConn(t, conn, "CREATE TEMP TABLE bind_result_formats_cte (id INTEGER, name TEXT, value TEXT)")
		t.Cleanup(func() { mustExecOnPgConn(t, conn, "DROP TABLE bind_result_formats_cte") })
		mustExecOnPgConn(t, conn, "INSERT INTO bind_result_formats_cte VALUES (1, 'first', 'old')")

		query := "WITH changed AS (UPDATE bind_result_formats_cte SET value = value || 'x' WHERE id = 1 RETURNING id, name) SELECT id, name FROM changed"
		for _, test := range []struct {
			name    string
			formats []int16
			want    []int16
		}{
			{name: "zero format codes", want: []int16{0, 0}},
			{name: "one binary format code", formats: []int16{1}, want: []int16{1, 1}},
			{name: "one code per output column", formats: []int16{1, 0}, want: []int16{1, 0}},
		} {
			t.Run(test.name, func(t *testing.T) {
				result := execBindWithoutDescribe(t, conn, query, nil, test.formats)
				requireTwoColumnResultFormats(t, result, test.want)
			})
		}

		var value string
		if err := conn.QueryRow(ctx, "SELECT value FROM bind_result_formats_cte WHERE id = 1").Scan(&value); err != nil {
			t.Fatalf("read writable CTE target: %v", err)
		}
		if value != "oldxxx" {
			t.Fatalf("value after successful writable CTE = %q, want oldxxx", value)
		}

		mismatch := execBindWithoutDescribe(t, conn, query, nil, []int16{0, 1, 0})
		requireProtocolViolation(t, mismatch.err)

		if err := conn.QueryRow(ctx, "SELECT value FROM bind_result_formats_cte WHERE id = 1").Scan(&value); err != nil {
			t.Fatalf("read writable CTE target after mismatched Bind: %v", err)
		}
		if value != "oldxxx" {
			t.Fatalf("mismatched writable CTE changed value to %q, want oldxxx", value)
		}
	})
}
