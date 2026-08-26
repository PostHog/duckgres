package integration

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Portal suspension regression (Hex 1024-row truncation): clients that page
// result sets by sending Execute with a nonzero row limit (JDBC
// setFetchSize, Hex) must receive PortalSuspended at each page boundary and
// be able to fetch the full result set with follow-up Executes. Pre-fix,
// duckgres answered the first page with CommandComplete, silently truncating
// every result set to the client's page size.
//
// psql/libpq clients never send a nonzero Execute row limit, so this can't
// be exercised through the usual helpers — the test drives the raw
// extended-query protocol via pgconn's frontend.
func TestPortalSuspensionPaging(t *testing.T) {
	ctx := context.Background()

	connStr := fmt.Sprintf("host=127.0.0.1 port=%d user=testuser password=testpass dbname=test sslmode=require", testHarness.dgPort)
	cfg, err := pgconn.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	const (
		totalRows = 1000
		pageSize  = 128
	)

	f := conn.Frontend()
	f.Send(&pgproto3.Parse{Query: fmt.Sprintf("SELECT * FROM range(%d)", totalRows)})
	f.Send(&pgproto3.Bind{})
	f.Send(&pgproto3.Describe{ObjectType: 'P'})
	f.Send(&pgproto3.Execute{MaxRows: pageSize})
	f.Send(&pgproto3.Sync{})
	if err := f.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	dataRows := 0
	suspensions := 0
	commandTag := ""
	awaitingPage := true
	for awaitingPage {
		msg, err := f.Receive()
		if err != nil {
			t.Fatalf("receive: %v", err)
		}
		switch m := msg.(type) {
		case *pgproto3.DataRow:
			dataRows++
		case *pgproto3.PortalSuspended:
			suspensions++
			if suspensions > totalRows/pageSize+1 {
				t.Fatalf("runaway suspension loop: %d suspensions for %d rows", suspensions, dataRows)
			}
			// Fetch the next page on the same (unnamed) portal.
			f.Send(&pgproto3.Execute{MaxRows: pageSize})
			f.Send(&pgproto3.Sync{})
			if err := f.Flush(); err != nil {
				t.Fatalf("flush next page: %v", err)
			}
		case *pgproto3.CommandComplete:
			commandTag = string(m.CommandTag)
		case *pgproto3.ErrorResponse:
			t.Fatalf("server error: %s %s", m.Code, m.Message)
		case *pgproto3.ReadyForQuery:
			// One ReadyForQuery arrives per Sync; the stream is done only
			// after the leg that carried CommandComplete.
			if commandTag != "" {
				awaitingPage = false
			}
		}
	}

	if dataRows != totalRows {
		t.Errorf("paged fetch returned %d rows, want %d (silent truncation)", dataRows, totalRows)
	}
	// 1000/128 = 7 full pages + a partial page; PostgreSQL semantics also
	// allow a trailing empty page when the total is an exact multiple.
	if suspensions < totalRows/pageSize {
		t.Errorf("expected at least %d PortalSuspended messages, got %d", totalRows/pageSize, suspensions)
	}
	if want := fmt.Sprintf("SELECT %d", totalRows); commandTag != want {
		t.Errorf("CommandComplete tag = %q, want %q (cumulative row count)", commandTag, want)
	}
}
