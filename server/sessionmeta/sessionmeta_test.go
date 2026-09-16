package sessionmeta

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/posthog/duckgres/server/sqlcore"
)

type countingExecutor struct {
	execCalls   int
	execQueries []string
	queryCalls  int
	queryRows   sqlcore.RowSet
}

func (e *countingExecutor) QueryContext(_ context.Context, _ string, _ ...any) (sqlcore.RowSet, error) {
	e.queryCalls++
	if e.queryRows == nil {
		return nil, errors.New("no query rows configured")
	}
	return e.queryRows, nil
}

func (e *countingExecutor) ExecContext(_ context.Context, query string, _ ...any) (sqlcore.ExecResult, error) {
	e.execCalls++
	e.execQueries = append(e.execQueries, query)
	return nil, nil
}

func (e *countingExecutor) Query(string, ...any) (sqlcore.RowSet, error) {
	return nil, errors.New("not implemented")
}

func (e *countingExecutor) Exec(string, ...any) (sqlcore.ExecResult, error) {
	return nil, errors.New("not implemented")
}

func (e *countingExecutor) ConnContext(context.Context) (sqlcore.RawConn, error) {
	return nil, errors.New("not implemented")
}

func (e *countingExecutor) PingContext(context.Context) error { return nil }
func (e *countingExecutor) Close() error                      { return nil }
func (e *countingExecutor) LastProfilingOutput() string       { return "" }

type singleIntRow struct {
	v        int
	consumed bool
}

func (r *singleIntRow) Columns() ([]string, error) { return []string{"count"}, nil }
func (r *singleIntRow) ColumnTypes() ([]sqlcore.ColumnTyper, error) {
	return nil, errors.New("not used")
}
func (r *singleIntRow) Next() bool {
	if r.consumed {
		return false
	}
	r.consumed = true
	return true
}
func (r *singleIntRow) Scan(dest ...any) error {
	if len(dest) != 1 {
		return errors.New("expected 1 dest")
	}
	ptr, ok := dest[0].(*any)
	if !ok {
		return errors.New("expected *any dest")
	}
	*ptr = int64(r.v)
	return nil
}
func (r *singleIntRow) Close() error                 { return nil }
func (r *singleIntRow) Err() error                   { return nil }
func (r *singleIntRow) RowsAffected() (int64, error) { return 0, nil }
func (r *singleIntRow) LastInsertId() (int64, error) { return 0, nil }
func (r *singleIntRow) LastProfilingOutput() string  { return "" }

func TestInitSessionDatabaseMetadataBatchesPerSessionStatements(t *testing.T) {
	exec := &countingExecutor{
		queryRows: &singleIntRow{v: 1}, // pretend ducklake is attached for HasAttachedCatalog
	}

	if err := InitSessionDatabaseMetadata(context.Background(), exec, "analytics"); err != nil {
		t.Fatalf("InitSessionDatabaseMetadata: %v", err)
	}

	// Per-conn statements that intrinsically run as separate calls:
	//   1. CREATE OR REPLACE TEMP MACRO current_database()
	//   2. USE memory
	//   3. USE ducklake               (deferred)
	//   4. SET search_path            (deferred)
	// Plus one batched ExecContext for the metadata views (was 6 calls).
	const expectedExecCalls = 5
	if exec.execCalls != expectedExecCalls {
		t.Errorf("ExecContext call count = %d, want %d.\nQueries:\n%s",
			exec.execCalls, expectedExecCalls,
			strings.Join(exec.execQueries, "\n---\n"))
	}
}

func TestBuildSessionMetadataSQLContainsAllExpectedStatements(t *testing.T) {
	got := buildSessionMetadataSQL("analytics")

	wants := []string{
		"CREATE TABLE IF NOT EXISTS main.__duckgres_column_metadata",
		"CREATE OR REPLACE VIEW main.pg_database",
		"CREATE OR REPLACE VIEW main.information_schema_columns_compat",
		"CREATE OR REPLACE VIEW main.information_schema_tables_compat",
		"CREATE OR REPLACE VIEW main.information_schema_schemata_compat",
		"CREATE OR REPLACE VIEW main.information_schema_views_compat",
	}
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("buildSessionMetadataSQL missing %q", w)
		}
	}

	// Database literal should appear (used in pg_database view).
	if !strings.Contains(got, "'analytics'") {
		t.Errorf("buildSessionMetadataSQL did not interpolate database literal")
	}
}

// TestInitSessionDatabaseMetadataWithAttachedSkipsProbe pins the duplicate-probe
// fix. Every session-init caller runs HasAttachedCatalog before calling in here,
// and this function used to run it a second time — two `duckdb_databases()`
// queries per session create. With DuckLake as the session default each one
// starts a DuckLake transaction, and a transaction on a catalog whose schema
// version moved pays a full catalog reload. The executor below has no queryRows
// configured, so any probe would both bump queryCalls and fail the call.
func TestInitSessionDatabaseMetadataWithAttachedSkipsProbe(t *testing.T) {
	for _, tc := range []struct {
		name     string
		attached bool
		// The deferred restore runs only when DuckLake is attached. Assert on
		// the statements themselves: a bare count would still pass if the
		// restore ran against the wrong catalog.
		wantRestore bool
	}{
		{name: "attached", attached: true, wantRestore: true},
		{name: "not attached", attached: false, wantRestore: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := &countingExecutor{}

			if err := InitSessionDatabaseMetadataWithAttached(
				context.Background(), exec, "analytics", nil, tc.attached,
			); err != nil {
				t.Fatalf("InitSessionDatabaseMetadataWithAttached: %v", err)
			}

			if exec.queryCalls != 0 {
				t.Errorf("issued %d probe queries, want 0 — the caller's answer must be reused", exec.queryCalls)
			}

			joined := strings.Join(exec.execQueries, "\n---\n")
			gotRestore := false
			gotSearchPath := false
			for _, q := range exec.execQueries {
				if strings.EqualFold(strings.TrimSpace(q), "USE ducklake") {
					gotRestore = true
				}
				if strings.HasPrefix(strings.TrimSpace(q), "SET search_path") {
					gotSearchPath = true
				}
			}
			if gotRestore != tc.wantRestore {
				t.Errorf("`USE ducklake` issued = %v, want %v.\nQueries:\n%s", gotRestore, tc.wantRestore, joined)
			}
			if gotSearchPath != tc.wantRestore {
				t.Errorf("`SET search_path` issued = %v, want %v.\nQueries:\n%s", gotSearchPath, tc.wantRestore, joined)
			}
			// The session must always leave the memory catalog and install the
			// metadata surfaces, attached or not.
			if !strings.Contains(joined, "USE memory") {
				t.Errorf("did not switch to the memory catalog.\nQueries:\n%s", joined)
			}
			if !strings.Contains(joined, "current_database()") {
				t.Errorf("did not create the current_database() macro.\nQueries:\n%s", joined)
			}
		})
	}
}

// TestInitSessionDatabaseMetadataProbesWhenAttachmentUnknown keeps the nil case
// probing, so the exported wrappers and any caller that does not already hold
// the answer behave exactly as before.
func TestInitSessionDatabaseMetadataProbesWhenAttachmentUnknown(t *testing.T) {
	exec := &countingExecutor{queryRows: &singleIntRow{v: 1}}

	// The probing entry point must still issue exactly one probe.
	if err := InitSessionDatabaseMetadataWithAccess(
		context.Background(), exec, "analytics", nil,
	); err != nil {
		t.Fatalf("InitSessionDatabaseMetadataWithAccess: %v", err)
	}

	if exec.queryCalls != 1 {
		t.Errorf("issued %d probe queries, want 1", exec.queryCalls)
	}
}
