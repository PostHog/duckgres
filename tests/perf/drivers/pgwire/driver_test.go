package pgwire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/posthog/duckgres/tests/perf/core"
)

type fakeExec struct {
	lastQuery string
}

func (f *fakeExec) Execute(_ context.Context, query string, _ []any) (int64, error) {
	f.lastQuery = query
	return 3, nil
}

func (f *fakeExec) Close() error { return nil }

func TestDriverUsesCanonicalRenderedSQL(t *testing.T) {
	exec := &fakeExec{}
	driver := NewWithExecutor(exec)
	_, err := driver.Execute(context.Background(), core.Query{
		QueryID:   "q1",
		IntentID:  "i1",
		PGWireSQL: "SELECT 1",
	}, nil)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if exec.lastQuery != "SELECT 1" {
		t.Fatalf("expected canonical rendered SQL, got %q", exec.lastQuery)
	}
}

func TestCacheVariantsPrepareLazilyOnPinnedConnection(t *testing.T) {
	for _, tc := range []struct {
		protocol core.Protocol
		external string
	}{
		{core.ProtocolPGWireUncached, "false"},
		{core.ProtocolPGWireCached, "true"},
	} {
		t.Run(string(tc.protocol), func(t *testing.T) {
			connector := &cacheTestConnector{setupDelay: 10 * time.Millisecond}
			db := sql.OpenDB(connector)
			// Without pinning, each operation would close its connection.
			db.SetMaxIdleConns(0)
			d, err := NewWithDBAndProtocol(db, tc.protocol)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = d.Close() })
			if d.Protocol() != tc.protocol || connector.connections != 0 {
				t.Fatalf("constructor must retain protocol without connecting: protocol=%s connections=%d", d.Protocol(), connector.connections)
			}

			started := time.Now()
			result, err := d.Execute(context.Background(), core.Query{QueryID: "warmup", PGWireSQL: "SELECT 1"}, nil)
			elapsed := time.Since(started)
			if err != nil || result.Rows != 1 {
				t.Fatalf("warmup result=%+v error=%v", result, err)
			}
			if elapsed-result.Duration < 3*connector.setupDelay {
				t.Fatalf("cache setup was included in query duration: elapsed=%s query=%s", elapsed, result.Duration)
			}
			if _, err := d.Execute(context.Background(), core.Query{QueryID: "measure", PGWireSQL: "SELECT 2"}, nil); err != nil {
				t.Fatal(err)
			}
			want := []string{
				"SET GLOBAL enable_external_file_cache = " + tc.external,
				"SET GLOBAL parquet_metadata_cache = false",
				"SET GLOBAL enable_http_metadata_cache = false",
				"SELECT 1", "SELECT 2",
			}
			if !reflect.DeepEqual(connector.statements, want) || connector.connections != 1 {
				t.Fatalf("all setup and queries must use one connection: statements=%v connections=%d", connector.statements, connector.connections)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			if connector.closes != 1 {
				t.Fatalf("Close did not release pinned connection: closes=%d", connector.closes)
			}
			if err := db.Ping(); err == nil {
				t.Fatal("Close must also close the owned DB")
			}
		})
	}
}

func TestCacheVariantFailsClosedOnSetupError(t *testing.T) {
	connector := &cacheTestConnector{failStatement: "SET GLOBAL parquet_metadata_cache = false"}
	d, err := NewWithDBAndProtocol(sql.OpenDB(connector), core.ProtocolPGWireUncached)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	for range 2 {
		_, err := d.Execute(context.Background(), core.Query{QueryID: "q", PGWireSQL: "SELECT 1"}, nil)
		if err == nil || !strings.Contains(err.Error(), "parquet_metadata_cache") {
			t.Fatalf("expected contextual cache setup error, got %v", err)
		}
	}
	want := []string{"SET GLOBAL enable_external_file_cache = false", "SET GLOBAL parquet_metadata_cache = false"}
	if !reflect.DeepEqual(connector.statements, want) {
		t.Fatalf("failed setup must never execute queries or retry: %v", connector.statements)
	}
}

func TestCacheVariantDoesNotReconnectAfterLostConnection(t *testing.T) {
	connector := &cacheTestConnector{loseConnection: true}
	d, err := NewWithDBAndProtocol(sql.OpenDB(connector), core.ProtocolPGWireUncached)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	for range 2 {
		if _, err := d.Execute(context.Background(), core.Query{QueryID: "q", PGWireSQL: "SELECT 1"}, nil); err == nil {
			t.Fatal("lost pinned connection must fail rather than run on an unconfigured replacement")
		}
	}
	if connector.connections != 1 || len(connector.statements) != 4 {
		t.Fatalf("unexpected replacement connection or query retry: connections=%d statements=%v", connector.connections, connector.statements)
	}
}

func TestDefaultDriverDoesNotChangeCacheSettings(t *testing.T) {
	connector := &cacheTestConnector{}
	d := NewWithDB(sql.OpenDB(connector))
	t.Cleanup(func() { _ = d.Close() })
	if _, err := d.Execute(context.Background(), core.Query{QueryID: "q", PGWireSQL: "SELECT 1"}, nil); err != nil {
		t.Fatal(err)
	}
	if d.Protocol() != core.ProtocolPGWire || !reflect.DeepEqual(connector.statements, []string{"SELECT 1"}) {
		t.Fatalf("default pgwire behavior changed: protocol=%s statements=%v", d.Protocol(), connector.statements)
	}
}

func TestCacheVariantRejectsUnsupportedProtocol(t *testing.T) {
	connector := &cacheTestConnector{}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := NewWithDBAndProtocol(db, core.ProtocolTrino); err == nil {
		t.Fatal("expected unsupported protocol error")
	}
	if connector.connections != 0 {
		t.Fatal("invalid protocol must not open a connection")
	}
}

type cacheTestConnector struct {
	connections    int
	closes         int
	statements     []string
	setupDelay     time.Duration
	failStatement  string
	loseConnection bool
}

func (c *cacheTestConnector) Connect(context.Context) (driver.Conn, error) {
	c.connections++
	return &cacheTestConn{connector: c}, nil
}

func (c *cacheTestConnector) Driver() driver.Driver { return cacheTestDriver{} }

type cacheTestDriver struct{}

func (cacheTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type cacheTestConn struct{ connector *cacheTestConnector }

func (c *cacheTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}

func (c *cacheTestConn) Close() error {
	c.connector.closes++
	return nil
}

func (c *cacheTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}

func (c *cacheTestConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.connector.statements = append(c.connector.statements, query)
	time.Sleep(c.connector.setupDelay)
	if query == c.connector.failStatement {
		return nil, errors.New("setting rejected")
	}
	return driver.RowsAffected(0), nil
}

func (c *cacheTestConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.connector.statements = append(c.connector.statements, query)
	if c.connector.loseConnection {
		return nil, driver.ErrBadConn
	}
	return &cacheTestRows{}, nil
}

type cacheTestRows struct{ read bool }

func (*cacheTestRows) Columns() []string { return []string{"value"} }
func (*cacheTestRows) Close() error      { return nil }
func (r *cacheTestRows) Next(values []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	values[0] = int64(1)
	return nil
}
