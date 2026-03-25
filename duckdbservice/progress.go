package duckdbservice

import (
	"database/sql"
	"fmt"
	"reflect"
	"sync/atomic"
	"unsafe"

	bindings "github.com/duckdb/duckdb-go-bindings"
)

// duckdbConnHandle wraps bindings.Connection for use outside this file
// without requiring other files to import the mapping package.
type duckdbConnHandle = bindings.Connection

// stallCheckThreshold is the number of consecutive health checks (each ~2s apart)
// with zero progress change before a session is considered stalled.
// 300 checks × 2s = ~10 minutes of zero progress.
const stallCheckThreshold = 300

// progressState tracks query activity and stall detection for a session.
// All fields are atomic because queryActive is written by query goroutines
// while lastRowsProcessed and stalledChecks are written by the health check
// loop (which holds only an RLock on the session pool).
type progressState struct {
	queryActive       atomic.Bool
	lastRowsProcessed atomic.Uint64
	stalledChecks     atomic.Int32
}

// extractDuckDBConnection extracts the raw bindings.Connection handle from a
// *sql.Conn by reaching into the underlying duckdb driver's Conn struct via
// reflection. This is needed for calling bindings.QueryProgress during health
// checks without interrupting the query.
//
// The duckdb-go driver's Conn struct has an unexported field:
//
//	type Conn struct {
//	    conn bindings.Connection  // struct { Ptr unsafe.Pointer }
//	    ...
//	}
//
// We use sql.Conn.Raw() to get the driver.Conn, then reflect to read the field.
// Returns (zero, error) if extraction fails (e.g., driver changed layout).
func extractDuckDBConnection(sqlConn *sql.Conn) (bindings.Connection, error) {
	var duckConn bindings.Connection
	err := sqlConn.Raw(func(driverConn interface{}) error {
		v := reflect.ValueOf(driverConn)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			return fmt.Errorf("expected struct, got %s", v.Kind())
		}
		connField := v.FieldByName("conn")
		if !connField.IsValid() {
			return fmt.Errorf("duckdb Conn struct has no 'conn' field")
		}
		// bindings.Connection is a struct { Ptr unsafe.Pointer }.
		// The field is unexported, so we must use unsafe to read it.
		// connField points to the Connection struct; read its Ptr field.
		ptrField := connField.FieldByName("Ptr")
		if !ptrField.IsValid() {
			return fmt.Errorf("bindings.Connection has no 'Ptr' field")
		}
		// Read the unsafe.Pointer value from the unexported field.
		ptr := *(*unsafe.Pointer)(ptrField.Addr().UnsafePointer())
		duckConn = bindings.Connection{Ptr: ptr}
		return nil
	})
	return duckConn, err
}
