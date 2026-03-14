package duckdbservice

import (
	"database/sql"
	"fmt"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/duckdb/duckdb-go/mapping"
)

// duckdbConnHandle wraps mapping.Connection for use outside this file
// without requiring other files to import the mapping package.
type duckdbConnHandle = mapping.Connection

// stallCheckThreshold is the number of consecutive health checks (each ~2s apart)
// with zero progress change before a session is considered stalled.
// 300 checks × 2s = ~10 minutes of zero progress.
const stallCheckThreshold = 300

// progressState tracks query activity and stall detection for a session.
type progressState struct {
	queryActive       atomic.Bool
	lastRowsProcessed uint64
	stalledChecks     int32
}

// extractDuckDBConnection extracts the raw mapping.Connection handle from a
// *sql.Conn by reaching into the underlying duckdb driver's Conn struct via
// reflection. This is needed for calling mapping.QueryProgress during health
// checks without interrupting the query.
//
// The duckdb-go driver's Conn struct has an unexported field:
//
//	type Conn struct {
//	    conn mapping.Connection  // struct { Ptr unsafe.Pointer }
//	    ...
//	}
//
// We use sql.Conn.Raw() to get the driver.Conn, then reflect to read the field.
// Returns (zero, error) if extraction fails (e.g., driver changed layout).
func extractDuckDBConnection(sqlConn *sql.Conn) (mapping.Connection, error) {
	var duckConn mapping.Connection
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
		// mapping.Connection is a struct { Ptr unsafe.Pointer }.
		// The field is unexported, so we must use unsafe to read it.
		// connField points to the Connection struct; read its Ptr field.
		ptrField := connField.FieldByName("Ptr")
		if !ptrField.IsValid() {
			return fmt.Errorf("mapping.Connection has no 'Ptr' field")
		}
		// Read the unsafe.Pointer value from the unexported field.
		ptr := *(*unsafe.Pointer)(ptrField.Addr().UnsafePointer())
		duckConn = mapping.Connection{Ptr: ptr}
		return nil
	})
	return duckConn, err
}
