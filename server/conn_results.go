package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/posthog/duckgres/duckdbservice/arrowmap"
	"github.com/posthog/duckgres/server/auth"
	"github.com/posthog/duckgres/server/wire"
)

// streamRowsToClientExtended sends result rows for the extended query protocol.
// Unlike streamRowsToClient, this does NOT send ReadyForQuery, and supports
// binary result formats and the described flag.
func (c *clientConn) streamRowsToClientExtended(p *portal, rows RowSet, cmdType string, resultFormats []int16, described bool, query string) {
	// Get column info
	cols, err := rows.Columns()
	if err != nil {
		c.logger().Error("Failed to get column info.", "query", query, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		return
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		c.logger().Error("Failed to get column types.", "query", query, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		return
	}
	if !c.cachePortalRowDescription(p, cols, colTypes) {
		return
	}

	// Get type OIDs for binary encoding
	typeOIDs := make([]int32, len(cols))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	// Send RowDescription if Describe wasn't called before Execute
	if !described && len(cols) > 0 {
		if err := c.writeCachedPortalRowDescription(p); err != nil {
			return
		}
	}

	// Stream DataRows with format codes
	rowCount := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			c.logger().Error("Failed to scan row.", "query", query, "error", err)
			c.sendError("ERROR", "42000", err.Error())
			c.setTxError()
			return
		}

		if err := c.sendDataRowWithFormats(values, resultFormats, typeOIDs); err != nil {
			return
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		if c.isCallerCancellation(err) {
			c.sendError("ERROR", "57014", "canceling statement due to user request")
		} else {
			c.logger().Error("Row iteration error.", "query", query, "error", err)
			c.sendError("ERROR", "42000", err.Error())
		}
		c.setTxError()
		return
	}

	// Send completion (no ReadyForQuery - that's done by Sync)
	tag := buildCommandTagFromRowCount(cmdType, int64(rowCount))
	_ = c.writeCommandComplete(tag)
}

// streamRowsToClient sends result rows over the wire protocol.
// The rows cursor must already be obtained before calling this function.
func (c *clientConn) streamRowsToClient(rows RowSet, cmdType string, query string) error {
	// Get column info
	cols, err := rows.Columns()
	if err != nil {
		c.logger().Error("Failed to get column info.", "query", query, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		c.logger().Error("Failed to get column types.", "query", query, "error", err)
		c.sendError("ERROR", "42000", err.Error())
		c.setTxError()
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// Send row description
	if err := c.sendRowDescription(cols, colTypes); err != nil {
		return err
	}

	// Extract type OIDs for JSON-aware text formatting
	typeOIDs := make([]int32, len(colTypes))
	for i, ct := range colTypes {
		typeOIDs[i] = getTypeInfo(ct).OID
	}

	// Stream DataRows
	rowCount := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			c.logger().Error("Failed to scan row.", "query", query, "error", err)
			c.sendError("ERROR", "42000", err.Error())
			c.setTxError()
			_ = c.writeReadyForQuery(c.txStatus)
			_ = c.flushWriter()
			return nil
		}

		if err := c.sendDataRowWithFormats(values, nil, typeOIDs); err != nil {
			return err
		}
		rowCount++
	}

	if err := rows.Err(); err != nil {
		if c.isCallerCancellation(err) {
			c.sendError("ERROR", "57014", "canceling statement due to user request")
		} else {
			c.logger().Error("Row iteration error.", "query", query, "error", err)
			c.sendError("ERROR", "42000", err.Error())
		}
		c.setTxError()
		_ = c.writeReadyForQuery(c.txStatus)
		_ = c.flushWriter()
		return nil
	}

	// Send completion
	tag := buildCommandTagFromRowCount(cmdType, int64(rowCount))
	_ = c.writeCommandComplete(tag)
	_ = c.writeReadyForQuery(c.txStatus)
	return c.flushWriter()
}

func (c *clientConn) sendRowDescription(cols []string, colTypes []ColumnTyper) error {
	return c.sendRowDescriptionWithFormats(cols, colTypes, nil)
}

// sendRowDescriptionWithFormats sends a RowDescription message with per-column format codes.
// formatCodes follow the same convention as Bind result format codes:
//   - nil or empty: all text (format=0)
//   - single element: applies to all columns
//   - one per column: per-column format
func (c *clientConn) sendRowDescriptionWithFormats(cols []string, colTypes []ColumnTyper, formatCodes []int16) error {
	return c.writeRowDescriptionBody(c.rowDescriptionBody(cols, colTypes, formatCodes))
}

// cachePortalRowDescription stores exactly the wire metadata a terminal
// portal needs for a later Describe(P). It intentionally serializes the
// description instead of retaining RowSet/driver objects or the full Bind
// result-format slice. PostgreSQL allows zero formats, one format for every
// output column, or one format per actual output column; the count is first
// knowable here for arbitrary SQL.
func (c *clientConn) cachePortalRowDescription(p *portal, cols []string, colTypes []ColumnTyper) bool {
	if p == nil {
		return false
	}
	if len(p.resultFormats) != 0 && len(p.resultFormats) != 1 && len(p.resultFormats) != len(cols) {
		c.sendError("ERROR", "08P01", "invalid result format count in Bind message")
		return false
	}
	if len(cols) == 0 {
		p.rowDescription = nil
		return true
	}
	p.rowDescription = c.rowDescriptionBody(cols, colTypes, p.resultFormats)
	return true
}

func (c *clientConn) validPortalResultFormats(p *portal, columns int) bool {
	if p == nil || len(p.resultFormats) == 0 || len(p.resultFormats) == 1 || len(p.resultFormats) == columns {
		return true
	}
	c.sendError("ERROR", "08P01", "invalid result format count in Bind message")
	return false
}

func (c *clientConn) writeCachedPortalRowDescription(p *portal) error {
	if p == nil || len(p.rowDescription) == 0 {
		return wire.WriteNoData(c.writer)
	}
	return c.writeRowDescriptionBody(p.rowDescription)
}

func (c *clientConn) writeRowDescriptionBody(body []byte) error {
	if err := wire.WriteMessage(c.writer, wire.MsgRowDescription, body); err != nil {
		c.markActiveQueryMetricsError(err)
		return err
	}
	return nil
}

func (c *clientConn) rowDescriptionBody(cols []string, colTypes []ColumnTyper, formatCodes []int16) []byte {
	var buf bytes.Buffer

	// Number of fields
	_ = binary.Write(&buf, binary.BigEndian, int16(len(cols)))

	for i, col := range cols {
		// Strip internal "memory.main." prefix from column names.
		// In DuckLake mode the transpiler qualifies our custom macros with
		// memory.main. so DuckDB can find them, but clients shouldn't see that.
		displayCol := strings.TrimPrefix(col, "memory.main.")

		// Column name (null-terminated)
		buf.WriteString(displayCol)
		buf.WriteByte(0)

		// Table OID (0 = not from a table)
		_ = binary.Write(&buf, binary.BigEndian, int32(0))

		// Column attribute number (0 = not from a table)
		_ = binary.Write(&buf, binary.BigEndian, int16(0))

		// Data type OID - check for pg_catalog column name overrides first,
		// then fall back to DuckDB type mapping
		oid := c.mapTypeOIDWithColumnName(displayCol, colTypes[i])
		_ = binary.Write(&buf, binary.BigEndian, oid)

		// Data type size - use appropriate size for overridden types
		typeSize := c.mapTypeSizeWithColumnName(displayCol, colTypes[i])
		_ = binary.Write(&buf, binary.BigEndian, typeSize)

		// Type modifier (e.g. precision/scale for NUMERIC, -1 = no modifier)
		typmod := getTypeInfo(colTypes[i]).Typmod
		_ = binary.Write(&buf, binary.BigEndian, typmod)

		// Format code (0 = text, 1 = binary)
		var format int16
		if len(formatCodes) == 1 {
			format = formatCodes[0]
		} else if i < len(formatCodes) {
			format = formatCodes[i]
		}
		_ = binary.Write(&buf, binary.BigEndian, format)
	}

	return buf.Bytes()
}

func (c *clientConn) mapTypeOIDWithColumnName(colName string, colType ColumnTyper) int32 {
	// Check if this column name has a specific pg_catalog type override
	if oid, ok := pgCatalogColumnOIDs[colName]; ok {
		return oid
	}
	return getTypeInfo(colType).OID
}

func (c *clientConn) mapTypeSizeWithColumnName(colName string, colType ColumnTyper) int16 {
	// Return appropriate sizes for overridden types
	if oid, ok := pgCatalogColumnOIDs[colName]; ok {
		switch oid {
		case OidName:
			return 64 // name is 64 bytes
		case OidChar:
			return 1 // "char" is 1 byte
		case OidText:
			return -1 // text is variable length
		case OidInt2:
			return 2 // smallint is 2 bytes
		}
	}
	return getTypeInfo(colType).Size
}

// sendDataRowWithFormats sends a data row with optional binary encoding
// formatCodes: per-column format codes (0=text, 1=binary), or nil for all text
// typeOIDs: per-column type OIDs for binary encoding, or nil
func (c *clientConn) sendDataRowWithFormats(values []interface{}, formatCodes []int16, typeOIDs []int32) error {
	var buf bytes.Buffer

	// Number of columns
	_ = binary.Write(&buf, binary.BigEndian, int16(len(values)))

	for i, v := range values {
		if v == nil {
			// NULL value
			_ = binary.Write(&buf, binary.BigEndian, int32(-1))
			continue
		}

		// Determine format: binary or text
		useBinary := false
		if formatCodes != nil {
			if len(formatCodes) == 1 {
				// Single format code applies to all columns
				useBinary = formatCodes[0] == 1
			} else if i < len(formatCodes) {
				useBinary = formatCodes[i] == 1
			}
		}

		if useBinary && typeOIDs != nil && i < len(typeOIDs) {
			// Binary encoding
			encoded := encodeBinary(v, typeOIDs[i])
			if encoded == nil {
				// Fallback to text if binary encoding fails
				str := formatValue(v)
				_ = binary.Write(&buf, binary.BigEndian, int32(len(str)))
				buf.WriteString(str)
			} else {
				_ = binary.Write(&buf, binary.BigEndian, int32(len(encoded)))
				buf.Write(encoded)
			}
		} else {
			// Text encoding must use the column OID for types whose PostgreSQL
			// representation cannot be inferred from the scanned Go value alone.
			var typeOID int32
			if typeOIDs != nil && i < len(typeOIDs) {
				typeOID = typeOIDs[i]
			}
			str := formatTextValue(v, typeOID)
			_ = binary.Write(&buf, binary.BigEndian, int32(len(str)))
			buf.WriteString(str)
		}
	}

	if err := wire.WriteMessage(c.writer, wire.MsgDataRow, buf.Bytes()); err != nil {
		c.markActiveQueryMetricsError(err)
		return err
	}
	return nil
}

// formatTextValue converts a value to its PostgreSQL text representation when
// the result column's OID is known. DuckDB scans DATE values into time.Time, so
// the OID is required to distinguish a DATE (YYYY-MM-DD) from timestamp types.
func formatTextValue(v interface{}, oid int32) string {
	switch oid {
	case OidJSON, OidJSONB:
		return string(encodeJSON(v))
	case OidDate:
		if date, ok := normalizeDriverValue(v).(time.Time); ok {
			return formatDate(date)
		}
	case OidDateArray:
		if dates, ok := normalizeDriverValue(v).([]any); ok {
			return formatArrayValueWithElementOID(dates, OidDate)
		}
	}
	return formatValue(v)
}

const secondsPerDay int64 = 24 * 60 * 60

var (
	// DuckDB stores DATE as signed days since 1970-01-01 and reserves
	// ±MaxInt32 for infinity. duckdb-go converts those values to time.Time.
	duckDBPositiveInfinityDate = time.Unix(int64(math.MaxInt32)*secondsPerDay, 0).UTC()
	duckDBNegativeInfinityDate = time.Unix(-int64(math.MaxInt32)*secondsPerDay, 0).UTC()
)

func formatDate(date time.Time) string {
	if sameDate(date, duckDBPositiveInfinityDate) {
		return "infinity"
	}
	if sameDate(date, duckDBNegativeInfinityDate) {
		return "-infinity"
	}
	if date.Year() <= 0 {
		return fmt.Sprintf("%04d-%02d-%02d BC", 1-date.Year(), date.Month(), date.Day())
	}
	return date.Format("2006-01-02")
}

func sameDate(a, b time.Time) bool {
	return a.Year() == b.Year() && a.Month() == b.Month() && a.Day() == b.Day()
}

// formatValue converts a value to its PostgreSQL text representation
func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}
	v = normalizeDriverValue(v)

	switch val := v.(type) {
	case []byte:
		return string(val)
	case string:
		return val
	case *string:
		if val == nil {
			return ""
		}
		return *val
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32:
		return fmt.Sprintf("%g", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "t"
		}
		return "f"
	case time.Time:
		// PostgreSQL timestamp format without timezone suffix
		if val.IsZero() {
			return ""
		}
		// Use microsecond precision if there are sub-second components
		if val.Nanosecond() != 0 {
			return val.Format("2006-01-02 15:04:05.999999")
		}
		return val.Format("2006-01-02 15:04:05")
	case []any:
		// PostgreSQL array text format: {1,2,3}
		return formatArrayValue(val)
	case arrowmap.IntervalValue:
		// PostgreSQL interval text format: "1 year 2 mons 3 days 04:05:06.123456".
		// Driver-specific interval types (e.g. duckdb.Interval) are converted to
		// arrowmap.IntervalValue by normalizeDriverValue before reaching this
		// switch — see server/value_normalize.go.
		return formatInterval(val)
	case map[string]any:
		// STRUCT text format: {"key1": val1, "key2": val2}
		return formatMapValue(val)
	case arrowmap.OrderedMapValue:
		return formatOrderedMapValue(val)
	default:
		// For other types, try to convert to string
		return fmt.Sprintf("%v", val)
	}
}

// formatInterval formats an arrowmap.IntervalValue as a PostgreSQL-compatible
// interval string. Examples: "00:13:08.917797", "1 day 02:30:00",
// "1 year 2 mons 3 days 04:05:06".
func formatInterval(iv arrowmap.IntervalValue) string {
	var parts []string
	if iv.Months != 0 {
		years := iv.Months / 12
		remMonths := iv.Months % 12
		if years != 0 {
			if years == 1 || years == -1 {
				parts = append(parts, fmt.Sprintf("%d year", years))
			} else {
				parts = append(parts, fmt.Sprintf("%d years", years))
			}
		}
		if remMonths != 0 {
			if remMonths == 1 || remMonths == -1 {
				parts = append(parts, fmt.Sprintf("%d mon", remMonths))
			} else {
				parts = append(parts, fmt.Sprintf("%d mons", remMonths))
			}
		}
	}
	if iv.Days != 0 {
		if iv.Days == 1 || iv.Days == -1 {
			parts = append(parts, fmt.Sprintf("%d day", iv.Days))
		} else {
			parts = append(parts, fmt.Sprintf("%d days", iv.Days))
		}
	}
	if iv.Micros != 0 || len(parts) == 0 {
		micros := iv.Micros
		neg := micros < 0
		if neg {
			micros = -micros
		}
		h := micros / 3600000000
		micros %= 3600000000
		m := micros / 60000000
		micros %= 60000000
		s := micros / 1000000
		rem := micros % 1000000
		var timePart string
		if rem > 0 {
			timePart = fmt.Sprintf("%02d:%02d:%02d.%06d", h, m, s, rem)
		} else {
			timePart = fmt.Sprintf("%02d:%02d:%02d", h, m, s)
		}
		if neg {
			timePart = "-" + timePart
		}
		parts = append(parts, timePart)
	}
	return strings.Join(parts, " ")
}

// formatArrayValue formats a []any slice as PostgreSQL text array: {1,2,3}
func formatArrayValue(arr []any) string {
	return formatArrayValueWithElementOID(arr, 0)
}

func formatArrayValueWithElementOID(arr []any, elementOID int32) string {
	var buf strings.Builder
	buf.WriteByte('{')
	for i, elem := range arr {
		if i > 0 {
			buf.WriteByte(',')
		}
		if elem == nil {
			buf.WriteString("NULL")
		} else {
			s := formatTextValue(elem, elementOID)
			// Quote strings that contain special characters
			if needsArrayQuoting(s) {
				buf.WriteByte('"')
				// Escape backslashes and double quotes
				for _, c := range s {
					if c == '"' || c == '\\' {
						buf.WriteByte('\\')
					}
					buf.WriteRune(c)
				}
				buf.WriteByte('"')
			} else {
				buf.WriteString(s)
			}
		}
	}
	buf.WriteByte('}')
	return buf.String()
}

// needsArrayQuoting returns true if a string value needs quoting inside a PostgreSQL array literal
func needsArrayQuoting(s string) bool {
	if s == "" {
		return true
	}
	for _, c := range s {
		if c == ',' || c == '{' || c == '}' || c == '"' || c == '\\' || c == ' ' {
			return true
		}
	}
	return false
}

// formatMapValue formats a map[string]any as a key-value text representation.
// Used for STRUCT values extracted from Arrow (keys are field names → always strings).
func formatMapValue(m map[string]any) string {
	var buf strings.Builder
	buf.WriteByte('{')
	first := true
	for k, v := range m {
		if !first {
			buf.WriteString(", ")
		}
		first = false
		buf.WriteString(k)
		buf.WriteString("=")
		buf.WriteString(formatValue(v))
	}
	buf.WriteByte('}')
	return buf.String()
}

// formatOrderedMapValue formats an OrderedMapValue as a key-value text
// representation, preserving the original insertion order from the Arrow array.
func formatOrderedMapValue(m arrowmap.OrderedMapValue) string {
	var buf strings.Builder
	buf.WriteByte('{')
	for i, k := range m.Keys {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(formatValue(k))
		buf.WriteString("=")
		buf.WriteString(formatValue(m.Values[i]))
	}
	buf.WriteByte('}')
	return buf.String()
}

func (c *clientConn) writeCommandComplete(tag string) error {
	if err := wire.WriteCommandComplete(c.writer, tag); err != nil {
		c.markActiveQueryMetricsError(err)
		return err
	}
	if c.activeQueryMetrics != nil {
		return c.flushWriter()
	}
	return nil
}

func (c *clientConn) writeReadyForQuery(txStatus byte) error {
	if err := wire.WriteReadyForQuery(c.writer, txStatus); err != nil {
		c.markActiveQueryMetricsError(err)
		return err
	}
	return nil
}

func (c *clientConn) flushWriter() error {
	if err := c.writer.Flush(); err != nil {
		c.markActiveQueryMetricsError(err)
		return err
	}
	return nil
}

func (c *clientConn) sendError(severity, code, message string) {
	// Class 28 = "Invalid Authorization Specification" (auth failures).
	// All current FATAL errors use class 28, so this covers both auth
	// failures and connection rejections (no SSL, no user, wrong password).
	// NOTE: If one adds a FATAL error with a non-28 code, be sure to add
	// a metric for it here.
	if strings.HasPrefix(code, "28") {
		auth.AuthFailuresCounter.Inc()
	}
	if severity != "" {
		c.lastErrorCode = code
	}
	c.logger().Debug("Sending error to client.", "severity", severity, "code", code, "message", message)
	c.errorResponsesSent++
	_ = wire.WriteErrorResponse(c.writer, severity, code, message)
	_ = c.flushWriter()
}

func (c *clientConn) sendNotice(severity, code, message string) {
	_ = wire.WriteNoticeResponse(c.writer, severity, code, message)
	// Don't flush here - let the caller decide when to flush
}
