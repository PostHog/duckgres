package server

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
)

// PostgreSQL type OIDs
const (
	OidBool        int32 = 16
	OidBytea       int32 = 17
	OidChar        int32 = 18   // "char" - single-byte internal type
	OidName        int32 = 19   // name - 64-byte internal type for identifiers
	OidInt8        int32 = 20   // bigint
	OidInt2        int32 = 21   // smallint
	OidInt4        int32 = 23   // integer
	OidText        int32 = 25
	OidOid         int32 = 26
	OidFloat4      int32 = 700  // real
	OidFloat8      int32 = 701  // double precision
	OidBpchar      int32 = 1042 // blank-padded char
	OidVarchar     int32 = 1043
	OidDate        int32 = 1082
	OidTime        int32 = 1083
	OidTimestamp   int32 = 1114
	OidTimestamptz int32 = 1184
	OidInterval    int32 = 1186
	OidNumeric     int32 = 1700
	OidUUID        int32 = 2950
	OidJSON        int32 = 114
	OidJSONB       int32 = 3802
)

// pgCatalogColumnOIDs maps pg_catalog column names to their correct PostgreSQL type OIDs.
// This ensures wire protocol compatibility with JDBC clients that expect specific types.
var pgCatalogColumnOIDs = map[string]int32{
	// "name" type columns (OID 19) - 64-byte identifiers
	"nspname":            OidName,
	"relname":            OidName,
	"attname":            OidName,
	"typname":            OidName,
	"datname":            OidName,
	"rolname":            OidName,
	"collname":           OidName,
	"conname":            OidName,
	"proname":            OidName,
	"usename":            OidName,
	"current_database()": OidName,
	"current_database":   OidName,
	// JDBC metadata query aliases that should be NAME type
	"TABLE_SCHEM":   OidName,
	"TABLE_CATALOG": OidName,
	"table_schem":   OidName,
	"table_name":    OidName,
	// "char" type columns (OID 18) - single-byte internal type
	"typtype":       OidChar,
	"typcategory":   OidChar,
	"typalign":      OidChar,
	"typstorage":    OidChar,
	"relkind":       OidChar,
	"relpersistence": OidChar,
	"attidentity":   OidChar,
	"attgenerated":  OidChar,
	// text type columns (OID 25)
	"table_type":  OidText,
	"adsrc":       OidText,
	"description": OidText,
	// smallint columns (OID 21)
	"attlen": OidInt2,
}

// TypeInfo contains PostgreSQL type information
type TypeInfo struct {
	OID  int32
	Size int16 // -1 for variable length
}

// mapDuckDBType maps a DuckDB type name to PostgreSQL type info
func mapDuckDBType(typeName string) TypeInfo {
	upper := strings.ToUpper(typeName)

	switch {
	case upper == "BOOLEAN" || upper == "BOOL":
		return TypeInfo{OID: OidBool, Size: 1}
	case upper == "TINYINT" || upper == "INT1":
		return TypeInfo{OID: OidInt2, Size: 2} // PostgreSQL doesn't have int1
	case upper == "SMALLINT" || upper == "INT2":
		return TypeInfo{OID: OidInt2, Size: 2}
	case upper == "INTEGER" || upper == "INT4" || upper == "INT":
		return TypeInfo{OID: OidInt4, Size: 4}
	case upper == "BIGINT" || upper == "INT8":
		return TypeInfo{OID: OidInt8, Size: 8}
	case upper == "HUGEINT" || upper == "INT128":
		return TypeInfo{OID: OidNumeric, Size: -1} // No direct equivalent
	case upper == "UTINYINT" || upper == "USMALLINT":
		return TypeInfo{OID: OidInt4, Size: 4}
	case upper == "UINTEGER":
		return TypeInfo{OID: OidOid, Size: 4} // PostgreSQL oid type for pg_catalog columns
	case upper == "UBIGINT":
		return TypeInfo{OID: OidNumeric, Size: -1}
	case upper == "REAL" || upper == "FLOAT4" || upper == "FLOAT":
		return TypeInfo{OID: OidFloat4, Size: 4}
	case upper == "DOUBLE" || upper == "FLOAT8":
		return TypeInfo{OID: OidFloat8, Size: 8}
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC"):
		return TypeInfo{OID: OidNumeric, Size: -1}
	case upper == "VARCHAR" || strings.HasPrefix(upper, "VARCHAR("):
		return TypeInfo{OID: OidVarchar, Size: -1}
	case upper == "TEXT" || upper == "STRING":
		return TypeInfo{OID: OidText, Size: -1}
	case upper == "BLOB" || upper == "BYTEA":
		return TypeInfo{OID: OidBytea, Size: -1}
	case upper == "DATE":
		return TypeInfo{OID: OidDate, Size: 4}
	case upper == "TIME":
		return TypeInfo{OID: OidTime, Size: 8}
	case upper == "TIMESTAMP":
		return TypeInfo{OID: OidTimestamp, Size: 8}
	case upper == "TIMESTAMP WITH TIME ZONE" || upper == "TIMESTAMPTZ":
		return TypeInfo{OID: OidTimestamptz, Size: 8}
	case upper == "INTERVAL":
		return TypeInfo{OID: OidInterval, Size: 16}
	case upper == "UUID":
		return TypeInfo{OID: OidUUID, Size: 16}
	case upper == "JSON":
		return TypeInfo{OID: OidJSON, Size: -1}
	default:
		// Default to text for unknown types
		return TypeInfo{OID: OidText, Size: -1}
	}
}

// getTypeInfo extracts type info from a sql.ColumnType
func getTypeInfo(colType *sql.ColumnType) TypeInfo {
	return mapDuckDBType(colType.DatabaseTypeName())
}

// encodeBinary encodes a value in PostgreSQL binary format
// Returns the encoded bytes, or nil if the value should be sent as NULL
func encodeBinary(v interface{}, oid int32) []byte {
	if v == nil {
		return nil
	}

	switch oid {
	case OidBool:
		return encodeBool(v)
	case OidInt2:
		return encodeInt2(v)
	case OidInt4:
		return encodeInt4(v)
	case OidInt8:
		return encodeInt8(v)
	case OidFloat4:
		return encodeFloat4(v)
	case OidFloat8:
		return encodeFloat8(v)
	case OidDate:
		return encodeDate(v)
	case OidTimestamp, OidTimestamptz:
		return encodeTimestamp(v)
	case OidBytea:
		return encodeBytea(v)
	default:
		// For text, varchar, and other types, encode as text bytes
		return encodeText(v)
	}
}

func encodeBool(v interface{}) []byte {
	var b bool
	switch val := v.(type) {
	case bool:
		b = val
	case int, int8, int16, int32, int64:
		b = val != 0
	default:
		return []byte{0}
	}
	if b {
		return []byte{1}
	}
	return []byte{0}
}

func encodeInt2(v interface{}) []byte {
	buf := make([]byte, 2)
	var n int16
	switch val := v.(type) {
	case int:
		n = int16(val)
	case int8:
		n = int16(val)
	case int16:
		n = val
	case int32:
		n = int16(val)
	case int64:
		n = int16(val)
	case uint8:
		n = int16(val)
	case uint16:
		n = int16(val)
	case float32:
		n = int16(val)
	case float64:
		n = int16(val)
	default:
		return nil
	}
	binary.BigEndian.PutUint16(buf, uint16(n))
	return buf
}

func encodeInt4(v interface{}) []byte {
	buf := make([]byte, 4)
	var n int32
	switch val := v.(type) {
	case int:
		n = int32(val)
	case int8:
		n = int32(val)
	case int16:
		n = int32(val)
	case int32:
		n = val
	case int64:
		n = int32(val)
	case uint8:
		n = int32(val)
	case uint16:
		n = int32(val)
	case uint32:
		n = int32(val)
	case float32:
		n = int32(val)
	case float64:
		n = int32(val)
	default:
		return nil
	}
	binary.BigEndian.PutUint32(buf, uint32(n))
	return buf
}

func encodeInt8(v interface{}) []byte {
	buf := make([]byte, 8)
	var n int64
	switch val := v.(type) {
	case int:
		n = int64(val)
	case int8:
		n = int64(val)
	case int16:
		n = int64(val)
	case int32:
		n = int64(val)
	case int64:
		n = val
	case uint8:
		n = int64(val)
	case uint16:
		n = int64(val)
	case uint32:
		n = int64(val)
	case uint64:
		n = int64(val)
	case float32:
		n = int64(val)
	case float64:
		n = int64(val)
	default:
		return nil
	}
	binary.BigEndian.PutUint64(buf, uint64(n))
	return buf
}

func encodeFloat4(v interface{}) []byte {
	buf := make([]byte, 4)
	var f float32
	switch val := v.(type) {
	case float32:
		f = val
	case float64:
		f = float32(val)
	case int:
		f = float32(val)
	case int32:
		f = float32(val)
	case int64:
		f = float32(val)
	default:
		return nil
	}
	binary.BigEndian.PutUint32(buf, math.Float32bits(f))
	return buf
}

func encodeFloat8(v interface{}) []byte {
	buf := make([]byte, 8)
	var f float64
	switch val := v.(type) {
	case float64:
		f = val
	case float32:
		f = float64(val)
	case int:
		f = float64(val)
	case int32:
		f = float64(val)
	case int64:
		f = float64(val)
	default:
		return nil
	}
	binary.BigEndian.PutUint64(buf, math.Float64bits(f))
	return buf
}

// PostgreSQL epoch is 2000-01-01, Unix epoch is 1970-01-01
// Difference in days: 10957
const pgEpochDays = 10957

// Difference in microseconds
const pgEpochMicros = pgEpochDays * 24 * 60 * 60 * 1000000

func encodeDate(v interface{}) []byte {
	buf := make([]byte, 4)
	var days int32

	switch val := v.(type) {
	case time.Time:
		// Days since PostgreSQL epoch (2000-01-01)
		unixDays := val.Unix() / 86400
		days = int32(unixDays - pgEpochDays)
	case string:
		// Try to parse date string
		t, err := time.Parse("2006-01-02", val)
		if err != nil {
			return nil
		}
		unixDays := t.Unix() / 86400
		days = int32(unixDays - pgEpochDays)
	default:
		return nil
	}

	binary.BigEndian.PutUint32(buf, uint32(days))
	return buf
}

func encodeTimestamp(v interface{}) []byte {
	buf := make([]byte, 8)
	var micros int64

	switch val := v.(type) {
	case time.Time:
		// Microseconds since PostgreSQL epoch (2000-01-01)
		unixMicros := val.UnixMicro()
		micros = unixMicros - pgEpochMicros
	case string:
		// Try to parse timestamp string
		t, err := time.Parse("2006-01-02 15:04:05", val)
		if err != nil {
			t, err = time.Parse("2006-01-02T15:04:05Z", val)
			if err != nil {
				return nil
			}
		}
		unixMicros := t.UnixMicro()
		micros = unixMicros - pgEpochMicros
	default:
		return nil
	}

	binary.BigEndian.PutUint64(buf, uint64(micros))
	return buf
}

func encodeBytea(v interface{}) []byte {
	switch val := v.(type) {
	case []byte:
		return val
	case string:
		return []byte(val)
	default:
		return nil
	}
}

func encodeText(v interface{}) []byte {
	str := formatValue(v)
	return []byte(str)
}

// decodeBinary decodes binary-format parameter bytes based on type OID.
// Returns (value, nil) on success, or (nil, error) for malformed data.
// Per PostgreSQL spec, returns error with "insufficient data" for truncated binary data.
func decodeBinary(data []byte, oid int32) (interface{}, error) {
	if data == nil {
		return nil, nil
	}

	switch oid {
	case OidBool:
		return decodeBool(data)
	case OidInt2:
		return decodeInt2(data)
	case OidInt4:
		return decodeInt4(data)
	case OidInt8:
		return decodeInt8(data)
	case OidFloat4:
		return decodeFloat4(data)
	case OidFloat8:
		return decodeFloat8(data)
	case OidDate:
		return decodeDate(data)
	case OidTimestamp, OidTimestamptz:
		return decodeTimestamp(data)
	case OidBytea:
		return data, nil // raw bytes
	default:
		// For text, varchar, and unknown types, return as string
		return string(data), nil
	}
}

func decodeBool(data []byte) (bool, error) {
	if len(data) < 1 {
		return false, fmt.Errorf("insufficient data for bool: got %d bytes, need 1", len(data))
	}
	return data[0] != 0, nil
}

func decodeInt2(data []byte) (int16, error) {
	if len(data) < 2 {
		return 0, fmt.Errorf("insufficient data for int2: got %d bytes, need 2", len(data))
	}
	return int16(binary.BigEndian.Uint16(data)), nil
}

func decodeInt4(data []byte) (int32, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("insufficient data for int4: got %d bytes, need 4", len(data))
	}
	return int32(binary.BigEndian.Uint32(data)), nil
}

func decodeInt8(data []byte) (int64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("insufficient data for int8: got %d bytes, need 8", len(data))
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

func decodeFloat4(data []byte) (float32, error) {
	if len(data) < 4 {
		return 0, fmt.Errorf("insufficient data for float4: got %d bytes, need 4", len(data))
	}
	bits := binary.BigEndian.Uint32(data)
	return math.Float32frombits(bits), nil
}

func decodeFloat8(data []byte) (float64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("insufficient data for float8: got %d bytes, need 8", len(data))
	}
	bits := binary.BigEndian.Uint64(data)
	return math.Float64frombits(bits), nil
}

func decodeDate(data []byte) (time.Time, error) {
	if len(data) < 4 {
		return time.Time{}, fmt.Errorf("insufficient data for date: got %d bytes, need 4", len(data))
	}
	// Days since PostgreSQL epoch (2000-01-01)
	days := int32(binary.BigEndian.Uint32(data))
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return pgEpoch.AddDate(0, 0, int(days)), nil
}

func decodeTimestamp(data []byte) (time.Time, error) {
	if len(data) < 8 {
		return time.Time{}, fmt.Errorf("insufficient data for timestamp: got %d bytes, need 8", len(data))
	}
	// Microseconds since PostgreSQL epoch (2000-01-01)
	micros := int64(binary.BigEndian.Uint64(data))
	pgEpoch := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return pgEpoch.Add(time.Duration(micros) * time.Microsecond), nil
}
