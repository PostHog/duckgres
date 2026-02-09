package server

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
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
	OidTimetz      int32 = 1266
	OidJSON        int32 = 114
	OidJSONB       int32 = 3802

	// Array OIDs
	OidBoolArray        int32 = 1000
	OidInt2Array        int32 = 1005
	OidInt4Array        int32 = 1007
	OidTextArray        int32 = 1009
	OidVarcharArray     int32 = 1015
	OidInt8Array        int32 = 1016
	OidFloat4Array      int32 = 1021
	OidFloat8Array      int32 = 1022
	OidTimestampArray   int32 = 1115
	OidDateArray        int32 = 1182
	OidTimeArray        int32 = 1183
	OidTimestamptzArray int32 = 1185
	OidIntervalArray    int32 = 1187
	OidNumericArray     int32 = 1231
	OidTimetzArray      int32 = 1270
	OidUUIDArray        int32 = 2951
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

// arrayElementOIDs maps array OID → element OID (for binary encoding)
var arrayElementOIDs = map[int32]int32{
	OidBoolArray:        OidBool,
	OidInt2Array:        OidInt2,
	OidInt4Array:        OidInt4,
	OidInt8Array:        OidInt8,
	OidFloat4Array:      OidFloat4,
	OidFloat8Array:      OidFloat8,
	OidTextArray:        OidText,
	OidVarcharArray:     OidVarchar,
	OidDateArray:        OidDate,
	OidTimeArray:        OidTime,
	OidTimetzArray:      OidTimetz,
	OidTimestampArray:   OidTimestamp,
	OidTimestamptzArray: OidTimestamptz,
	OidIntervalArray:    OidInterval,
	OidNumericArray:     OidNumeric,
	OidUUIDArray:        OidUUID,
}

// elementToArrayOID maps scalar element OID → array OID
var elementToArrayOID = map[int32]int32{
	OidBool:        OidBoolArray,
	OidInt2:        OidInt2Array,
	OidInt4:        OidInt4Array,
	OidInt8:        OidInt8Array,
	OidFloat4:      OidFloat4Array,
	OidFloat8:      OidFloat8Array,
	OidText:        OidTextArray,
	OidVarchar:     OidVarcharArray,
	OidDate:        OidDateArray,
	OidTime:        OidTimeArray,
	OidTimetz:      OidTimetzArray,
	OidTimestamp:    OidTimestampArray,
	OidTimestamptz: OidTimestamptzArray,
	OidInterval:    OidIntervalArray,
	OidNumeric:     OidNumericArray,
	OidUUID:        OidUUIDArray,
}

// TypeInfo contains PostgreSQL type information
type TypeInfo struct {
	OID    int32
	Size   int16 // -1 for variable length
	Typmod int32 // -1 = no modifier; for NUMERIC: ((precision << 16) | scale) + 4
}

// mapDuckDBType maps a DuckDB type name to PostgreSQL type info
func mapDuckDBType(typeName string) TypeInfo {
	upper := strings.ToUpper(typeName)

	// Detect array types: DuckDB Go driver reports LIST columns as "INTEGER[]", "VARCHAR[]", etc.
	if strings.HasSuffix(upper, "[]") {
		elementTypeName := typeName[:len(typeName)-2] // preserve original case for DECIMAL parsing
		elemInfo := mapDuckDBType(elementTypeName)
		if arrayOID, ok := elementToArrayOID[elemInfo.OID]; ok {
			return TypeInfo{OID: arrayOID, Size: -1, Typmod: elemInfo.Typmod}
		}
		// Unknown element type — fall through to text
		return TypeInfo{OID: OidText, Size: -1}
	}

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
		// Map to NUMERIC(38,0) so postgres_scanner reads it as DECIMAL(38,0) → INT128,
		// matching the HUGEINT physical type. Typmod = ((38 << 16) | 0) + 4 = 2490372.
		return TypeInfo{OID: OidNumeric, Size: -1, Typmod: 2490372}
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
		return TypeInfo{OID: OidNumeric, Size: -1, Typmod: parseNumericTypmod(typeName)}
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
	case upper == "TIME WITH TIME ZONE" || upper == "TIMETZ":
		return TypeInfo{OID: OidTimetz, Size: 12}
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

// parseNumericTypmod parses precision and scale from a type name like "DECIMAL(10,2)"
// and encodes them as a PostgreSQL typmod: ((precision << 16) | scale) + 4.
// Returns -1 if precision/scale cannot be extracted.
func parseNumericTypmod(typeName string) int32 {
	lparen := strings.IndexByte(typeName, '(')
	rparen := strings.IndexByte(typeName, ')')
	if lparen < 0 || rparen < 0 || rparen <= lparen {
		return -1
	}
	inner := typeName[lparen+1 : rparen]
	parts := strings.SplitN(inner, ",", 2)
	if len(parts) != 2 {
		return -1
	}
	precision := strings.TrimSpace(parts[0])
	scale := strings.TrimSpace(parts[1])
	var p, s int
	if _, err := fmt.Sscanf(precision, "%d", &p); err != nil {
		return -1
	}
	if _, err := fmt.Sscanf(scale, "%d", &s); err != nil {
		return -1
	}
	if p <= 0 || s < 0 || p > 38 {
		return -1
	}
	return int32((p << 16) | s) + 4
}

// encodeBinary encodes a value in PostgreSQL binary format
// Returns the encoded bytes, or nil if the value should be sent as NULL
func encodeBinary(v interface{}, oid int32) []byte {
	if v == nil {
		return nil
	}

	// Check if this is an array OID
	if elemOID, ok := arrayElementOIDs[oid]; ok {
		return encodeArray(v, elemOID)
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
	case OidNumeric:
		return encodeNumeric(v)
	case OidDate:
		return encodeDate(v)
	case OidTimestamp, OidTimestamptz:
		return encodeTimestamp(v)
	case OidTime:
		return encodeTime(v)
	case OidInterval:
		return encodeInterval(v)
	case OidUUID:
		return encodeUUID(v)
	case OidBytea:
		return encodeBytea(v)
	case OidJSON, OidJSONB:
		// The Go DuckDB driver deserializes JSON columns into native Go types
		// (e.g., JSON string "hello" → Go string hello, without quotes).
		// Re-serialize to JSON text before sending on the wire.
		return encodeJSON(v)
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

// encodeUUID encodes a UUID value as 16 raw bytes (PostgreSQL binary UUID format).
// DuckDB Go driver returns UUID as []byte (16 bytes).
func encodeUUID(v interface{}) []byte {
	switch val := v.(type) {
	case []byte:
		if len(val) == 16 {
			return val
		}
		return nil
	case string:
		s := strings.ReplaceAll(val, "-", "")
		if len(s) != 32 {
			return nil
		}
		data, err := hex.DecodeString(s)
		if err != nil {
			return nil
		}
		return data
	default:
		// Try Stringer interface (e.g., duckdb.UUID)
		if stringer, ok := v.(fmt.Stringer); ok {
			return encodeUUID(stringer.String())
		}
		return nil
	}
}

// decodeUUID decodes 16 raw bytes into a UUID string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".
func decodeUUID(data []byte) (string, error) {
	if len(data) != 16 {
		return "", fmt.Errorf("invalid UUID binary data: got %d bytes, need 16", len(data))
	}
	s := hex.EncodeToString(data)
	return s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32], nil
}

// encodeTime encodes a TIME value as int64 microseconds since midnight (PostgreSQL binary format).
// DuckDB Go driver returns TIME as time.Time with date fixed to 0001-01-01.
func encodeTime(v interface{}) []byte {
	buf := make([]byte, 8)
	var micros int64

	switch val := v.(type) {
	case time.Time:
		micros = int64(val.Hour())*3600000000 + int64(val.Minute())*60000000 +
			int64(val.Second())*1000000 + int64(val.Nanosecond())/1000
	case string:
		t, err := time.Parse("15:04:05", val)
		if err != nil {
			t, err = time.Parse("15:04:05.000000", val)
			if err != nil {
				return nil
			}
		}
		micros = int64(t.Hour())*3600000000 + int64(t.Minute())*60000000 +
			int64(t.Second())*1000000 + int64(t.Nanosecond())/1000
	default:
		return nil
	}

	binary.BigEndian.PutUint64(buf, uint64(micros))
	return buf
}

// decodeTime decodes int64 microseconds since midnight into a time string.
func decodeTime(data []byte) (string, error) {
	if len(data) < 8 {
		return "", fmt.Errorf("insufficient data for time: got %d bytes, need 8", len(data))
	}
	micros := int64(binary.BigEndian.Uint64(data))
	hours := micros / 3600000000
	micros %= 3600000000
	minutes := micros / 60000000
	micros %= 60000000
	seconds := micros / 1000000
	micros %= 1000000
	if micros > 0 {
		return fmt.Sprintf("%02d:%02d:%02d.%06d", hours, minutes, seconds, micros), nil
	}
	return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds), nil
}

// encodeInterval encodes an INTERVAL value in PostgreSQL binary format:
// int64 microseconds + int32 days + int32 months = 16 bytes.
func encodeInterval(v interface{}) []byte {
	buf := make([]byte, 16)
	switch val := v.(type) {
	case duckdb.Interval:
		binary.BigEndian.PutUint64(buf[0:8], uint64(val.Micros))
		binary.BigEndian.PutUint32(buf[8:12], uint32(val.Days))
		binary.BigEndian.PutUint32(buf[12:16], uint32(val.Months))
	default:
		_ = val
		return nil
	}
	return buf
}

// decodeInterval decodes PostgreSQL binary INTERVAL (16 bytes) into an interval string.
func decodeInterval(data []byte) (string, error) {
	if len(data) < 16 {
		return "", fmt.Errorf("insufficient data for interval: got %d bytes, need 16", len(data))
	}
	micros := int64(binary.BigEndian.Uint64(data[0:8]))
	days := int32(binary.BigEndian.Uint32(data[8:12]))
	months := int32(binary.BigEndian.Uint32(data[12:16]))

	var parts []string
	if months != 0 {
		years := months / 12
		remMonths := months % 12
		if years != 0 {
			parts = append(parts, fmt.Sprintf("%d year", years))
		}
		if remMonths != 0 {
			parts = append(parts, fmt.Sprintf("%d month", remMonths))
		}
	}
	if days != 0 {
		parts = append(parts, fmt.Sprintf("%d day", days))
	}
	if micros != 0 || len(parts) == 0 {
		neg := micros < 0
		if neg {
			micros = -micros
		}
		h := micros / 3600000000
		micros %= 3600000000
		m := micros / 60000000
		micros %= 60000000
		s := micros / 1000000
		remainMicros := micros % 1000000
		var timePart string
		if remainMicros > 0 {
			timePart = fmt.Sprintf("%02d:%02d:%02d.%06d", h, m, s, remainMicros)
		} else {
			timePart = fmt.Sprintf("%02d:%02d:%02d", h, m, s)
		}
		if neg {
			timePart = "-" + timePart
		}
		parts = append(parts, timePart)
	}
	return strings.Join(parts, " "), nil
}

// encodeNumeric encodes a value in PostgreSQL binary numeric format.
//
// PostgreSQL binary numeric layout:
//
//	int16 ndigits  - number of base-10000 digit groups
//	int16 weight   - weight of first digit (number of groups before decimal point - 1)
//	int16 sign     - 0x0000 = positive, 0x4000 = negative
//	int16 dscale   - number of digits after decimal point (display scale)
//	int16[] digits - base-10000 digit groups
func encodeNumeric(v interface{}) []byte {
	var val *big.Int
	var dscale int16

	switch x := v.(type) {
	case duckdb.Decimal:
		val = new(big.Int).Set(x.Value)
		dscale = int16(x.Scale)
	case *big.Int:
		// HUGEINT comes from the Go driver as *big.Int (scale 0)
		val = new(big.Int).Set(x)
		dscale = 0
	default:
		// Fallback: try to format as text and let the caller handle it
		return encodeText(v)
	}

	// Handle sign
	var sign int16
	if val.Sign() < 0 {
		sign = 0x4000 // NUMERIC_NEG
		val.Neg(val)
	}

	// Handle zero
	if val.Sign() == 0 {
		buf := make([]byte, 8)
		// ndigits=0, weight=0, sign=0, dscale=dscale
		binary.BigEndian.PutUint16(buf[6:], uint16(dscale))
		return buf
	}

	// Convert unscaled value to base-10000 digits aligned to the decimal point.
	// The unscaled value represents: val * 10^(-scale)
	// PostgreSQL numeric uses base-10000 groups where each group covers 4 decimal digits.
	// We need to pad the unscaled value so the fractional part fills complete groups.
	fracGroups := (int(dscale) + 3) / 4 // ceiling division
	padding := fracGroups*4 - int(dscale)
	if padding > 0 {
		pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(padding)), nil)
		val.Mul(val, pow)
	}

	base := big.NewInt(10000)
	var allDigits []int16

	// Extract base-10000 digits (least significant first)
	tmp := new(big.Int).Set(val)
	for tmp.Sign() > 0 {
		mod := new(big.Int)
		tmp.DivMod(tmp, base, mod)
		allDigits = append(allDigits, int16(mod.Int64()))
	}

	// Reverse to most-significant first
	for i, j := 0, len(allDigits)-1; i < j; i, j = i+1, j-1 {
		allDigits[i], allDigits[j] = allDigits[j], allDigits[i]
	}

	// The last fracGroups entries are the fractional part
	totalGroups := len(allDigits)
	intGroups := totalGroups - fracGroups
	if intGroups < 0 {
		// Need to pad with leading zero groups
		pad := make([]int16, -intGroups)
		allDigits = append(pad, allDigits...)
		intGroups = 0
		totalGroups = len(allDigits)
	}

	// Weight = number of integer groups - 1
	weight := int16(intGroups - 1)

	// Strip trailing zero groups (PostgreSQL weight handles implicit zeros)
	ndigits := totalGroups
	for ndigits > 0 && allDigits[ndigits-1] == 0 {
		ndigits--
	}

	// Strip leading zero groups (and adjust weight)
	startIdx := 0
	for startIdx < ndigits && allDigits[startIdx] == 0 {
		startIdx++
		weight--
	}
	digits := allDigits[startIdx:ndigits]
	nd := int16(len(digits))

	// Build binary buffer: 8 byte header + 2 bytes per digit
	buf := make([]byte, 8+2*int(nd))
	binary.BigEndian.PutUint16(buf[0:], uint16(nd))
	binary.BigEndian.PutUint16(buf[2:], uint16(weight))
	binary.BigEndian.PutUint16(buf[4:], uint16(sign))
	binary.BigEndian.PutUint16(buf[6:], uint16(dscale))
	for i, d := range digits {
		binary.BigEndian.PutUint16(buf[8+2*i:], uint16(d))
	}
	return buf
}

// encodeArray encodes a []any slice in PostgreSQL binary ARRAY format.
// PostgreSQL binary array format:
//
//	int32 ndim       - number of dimensions (1 for flat arrays)
//	int32 has_null   - 1 if any element is NULL
//	int32 element_oid - OID of the element type
//	int32 dim_len    - length of the dimension
//	int32 dim_lbound - lower bound (always 1)
//	For each element:
//	  int32 len      - byte length of element, or -1 for NULL
//	  bytes data     - element data (absent for NULL)
func encodeArray(v interface{}, elementOID int32) []byte {
	slice, ok := v.([]any)
	if !ok {
		return nil
	}

	// Check for NULLs
	hasNull := int32(0)
	for _, elem := range slice {
		if elem == nil {
			hasNull = 1
			break
		}
	}

	var buf bytes.Buffer
	// Header
	_ = binary.Write(&buf, binary.BigEndian, int32(1))          // ndim = 1
	_ = binary.Write(&buf, binary.BigEndian, hasNull)           // has_null flag
	_ = binary.Write(&buf, binary.BigEndian, elementOID)        // element OID
	_ = binary.Write(&buf, binary.BigEndian, int32(len(slice))) // dimension length
	_ = binary.Write(&buf, binary.BigEndian, int32(1))          // lower bound = 1

	// Elements
	for _, elem := range slice {
		if elem == nil {
			_ = binary.Write(&buf, binary.BigEndian, int32(-1))
		} else {
			data := encodeBinary(elem, elementOID)
			if data == nil {
				_ = binary.Write(&buf, binary.BigEndian, int32(-1))
			} else {
				_ = binary.Write(&buf, binary.BigEndian, int32(len(data)))
				buf.Write(data)
			}
		}
	}

	return buf.Bytes()
}

func encodeText(v interface{}) []byte {
	str := formatValue(v)
	return []byte(str)
}

// encodeJSON re-serializes a Go value to JSON bytes.
// The Go DuckDB driver deserializes JSON columns into native Go types
// (json.Unmarshal: string→string without quotes, object→map, array→slice, etc.).
// We must reverse this to produce valid JSON text for the wire protocol.
func encodeJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// Fallback: encode as text (best effort)
		return encodeText(v)
	}
	return b
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
	case OidNumeric:
		return decodeNumeric(data)
	case OidDate:
		return decodeDate(data)
	case OidTimestamp, OidTimestamptz:
		return decodeTimestamp(data)
	case OidTime:
		return decodeTime(data)
	case OidInterval:
		return decodeInterval(data)
	case OidUUID:
		return decodeUUID(data)
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
	// Use time.Unix to avoid time.Duration overflow for dates far from epoch.
	// time.Duration is int64 nanoseconds, which overflows at ~292 years.
	const pgEpochUnix int64 = 946684800 // 2000-01-01 00:00:00 UTC in Unix seconds
	secs := micros / 1_000_000
	remainMicros := micros % 1_000_000
	if remainMicros < 0 {
		secs--
		remainMicros += 1_000_000
	}
	return time.Unix(pgEpochUnix+secs, remainMicros*1000).UTC(), nil
}

// decodeNumeric decodes PostgreSQL binary numeric format into a string.
func decodeNumeric(data []byte) (string, error) {
	if len(data) < 8 {
		return "", fmt.Errorf("insufficient data for numeric: got %d bytes, need at least 8", len(data))
	}

	ndigits := int(binary.BigEndian.Uint16(data[0:]))
	weight := int16(binary.BigEndian.Uint16(data[2:]))
	sign := binary.BigEndian.Uint16(data[4:])
	dscale := int(binary.BigEndian.Uint16(data[6:]))

	if len(data) < 8+2*ndigits {
		return "", fmt.Errorf("insufficient data for numeric digits: got %d bytes, need %d", len(data), 8+2*ndigits)
	}

	// Special values
	if sign == 0xC000 {
		return "NaN", nil
	}

	// Read base-10000 digits
	digits := make([]int16, ndigits)
	for i := 0; i < ndigits; i++ {
		digits[i] = int16(binary.BigEndian.Uint16(data[8+2*i:]))
	}

	// Reconstruct the unscaled value
	val := new(big.Int)
	base := big.NewInt(10000)
	for _, d := range digits {
		val.Mul(val, base)
		val.Add(val, big.NewInt(int64(d)))
	}

	// The digits represent: sum(digits[i] * 10000^(weight-i))
	// After reading all ndigits groups, we have val * 10000^(weight - ndigits + 1)
	// We need to convert to a fixed-point number with dscale decimal places.
	// Exponent in terms of decimal digits: 4 * (weight - ndigits + 1)
	exp := 4 * (int(weight) - ndigits + 1)

	// We want the value as: val * 10^exp, displayed with dscale decimal places.
	// Shift so we have dscale fractional digits: multiply by 10^(dscale + exp)
	shift := dscale + exp
	if shift > 0 {
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(shift)), nil)
		val.Mul(val, factor)
	} else if shift < 0 {
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-shift)), nil)
		val.Div(val, factor)
	}

	// Now val is the unscaled integer with dscale implied decimal places
	negative := sign == 0x4000
	if negative {
		val.Neg(val)
	}

	// Format as decimal string
	str := val.String()
	if dscale == 0 {
		return str, nil
	}

	isNeg := false
	if len(str) > 0 && str[0] == '-' {
		isNeg = true
		str = str[1:]
	}

	// Pad with leading zeros if needed
	for len(str) <= dscale {
		str = "0" + str
	}

	intPart := str[:len(str)-dscale]
	fracPart := str[len(str)-dscale:]

	result := intPart + "." + fracPart
	if isNeg {
		result = "-" + result
	}
	return result, nil
}
