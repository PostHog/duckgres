package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
)

// CompareResult holds the result of comparing two query results
type CompareResult struct {
	Match       bool
	Differences []string
	PGRowCount  int
	DGRowCount  int
	PGColumns   []string
	DGColumns   []string
	PGError     error
	DGError     error
}

// CompareOptions configures comparison behavior
type CompareOptions struct {
	// IgnoreColumnOrder ignores the order of columns (compare by name)
	IgnoreColumnOrder bool
	// IgnoreRowOrder ignores the order of rows (compare as sets)
	IgnoreRowOrder bool
	// IgnoreColumnNames ignores column name differences (only compare data)
	IgnoreColumnNames bool
	// FloatTolerance is the tolerance for floating point comparisons
	FloatTolerance float64
	// TimeTolerance is the tolerance for timestamp comparisons
	TimeTolerance time.Duration
	// IgnoreCase ignores case differences in string comparisons
	IgnoreCase bool
	// IgnoreWhitespace trims whitespace before comparing strings
	IgnoreWhitespace bool
	// AllowNullEquality treats NULL == NULL as true for comparisons
	AllowNullEquality bool
	// IgnoreColumns lists columns to skip in comparison
	IgnoreColumns []string
}

// DefaultCompareOptions returns sensible defaults for comparison
func DefaultCompareOptions() CompareOptions {
	return CompareOptions{
		IgnoreColumnOrder: false,
		IgnoreRowOrder:    true, // Row order is undefined without ORDER BY
		IgnoreColumnNames: true, // DuckDB names anonymous columns differently than PostgreSQL
		FloatTolerance:    1e-9,
		TimeTolerance:     time.Microsecond,
		IgnoreCase:        false,
		IgnoreWhitespace:  true,
		AllowNullEquality: true,
		IgnoreColumns:     nil,
	}
}

// QueryResult holds the result of executing a query
type QueryResult struct {
	Columns []string
	Rows    [][]interface{}
	Error   error
}

// ExecuteQuery executes a query and returns the result
func ExecuteQuery(db *sql.DB, query string) (*QueryResult, error) {
	rows, err := db.Query(query)
	if err != nil {
		return &QueryResult{Error: err}, nil
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var result [][]interface{}
	for rows.Next() {
		// Use sql.RawBytes to completely avoid driver parsing issues
		rawValues := make([]sql.RawBytes, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range rawValues {
			valuePtrs[i] = &rawValues[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert RawBytes to appropriate types
		values := make([]interface{}, len(columns))
		for i := range rawValues {
			if rawValues[i] == nil {
				values[i] = nil
			} else {
				values[i] = parseValue(string(rawValues[i]))
			}
		}

		result = append(result, values)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return &QueryResult{
		Columns: columns,
		Rows:    result,
	}, nil
}

// parseValue attempts to parse a string value into an appropriate Go type
func parseValue(s string) interface{} {
	// Empty string
	if s == "" {
		return ""
	}

	// Boolean
	if s == "t" || s == "true" || s == "TRUE" {
		return true
	}
	if s == "f" || s == "false" || s == "FALSE" {
		return false
	}

	// Integer - only if the string is purely numeric
	var i int64
	if _, err := fmt.Sscanf(s, "%d", &i); err == nil && fmt.Sprintf("%d", i) == s {
		return i
	}

	// Float - only if it contains decimal point or scientific notation
	if strings.Contains(s, ".") || strings.ContainsAny(s, "eE") {
		var f float64
		if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
			return f
		}
	}

	// Timestamp formats - try multiple layouts
	timestampLayouts := []string{
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999+00",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05-07",
		"2006-01-02 15:04:05+00",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05 +0000 UTC",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.999999Z",
		"2006-01-02",
		"15:04:05",
		"15:04:05.999999",
	}
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}

	// Return as string if no other type matched
	return s
}

// CompareQueries executes a query on both databases and compares results
func CompareQueries(pgDB, dgDB *sql.DB, query string, opts CompareOptions) *CompareResult {
	pgResult, pgErr := ExecuteQuery(pgDB, query)
	dgResult, dgErr := ExecuteQuery(dgDB, query)

	result := &CompareResult{
		PGError: pgErr,
		DGError: dgErr,
	}

	// Handle execution errors
	if pgErr != nil {
		result.PGError = pgErr
	} else if pgResult.Error != nil {
		result.PGError = pgResult.Error
	}
	if dgErr != nil {
		result.DGError = dgErr
	} else if dgResult.Error != nil {
		result.DGError = dgResult.Error
	}

	// If both have errors, that's a match (both failed)
	if result.PGError != nil && result.DGError != nil {
		result.Match = true
		return result
	}

	// If only one has an error, that's a mismatch
	if result.PGError != nil || result.DGError != nil {
		result.Match = false
		if result.PGError != nil {
			result.Differences = append(result.Differences, fmt.Sprintf("PostgreSQL error: %v", result.PGError))
		}
		if result.DGError != nil {
			result.Differences = append(result.Differences, fmt.Sprintf("Duckgres error: %v", result.DGError))
		}
		return result
	}

	result.PGColumns = pgResult.Columns
	result.DGColumns = dgResult.Columns
	result.PGRowCount = len(pgResult.Rows)
	result.DGRowCount = len(dgResult.Rows)

	// Compare results
	result.Match, result.Differences = compareResults(pgResult, dgResult, opts)
	return result
}

// compareResults compares two query results
func compareResults(pg, dg *QueryResult, opts CompareOptions) (bool, []string) {
	var diffs []string

	// Compare column count
	if len(pg.Columns) != len(dg.Columns) {
		diffs = append(diffs, fmt.Sprintf("Column count mismatch: PostgreSQL=%d, Duckgres=%d", len(pg.Columns), len(dg.Columns)))
		return false, diffs
	}

	// Compare column names (skip if IgnoreColumnNames is set)
	if !opts.IgnoreColumnNames {
		pgCols := pg.Columns
		dgCols := dg.Columns
		if opts.IgnoreColumnOrder {
			pgCols = sortedCopy(pgCols)
			dgCols = sortedCopy(dgCols)
		}
		for i := range pgCols {
			pgCol := pgCols[i]
			dgCol := dgCols[i]
			if opts.IgnoreCase {
				pgCol = strings.ToLower(pgCol)
				dgCol = strings.ToLower(dgCol)
			}
			if pgCol != dgCol {
				diffs = append(diffs, fmt.Sprintf("Column name mismatch at position %d: PostgreSQL=%q, Duckgres=%q", i, pg.Columns[i], dg.Columns[i]))
			}
		}
	}

	// Compare row count
	if len(pg.Rows) != len(dg.Rows) {
		diffs = append(diffs, fmt.Sprintf("Row count mismatch: PostgreSQL=%d, Duckgres=%d", len(pg.Rows), len(dg.Rows)))
		return false, diffs
	}

	if len(pg.Rows) == 0 {
		return len(diffs) == 0, diffs
	}

	// Build column mapping (for when column order differs)
	colMapping := make(map[int]int) // dg index -> pg index
	if opts.IgnoreColumnOrder {
		for i, dgCol := range dg.Columns {
			for j, pgCol := range pg.Columns {
				if strings.EqualFold(dgCol, pgCol) {
					colMapping[i] = j
					break
				}
			}
		}
	} else {
		for i := range pg.Columns {
			colMapping[i] = i
		}
	}

	// Compare rows
	pgRows := pg.Rows
	dgRows := dg.Rows
	if opts.IgnoreRowOrder {
		pgRows = sortRows(pgRows)
		dgRows = sortRows(dgRows)
	}

	for rowIdx := range pgRows {
		pgRow := pgRows[rowIdx]
		dgRow := dgRows[rowIdx]

		for colIdx := range dgRow {
			pgColIdx := colMapping[colIdx]
			if pgColIdx >= len(pgRow) {
				continue
			}

			// Skip ignored columns
			colName := dg.Columns[colIdx]
			if contains(opts.IgnoreColumns, colName) {
				continue
			}

			pgVal := pgRow[pgColIdx]
			dgVal := dgRow[colIdx]

			if !valuesEqual(pgVal, dgVal, opts) {
				diffs = append(diffs, fmt.Sprintf("Row %d, column %q: PostgreSQL=%v (%T), Duckgres=%v (%T)",
					rowIdx, colName, pgVal, pgVal, dgVal, dgVal))
			}
		}
	}

	return len(diffs) == 0, diffs
}

// valuesEqual compares two values with tolerance
func valuesEqual(pg, dg interface{}, opts CompareOptions) bool {
	// Handle NULL
	if pg == nil && dg == nil {
		return opts.AllowNullEquality
	}
	if pg == nil || dg == nil {
		return false
	}

	// Try to normalize types and compare
	pgNorm := normalizeValue(pg)
	dgNorm := normalizeValue(dg)

	// String comparison
	if pgStr, ok := pgNorm.(string); ok {
		if dgStr, ok := dgNorm.(string); ok {
			if opts.IgnoreWhitespace {
				pgStr = strings.TrimSpace(pgStr)
				dgStr = strings.TrimSpace(dgStr)
			}
			if opts.IgnoreCase {
				return strings.EqualFold(pgStr, dgStr)
			}
			return pgStr == dgStr
		}
	}

	// Float comparison with tolerance
	if pgFloat, ok := toFloat64(pgNorm); ok {
		if dgFloat, ok := toFloat64(dgNorm); ok {
			if math.IsNaN(pgFloat) && math.IsNaN(dgFloat) {
				return true
			}
			if math.IsInf(pgFloat, 1) && math.IsInf(dgFloat, 1) {
				return true
			}
			if math.IsInf(pgFloat, -1) && math.IsInf(dgFloat, -1) {
				return true
			}
			return math.Abs(pgFloat-dgFloat) <= opts.FloatTolerance
		}
	}

	// Time comparison with tolerance
	if pgTime, ok := pgNorm.(time.Time); ok {
		if dgTime, ok := dgNorm.(time.Time); ok {
			diff := pgTime.Sub(dgTime)
			if diff < 0 {
				diff = -diff
			}
			return diff <= opts.TimeTolerance
		}
	}

	// Boolean comparison
	if pgBool, ok := pgNorm.(bool); ok {
		if dgBool, ok := dgNorm.(bool); ok {
			return pgBool == dgBool
		}
	}

	// JSON comparison
	if isJSON(pgNorm) && isJSON(dgNorm) {
		return jsonEqual(pgNorm, dgNorm)
	}

	// Array/slice comparison
	if reflect.TypeOf(pgNorm) != nil && reflect.TypeOf(pgNorm).Kind() == reflect.Slice {
		if reflect.TypeOf(dgNorm) != nil && reflect.TypeOf(dgNorm).Kind() == reflect.Slice {
			return slicesEqual(pgNorm, dgNorm, opts)
		}
	}

	// Default comparison
	return reflect.DeepEqual(pgNorm, dgNorm)
}

// normalizeValue normalizes a value for comparison
func normalizeValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case []byte:
		return string(val)
	case int:
		return int64(val)
	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case uint:
		return int64(val)
	case uint8:
		return int64(val)
	case uint16:
		return int64(val)
	case uint32:
		return int64(val)
	case uint64:
		return int64(val)
	case float32:
		return float64(val)
	default:
		return val
	}
}

// toFloat64 attempts to convert a value to float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	case string:
		// Try parsing as float
		var f float64
		if _, err := fmt.Sscanf(val, "%f", &f); err == nil {
			return f, true
		}
	}
	return 0, false
}

// isJSON checks if a value looks like JSON
func isJSON(v interface{}) bool {
	str, ok := v.(string)
	if !ok {
		return false
	}
	str = strings.TrimSpace(str)
	return (strings.HasPrefix(str, "{") && strings.HasSuffix(str, "}")) ||
		(strings.HasPrefix(str, "[") && strings.HasSuffix(str, "]"))
}

// jsonEqual compares two JSON values
func jsonEqual(a, b interface{}) bool {
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)
	if !aOk || !bOk {
		return false
	}

	var aVal, bVal interface{}
	if err := json.Unmarshal([]byte(aStr), &aVal); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(bStr), &bVal); err != nil {
		return false
	}

	return reflect.DeepEqual(aVal, bVal)
}

// slicesEqual compares two slices
func slicesEqual(a, b interface{}, opts CompareOptions) bool {
	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	if aVal.Len() != bVal.Len() {
		return false
	}

	for i := 0; i < aVal.Len(); i++ {
		if !valuesEqual(aVal.Index(i).Interface(), bVal.Index(i).Interface(), opts) {
			return false
		}
	}
	return true
}

// sortRows sorts rows for order-independent comparison
func sortRows(rows [][]interface{}) [][]interface{} {
	result := make([][]interface{}, len(rows))
	copy(result, rows)
	sort.Slice(result, func(i, j int) bool {
		for col := 0; col < len(result[i]) && col < len(result[j]); col++ {
			cmp := compareValues(result[i][col], result[j][col])
			if cmp != 0 {
				return cmp < 0
			}
		}
		return len(result[i]) < len(result[j])
	})
	return result
}

// compareValues compares two values for sorting
func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison first
	if aNum, aOk := toFloat64(a); aOk {
		if bNum, bOk := toFloat64(b); bOk {
			if aNum < bNum {
				return -1
			}
			if aNum > bNum {
				return 1
			}
			return 0
		}
	}

	// Try time comparison
	if aTime, aOk := a.(time.Time); aOk {
		if bTime, bOk := b.(time.Time); bOk {
			if aTime.Before(bTime) {
				return -1
			}
			if aTime.After(bTime) {
				return 1
			}
			return 0
		}
	}

	// Fall back to string comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}
	return 0
}

// sortedCopy returns a sorted copy of a string slice
func sortedCopy(s []string) []string {
	result := make([]string, len(s))
	copy(result, s)
	sort.Strings(result)
	return result
}

// contains checks if a string slice contains a value
func contains(slice []string, val string) bool {
	for _, s := range slice {
		if strings.EqualFold(s, val) {
			return true
		}
	}
	return false
}

// QueryTest represents a single query test case
type QueryTest struct {
	Name         string         // Test name
	Query        string         // SQL query to execute
	Skip         string         // If non-empty, skip with this reason
	DuckgresOnly bool           // Only run on Duckgres (no PostgreSQL comparison)
	ExpectError  bool           // Expect query to fail
	Options      CompareOptions // Custom comparison options
}

// SkipReason constants for common skip reasons
const (
	SkipUnsupportedByDuckDB = "not supported by DuckDB"
	SkipDifferentBehavior   = "intentionally different behavior"
	SkipOLTPFeature         = "OLTP feature - out of scope"
	SkipNotImplemented      = "not yet implemented in Duckgres"
	SkipDuckDBLimitation    = "DuckDB limitation"
	SkipPostgresSpecific    = "PostgreSQL-specific feature"
	SkipRequiresExtension   = "requires PostgreSQL extension"
	SkipNetworkType         = "network types not supported"
	SkipGeometricType       = "geometric types not supported"
	SkipRangeType           = "range types not supported"
	SkipTextSearch          = "full text search not supported"
)
