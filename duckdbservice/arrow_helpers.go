package duckdbservice

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/duckdbservice/arrowmap"
	"github.com/posthog/duckgres/server"
)

// DuckDBTypeToArrow re-exports arrowmap.DuckDBTypeToArrow for backward
// compatibility with existing callers in this package.
var DuckDBTypeToArrow = arrowmap.DuckDBTypeToArrow

// QualifyTableName re-exports arrowmap.QualifyTableName for backward
// compatibility with existing callers in this package.
var QualifyTableName = arrowmap.QualifyTableName

// AppendValue is the only helper in this package that still needs to live with
// the duckdb-go import (because it switches on duckdb.Interval / Decimal /
// UUID / OrderedMap / Map types).

// RowsToRecord converts sql.Rows into an Arrow RecordBatch of up to batchSize rows.
// Returns nil when there are no more rows.
func RowsToRecord(alloc memory.Allocator, rows *sql.Rows, schema *arrow.Schema, batchSize int) (arrow.RecordBatch, error) {
	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	numFields := schema.NumFields()
	count := 0
	for rows.Next() && count < batchSize {
		values := make([]interface{}, numFields)
		valuePtrs := make([]interface{}, numFields)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		for i, val := range values {
			AppendValue(builder.Field(i), val)
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, nil
	}
	return builder.NewRecordBatch(), nil
}

// AppendValue appends a value to an Arrow array builder with type coercion.
func AppendValue(builder array.Builder, val interface{}) {
	if val == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int32:
			b.Append(int64(v))
		case int:
			b.Append(int64(v))
		default:
			b.AppendNull()
		}
	case *array.Int32Builder:
		switch v := val.(type) {
		case int32:
			b.Append(v)
		case int64:
			b.Append(int32(v))
		case int:
			b.Append(int32(v))
		default:
			b.AppendNull()
		}
	case *array.Int16Builder:
		switch v := val.(type) {
		case int16:
			b.Append(v)
		case int32:
			b.Append(int16(v))
		default:
			b.AppendNull()
		}
	case *array.Int8Builder:
		switch v := val.(type) {
		case int8:
			b.Append(v)
		case int32:
			b.Append(int8(v))
		default:
			b.AppendNull()
		}
	case *array.Uint8Builder:
		switch v := val.(type) {
		case uint8:
			b.Append(v)
		case uint16:
			b.Append(uint8(v))
		default:
			b.AppendNull()
		}
	case *array.Uint16Builder:
		switch v := val.(type) {
		case uint16:
			b.Append(v)
		case uint32:
			b.Append(uint16(v))
		default:
			b.AppendNull()
		}
	case *array.Uint32Builder:
		switch v := val.(type) {
		case uint32:
			b.Append(v)
		case uint64:
			b.Append(uint32(v))
		default:
			b.AppendNull()
		}
	case *array.Uint64Builder:
		switch v := val.(type) {
		case uint64:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch v := val.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		default:
			b.AppendNull()
		}
	case *array.Float32Builder:
		switch v := val.(type) {
		case float32:
			b.Append(v)
		case float64:
			b.Append(float32(v))
		default:
			b.AppendNull()
		}
	case *array.BooleanBuilder:
		if v, ok := val.(bool); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Date32Builder:
		switch v := val.(type) {
		case time.Time:
			// Floor division to handle pre-epoch dates correctly.
			// Go's integer division truncates toward zero, but Date32
			// needs days since epoch rounded toward negative infinity.
			unix := v.Unix()
			days := unix / 86400
			if unix%86400 < 0 {
				days--
			}
			b.Append(arrow.Date32(days))
		default:
			b.AppendNull()
		}
	case *array.TimestampBuilder:
		switch v := val.(type) {
		case time.Time:
			b.AppendTime(v)
		default:
			b.AppendNull()
		}
	case *array.Time64Builder:
		switch v := val.(type) {
		case time.Time:
			micros := int64(v.Hour())*3600000000 + int64(v.Minute())*60000000 +
				int64(v.Second())*1000000 + int64(v.Nanosecond())/1000
			b.Append(arrow.Time64(micros))
		default:
			b.AppendNull()
		}
	case *array.MonthDayNanoIntervalBuilder:
		switch v := val.(type) {
		case duckdb.Interval:
			b.Append(arrow.MonthDayNanoInterval{
				Months:      v.Months,
				Days:        v.Days,
				Nanoseconds: v.Micros * 1000,
			})
		default:
			b.AppendNull()
		}
	case *array.Decimal128Builder:
		switch v := val.(type) {
		case duckdb.Decimal:
			b.Append(decimal128.FromBigInt(v.Value))
		case *big.Int:
			b.Append(decimal128.FromBigInt(v))
		default:
			b.AppendNull()
		}
	case *array.FixedSizeBinaryBuilder:
		switch v := val.(type) {
		case duckdb.UUID:
			b.Append(v[:])
		case []byte:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.ListBuilder:
		switch v := val.(type) {
		case []any:
			b.Append(true)
			vb := b.ValueBuilder()
			for _, elem := range v {
				AppendValue(vb, elem)
			}
		default:
			b.AppendNull()
		}
	case *array.StructBuilder:
		switch v := val.(type) {
		case map[string]any:
			b.Append(true)
			st := b.Type().(*arrow.StructType)
			for i := 0; i < st.NumFields(); i++ {
				fieldVal, ok := v[st.Field(i).Name]
				if !ok {
					b.FieldBuilder(i).AppendNull()
				} else {
					AppendValue(b.FieldBuilder(i), fieldVal)
				}
			}
		default:
			b.AppendNull()
		}
	case *array.MapBuilder:
		switch v := val.(type) {
		case duckdb.OrderedMap:
			b.Append(true)
			kb, ib := b.KeyBuilder(), b.ItemBuilder()
			keys, values := v.Keys(), v.Values()
			for i, k := range keys {
				AppendValue(kb, k)
				AppendValue(ib, values[i])
			}
		case duckdb.Map:
			b.Append(true)
			kb, ib := b.KeyBuilder(), b.ItemBuilder()
			for k, item := range v {
				AppendValue(kb, k)
				AppendValue(ib, item)
			}
		case map[any]any:
			b.Append(true)
			kb, ib := b.KeyBuilder(), b.ItemBuilder()
			for k, item := range v {
				AppendValue(kb, k)
				AppendValue(ib, item)
			}
		case server.OrderedMapValue:
			b.Append(true)
			kb, ib := b.KeyBuilder(), b.ItemBuilder()
			for i, k := range v.Keys {
				AppendValue(kb, k)
				AppendValue(ib, v.Values[i])
			}
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		case duckdb.UUID:
			b.Append(v.String())
		case []byte:
			// TODO: This heuristic (16 bytes + invalid UTF-8 → UUID) is coupled to
			// DuckDBTypeToArrow("UUID") returning String. If UUID mapping changes,
			// update this branch accordingly. The Go driver returns []byte (not
			// duckdb.UUID) when scanning UUID columns into interface{}.
			if len(v) == 16 && !utf8.Valid(v) {
				s := hex.EncodeToString(v)
				b.Append(s[0:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:32])
			} else {
				b.Append(string(v))
			}
		default:
			b.Append(fmt.Sprintf("%v", v))
		}
	case *array.BinaryBuilder:
		switch v := val.(type) {
		case []byte:
			b.Append(v)
		case string:
			b.Append([]byte(v))
		default:
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

type contextQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func isNil(i contextQueryer) bool {
	if i == nil {
		return true
	}
	v := reflect.ValueOf(i)
	return v.Kind() == reflect.Ptr && v.IsNil()
}

// GetQuerySchema executes a query with LIMIT 0 to discover the result schema.
func GetQuerySchema(ctx context.Context, db contextQueryer, query string, tx contextQueryer) (*arrow.Schema, error) {
	q := strings.TrimRight(strings.TrimSpace(query), ";")
	queryWithLimit := q
	upper := strings.ToUpper(q)
	// Only append LIMIT 0 for SELECT/WITH/VALUES/TABLE statements.
	// SHOW, DESCRIBE, EXPLAIN, PRAGMA, CALL etc. don't support LIMIT.
	if !strings.Contains(upper, "LIMIT") && arrowmap.SupportsLimit(upper) {
		queryWithLimit = q + " LIMIT 0"
	}
	var rows *sql.Rows
	var err error
	if !isNil(tx) {
		rows, err = tx.QueryContext(ctx, queryWithLimit)
	} else {
		rows, err = db.QueryContext(ctx, queryWithLimit)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, len(colTypes))
	for i, ct := range colTypes {
		fields[i] = arrow.Field{Name: ct.Name(), Type: DuckDBTypeToArrow(ct.DatabaseTypeName()), Nullable: true}
	}
	return arrow.NewSchema(fields, nil), nil
}

// QuoteIdent re-exports arrowmap.QuoteIdent for backward compatibility.
var QuoteIdent = arrowmap.QuoteIdent
