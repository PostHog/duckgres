package duckdbservice

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/posthog/duckgres/duckdbservice/arrowmap"
)

// init registers handlers for the duckdb-go driver value types so that any
// caller (including arrowmap.AppendValue and the wrapper duckdbservice.AppendValue)
// gets full type coverage when this package is linked into the binary.
//
// Binaries that don't link duckdbservice (e.g., a future control-plane-only
// binary) won't see these registrations — which is correct, because they
// also won't be the ones scanning rows from a duckdb-go driver connection.
func init() {
	arrowmap.RegisterAppender(handleDuckDBValue)
}

// handleDuckDBValue implements arrowmap.Appender for duckdb-go's driver
// value types. Returns true when it claimed the value, false to fall
// through to arrowmap's built-in handling.
func handleDuckDBValue(builder array.Builder, val any) bool {
	switch b := builder.(type) {
	case *array.MonthDayNanoIntervalBuilder:
		v, ok := val.(duckdb.Interval)
		if !ok {
			return false
		}
		b.Append(arrow.MonthDayNanoInterval{
			Months:      v.Months,
			Days:        v.Days,
			Nanoseconds: v.Micros * 1000,
		})
		return true
	case *array.Decimal128Builder:
		v, ok := val.(duckdb.Decimal)
		if !ok {
			return false
		}
		b.Append(decimal128.FromBigInt(v.Value))
		return true
	case *array.FixedSizeBinaryBuilder:
		v, ok := val.(duckdb.UUID)
		if !ok {
			return false
		}
		b.Append(v[:])
		return true
	case *array.MapBuilder:
		switch v := val.(type) {
		case duckdb.OrderedMap:
			b.Append(true)
			kb, ib := b.KeyBuilder(), b.ItemBuilder()
			keys, values := v.Keys(), v.Values()
			for i, k := range keys {
				arrowmap.AppendValue(kb, k)
				arrowmap.AppendValue(ib, values[i])
			}
			return true
		case duckdb.Map:
			b.Append(true)
			kb, ib := b.KeyBuilder(), b.ItemBuilder()
			for k, item := range v {
				arrowmap.AppendValue(kb, k)
				arrowmap.AppendValue(ib, item)
			}
			return true
		}
		return false
	case *array.StringBuilder:
		v, ok := val.(duckdb.UUID)
		if !ok {
			return false
		}
		b.Append(v.String())
		return true
	}
	return false
}
