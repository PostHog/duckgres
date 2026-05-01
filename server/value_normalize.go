package server

import "sync/atomic"

// ValueNormalizer is a hook that converts driver-specific value types
// (e.g., duckdb.Interval, duckdb.Decimal) into the duckdb-free equivalents
// in arrowmap (IntervalValue, DecimalValue) so the binary-format encoders
// in types.go can handle them without importing the duckdb-go driver.
//
// A normalizer must return the input unchanged when it doesn't recognize
// the type; the encode helpers fall back to AppendNull/return nil when
// the final value still isn't a recognized type.
type ValueNormalizer func(any) any

// driverNormalizers is loaded once into an atomic.Value as []ValueNormalizer.
// Reads on the hot path (every encoded row value) are lock-free; registration
// happens at init time so contention is rare.
var driverNormalizers atomic.Value // []ValueNormalizer

// RegisterValueNormalizer adds a hook consulted by normalizeDriverValue
// before the binary encoders dispatch on the value's type. Intended for use
// from init() in importers that own driver-specific value types — duckdbservice
// registers a normalizer that converts duckdb.Interval and duckdb.Decimal to
// their arrowmap equivalents.
func RegisterValueNormalizer(n ValueNormalizer) {
	if n == nil {
		return
	}
	cur, _ := driverNormalizers.Load().([]ValueNormalizer)
	next := make([]ValueNormalizer, 0, len(cur)+1)
	next = append(next, cur...)
	next = append(next, n)
	driverNormalizers.Store(next)
}

// normalizeDriverValue runs every registered ValueNormalizer over the value.
// Each normalizer returns the input unchanged when it doesn't claim it; the
// final value is whatever the chain produced (or the original input if no
// normalizers ran or none matched).
func normalizeDriverValue(v any) any {
	hooks, _ := driverNormalizers.Load().([]ValueNormalizer)
	for _, h := range hooks {
		v = h(v)
	}
	return v
}
