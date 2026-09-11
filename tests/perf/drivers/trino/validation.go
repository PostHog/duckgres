package trino

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"time"

	trinoclient "github.com/trinodb/trino-go-client/trino"
)

type resultExecutor interface {
	Executor
	ReadResult(context.Context, string, []any) (resultDigest, error)
}

// A multiset preserves duplicate rows while allowing arbitrary SQL result order.
// Hashing avoids retaining result values, which may be large or sensitive.
type resultDigest struct {
	columns int
	count   int64
	rows    map[[32]byte]int64
}

func (r *resultDigest) add(values []any) error {
	encoded, err := encodeValues(values)
	if err != nil {
		return err
	}
	r.rows[sha256.Sum256(encoded)]++
	r.count++
	return nil
}

func (r resultDigest) equal(other resultDigest) bool {
	return r.columns == other.columns && r.count == other.count && reflect.DeepEqual(r.rows, other.rows)
}

func encodeValues(values []any) ([]byte, error) {
	normalized := make([]any, len(values))
	for i, value := range values {
		normalized[i] = canonicalValue(value)
	}
	return json.Marshal(normalized)
}

func canonicalValue(value any) any {
	switch v := value.(type) {
	case time.Time:
		return []any{"time", v.UTC().Format(time.RFC3339Nano)}
	case float64:
		// JSON cannot represent NaN or infinities. Also normalize signed zero.
		if math.IsNaN(v) {
			return []any{"float64", "NaN"}
		}
		if math.IsInf(v, 1) {
			return []any{"float64", "+Inf"}
		}
		if math.IsInf(v, -1) {
			return []any{"float64", "-Inf"}
		}
		if v == 0 {
			v = 0
		}
		return []any{"float64", v}
	case []any:
		result := make([]any, len(v))
		for i, item := range v {
			result[i] = canonicalValue(item)
		}
		return []any{"array", result}
	case map[string]any:
		result := make(map[string]any, len(v))
		for key, item := range v {
			result[key] = canonicalValue(item)
		}
		return []any{"map", result}
	default:
		return []any{fmt.Sprintf("%T", value), value}
	}
}

func (d *Driver) validate(ctx context.Context, query string, args []any) error {
	d.validationMu.Lock()
	defer d.validationMu.Unlock()
	encodedArgs, err := encodeValues(args)
	if err != nil {
		return fmt.Errorf("cannot encode correctness validation arguments")
	}
	key := query + "\x00" + string(encodedArgs)
	if d.validated[key] {
		return nil
	}
	target, ok := d.exec.(resultExecutor)
	if !ok {
		return fmt.Errorf("hoglake executor cannot validate query results")
	}
	actual, err := target.ReadResult(ctx, query, args)
	if err != nil {
		return safeValidationError("hoglake correctness validation query", err)
	}
	expected, err := d.reference.ReadResult(ctx, query, args)
	if err != nil {
		return safeValidationError("reference correctness validation query", err)
	}
	if !actual.equal(expected) {
		return fmt.Errorf("hoglake correctness validation result mismatch")
	}
	if d.validated == nil {
		d.validated = make(map[string]bool)
	}
	d.validated[key] = true
	return nil
}

func (e *sqlExecutor) ReadResult(ctx context.Context, query string, args []any) (resultDigest, error) {
	result := resultDigest{rows: make(map[[32]byte]int64)}
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return result, err
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return result, err
	}
	result.columns = len(columns)
	values := make([]any, len(columns))
	pointers := make([]any, len(columns))
	for i := range values {
		pointers[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return result, err
		}
		if err := result.add(values); err != nil {
			return result, err
		}
	}
	return result, rows.Err()
}

func validateCatalogs(ctx context.Context, lookup func(context.Context, string) (string, error), target, reference string) error {
	for _, expected := range []struct{ catalog, connector, label string }{{target, "hoglake", "target"}, {reference, "ducklake", "reference"}} {
		actual, err := lookup(ctx, expected.catalog)
		if err != nil {
			return safeValidationError(expected.label+" catalog identity query", err)
		}
		if actual != expected.connector {
			return fmt.Errorf("%s catalog must use the %s connector", expected.label, expected.connector)
		}
	}
	return nil
}

var safeErrorIdentifier = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)

func safeValidationError(stage string, err error) error {
	var trinoErr *trinoclient.ErrTrino
	if errors.As(err, &trinoErr) {
		name, kind := trinoErr.ErrorName, trinoErr.ErrorType
		if !safeErrorIdentifier.MatchString(name) {
			name = "UNKNOWN"
		}
		if !safeErrorIdentifier.MatchString(kind) {
			kind = "UNKNOWN"
		}
		return fmt.Errorf("%s failed: %s %s code=%d", stage, kind, name, trinoErr.ErrorCode)
	}
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s canceled", stage)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s timed out", stage)
	}
	return fmt.Errorf("%s failed", stage)
}
