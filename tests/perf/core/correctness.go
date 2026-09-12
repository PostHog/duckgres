package core

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

// ResultReader collects complete result values only for the untimed correctness
// gate. Nil cells retain SQL NULL, distinct from empty strings and literal null.
type ResultReader interface {
	ReadResults(context.Context, Query, []any) ([][]*string, error)
}

func (r *QueryRunner) validateRepresentations(ctx context.Context) error {
	baselines := map[string][][]*string{}
	// Read every JSON baseline first, including each engine/storage implementation.
	for _, representation := range []string{"json", "other"} {
		for _, protocol := range r.cfg.Catalog.Targets {
			for _, q := range r.cfg.Catalog.Queries {
				if q.Representation == "" || !querySupportsProtocol(q, protocol) || (q.Representation == "json") != (representation == "json") {
					continue
				}
				reader, ok := r.cfg.Drivers[protocol].(ResultReader)
				if !ok {
					return fmt.Errorf("correctness gate: %s driver cannot read result values", protocol)
				}
				expected, exists := baselines[q.IntentID]
				if representation != "json" && !exists {
					return fmt.Errorf("correctness gate: intent %s has no JSON baseline", q.IntentID)
				}
				actual, err := reader.ReadResults(ctx, q, orderedParamValues(q.Params))
				if err != nil {
					return fmt.Errorf("correctness gate (%s/%s): %w", protocol, q.QueryID, err)
				}
				// Avoid including raw result values in diagnostics or public artifacts.
				if exists && !reflect.DeepEqual(expected, actual) {
					return fmt.Errorf("correctness gate (%s/%s): complete ordered results differ from JSON baseline for intent %s", protocol, q.QueryID, q.IntentID)
				}
				if !exists {
					baselines[q.IntentID] = actual
				}
			}
		}
	}
	return nil
}

// ReadSQLResults normalizes driver string/byte/integer representations through
// database/sql conversion without conflating SQL NULL with textual values.
func ReadSQLResults(rows *sql.Rows) ([][]*string, error) {
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	results := make([][]*string, 0)
	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		row := make([]*string, len(values))
		for i, value := range values {
			if value.Valid {
				s := value.String
				row[i] = &s
			}
		}
		results = append(results, row)
	}
	return results, rows.Err()
}
