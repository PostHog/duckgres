package sqlcore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const preparedStatementCleanupTimeout = 5 * time.Second

var resultColumnCountStatementSequence atomic.Uint64

// preparedStatementResultColumnCountExecutor is intentionally smaller than
// QueryExecutor so the helper can be reused by local, pinned, and Flight
// executors without coupling it to connection-management details.
type preparedStatementResultColumnCountExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (ExecResult, error)
	QueryContext(ctx context.Context, query string, args ...any) (RowSet, error)
}

// PreparedStatementResultColumnCount obtains a statement's result-column
// count through DuckDB's SQL PREPARE metadata. PREPARE binds and plans the
// statement without executing it; duckdb_prepared_statements() exposes the
// result type list produced by that planning step.
//
// The caller must pass a single statement that is safe to prepare. The helper
// always attempts DEALLOCATE after a successful PREPARE, including when the
// metadata lookup or scan fails, so its temporary session state does not leak.
func PreparedStatementResultColumnCount(ctx context.Context, executor preparedStatementResultColumnCountExecutor, query string) (count int, err error) {
	query = strings.TrimSpace(query)
	query = strings.TrimSpace(strings.TrimSuffix(query, ";"))
	if query == "" {
		return 0, errors.New("cannot determine result column count for an empty query")
	}

	name := fmt.Sprintf("__duckgres_result_columns_%d", resultColumnCountStatementSequence.Add(1))
	quotedName := quotePreparedStatementIdentifier(name)
	if _, err = executor.ExecContext(ctx, "PREPARE "+quotedName+" AS "+query); err != nil {
		return 0, fmt.Errorf("prepare result column metadata: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), preparedStatementCleanupTimeout)
		defer cancel()
		if _, cleanupErr := executor.ExecContext(cleanupCtx, "DEALLOCATE "+quotedName); cleanupErr != nil {
			cleanupErr = fmt.Errorf("deallocate result column metadata: %w", cleanupErr)
			if err == nil {
				err = cleanupErr
			} else {
				err = errors.Join(err, cleanupErr)
			}
		}
	}()

	rows, err := executor.QueryContext(ctx,
		"SELECT array_length(result_types) FROM duckdb_prepared_statements() WHERE name = ?", name)
	if err != nil {
		return 0, fmt.Errorf("query result column metadata: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			closeErr = fmt.Errorf("close result column metadata: %w", closeErr)
			if err == nil {
				err = closeErr
			} else {
				err = errors.Join(err, closeErr)
			}
		}
	}()

	if !rows.Next() {
		if rowsErr := rows.Err(); rowsErr != nil {
			return 0, fmt.Errorf("read result column metadata: %w", rowsErr)
		}
		return 0, errors.New("prepared statement metadata was not found")
	}
	var raw any
	if err := rows.Scan(&raw); err != nil {
		return 0, fmt.Errorf("scan result column metadata: %w", err)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return 0, fmt.Errorf("read result column metadata: %w", rowsErr)
	}
	count, err = resultColumnCountFromMetadata(raw)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func quotePreparedStatementIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func resultColumnCountFromMetadata(raw any) (int, error) {
	var count int64
	switch value := raw.(type) {
	case nil:
		return 0, nil
	case int:
		count = int64(value)
	case int8:
		count = int64(value)
	case int16:
		count = int64(value)
	case int32:
		count = int64(value)
	case int64:
		count = value
	case uint:
		if uint64(value) > uint64(maxInt()) {
			return 0, fmt.Errorf("result column count %d overflows int", value)
		}
		return int(value), nil
	case uint8:
		count = int64(value)
	case uint16:
		count = int64(value)
	case uint32:
		count = int64(value)
	case uint64:
		if value > uint64(maxInt()) {
			return 0, fmt.Errorf("result column count %d overflows int", value)
		}
		return int(value), nil
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid result column count %q: %w", value, err)
		}
		count = parsed
	case []byte:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid result column count %q: %w", value, err)
		}
		count = parsed
	default:
		return 0, fmt.Errorf("unsupported result column count type %T", raw)
	}
	if count < 0 || count > int64(maxInt()) {
		return 0, fmt.Errorf("result column count %d overflows int", count)
	}
	return int(count), nil
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
