package server

import (
	"database/sql"
	"log/slog"
)

// initClickHouseMacros registers ClickHouse SQL function macros so that
// ClickHouse-flavoured queries work out of the box against DuckDB.
// This is a subset of the chsql community extension's functions, implemented
// as plain SQL macros following the same pattern as initUtilityMacros.
func initClickHouseMacros(db *sql.DB) {
	macros := []string{
		// -- Type conversion --
		`CREATE OR REPLACE MACRO toString(x) AS CAST(x AS VARCHAR)`,
		`CREATE OR REPLACE MACRO toInt32(x) AS CAST(x AS INTEGER)`,
		`CREATE OR REPLACE MACRO toInt64(x) AS CAST(x AS BIGINT)`,
		`CREATE OR REPLACE MACRO toFloat(x) AS CAST(x AS DOUBLE)`,
		`CREATE OR REPLACE MACRO toInt32OrNull(x) AS TRY_CAST(x AS INTEGER)`,
		`CREATE OR REPLACE MACRO toInt32OrZero(x) AS COALESCE(TRY_CAST(x AS INTEGER), 0)`,

		// -- Arithmetic --
		`CREATE OR REPLACE MACRO intDiv(a, b) AS CAST(a / b AS BIGINT)`,
		`CREATE OR REPLACE MACRO modulo(a, b) AS a % b`,

		// -- String --
		`CREATE OR REPLACE MACRO empty(s) AS (length(s) = 0)`,
		`CREATE OR REPLACE MACRO notEmpty(s) AS (length(s) > 0)`,
		`CREATE OR REPLACE MACRO splitByChar(sep, s) AS string_split(s, sep)`,
		`CREATE OR REPLACE MACRO lengthUTF8(s) AS length(s)`,

		// -- Date --
		`CREATE OR REPLACE MACRO toYear(d) AS date_part('year', d)`,
		`CREATE OR REPLACE MACRO toMonth(d) AS date_part('month', d)`,
		`CREATE OR REPLACE MACRO toDayOfMonth(d) AS date_part('day', d)`,
		`CREATE OR REPLACE MACRO toYYYYMMDD(d) AS CAST(strftime(CAST(d AS DATE), '%Y%m%d') AS INTEGER)`,
		`CREATE OR REPLACE MACRO toYYYYMM(d) AS CAST(strftime(CAST(d AS DATE), '%Y%m') AS INTEGER)`,

		// -- URL --
		// protocol: extract scheme before "://"
		`CREATE OR REPLACE MACRO protocol(url) AS CASE WHEN contains(url, '://') THEN split_part(url, '://', 1) ELSE '' END`,
		// domain: extract host from URL
		`CREATE OR REPLACE MACRO domain(url) AS split_part(split_part(split_part(url, '://', 2), '/', 1), ':', 1)`,
		// topLevelDomain: last dot-separated part of the domain
		`CREATE OR REPLACE MACRO topLevelDomain(url) AS split_part(split_part(split_part(split_part(url, '://', 2), '/', 1), ':', 1), '.', -1)`,

		// -- IP --
		// IPv4NumToString: convert a 32-bit integer to dotted-decimal notation
		`CREATE OR REPLACE MACRO IPv4NumToString(n) AS concat_ws('.', (n >> 24) & 255, (n >> 16) & 255, (n >> 8) & 255, n & 255)`,

		// -- JSON --
		`CREATE OR REPLACE MACRO JSONExtractString(json, path) AS CAST(json_extract(json, path) AS VARCHAR)`,
		`CREATE OR REPLACE MACRO JSONHas(json, path) AS (json_extract(json, path) IS NOT NULL)`,

		// -- Misc --
		`CREATE OR REPLACE MACRO generateUUIDv4() AS uuid()`,
		`CREATE OR REPLACE MACRO ifNull(x, alt) AS COALESCE(x, alt)`,
	}

	for _, m := range macros {
		if _, err := db.Exec(m); err != nil {
			slog.Warn("Failed to create ClickHouse macro", "sql", m, "error", err)
		}
	}
}
