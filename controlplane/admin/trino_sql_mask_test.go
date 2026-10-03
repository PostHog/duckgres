//go:build kubernetes

package admin

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestMaskTrinoSQLLiterals(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  string
	}{
		{"string and number", `SELECT * FROM t WHERE email = 'a@example.com' AND n > 42`, `SELECT * FROM t WHERE email = ? AND n > ?`},
		{"escaped quote stays inside the literal", `SELECT 'it''s', "col""x" FROM t`, `SELECT ?, "col""x" FROM t`},
		{"number forms and identifiers with digits", `SELECT 1.5e-3, .5, 0x1F, 1_000, col1, t2.c3 FROM t2 LIMIT 10`, `SELECT ?, ?, ?, ?, col1, t2.c3 FROM t2 LIMIT ?`},
		{"subtraction is not an exponent sign", `SELECT a-1, 2-b FROM t`, `SELECT a-?, ?-b FROM t`},
		{"typed and prefixed literals", `SELECT DATE '2026-01-01', X'CAFE', U&'\0041', INTERVAL '3' DAY FROM t`, `SELECT DATE ?, ?, ?, INTERVAL ? DAY FROM t`},
		{"comments are dropped", "SELECT 1 -- secret 'x'\nFROM t /* hidden 42 */ WHERE a = 2", `SELECT ? FROM t WHERE a = ?`},
		{"whitespace collapses", "  SELECT\n\t a\n FROM   t  ", `SELECT a FROM t`},
		{"quoted identifiers are kept", "SELECT \"my col\", `other` FROM \"my schema\".t", "SELECT \"my col\", `other` FROM \"my schema\".t"},
		{"non-ASCII identifier stays whole", `SELECT "prénom", größe FROM t WHERE x = 'é'`, `SELECT "prénom", größe FROM t WHERE x = ?`},
		{"secret redaction placeholder passes through", `CREATE SECRET s (…redacted)`, `CREATE SECRET s (…redacted)`},
		{"empty", ``, ``},
		{"unterminated string", `SELECT 'abc`, trinoMaskedQueryUnavailable},
		{"unterminated block comment", `SELECT 1 /* never closed`, trinoMaskedQueryUnavailable},
		{"unterminated quoted identifier", `SELECT "abc FROM t`, trinoMaskedQueryUnavailable},
		{"unterminated prefixed literal", `SELECT X'CAFE`, trinoMaskedQueryUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := maskTrinoSQLLiterals(tc.query); got != tc.want {
				t.Errorf("maskTrinoSQLLiterals(%q)\n got: %q\nwant: %q", tc.query, got, tc.want)
			}
		})
	}
}

func TestMaskTrinoSQLLiteralsTruncatesLongStatements(t *testing.T) {
	got := maskTrinoSQLLiterals("SELECT " + strings.Repeat("column_name, ", 400) + "x FROM t")

	if count := utf8.RuneCountInString(got); count != trinoMaskedQueryMaxRunes+1 {
		t.Fatalf("rune count = %d, want %d", count, trinoMaskedQueryMaxRunes+1)
	}
	if !strings.HasSuffix(got, "…") {
		t.Fatalf("truncated statement must end with an ellipsis: %q", got[len(got)-10:])
	}
}
