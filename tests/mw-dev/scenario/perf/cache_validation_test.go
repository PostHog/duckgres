package perf

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	perfcore "github.com/posthog/duckgres/tests/perf/core"
)

type cacheModeRow struct {
	count int64
	mode  *string
	err   error
}

func (r cacheModeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*int64) = r.count
	*dest[1].(**string) = r.mode
	return nil
}

type cacheModeQuery struct {
	row     cacheModeRow
	catalog string
}

func (q *cacheModeQuery) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	q.catalog = args[0].(string)
	return q.row
}
func TestCatalogCacheModeMatchesBenchmarkLabel(t *testing.T) {
	on, off, invalid := "true", "false", "unexpected"
	for _, tc := range []struct {
		name     string
		protocol perfcore.Protocol
		row      cacheModeRow
		want     string
	}{
		{"cached enabled", perfcore.ProtocolTrinoCached, cacheModeRow{1, &on, nil}, ""},
		{"baseline disabled", perfcore.ProtocolTrino, cacheModeRow{1, &off, nil}, ""},
		{"old image cached", perfcore.ProtocolTrinoCached, cacheModeRow{1, nil, nil}, "explicit fs.cache.enabled"},
		{"old image baseline", perfcore.ProtocolTrino, cacheModeRow{1, nil, nil}, "explicit fs.cache.enabled"},
		{"cached disabled", perfcore.ProtocolTrinoCached, cacheModeRow{1, &off, nil}, "requires fs.cache.enabled=true"},
		{"baseline enabled", perfcore.ProtocolTrino, cacheModeRow{1, &on, nil}, "requires fs.cache.enabled=false"},
		{"missing catalog", perfcore.ProtocolTrinoCached, cacheModeRow{}, "exactly one"},
		{"ambiguous catalog", perfcore.ProtocolTrinoCached, cacheModeRow{2, &on, nil}, "exactly one"},
		{"invalid property", perfcore.ProtocolTrinoCached, cacheModeRow{1, &invalid, nil}, "requires fs.cache.enabled=true"},
		{"database unavailable", perfcore.ProtocolTrinoCached, cacheModeRow{err: errors.New("secret database detail")}, "read Trino catalog cache setting"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := &cacheModeQuery{row: tc.row}
			err := checkTrinoCatalogCacheMode(context.Background(), q, "org_fixture", tc.protocol)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
			if q.catalog != "org_fixture" {
				t.Fatalf("checked wrong catalog %q", q.catalog)
			}
			if err != nil && strings.Contains(err.Error(), "secret database detail") {
				t.Fatal("database details leaked")
			}
		})
	}
}
