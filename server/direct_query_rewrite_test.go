package server

import "testing"

func TestRewriteDirectQuery(t *testing.T) {
	c := &clientConn{
		server: &Server{
			cfg: Config{
				DuckLake: DuckLakeConfig{
					MetadataStore: "postgres:host=127.0.0.1 dbname=ducklake",
				},
				Iceberg: IcebergConfig{Enabled: true},
			},
		},
		database:          "ducklake",
		catalogUseRewrite: true,
	}

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			// `USE ducklake` while currently in the iceberg catalog would
			// otherwise resolve to a bogus iceberg.ducklake — two-part fixes it.
			name:  "rewrites bare ducklake to two-part ducklake.main",
			query: "USE ducklake",
			want:  "USE ducklake.main",
		},
		{
			name:  "rewrites quoted ducklake to two-part ducklake.main",
			query: `USE "ducklake"`,
			want:  "USE ducklake.main",
		},
		{
			name:  "rewrites commented ducklake use",
			query: "/* switch */ USE ducklake;",
			want:  "USE ducklake.main;",
		},
		{
			name:  "rewrites bare iceberg to its default schema",
			query: "USE iceberg",
			want:  "USE iceberg.public",
		},
		{
			name:  "rewrites quoted iceberg to its default schema",
			query: `USE "iceberg";`,
			want:  "USE iceberg.public;",
		},
		{
			// already two-part — left untouched.
			name:  "preserves two-part iceberg use",
			query: "USE iceberg.billing",
			want:  "USE iceberg.billing",
		},
		{
			name:  "preserves physical use command",
			query: "USE memory",
			want:  "USE memory",
		},
		{
			// An arbitrary database name is no longer remapped onto a catalog.
			name:  "preserves use of an arbitrary name",
			query: "USE analytics",
			want:  "USE analytics",
		},
		{
			name:  "preserves non-use query",
			query: "SELECT current_database()",
			want:  "SELECT current_database()",
		},
		{
			// SHOW DATABASES now lists the real attached catalogs — no rewrite.
			name:  "preserves show databases",
			query: "SHOW DATABASES",
			want:  "SHOW DATABASES",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.rewriteDirectQuery(tc.query); got != tc.want {
				t.Fatalf("rewriteDirectQuery(%q) = %q, want %q", tc.query, got, tc.want)
			}
		})
	}
}

// `USE iceberg` is rewritten regardless of cfg.Iceberg.Enabled, because the
// rewrite runs on the control-plane proxy conn where cfg.Iceberg.Enabled is
// always false (the per-org flag lives on the worker). Gating on it there
// silently disabled the rewrite for every tenant.
func TestRewriteDirectQueryUseIcebergRewrittenEvenWhenCfgDisabled(t *testing.T) {
	c := &clientConn{
		server: &Server{cfg: Config{
			DuckLake: DuckLakeConfig{MetadataStore: "postgres:host=127.0.0.1 dbname=ducklake"},
			Iceberg:  IcebergConfig{Enabled: false},
		}},
		database:          "iceberg",
		catalogUseRewrite: true,
	}
	if got, want := c.rewriteDirectQuery("USE iceberg"), "USE iceberg.public"; got != want {
		t.Fatalf("rewriteDirectQuery(USE iceberg) = %q, want %q", got, want)
	}
}

func TestRewriteDirectQueryPassthroughPreservesUse(t *testing.T) {
	c := &clientConn{
		server:      &Server{},
		database:    "ducklake",
		passthrough: true,
	}

	if got, want := c.rewriteDirectQuery("USE ducklake"), "USE ducklake"; got != want {
		t.Fatalf("rewriteDirectQuery(USE ducklake) = %q, want %q", got, want)
	}
}

func TestRewriteDirectQueryPreservesUseWithoutCatalogRewrite(t *testing.T) {
	c := &clientConn{
		server:   &Server{},
		database: "ducklake",
	}

	if got, want := c.rewriteDirectQuery("USE ducklake"), "USE ducklake"; got != want {
		t.Fatalf("rewriteDirectQuery(USE ducklake) = %q, want %q", got, want)
	}
}
