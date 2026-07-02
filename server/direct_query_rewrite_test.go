package server

import "testing"

func TestRewriteDirectQuery(t *testing.T) {
	c := &clientConn{
		server: &Server{
			cfg: Config{
				DuckLake: DuckLakeConfig{
					MetadataStore: "postgres:host=127.0.0.1 dbname=ducklake",
				},
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
			// `USE ducklake` while currently in another catalog would
			// otherwise resolve to a bogus <catalog>.ducklake — two-part fixes it.
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
			// Only `USE ducklake` is rewritten — any other catalog passes through.
			name:  "preserves bare use of a non-ducklake catalog",
			query: "USE iceberg",
			want:  "USE iceberg",
		},
		{
			name:  "preserves two-part use of a non-ducklake catalog",
			query: "USE other.billing",
			want:  "USE other.billing",
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
