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
		database:              "test",
		logicalCatalogMapping: true,
	}

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "rewrites logical use command",
			query: "USE test",
			want:  "USE ducklake",
		},
		{
			name:  "rewrites quoted logical use command",
			query: `USE "test"`,
			want:  `USE "ducklake"`,
		},
		{
			name:  "rewrites commented logical use command",
			query: "/* switch */ USE test;",
			want:  "USE ducklake;",
		},
		{
			name:  "preserves physical use command",
			query: "USE memory",
			want:  "USE memory",
		},
		{
			name:  "preserves non-use query",
			query: "SELECT current_database()",
			want:  "SELECT current_database()",
		},
		{
			name:  "rewrites show databases to logical catalog",
			query: "SHOW DATABASES",
			want:  "SELECT current_database() AS database_name",
		},
		{
			name:  "rewrites commented show databases with semicolon",
			query: "/* list */ SHOW DATABASES;",
			want:  "SELECT current_database() AS database_name;",
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

func TestRewriteDirectQueryMultitenantShowDatabases(t *testing.T) {
	c := &clientConn{
		server:                &Server{},
		database:              "duckgres",
		logicalCatalogMapping: true,
	}

	if got, want := c.rewriteDirectQuery("SHOW DATABASES"), "SELECT current_database() AS database_name"; got != want {
		t.Fatalf("rewriteDirectQuery(SHOW DATABASES) = %q, want %q", got, want)
	}
}

func TestRewriteDirectQueryPassthroughPreservesShowDatabases(t *testing.T) {
	c := &clientConn{
		server:      &Server{},
		database:    "duckgres",
		passthrough: true,
	}

	if got, want := c.rewriteDirectQuery("SHOW DATABASES"), "SHOW DATABASES"; got != want {
		t.Fatalf("rewriteDirectQuery(SHOW DATABASES) = %q, want %q", got, want)
	}
}

func TestRewriteDirectQueryPreservesShowDatabasesWithoutLogicalCatalogMapping(t *testing.T) {
	c := &clientConn{
		server:   &Server{},
		database: "duckgres",
	}

	if got, want := c.rewriteDirectQuery("SHOW DATABASES"), "SHOW DATABASES"; got != want {
		t.Fatalf("rewriteDirectQuery(SHOW DATABASES) = %q, want %q", got, want)
	}
}
