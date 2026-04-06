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
		database: "test",
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.rewriteDirectQuery(tc.query); got != tc.want {
				t.Fatalf("rewriteDirectQuery(%q) = %q, want %q", tc.query, got, tc.want)
			}
		})
	}
}
