package server

import (
	"testing"
)

// Note: DDL rewriting tests (rewriteForDuckLake, isNoOpCommand, getNoOpCommandTag)
// have been moved to transpiler/transpiler_test.go.
// The transpiler package now handles all SQL rewriting via AST transformation.

func TestGetCommandTypeWithConstraints(t *testing.T) {
	c := &clientConn{}
	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "ALTER TABLE ADD CONSTRAINT",
			query:    "ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id)",
			expected: "ALTER TABLE ADD CONSTRAINT",
		},
		{
			name:     "ALTER TABLE ADD PRIMARY KEY",
			query:    "ALTER TABLE users ADD PRIMARY KEY (id)",
			expected: "ALTER TABLE ADD CONSTRAINT",
		},
		{
			name:     "ALTER TABLE ADD UNIQUE",
			query:    "ALTER TABLE users ADD UNIQUE (email)",
			expected: "ALTER TABLE ADD CONSTRAINT",
		},
		{
			name:     "ALTER TABLE ADD FOREIGN KEY",
			query:    "ALTER TABLE orders ADD FOREIGN KEY (user_id) REFERENCES users(id)",
			expected: "ALTER TABLE ADD CONSTRAINT",
		},
		{
			name:     "ALTER TABLE ADD CHECK",
			query:    "ALTER TABLE users ADD CHECK (age > 0)",
			expected: "ALTER TABLE ADD CONSTRAINT",
		},
		{
			name:     "ALTER TABLE ADD COLUMN (not constraint)",
			query:    "ALTER TABLE users ADD COLUMN age INTEGER",
			expected: "ALTER TABLE",
		},
		{
			name:     "ALTER TABLE DROP COLUMN",
			query:    "ALTER TABLE users DROP COLUMN age",
			expected: "ALTER TABLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := c.getCommandType(tt.query)
			if result != tt.expected {
				t.Errorf("getCommandType(%q) = %q, expected %q", tt.query, result, tt.expected)
			}
		})
	}
}

func TestBuildDeltaCatalogAttachStmt(t *testing.T) {
	cfg := DuckLakeConfig{
		ObjectStore:         "s3://warehouse/ducklake/",
		DeltaCatalogEnabled: true,
	}
	if got, want := buildDeltaCatalogAttachStmt(cfg), "ATTACH 's3://warehouse/delta/' AS delta (TYPE delta)"; got != want {
		t.Fatalf("buildDeltaCatalogAttachStmt() = %q, want %q", got, want)
	}

	cfg.DeltaCatalogPath = "s3://warehouse/custom-delta/"
	if got, want := buildDeltaCatalogAttachStmt(cfg), "ATTACH 's3://warehouse/custom-delta/' AS delta (TYPE delta)"; got != want {
		t.Fatalf("buildDeltaCatalogAttachStmt() = %q, want %q", got, want)
	}
}

func TestDefaultDeltaCatalogPath(t *testing.T) {
	tests := []struct {
		name string
		cfg  DuckLakeConfig
		want string
	}{
		{
			name: "object store at bucket root",
			cfg:  DuckLakeConfig{ObjectStore: "s3://warehouse/ducklake/"},
			want: "s3://warehouse/delta/",
		},
		{
			name: "object store nested under tenant prefix",
			cfg:  DuckLakeConfig{ObjectStore: "s3://warehouse/team-a/ducklake/"},
			want: "s3://warehouse/team-a/delta/",
		},
		{
			name: "object store deeply nested",
			cfg:  DuckLakeConfig{ObjectStore: "s3://warehouse/region/team-a/ducklake/"},
			want: "s3://warehouse/region/team-a/delta/",
		},
		{
			name: "object store with no trailing slash",
			cfg:  DuckLakeConfig{ObjectStore: "s3://warehouse/ducklake"},
			want: "s3://warehouse/delta/",
		},
		{
			name: "bare bucket",
			cfg:  DuckLakeConfig{ObjectStore: "s3://warehouse"},
			want: "s3://warehouse/delta/",
		},
		{
			name: "bare bucket with trailing slash",
			cfg:  DuckLakeConfig{ObjectStore: "s3://warehouse/"},
			want: "s3://warehouse/delta/",
		},
		{
			name: "local data path",
			cfg:  DuckLakeConfig{DataPath: "/var/lib/duckgres/ducklake"},
			want: "/var/lib/duckgres/delta",
		},
		{
			name: "no object store or data path",
			cfg:  DuckLakeConfig{},
			want: "",
		},
		{
			name: "explicit delta path is not overridden by default derivation",
			cfg: DuckLakeConfig{
				ObjectStore:      "s3://warehouse/team-a/ducklake/",
				DeltaCatalogPath: "s3://other/explicit/",
			},
			want: "s3://warehouse/team-a/delta/", // default ignores explicit; deltaCatalogPath() handles override
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := DefaultDeltaCatalogPath(tt.cfg); got != tt.want {
				t.Fatalf("DefaultDeltaCatalogPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestObjectStoreParentPrefix(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "scheme with multi-segment path", path: "s3://bucket/team/ducklake/", want: "s3://bucket/team/"},
		{name: "scheme with single-segment path", path: "s3://bucket/ducklake/", want: "s3://bucket/"},
		{name: "scheme with no trailing slash", path: "s3://bucket/ducklake", want: "s3://bucket/"},
		{name: "scheme with bare bucket", path: "s3://bucket", want: "s3://bucket/"},
		{name: "scheme with bare bucket trailing slash", path: "s3://bucket/", want: "s3://bucket/"},
		{name: "no scheme, multi-segment path", path: "/var/lib/duckgres/ducklake/", want: "/var/lib/duckgres/"},
		{name: "no scheme, no trailing slash", path: "/var/lib/duckgres/ducklake", want: "/var/lib/duckgres/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := objectStoreParentPrefix(tt.path); got != tt.want {
				t.Fatalf("objectStoreParentPrefix(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}

func TestDeltaCatalogNeedsS3Secret(t *testing.T) {
	tests := []struct {
		name string
		path string
		cfg  DuckLakeConfig
		want bool
	}{
		{
			name: "default credential chain for explicit s3 delta path",
			path: "s3://warehouse/delta/",
			cfg:  DuckLakeConfig{},
			want: true,
		},
		{
			name: "local delta path",
			path: "/var/lib/duckgres/delta",
			cfg:  DuckLakeConfig{},
			want: false,
		},
		{
			name: "explicit config credentials",
			path: "s3://warehouse/delta/",
			cfg: DuckLakeConfig{
				S3AccessKey: "AKIA...",
			},
			want: true,
		},
		{
			name: "aws sdk provider",
			path: "s3://warehouse/delta/",
			cfg: DuckLakeConfig{
				S3Provider: "aws_sdk",
			},
			want: true,
		},
		{
			name: "credential chain profile",
			path: "s3://warehouse/delta/",
			cfg: DuckLakeConfig{
				S3Profile: "prod",
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := deltaCatalogNeedsS3Secret(tt.path, tt.cfg); got != tt.want {
				t.Fatalf("deltaCatalogNeedsS3Secret() = %v, want %v", got, tt.want)
			}
		})
	}
}
