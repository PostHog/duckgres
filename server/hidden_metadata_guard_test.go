package server

import "testing"

func TestQueryReferencesHiddenDuckLakeMetadataCatalog(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "unquoted hidden catalog",
			query: "SELECT * FROM __ducklake_metadata_ducklake.querylog.query_log_entries",
			want:  true,
		},
		{
			name:  "quoted hidden catalog",
			query: `SELECT * FROM "__ducklake_metadata_ducklake".querylog.query_log_entries`,
			want:  true,
		},
		{
			name:  "case-insensitive hidden catalog",
			query: "SELECT * FROM __DUCKLAKE_METADATA_DUCKLAKE.querylog.query_log_entries",
			want:  true,
		},
		{
			name:  "supported query log surface",
			query: "SELECT * FROM ducklake.system.query_log",
			want:  false,
		},
		{
			name:  "single-quoted diagnostic string",
			query: "SELECT '__ducklake_metadata_ducklake'",
			want:  false,
		},
		{
			name:  "dollar-quoted diagnostic string",
			query: "SELECT $tag$__ducklake_metadata_ducklake$tag$",
			want:  false,
		},
		{
			name:  "line comment",
			query: "-- SELECT * FROM __ducklake_metadata_ducklake.querylog.query_log_entries\nSELECT 1",
			want:  false,
		},
		{
			name:  "block comment",
			query: "/* SELECT * FROM __ducklake_metadata_ducklake.querylog.query_log_entries */ SELECT 1",
			want:  false,
		},
		{
			name:  "ordinary identifier",
			query: "SELECT * FROM querylog.query_log_entries",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QueryReferencesHiddenDuckLakeMetadataCatalog(tt.query)
			if got != tt.want {
				t.Fatalf("QueryReferencesHiddenDuckLakeMetadataCatalog(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}
