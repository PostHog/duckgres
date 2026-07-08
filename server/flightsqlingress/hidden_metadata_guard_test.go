package flightsqlingress

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRejectHiddenDuckLakeMetadataCatalogQuery(t *testing.T) {
	if err := rejectHiddenDuckLakeMetadataCatalogQuery("SELECT * FROM ducklake.system.query_log"); err != nil {
		t.Fatalf("supported query_log view rejected: %v", err)
	}

	err := rejectHiddenDuckLakeMetadataCatalogQuery(`SELECT * FROM "__ducklake_metadata_ducklake".querylog.query_log_entries`)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("status.Code(err) = %v, want %v (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}

	err = rejectHiddenDuckLakeMetadataCatalogQuery(`SELECT * FROM query('SELECT * FROM "__ducklake_metadata_ducklake".querylog.query_log_entries')`)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("dynamic SQL status.Code(err) = %v, want %v (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}

	err = rejectHiddenDuckLakeMetadataCatalogQuery(`SELECT * FROM query('SELECT * FROM "__ducklake_metadata_' || 'ducklake".querylog.query_log_entries')`)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("constructed dynamic SQL status.Code(err) = %v, want %v (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
}

func TestExcludeHiddenDuckLakeMetadataCatalogSQL(t *testing.T) {
	got := excludeHiddenDuckLakeMetadataCatalogSQL("table_catalog")
	for _, want := range []string{
		"lower(table_catalog) NOT LIKE",
		`'\_\_ducklake\_metadata\_%'`,
		"ESCAPE '\\'",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("excludeHiddenDuckLakeMetadataCatalogSQL() = %q, want substring %q", got, want)
		}
	}
}
