package flightsqlingress

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRejectHiddenDuckLakeMetadataCatalogQuery(t *testing.T) {
	if err := rejectHiddenDuckLakeMetadataCatalogQuery("SELECT * FROM ducklake.system.query_log"); err != nil {
		t.Fatalf("supported query_log view rejected: %v", err)
	}
	if err := rejectHiddenDuckLakeMetadataCatalogQuery("SELECT '__ducklake_metadata_ducklake'"); err != nil {
		t.Fatalf("diagnostic string rejected: %v", err)
	}

	err := rejectHiddenDuckLakeMetadataCatalogQuery(`SELECT * FROM "__ducklake_metadata_ducklake".querylog.query_log_entries`)
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("status.Code(err) = %v, want %v (err=%v)", status.Code(err), codes.PermissionDenied, err)
	}
}
