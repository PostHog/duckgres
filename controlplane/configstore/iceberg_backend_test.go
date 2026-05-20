package configstore

import "testing"

func TestManagedWarehouseIceberg_ResolvedBackend(t *testing.T) {
	cases := []struct {
		name  string
		in    ManagedWarehouseIceberg
		want  string
	}{
		{"empty defaults to lakekeeper", ManagedWarehouseIceberg{}, IcebergBackendLakekeeper},
		{"explicit lakekeeper", ManagedWarehouseIceberg{Backend: IcebergBackendLakekeeper}, IcebergBackendLakekeeper},
		{"explicit s3_tables", ManagedWarehouseIceberg{Backend: IcebergBackendS3Tables}, IcebergBackendS3Tables},
		{"unknown passthrough", ManagedWarehouseIceberg{Backend: "future"}, "future"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.in.ResolvedBackend(); got != c.want {
				t.Errorf("ResolvedBackend() = %q, want %q", got, c.want)
			}
		})
	}
}
