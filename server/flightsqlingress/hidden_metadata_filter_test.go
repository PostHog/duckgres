package flightsqlingress

import (
	"strings"
	"testing"
)

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
