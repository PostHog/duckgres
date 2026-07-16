package transpiler

import (
	"strings"
	"testing"
)

func TestTranspileWritableCTESchemaQueryRestoresLongIdentifiers(t *testing.T) {
	cte := `"My CTE With Spaces And Punctuation!! that runs well past sixty-three bytes"`
	res, err := New(DefaultConfig()).Transpile(
		"WITH " + cte + " AS (DELETE FROM t RETURNING id) SELECT * FROM " + cte,
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.SchemaQuery == "" {
		t.Fatal("expected a schema query")
	}
	if !strings.Contains(res.SchemaQuery, cte) {
		t.Fatalf("schema query did not restore long CTE identifier:\n%s", res.SchemaQuery)
	}
	if strings.Contains(res.SchemaQuery, "dglongident_") {
		t.Fatalf("schema query leaked a long-identifier placeholder:\n%s", res.SchemaQuery)
	}
}
