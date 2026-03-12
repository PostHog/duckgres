package server

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func TestEmptyRowSetColumnTypes(t *testing.T) {
	// Before fix: emptyRowSet returns nil ColumnTypes
	e := &emptyRowSet{}
	cols, _ := e.Columns()
	colTypes, _ := e.ColumnTypes()

	if cols != nil {
		t.Errorf("emptyRowSet.Columns() = %v, want nil", cols)
	}
	if colTypes != nil {
		t.Errorf("emptyRowSet.ColumnTypes() = %v, want nil", colTypes)
	}

	// Simulate BLOB detection with nil colTypes (before fix)
	var blobColIndices []int
	for i, ct := range colTypes {
		if ct.DatabaseTypeName() == "BLOB" {
			blobColIndices = append(blobColIndices, i)
		}
	}
	if len(blobColIndices) > 0 {
		t.Error("should not detect BLOB with nil colTypes")
	}
	t.Log("BEFORE fix: emptyRowSet returns nil colTypes → BLOB detection finds nothing → falls through to broken CSV path")
}

func TestEmptySchemaRowSetColumnTypes(t *testing.T) {
	// After fix: emptySchemaRowSet preserves schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "data", Type: arrow.BinaryTypes.Binary},
		{Name: "count", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	e := &emptySchemaRowSet{schema: schema}
	cols, err := e.Columns()
	if err != nil {
		t.Fatal(err)
	}
	colTypes, err := e.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}

	// Verify column names
	if len(cols) != 3 || cols[0] != "id" || cols[1] != "data" || cols[2] != "count" {
		t.Errorf("Columns() = %v, want [id data count]", cols)
	}

	// Verify column types
	expectedTypes := []string{"VARCHAR", "BLOB", "INTEGER"}
	for i, ct := range colTypes {
		if ct.DatabaseTypeName() != expectedTypes[i] {
			t.Errorf("colTypes[%d].DatabaseTypeName() = %q, want %q", i, ct.DatabaseTypeName(), expectedTypes[i])
		}
	}

	// Simulate BLOB detection with real colTypes (after fix)
	var blobColIndices []int
	for i, ct := range colTypes {
		if ct.DatabaseTypeName() == "BLOB" {
			blobColIndices = append(blobColIndices, i)
		}
	}
	if len(blobColIndices) != 1 || blobColIndices[0] != 1 {
		t.Errorf("BLOB detection = %v, want [1]", blobColIndices)
	}
	t.Log("AFTER fix: emptySchemaRowSet returns real colTypes → BLOB detected at index 1 → triggers CSV-with-BLOB fallback")
}
