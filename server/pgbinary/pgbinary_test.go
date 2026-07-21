package pgbinary

import (
	"bytes"
	"encoding/binary"
	"math/big"
	"strconv"
	"strings"
	"testing"
)

func binaryCopy(t *testing.T, extension []byte, rows [][][]byte) []byte {
	t.Helper()
	var out bytes.Buffer
	out.Write([]byte("PGCOPY\n\xff\r\n\x00"))
	if err := binary.Write(&out, binary.BigEndian, uint32(0)); err != nil {
		t.Fatal(err)
	}
	if err := binary.Write(&out, binary.BigEndian, uint32(len(extension))); err != nil {
		t.Fatal(err)
	}
	out.Write(extension)
	for _, row := range rows {
		if err := binary.Write(&out, binary.BigEndian, int16(len(row))); err != nil {
			t.Fatal(err)
		}
		for _, field := range row {
			if field == nil {
				if err := binary.Write(&out, binary.BigEndian, int32(-1)); err != nil {
					t.Fatal(err)
				}
				continue
			}
			if err := binary.Write(&out, binary.BigEndian, int32(len(field))); err != nil {
				t.Fatal(err)
			}
			out.Write(field)
		}
	}
	if err := binary.Write(&out, binary.BigEndian, int16(-1)); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func numeric(t *testing.T, value string) []byte {
	t.Helper()
	neg := strings.HasPrefix(value, "-")
	value = strings.TrimPrefix(value, "-")
	parts := strings.SplitN(value, ".", 2)
	scale := 0
	digits := parts[0]
	if len(parts) == 2 {
		scale = len(parts[1])
		digits += parts[1]
	}
	unscaled, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		t.Fatalf("invalid test numeric %q", value)
	}
	return encodeNumeric(unscaled, neg, scale)
}

func TestInspectRejectsTupleWidthMismatch(t *testing.T) {
	payload := binaryCopy(t, nil, [][][]byte{{[]byte("one"), []byte("two")}})
	_, err := Inspect(bytes.NewReader(payload), Schema{Columns: []Column{{}}})
	if err == nil || !strings.Contains(err.Error(), "row 1 has 2 fields, expected 1") {
		t.Fatalf("Inspect() error = %v, want tuple-width error", err)
	}
}

func TestInspectRejectsFixedWidthFieldLengthMismatch(t *testing.T) {
	for _, tt := range []struct {
		typeName string
		field    []byte
		want     int
	}{
		{typeName: "BOOLEAN", field: make([]byte, 8), want: 1},
		{typeName: "BIGINT", field: make([]byte, 2), want: 8},
		{typeName: "DOUBLE", field: make([]byte, 4), want: 8},
		{typeName: "DATE", field: make([]byte, 8), want: 4},
		{typeName: "TIMESTAMP", field: make([]byte, 4), want: 8},
		{typeName: "TIMESTAMPTZ", field: make([]byte, 4), want: 8},
	} {
		t.Run(tt.typeName, func(t *testing.T) {
			schema, err := SchemaFromDatabaseTypes([]string{tt.typeName})
			if err != nil {
				t.Fatal(err)
			}
			payload := binaryCopy(t, nil, [][][]byte{{tt.field}})
			_, err = Inspect(bytes.NewReader(payload), schema)
			want := tt.typeName + " field length "
			if err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "expected "+strconv.Itoa(tt.want)) {
				t.Fatalf("Inspect() error = %v, want %s width %d", err, tt.typeName, tt.want)
			}
		})
	}
}

func TestInspectAllowsNullFixedWidthField(t *testing.T) {
	schema, err := SchemaFromDatabaseTypes([]string{"BIGINT"})
	if err != nil {
		t.Fatal(err)
	}
	payload := binaryCopy(t, nil, [][][]byte{{nil}})
	if _, err := Inspect(bytes.NewReader(payload), schema); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaFromDatabaseTypesPreservesTupleWidthAndNumericTypmod(t *testing.T) {
	schema, err := SchemaFromDatabaseTypes([]string{
		"BIGINT", "DECIMAL(18,4)", "NUMERIC(9, 2)", "VARCHAR",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.Columns) != 4 {
		t.Fatalf("columns = %d, want 4", len(schema.Columns))
	}
	if schema.Columns[0].Numeric != nil || schema.Columns[3].Numeric != nil {
		t.Fatal("non-numeric columns unexpectedly have numeric typmods")
	}
	for index, want := range []Numeric{{Precision: 18, Scale: 4}, {Precision: 9, Scale: 2}} {
		got := schema.Columns[index+1].Numeric
		if got == nil || *got != want {
			t.Fatalf("column %d NUMERIC = %#v, want %#v", index+2, got, want)
		}
	}
}

func TestSchemaFromDatabaseTypesRejectsUnparameterizedNumeric(t *testing.T) {
	_, err := SchemaFromDatabaseTypes([]string{"NUMERIC"})
	if err == nil || !strings.Contains(err.Error(), "precision and scale") {
		t.Fatalf("SchemaFromDatabaseTypes() error = %v, want typmod error", err)
	}
}

func TestInspectRequiresCompleteTrailerAndNoTrailingBytes(t *testing.T) {
	payload := binaryCopy(t, nil, [][][]byte{{[]byte("one")}})
	for _, tt := range []struct {
		name    string
		payload []byte
		want    string
	}{
		{name: "truncated trailer", payload: payload[:len(payload)-1], want: "trailer"},
		{name: "trailing bytes", payload: append(append([]byte{}, payload...), 0), want: "trailing data"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Inspect(bytes.NewReader(tt.payload), Schema{Columns: []Column{{}}})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Inspect() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestInspectDetectsNumericScaleMismatch(t *testing.T) {
	payload := binaryCopy(t, nil, [][][]byte{
		{numeric(t, "1.2")},
		{numeric(t, "1.2345")},
	})
	inspection, err := Inspect(bytes.NewReader(payload), Schema{Columns: []Column{
		{Numeric: &Numeric{Precision: 18, Scale: 2}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !inspection.NeedsRewrite {
		t.Fatal("Inspect() did not request normalization for mismatched NUMERIC dscale")
	}
	if inspection.Rows != 2 {
		t.Fatalf("Inspect() rows = %d, want 2", inspection.Rows)
	}
}

func TestRewriteNormalizesNumericScaleAndRoundsLikePostgres(t *testing.T) {
	payload := binaryCopy(t, nil, [][][]byte{
		{numeric(t, "1.2")},
		{numeric(t, "1.2345")},
		{numeric(t, "1.235")},
		{numeric(t, "-1.235")},
		{nil},
	})
	schema := Schema{Columns: []Column{{Numeric: &Numeric{Precision: 18, Scale: 2}}}}

	var normalized bytes.Buffer
	rows, err := Rewrite(&normalized, bytes.NewReader(payload), schema)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 5 {
		t.Fatalf("Rewrite() rows = %d, want 5", rows)
	}

	want := binaryCopy(t, nil, [][][]byte{
		{numeric(t, "1.20")},
		{numeric(t, "1.23")},
		{numeric(t, "1.24")},
		{numeric(t, "-1.24")},
		{nil},
	})
	if !bytes.Equal(normalized.Bytes(), want) {
		t.Fatalf("normalized binary COPY differs\n got: %x\nwant: %x", normalized.Bytes(), want)
	}

	inspection, err := Inspect(bytes.NewReader(normalized.Bytes()), schema)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.NeedsRewrite {
		t.Fatal("normalized output still requires rewrite")
	}
}

func TestRewriteRejectsNumericPrecisionOverflow(t *testing.T) {
	payload := binaryCopy(t, nil, [][][]byte{{numeric(t, "999.995")}})
	var normalized bytes.Buffer
	_, err := Rewrite(&normalized, bytes.NewReader(payload), Schema{Columns: []Column{
		{Numeric: &Numeric{Precision: 5, Scale: 2}},
	}})
	if err == nil || !strings.Contains(err.Error(), "exceeds DECIMAL(5,2)") {
		t.Fatalf("Rewrite() error = %v, want precision overflow", err)
	}
}

func TestInspectRejectsNumericPrecisionOverflowWhenWireScaleMatches(t *testing.T) {
	payload := binaryCopy(t, nil, [][][]byte{{numeric(t, "999.99")}})
	_, err := Inspect(bytes.NewReader(payload), Schema{Columns: []Column{
		{Numeric: &Numeric{Precision: 4, Scale: 2}},
	}})
	if err == nil || !strings.Contains(err.Error(), "exceeds DECIMAL(4,2)") {
		t.Fatalf("Inspect() error = %v, want precision overflow", err)
	}
}

func TestRewriteTruncatesDigitsBeyondWireScaleBeforeTargetCoercion(t *testing.T) {
	field := numeric(t, "1.2345")
	// PostgreSQL numeric_recv treats dscale as authoritative and truncates the
	// parsed base-10000 digits to it before applying the destination typmod.
	binary.BigEndian.PutUint16(field[6:8], 2)
	payload := binaryCopy(t, nil, [][][]byte{{field}})

	var normalized bytes.Buffer
	_, err := Rewrite(&normalized, bytes.NewReader(payload), Schema{Columns: []Column{
		{Numeric: &Numeric{Precision: 18, Scale: 4}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := binaryCopy(t, nil, [][][]byte{{numeric(t, "1.2300")}})
	if !bytes.Equal(normalized.Bytes(), want) {
		t.Fatalf("normalized binary COPY differs\n got: %x\nwant: %x", normalized.Bytes(), want)
	}
}

func TestInspectRejectsNumericDScaleOutsidePostgresMask(t *testing.T) {
	field := numeric(t, "1.2")
	binary.BigEndian.PutUint16(field[6:8], 0xffff)
	payload := binaryCopy(t, nil, [][][]byte{{field}})
	_, err := Inspect(bytes.NewReader(payload), Schema{Columns: []Column{
		{Numeric: &Numeric{Precision: 18, Scale: 2}},
	}})
	if err == nil || !strings.Contains(err.Error(), "dscale") {
		t.Fatalf("Inspect() error = %v, want invalid dscale", err)
	}
}

func TestRewriteStripsHeaderExtensionForScannerCompatibility(t *testing.T) {
	payload := binaryCopy(t, []byte{1, 2, 3, 4}, [][][]byte{{[]byte("one")}})
	schema := Schema{Columns: []Column{{}}}
	inspection, err := Inspect(bytes.NewReader(payload), schema)
	if err != nil {
		t.Fatal(err)
	}
	if !inspection.NeedsRewrite {
		t.Fatal("header extension must request a scanner-compatible rewrite")
	}

	var normalized bytes.Buffer
	if _, err := Rewrite(&normalized, bytes.NewReader(payload), schema); err != nil {
		t.Fatal(err)
	}
	want := binaryCopy(t, nil, [][][]byte{{[]byte("one")}})
	if !bytes.Equal(normalized.Bytes(), want) {
		t.Fatalf("normalized header extension differs\n got: %x\nwant: %x", normalized.Bytes(), want)
	}
}

func TestInspectRejectsNegativeHeaderExtensionLength(t *testing.T) {
	payload := binaryCopy(t, nil, [][][]byte{{[]byte("one")}})
	binary.BigEndian.PutUint32(payload[15:19], 0xffffffff)
	_, err := Inspect(bytes.NewReader(payload), Schema{Columns: []Column{{}}})
	if err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("Inspect() error = %v, want negative extension-length error", err)
	}
}

func TestRewriteClearsCompatibleLowHeaderFlags(t *testing.T) {
	payload := binaryCopy(t, nil, [][][]byte{{[]byte("one")}})
	binary.BigEndian.PutUint32(payload[11:15], 0x00000001)
	schema := Schema{Columns: []Column{{}}}
	inspection, err := Inspect(bytes.NewReader(payload), schema)
	if err != nil {
		t.Fatal(err)
	}
	if !inspection.NeedsRewrite {
		t.Fatal("compatible header flags must request a scanner-compatible rewrite")
	}
	var normalized bytes.Buffer
	if _, err := Rewrite(&normalized, bytes.NewReader(payload), schema); err != nil {
		t.Fatal(err)
	}
	want := binaryCopy(t, nil, [][][]byte{{[]byte("one")}})
	if !bytes.Equal(normalized.Bytes(), want) {
		t.Fatalf("normalized header flags differ\n got: %x\nwant: %x", normalized.Bytes(), want)
	}
}

func TestInspectRejectsCriticalHeaderFlags(t *testing.T) {
	payload := binaryCopy(t, nil, [][][]byte{{[]byte("one")}})
	binary.BigEndian.PutUint32(payload[11:15], 0x00010000)
	_, err := Inspect(bytes.NewReader(payload), Schema{Columns: []Column{{}}})
	if err == nil || !strings.Contains(err.Error(), "critical") {
		t.Fatalf("Inspect() error = %v, want critical-flags error", err)
	}
}
