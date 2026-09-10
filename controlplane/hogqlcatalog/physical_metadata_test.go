package hogqlcatalog

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type stubPhysicalMetadataProvider struct {
	requested PhysicalIdentifier
	metadata  *PhysicalCatalogMetadata
	err       error
	called    int
}

func (s *stubPhysicalMetadataProvider) PhysicalCatalog(_ context.Context, catalog PhysicalIdentifier) (*PhysicalCatalogMetadata, error) {
	s.called++
	s.requested = catalog
	return s.metadata, s.err
}

func TestBuildPhysicalSnapshotUsesExactProviderMetadata(t *testing.T) {
	provider := &stubPhysicalMetadataProvider{metadata: &PhysicalCatalogMetadata{
		Catalog: PhysicalIdentifier{Value: "ducklake"},
		Tables: []PhysicalTableMetadata{
			{
				Schema: PhysicalIdentifier{Value: "Product Data", Delimited: true},
				Table:  PhysicalIdentifier{Value: "Order", Delimited: true},
				Columns: []PhysicalColumnMetadata{
					{Name: PhysicalIdentifier{Value: "internal", Delimited: false}, Ordinal: 2, TrinoTypeSignature: "array(bigint)", Nullability: ColumnNullable, StarVisibility: ColumnStarHidden},
					{Name: PhysicalIdentifier{Value: "Line Item", Delimited: true}, Ordinal: 1, TrinoTypeSignature: `row("Plan Code" varchar, amount decimal(20, 4))`, Nullability: ColumnNotNull, StarVisibility: ColumnStarVisible},
				},
			},
			{
				Schema: PhysicalIdentifier{Value: "default"},
				Table:  PhysicalIdentifier{Value: "events"},
				Columns: []PhysicalColumnMetadata{
					{Name: PhysicalIdentifier{Value: "id"}, Ordinal: 1, TrinoTypeSignature: "uuid", Nullability: ColumnNotNull, StarVisibility: ColumnStarVisible},
				},
			},
		},
	}}

	snapshot, err := BuildPhysicalSnapshot(context.Background(), provider, PhysicalIdentifier{Value: "ducklake"}, "1.0.0", 7)
	if err != nil {
		t.Fatalf("build physical snapshot: %v", err)
	}
	if provider.requested != (PhysicalIdentifier{Value: "ducklake"}) {
		t.Fatalf("provider catalog = %#v, want ducklake", provider.requested)
	}
	if snapshot.Generation != 7 || snapshot.LanguageVersion != "1.0.0" {
		t.Fatalf("snapshot identity = generation %d language %q", snapshot.Generation, snapshot.LanguageVersion)
	}
	if len(snapshot.LogicalTables) != 2 {
		t.Fatalf("logical table count = %d, want 2", len(snapshot.LogicalTables))
	}

	events := snapshot.LogicalTables[0]
	if events.Name != "events" || events.PhysicalTable.Table != (PhysicalIdentifier{Value: "events"}) {
		t.Fatalf("first table = %#v, want default.events", events)
	}
	if got := events.Fields[0]; got.TrinoTypeSignature != "uuid" || got.LogicalType != LogicalTypeUUID || got.Nullable || !got.StarVisible {
		t.Fatalf("events.id metadata = %#v", got)
	}

	orders := snapshot.LogicalTables[1]
	if orders.Name != "Order" || orders.PhysicalTable.Schema != (PhysicalIdentifier{Value: "Product Data", Delimited: true}) || orders.PhysicalTable.Table != (PhysicalIdentifier{Value: "Order", Delimited: true}) {
		t.Fatalf("quoted table metadata = %#v", orders)
	}
	if got := orders.Fields[0]; got.Name != "Line Item" || got.PhysicalColumn != (PhysicalIdentifier{Value: "Line Item", Delimited: true}) || got.TrinoTypeSignature != `row("Plan Code" varchar, amount decimal(20, 4))` || got.LogicalType != LogicalTypeRow || got.Nullable || !got.StarVisible {
		t.Fatalf("first ordered column metadata = %#v", got)
	}
	if got := orders.Fields[1]; got.Name != "internal" || got.LogicalType != LogicalTypeArray || !got.Nullable || got.StarVisible {
		t.Fatalf("second ordered column metadata = %#v", got)
	}
	if !reflect.DeepEqual(orders.Properties, []PropertyDefinition{}) || !reflect.DeepEqual(orders.Relationships, []RelationshipDefinition{}) {
		t.Fatalf("physical table must have explicit empty semantic overlays: %#v", orders)
	}
	if snapshot.ExpressionFields == nil || snapshot.VirtualTables == nil || snapshot.SavedQueries == nil || snapshot.MaterializedViews == nil || snapshot.Functions == nil || snapshot.ModifierDefaults == nil {
		t.Fatal("required semantic lists must be present")
	}
}

func TestBuildPhysicalSnapshotFailsClosed(t *testing.T) {
	providerFailure := errors.New("metadata unavailable")
	provider := &stubPhysicalMetadataProvider{err: providerFailure}
	if snapshot, err := BuildPhysicalSnapshot(context.Background(), provider, testCatalog(), "1.0.0", 1); snapshot != nil || !errors.Is(err, providerFailure) {
		t.Fatalf("provider failure = (%#v, %v), want nil snapshot and source error", snapshot, err)
	}

	tests := []struct {
		name     string
		metadata *PhysicalCatalogMetadata
	}{
		{name: "missing provider result"},
		{
			name: "catalog mismatch",
			metadata: &PhysicalCatalogMetadata{
				Catalog: PhysicalIdentifier{Value: "other"},
				Tables:  []PhysicalTableMetadata{},
			},
		},
		{
			name: "missing table inventory",
			metadata: &PhysicalCatalogMetadata{
				Catalog: testCatalog(),
			},
		},
		{
			name: "missing column inventory",
			metadata: &PhysicalCatalogMetadata{
				Catalog: testCatalog(),
				Tables: []PhysicalTableMetadata{{
					Schema: PhysicalIdentifier{Value: "default"},
					Table:  PhysicalIdentifier{Value: "events"},
				}},
			},
		},
		{
			name: "duplicate column ordinal",
			metadata: physicalMetadataWithColumns(
				PhysicalColumnMetadata{Name: PhysicalIdentifier{Value: "id"}, Ordinal: 1, TrinoTypeSignature: "uuid"},
				PhysicalColumnMetadata{Name: PhysicalIdentifier{Value: "name"}, Ordinal: 1, TrinoTypeSignature: "varchar"},
			),
		},
		{
			name: "nonpositive column ordinal",
			metadata: physicalMetadataWithColumns(
				PhysicalColumnMetadata{Name: PhysicalIdentifier{Value: "id"}, TrinoTypeSignature: "uuid"},
			),
		},
		{
			name: "noncanonical unquoted identifier",
			metadata: physicalMetadataWithColumns(
				PhysicalColumnMetadata{Name: PhysicalIdentifier{Value: "MixedCase"}, Ordinal: 1, TrinoTypeSignature: "varchar"},
			),
		},
		{
			name: "inexact type signature",
			metadata: physicalMetadataWithColumns(
				PhysicalColumnMetadata{Name: PhysicalIdentifier{Value: "id"}, Ordinal: 1, TrinoTypeSignature: " varchar"},
			),
		},
		{
			name: "missing nullability",
			metadata: physicalMetadataWithColumns(
				PhysicalColumnMetadata{Name: PhysicalIdentifier{Value: "id"}, Ordinal: 1, TrinoTypeSignature: "uuid", StarVisibility: ColumnStarVisible},
			),
		},
		{
			name: "missing star visibility",
			metadata: physicalMetadataWithColumns(
				PhysicalColumnMetadata{Name: PhysicalIdentifier{Value: "id"}, Ordinal: 1, TrinoTypeSignature: "uuid", Nullability: ColumnNotNull},
			),
		},
		{
			name: "ambiguous logical table name",
			metadata: &PhysicalCatalogMetadata{
				Catalog: testCatalog(),
				Tables: []PhysicalTableMetadata{
					{Schema: PhysicalIdentifier{Value: "default"}, Table: PhysicalIdentifier{Value: "events"}, Columns: []PhysicalColumnMetadata{}},
					{Schema: PhysicalIdentifier{Value: "archive"}, Table: PhysicalIdentifier{Value: "events"}, Columns: []PhysicalColumnMetadata{}},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := &stubPhysicalMetadataProvider{metadata: test.metadata}
			snapshot, err := BuildPhysicalSnapshot(context.Background(), provider, testCatalog(), "1.0.0", 1)
			if snapshot != nil || !errors.Is(err, ErrInvalidPhysicalMetadata) {
				t.Fatalf("build result = (%#v, %v), want nil snapshot and ErrInvalidPhysicalMetadata", snapshot, err)
			}
		})
	}
}

func TestBuildPhysicalSnapshotRejectsInvalidIdentityBeforeLoadingMetadata(t *testing.T) {
	tests := []struct {
		name            string
		languageVersion string
		generation      int64
	}{
		{name: "invalid language version", languageVersion: "latest", generation: 1},
		{name: "nonpositive generation", languageVersion: "1.0.0"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := &stubPhysicalMetadataProvider{}
			snapshot, err := BuildPhysicalSnapshot(context.Background(), provider, testCatalog(), test.languageVersion, test.generation)
			if snapshot != nil || !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("build result = (%#v, %v), want nil snapshot and ErrInvalidSnapshot", snapshot, err)
			}
			if provider.called != 0 {
				t.Fatalf("provider calls = %d, want 0", provider.called)
			}
		})
	}
}

func TestLogicalTypeForTrinoSignature(t *testing.T) {
	tests := map[string]LogicalType{
		"boolean":                            LogicalTypeBoolean,
		"bigint":                             LogicalTypeInteger,
		"double":                             LogicalTypeFloat,
		"decimal(38, 9)":                     LogicalTypeDecimal,
		"varchar(255)":                       LogicalTypeString,
		"date":                               LogicalTypeDate,
		"timestamp(6) with time zone":        LogicalTypeTimestamp,
		"interval day to second":             LogicalTypeInterval,
		"uuid":                               LogicalTypeUUID,
		"json":                               LogicalTypeJSON,
		"array(map(varchar, json))":          LogicalTypeArray,
		"map(varchar, row(value varbinary))": LogicalTypeMap,
		"row(id bigint)":                     LogicalTypeRow,
		"varbinary":                          LogicalTypeUnknown,
	}
	for signature, want := range tests {
		t.Run(signature, func(t *testing.T) {
			if got := logicalTypeForTrinoSignature(signature); got != want {
				t.Fatalf("logical type = %q, want %q", got, want)
			}
		})
	}
}

func physicalMetadataWithColumns(columns ...PhysicalColumnMetadata) *PhysicalCatalogMetadata {
	return &PhysicalCatalogMetadata{
		Catalog: testCatalog(),
		Tables: []PhysicalTableMetadata{{
			Schema:  PhysicalIdentifier{Value: "default"},
			Table:   PhysicalIdentifier{Value: "events"},
			Columns: columns,
		}},
	}
}
