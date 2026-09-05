package hogqlcatalog

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStorePublishesImmutableMonotonicSnapshots(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	first := testSnapshot(1)

	if err := store.Publish(ctx, first); err != nil {
		t.Fatalf("publish generation 1: %v", err)
	}
	first.LogicalTables[0].Fields[0].Name = "mutated"

	pinned, err := store.Generation(ctx, testCatalog(), 1)
	if err != nil {
		t.Fatalf("read generation 1: %v", err)
	}
	if got := pinned.LogicalTables[0].Fields[0].Name; got != "id" {
		t.Fatalf("published snapshot leaked caller mutation: field = %q", got)
	}
	pinned.LogicalTables[0].Fields[0].Name = "mutated-again"

	again, err := store.Generation(ctx, testCatalog(), 1)
	if err != nil {
		t.Fatalf("reread generation 1: %v", err)
	}
	if got := again.LogicalTables[0].Fields[0].Name; got != "id" {
		t.Fatalf("read snapshot leaked caller mutation: field = %q", got)
	}

	if err := store.Publish(ctx, testSnapshot(3)); err != nil {
		t.Fatalf("publish generation 3: %v", err)
	}
	conflict := testSnapshot(3)
	conflict.LogicalTables[0].Fields[0].Nullable = true
	if err := store.Publish(ctx, conflict); !errors.Is(err, ErrGenerationConflict) {
		t.Fatalf("publish conflicting generation error = %v, want ErrGenerationConflict", err)
	}
	if err := store.Publish(ctx, testSnapshot(2)); !errors.Is(err, ErrGenerationRegression) {
		t.Fatalf("publish older generation error = %v, want ErrGenerationRegression", err)
	}

	latest, err := store.Latest(ctx, testCatalog())
	if err != nil {
		t.Fatalf("read latest: %v", err)
	}
	if latest.Generation != 3 {
		t.Fatalf("latest generation = %d, want 3", latest.Generation)
	}
	if pinned, err := store.Generation(ctx, testCatalog(), 1); err != nil || pinned.Generation != 1 {
		t.Fatalf("pinned generation after later publish = (%v, %v), want generation 1", pinned, err)
	}
}

func TestMemoryStoreFailsClosedForUnknownCatalogAndGeneration(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	if _, err := store.Latest(ctx, testCatalog()); !errors.Is(err, ErrCatalogNotFound) {
		t.Fatalf("latest unknown catalog error = %v, want ErrCatalogNotFound", err)
	}
	if err := store.Publish(ctx, testSnapshot(1)); err != nil {
		t.Fatalf("publish generation 1: %v", err)
	}
	if _, err := store.Generation(ctx, testCatalog(), 2); !errors.Is(err, ErrGenerationNotFound) {
		t.Fatalf("unknown generation error = %v, want ErrGenerationNotFound", err)
	}
}

func TestPublishAllowsPropertyContainerBackedBySameLogicalField(t *testing.T) {
	snapshot := testSnapshot(1)
	snapshot.LogicalTables[0].Fields = append(snapshot.LogicalTables[0].Fields, LogicalFieldDefinition{
		Name:               "properties",
		PhysicalColumn:     PhysicalIdentifier{Value: "properties_blob"},
		TrinoTypeSignature: "map(varchar, json)",
		LogicalType:        LogicalTypeMap,
		Nullable:           true,
	})
	snapshot.LogicalTables[0].Properties[0].SourceField = "properties"

	if err := NewMemoryStore().Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("publish property container backed by the same field: %v", err)
	}
}

func TestPublishRejectsInvalidSemanticOverlay(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name   string
		mutate func(*HogQLSemanticCatalogSnapshot)
	}{
		{
			name: "unsupported protocol version",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ProtocolVersion = 2
			},
		},
		{
			name: "physical catalog mismatch",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LogicalTables[0].PhysicalTable.Catalog = PhysicalIdentifier{Value: "other"}
			},
		},
		{
			name: "raw executable definition",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LogicalTables[0].Fields[0].TrinoTypeSignature = "varchar; DROP TABLE events"
			},
		},
		{
			name: "unknown relationship target",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LogicalTables[0].Relationships[0].TargetTable = "missing"
			},
		},
		{
			name: "missing required relationship list",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LogicalTables[1].Relationships = nil
			},
		},
		{
			name: "property collides with unrelated field",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LogicalTables[0].Fields = append(snapshot.LogicalTables[0].Fields, LogicalFieldDefinition{
					Name:               "properties",
					PhysicalColumn:     PhysicalIdentifier{Value: "properties_blob"},
					TrinoTypeSignature: "map(varchar, json)",
					LogicalType:        LogicalTypeMap,
					Nullable:           true,
				})
				snapshot.LogicalTables[0].Properties[0].Name = "id"
				snapshot.LogicalTables[0].Properties[0].SourceField = "properties"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testSnapshot(1)
			test.mutate(snapshot)
			if err := NewMemoryStore().Publish(ctx, snapshot); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("publish error = %v, want ErrInvalidSnapshot", err)
			}
		})
	}
}

func testCatalog() PhysicalIdentifier {
	return PhysicalIdentifier{Value: "ducklake"}
}

func testSnapshot(generation int64) *HogQLSemanticCatalogSnapshot {
	catalog := testCatalog()
	return &HogQLSemanticCatalogSnapshot{
		ProtocolVersion: SnapshotProtocolVersion,
		SchemaVersion:   SnapshotSchemaVersion,
		LanguageVersion: "1.0.0",
		Catalog:         catalog,
		Generation:      generation,
		LogicalTables: []LogicalTableDefinition{
			{
				Name: "events",
				PhysicalTable: PhysicalQualifiedName{
					Catalog: catalog,
					Schema:  PhysicalIdentifier{Value: "default"},
					Table:   PhysicalIdentifier{Value: "events"},
				},
				Fields: []LogicalFieldDefinition{
					{
						Name:               "id",
						PhysicalColumn:     PhysicalIdentifier{Value: "id"},
						TrinoTypeSignature: "varchar",
						LogicalType:        LogicalTypeString,
						Nullable:           false,
						StarVisible:        true,
					},
				},
				Properties: []PropertyDefinition{
					{
						Name:        "properties",
						SourceField: "id",
						Storage:     PropertyStorageJSONObject,
						LogicalType: LogicalTypeJSON,
						Nullable:    true,
					},
				},
				Relationships: []RelationshipDefinition{
					{
						Name:        "person",
						TargetTable: "persons",
						Cardinality: RelationshipCardinalityManyToOne,
						JoinKeys: []JoinKey{
							{SourceField: "id", TargetField: "id"},
						},
					},
				},
			},
			{
				Name: "persons",
				PhysicalTable: PhysicalQualifiedName{
					Catalog: catalog,
					Schema:  PhysicalIdentifier{Value: "default"},
					Table:   PhysicalIdentifier{Value: "persons"},
				},
				Fields: []LogicalFieldDefinition{
					{
						Name:               "id",
						PhysicalColumn:     PhysicalIdentifier{Value: "id"},
						TrinoTypeSignature: "varchar",
						LogicalType:        LogicalTypeString,
						StarVisible:        true,
					},
				},
				Properties:    []PropertyDefinition{},
				Relationships: []RelationshipDefinition{},
			},
		},
		ExpressionFields:  []ExpressionFieldDefinition{},
		VirtualTables:     []VirtualTableDefinition{},
		SavedQueries:      []SavedQueryReference{},
		MaterializedViews: []MaterializedViewReference{},
		Functions:         []FunctionCapabilityDefinition{},
		ModifierDefaults:  []SemanticModifierDefault{},
	}
}
