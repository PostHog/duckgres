package hogqlcatalog

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestPhysicalRefreshBootstrapsPostHogV0Profile(t *testing.T) {
	store := NewMemoryStore()
	lease := acquirePhysicalRefresh(t, store)
	snapshot, published, err := store.PublishPhysicalRefresh(context.Background(), lease, postHogV0PhysicalCatalog(), "1.0.0", time.Hour)
	if err != nil || !published || snapshot.Generation != 1 {
		t.Fatalf("physical refresh = (%#v, %t, %v), want published generation 1", snapshot, published, err)
	}

	events := logicalTableByName(t, snapshot, "events")
	persons := logicalTableByName(t, snapshot, "persons")
	assertPostHogV0Property(t, events)
	assertPostHogV0Property(t, persons)
	if len(events.Relationships) != 1 {
		t.Fatalf("events relationships = %#v, want one person relationship", events.Relationships)
	}
	relationship := events.Relationships[0]
	if relationship.Name != "person" || relationship.TargetTable != "persons" || relationship.Cardinality != RelationshipCardinalityManyToOne || !reflect.DeepEqual(relationship.JoinKeys, []JoinKey{{SourceField: "person_id", TargetField: "id"}}) {
		t.Fatalf("events.person relationship = %#v", relationship)
	}
	if snapshot.LazyTables != nil || snapshot.Actions != nil || snapshot.Cohorts != nil || len(snapshot.Functions) != 0 || len(snapshot.ModifierDefaults) != 0 {
		t.Fatalf("v0 profile added non-MVP semantic definitions: %#v", snapshot)
	}
}

func TestPhysicalRefreshSkipsPostHogV0ProfileForIneligibleSchemas(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*PhysicalCatalogMetadata)
	}{
		{
			name: "missing persons table",
			mutate: func(metadata *PhysicalCatalogMetadata) {
				metadata.Tables = metadata.Tables[:1]
			},
		},
		{
			name: "missing person properties",
			mutate: func(metadata *PhysicalCatalogMetadata) {
				metadata.Tables[1].Columns = metadata.Tables[1].Columns[:1]
			},
		},
		{
			name: "non-varchar event properties",
			mutate: func(metadata *PhysicalCatalogMetadata) {
				metadata.Tables[0].Columns[0].TrinoTypeSignature = "json"
			},
		},
		{
			name: "incompatible person ID",
			mutate: func(metadata *PhysicalCatalogMetadata) {
				metadata.Tables[1].Columns[0].TrinoTypeSignature = "uuid"
			},
		},
		{
			name: "different schemas",
			mutate: func(metadata *PhysicalCatalogMetadata) {
				metadata.Tables[1].Schema = PhysicalIdentifier{Value: "other"}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata := postHogV0PhysicalCatalog()
			test.mutate(metadata)
			store := NewMemoryStore()
			lease := acquirePhysicalRefresh(t, store)
			snapshot, published, err := store.PublishPhysicalRefresh(context.Background(), lease, metadata, "1.0.0", time.Hour)
			if err != nil || !published {
				t.Fatalf("physical refresh = (%#v, %t, %v), want published physical snapshot", snapshot, published, err)
			}
			for _, table := range snapshot.LogicalTables {
				if len(table.Properties) != 0 || len(table.Relationships) != 0 {
					t.Fatalf("ineligible table %q received v0 profile: %#v", table.Name, table)
				}
			}
		})
	}
}

func TestPhysicalRefreshSkipsAmbiguousPostHogV0Profile(t *testing.T) {
	ctx := context.Background()
	metadata := postHogV0PhysicalCatalog()
	generic, err := buildPhysicalSnapshot(ctx, metadata, testCatalog(), "1.0.0", 1)
	if err != nil {
		t.Fatalf("build generic snapshot: %v", err)
	}
	events := generic.LogicalTables[0]
	alias := events
	alias.Name = "event_stream"
	generic.LogicalTables = []LogicalTableDefinition{events, alias, generic.LogicalTables[1]}

	store := NewMemoryStore()
	if err := store.Publish(ctx, generic); err != nil {
		t.Fatalf("publish ambiguous snapshot: %v", err)
	}
	lease := acquirePhysicalRefresh(t, store)
	refreshed, published, err := store.PublishPhysicalRefresh(ctx, lease, metadata, "1.0.0", time.Hour)
	if err != nil {
		t.Fatalf("refresh ambiguous snapshot: %v", err)
	}
	if published || refreshed.Generation != 1 {
		t.Fatalf("ambiguous refresh = generation %d published %t, want unchanged generation 1", refreshed.Generation, published)
	}
	for _, table := range refreshed.LogicalTables {
		if len(table.Properties) != 0 || len(table.Relationships) != 0 {
			t.Fatalf("ambiguous table %q received v0 profile: %#v", table.Name, table)
		}
	}
}

func TestPhysicalRefreshDoesNotOverwritePublishedPostHogMembers(t *testing.T) {
	ctx := context.Background()
	metadata := postHogV0PhysicalCatalog()
	external, err := buildPhysicalSnapshot(ctx, metadata, testCatalog(), "1.0.0", 1)
	if err != nil {
		t.Fatalf("build external snapshot: %v", err)
	}
	events := &external.LogicalTables[0]
	persons := &external.LogicalTables[1]
	events.Properties = []PropertyDefinition{externalProperty("properties", "properties", true)}
	persons.Properties = []PropertyDefinition{externalProperty("properties", "properties", false)}
	events.Relationships = []RelationshipDefinition{{
		Name: "person", TargetTable: "persons", Cardinality: RelationshipCardinalityOneToOne,
		JoinKeys: []JoinKey{{SourceField: "person_id", TargetField: "id"}},
	}}

	store := NewMemoryStore()
	if err := store.Publish(ctx, external); err != nil {
		t.Fatalf("publish external snapshot: %v", err)
	}
	lease := acquirePhysicalRefresh(t, store)
	refreshed, published, err := store.PublishPhysicalRefresh(ctx, lease, metadata, "1.0.0", time.Hour)
	if err != nil {
		t.Fatalf("refresh external snapshot: %v", err)
	}
	if published || refreshed.Generation != 1 {
		t.Fatalf("external refresh = generation %d published %t, want unchanged generation 1", refreshed.Generation, published)
	}
	if !reflect.DeepEqual(logicalTableByName(t, refreshed, "events").Properties, events.Properties) ||
		!reflect.DeepEqual(logicalTableByName(t, refreshed, "persons").Properties, persons.Properties) ||
		!reflect.DeepEqual(logicalTableByName(t, refreshed, "events").Relationships, events.Relationships) {
		t.Fatalf("physical refresh overwrote external members: %#v", refreshed.LogicalTables)
	}
}

func TestPhysicalRefreshPublishesSemanticOnlyPostHogProfileChange(t *testing.T) {
	ctx := context.Background()
	metadata := postHogV0PhysicalCatalog()
	generic, err := buildPhysicalSnapshot(ctx, metadata, testCatalog(), "1.0.0", 1)
	if err != nil {
		t.Fatalf("build generic snapshot: %v", err)
	}
	store := NewMemoryStore()
	if err := store.Publish(ctx, generic); err != nil {
		t.Fatalf("publish generic snapshot: %v", err)
	}

	lease := acquirePhysicalRefresh(t, store)
	profiled, published, err := store.PublishPhysicalRefresh(ctx, lease, metadata, "1.0.0", time.Hour)
	if err != nil || !published || profiled.Generation != 2 {
		t.Fatalf("semantic-only refresh = (%#v, %t, %v), want published generation 2", profiled, published, err)
	}
	assertPostHogV0Property(t, logicalTableByName(t, profiled, "events"))

	lease = acquirePhysicalRefresh(t, store)
	retried, published, err := store.PublishPhysicalRefresh(ctx, lease, metadata, "1.0.0", time.Hour)
	if err != nil || published || retried.Generation != 2 {
		t.Fatalf("identical profiled refresh = (%#v, %t, %v), want unchanged generation 2", retried, published, err)
	}
}

func postHogV0PhysicalCatalog() *PhysicalCatalogMetadata {
	return &PhysicalCatalogMetadata{
		Catalog: testCatalog(),
		Tables: []PhysicalTableMetadata{
			{
				Schema: PhysicalIdentifier{Value: "posthog"},
				Table:  PhysicalIdentifier{Value: postHogEventsTableName},
				Columns: []PhysicalColumnMetadata{
					{Name: PhysicalIdentifier{Value: postHogPropertiesName}, Ordinal: 1, TrinoTypeSignature: "varchar", Nullability: ColumnNotNull, StarVisibility: ColumnStarVisible},
					{Name: PhysicalIdentifier{Value: postHogEventPersonIDName}, Ordinal: 2, TrinoTypeSignature: "varchar", Nullability: ColumnNullable, StarVisibility: ColumnStarVisible},
				},
			},
			{
				Schema: PhysicalIdentifier{Value: "posthog"},
				Table:  PhysicalIdentifier{Value: postHogPersonsTableName},
				Columns: []PhysicalColumnMetadata{
					{Name: PhysicalIdentifier{Value: postHogPersonIDName}, Ordinal: 1, TrinoTypeSignature: "varchar", Nullability: ColumnNotNull, StarVisibility: ColumnStarVisible},
					{Name: PhysicalIdentifier{Value: postHogPropertiesName}, Ordinal: 2, TrinoTypeSignature: "varchar", Nullability: ColumnNullable, StarVisibility: ColumnStarVisible},
				},
			},
		},
	}
}

func assertPostHogV0Property(t *testing.T, table *LogicalTableDefinition) {
	t.Helper()
	if len(table.Properties) != 1 {
		t.Fatalf("%s properties = %#v, want one JSON property definition", table.Name, table.Properties)
	}
	property := table.Properties[0]
	if property.Name != "properties" || property.SourceField != "properties" || property.Storage != PropertyStorageJSONObject || property.LogicalType != LogicalTypeString || property.KeyTypeSignature != "varchar" || property.ValueTypeSignature != "varchar" {
		t.Fatalf("%s property definition = %#v", table.Name, property)
	}
	if property.LookupRecipe == nil || property.LookupRecipe.Kind != ExpressionRecipeOperator || property.LookupRecipe.Operator == nil || property.LookupRecipe.Operator.Operator != SemanticOperatorJSONObjectLookup {
		t.Fatalf("%s property lookup recipe = %#v", table.Name, property.LookupRecipe)
	}
	wantArguments := []ExpressionRecipe{
		argumentReferenceRecipe(ExpressionArgumentPropertySource),
		argumentReferenceRecipe(ExpressionArgumentPropertyKey),
	}
	if !reflect.DeepEqual(property.LookupRecipe.Operator.Arguments, wantArguments) {
		t.Fatalf("%s property lookup arguments = %#v, want %#v", table.Name, property.LookupRecipe.Operator.Arguments, wantArguments)
	}
}

func logicalTableByName(t *testing.T, snapshot *HogQLSemanticCatalogSnapshot, name string) *LogicalTableDefinition {
	t.Helper()
	for index := range snapshot.LogicalTables {
		if snapshot.LogicalTables[index].Name == name {
			return &snapshot.LogicalTables[index]
		}
	}
	t.Fatalf("snapshot has no logical table %q", name)
	return nil
}

func externalProperty(name, source string, nullable bool) PropertyDefinition {
	return PropertyDefinition{
		Name: name, SourceField: source, Storage: PropertyStorageJSONObject, LogicalType: LogicalTypeString, Nullable: nullable,
		KeyTypeSignature: "varchar", ValueTypeSignature: "varchar",
		LookupRecipe: &ExpressionRecipe{
			Kind: ExpressionRecipeOperator,
			Operator: &OperatorRecipe{
				Operator: SemanticOperatorJSONObjectLookup,
				Arguments: []ExpressionRecipe{
					argumentReferenceRecipe(ExpressionArgumentPropertySource),
					argumentReferenceRecipe(ExpressionArgumentPropertyKey),
				},
			},
		},
	}
}
