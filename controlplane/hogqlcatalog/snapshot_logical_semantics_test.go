package hogqlcatalog

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestLogicalSemanticRecipesRoundTripWithoutAliasing(t *testing.T) {
	snapshot := logicalSemanticSnapshot(1)
	document, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	decoded, err := DecodeSnapshot(strings.NewReader(string(document)))
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("remarshal snapshot: %v", err)
	}
	if string(encoded) != string(document) {
		t.Fatalf("JSON contract changed during decode\n got: %s\nwant: %s", encoded, document)
	}

	store := NewMemoryStore()
	if err := store.Publish(context.Background(), snapshot); err != nil {
		t.Fatalf("publish snapshot: %v", err)
	}
	read, err := store.Latest(context.Background(), testCatalog())
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if !reflect.DeepEqual(read, snapshot) {
		t.Fatalf("publisher round trip changed snapshot\n got: %#v\nwant: %#v", read, snapshot)
	}

	snapshot.LogicalTables[0].Properties[0].LookupRecipe.Operator.Arguments[0].ArgumentReference.Argument = ExpressionArgumentPropertyKey
	read.LogicalTables[0].Relationships[0].JoinPredicate.Operator.Arguments[0].ScopedFieldReference.Field = "mutated"
	read.LazyTables[0].Projections[0].Recipe.PropertyLookup.Property = "mutated"
	read.Actions[0].Representation.Predicate.Operator.Arguments[0].PropertyLookup.Property = "mutated"
	read.Cohorts[0].Representation.Relation.Relation.Name = "mutated"

	again, err := store.Latest(context.Background(), testCatalog())
	if err != nil {
		t.Fatalf("reread snapshot: %v", err)
	}
	if got := again.LogicalTables[0].Properties[0].LookupRecipe.Operator.Arguments[0].ArgumentReference.Argument; got != ExpressionArgumentPropertySource {
		t.Fatalf("published property recipe leaked caller mutation: argument = %q", got)
	}
	if got := again.LogicalTables[0].Relationships[0].JoinPredicate.Operator.Arguments[0].ScopedFieldReference.Field; got != "id" {
		t.Fatalf("published join recipe leaked reader mutation: field = %q", got)
	}
	if got := again.LazyTables[0].Projections[0].Recipe.PropertyLookup.Property; got != "properties" {
		t.Fatalf("published lazy projection leaked reader mutation: property = %q", got)
	}
	if got := again.Actions[0].Representation.Predicate.Operator.Arguments[0].PropertyLookup.Property; got != "properties" {
		t.Fatalf("published action predicate leaked reader mutation: property = %q", got)
	}
	if got := again.Cohorts[0].Representation.Relation.Relation.Name; got != "daily_events" {
		t.Fatalf("published cohort relation leaked reader mutation: relation = %q", got)
	}
}

func TestOptionalLogicalSemanticListsHaveOneCanonicalEncoding(t *testing.T) {
	store := NewMemoryStore()
	withoutOptionalLists := completeSemanticSnapshot(1)
	if err := store.Publish(context.Background(), withoutOptionalLists); err != nil {
		t.Fatalf("publish snapshot without optional lists: %v", err)
	}
	withEmptyOptionalLists := completeSemanticSnapshot(1)
	withEmptyOptionalLists.LazyTables = []LazyTableDefinition{}
	withEmptyOptionalLists.Actions = []ActionReference{}
	withEmptyOptionalLists.Cohorts = []CohortReference{}
	if err := store.Publish(context.Background(), withEmptyOptionalLists); err != nil {
		t.Fatalf("idempotent publish with empty optional lists: %v", err)
	}
}

func TestPublishRejectsInvalidLogicalSemanticRecipes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*HogQLSemanticCatalogSnapshot)
	}{
		{
			name: "property lookup missing key input",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LogicalTables[0].Properties[0].LookupRecipe = ptrExpressionRecipe(argumentRecipe(ExpressionArgumentPropertySource))
			},
		},
		{
			name: "property lookup function arity",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LogicalTables[0].Properties[0].LookupRecipe = ptrExpressionRecipe(ExpressionRecipe{
					Kind: ExpressionRecipeFunctionCall,
					FunctionCall: &FunctionCallRecipe{
						Name:      "concat",
						Arguments: []ExpressionRecipe{argumentRecipe(ExpressionArgumentPropertySource)},
					},
				})
			},
		},
		{
			name: "JSON object lookup operator arity",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LogicalTables[0].Properties[0].LookupRecipe.Operator.Arguments = snapshot.LogicalTables[0].Properties[0].LookupRecipe.Operator.Arguments[:1]
			},
		},
		{
			name: "property reference unknown property",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Actions[0].Representation.Predicate.Operator.Arguments[0].PropertyLookup.Property = "missing"
			},
		},
		{
			name: "property key recipe depth limit",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				recipe := ExpressionRecipe{Kind: ExpressionRecipeLiteral, Literal: &TypedLiteral{TypeSignature: "varchar", Encoding: LiteralEncodingString, Value: "plan"}}
				for range maxExpressionRecipeDepth {
					nested := recipe
					recipe = ExpressionRecipe{Kind: ExpressionRecipeCast, Cast: &CastRecipe{Expression: &nested, TargetTypeSignature: "varchar"}}
				}
				snapshot.Actions[0].Representation.Predicate.Operator.Arguments[0].PropertyLookup.Key = &recipe
			},
		},
		{
			name: "join predicate unknown source field",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LogicalTables[0].Relationships[0].JoinPredicate.Operator.Arguments[0].ScopedFieldReference.Field = "missing"
			},
		},
		{
			name: "join predicate ordinary field reference",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LogicalTables[0].Relationships[0].JoinPredicate.Operator.Arguments[0] = fieldRecipe("events", "id")
			},
		},
		{
			name: "lazy table unknown relationship",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LazyTables[0].RelationshipPath[0] = "missing"
			},
		},
		{
			name: "lazy projection references source table",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LazyTables[0].Projections[0].Recipe.PropertyLookup.Table = "events"
			},
		},
		{
			name: "lazy table member conflict",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LazyTables[0].Name = "person"
			},
		},
		{
			name: "lazy relationship path depth limit",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.LazyTables[0].RelationshipPath = make([]string, maxSemanticRelationDepth+1)
				for index := range snapshot.LazyTables[0].RelationshipPath {
					snapshot.LazyTables[0].RelationshipPath[index] = "person"
				}
			},
		},
		{
			name: "action mismatched representation payload",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Actions[0].Representation.Relation = &RelationMembershipRecipe{
					Relation:    RelationReference{Kind: RelationKindMaterializedView, Name: "daily_events"},
					SourceField: "id", TargetField: "day",
				}
			},
		},
		{
			name: "action unknown owner table",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Actions[0].Table = "missing"
			},
		},
		{
			name: "cohort relation kind mismatch",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Cohorts[0].Representation.Relation.Relation.Kind = RelationKindVirtualTable
			},
		},
		{
			name: "cohort unknown target field",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Cohorts[0].Representation.Relation.TargetField = "missing"
			},
		},
		{
			name: "expression argument outside property recipe",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Actions[0].Representation.Predicate = ptrExpressionRecipe(argumentRecipe(ExpressionArgumentPropertyKey))
			},
		},
		{
			name: "scoped field outside join predicate",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Actions[0].Representation.Predicate = ptrExpressionRecipe(scopedFieldRecipe(RelationshipJoinSource, "id"))
			},
		},
		{
			name: "global semantic recipe node limit",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Actions = make([]ActionReference, maxExpressionRecipeNodes+1)
				for index := range snapshot.Actions {
					snapshot.Actions[index] = ActionReference{
						Name: "action_" + strconv.Itoa(index), ActionID: "id_" + strconv.Itoa(index), Table: "events",
						Representation: SemanticEntityRepresentation{
							Kind: SemanticEntityPredicate,
							Predicate: ptrExpressionRecipe(ExpressionRecipe{
								Kind:    ExpressionRecipeLiteral,
								Literal: &TypedLiteral{TypeSignature: "boolean", Encoding: LiteralEncodingBoolean, Value: "true"},
							}),
						},
					}
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := logicalSemanticSnapshot(1)
			test.mutate(snapshot)
			if err := NewMemoryStore().Publish(context.Background(), snapshot); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("publish error = %v, want ErrInvalidSnapshot", err)
			}
		})
	}
}

func logicalSemanticSnapshot(generation int64) *HogQLSemanticCatalogSnapshot {
	snapshot := completeSemanticSnapshot(generation)
	snapshot.LogicalTables[0].Properties[0].KeyTypeSignature = "varchar"
	snapshot.LogicalTables[0].Properties[0].ValueTypeSignature = "json"
	snapshot.LogicalTables[0].Properties[0].LookupRecipe = &ExpressionRecipe{
		Kind: ExpressionRecipeOperator,
		Operator: &OperatorRecipe{
			Operator: SemanticOperatorJSONObjectLookup,
			Arguments: []ExpressionRecipe{
				argumentRecipe(ExpressionArgumentPropertySource),
				argumentRecipe(ExpressionArgumentPropertyKey),
			},
		},
	}
	snapshot.LogicalTables[1].Properties = []PropertyDefinition{{
		Name: "properties", SourceField: "id", Storage: PropertyStorageJSONObject, LogicalType: LogicalTypeJSON, Nullable: true,
		KeyTypeSignature: "varchar", ValueTypeSignature: "json",
		LookupRecipe: ptrExpressionRecipe(ExpressionRecipe{
			Kind: ExpressionRecipeOperator,
			Operator: &OperatorRecipe{
				Operator: SemanticOperatorJSONObjectLookup,
				Arguments: []ExpressionRecipe{
					argumentRecipe(ExpressionArgumentPropertySource),
					argumentRecipe(ExpressionArgumentPropertyKey),
				},
			},
		}),
	}}
	snapshot.LogicalTables[0].Relationships[0].JoinPredicate = &ExpressionRecipe{
		Kind: ExpressionRecipeOperator,
		Operator: &OperatorRecipe{
			Operator: SemanticOperatorEqual,
			Arguments: []ExpressionRecipe{
				scopedFieldRecipe(RelationshipJoinSource, "id"),
				scopedFieldRecipe(RelationshipJoinTarget, "id"),
			},
		},
	}
	snapshot.LazyTables = []LazyTableDefinition{{
		Table: "events", Name: "person_profile", RelationshipPath: []string{"person"},
		Projections: []LazyProjectionDefinition{{
			Name: "properties", TrinoTypeSignature: "json", LogicalType: LogicalTypeJSON, Nullable: true, StarVisible: true,
			Recipe: propertyLookupRecipe("persons", "properties", "plan"),
		}},
	}}
	snapshot.Actions = []ActionReference{{
		Name: "paid_event", ActionID: "action_example", Table: "events",
		Representation: SemanticEntityRepresentation{
			Kind: SemanticEntityPredicate,
			Predicate: ptrExpressionRecipe(ExpressionRecipe{
				Kind: ExpressionRecipeOperator,
				Operator: &OperatorRecipe{
					Operator: SemanticOperatorEqual,
					Arguments: []ExpressionRecipe{
						propertyLookupRecipe("events", "properties", "plan"),
						{Kind: ExpressionRecipeLiteral, Literal: &TypedLiteral{TypeSignature: "varchar", Encoding: LiteralEncodingString, Value: "paid"}},
					},
				},
			}),
		},
	}}
	snapshot.Cohorts = []CohortReference{{
		Name: "active_people", CohortID: "cohort_example", Table: "events",
		Representation: SemanticEntityRepresentation{
			Kind: SemanticEntityRelation,
			Relation: &RelationMembershipRecipe{
				Relation:    RelationReference{Kind: RelationKindMaterializedView, Name: "daily_events"},
				SourceField: "id", TargetField: "day",
			},
		},
	}}
	return snapshot
}

func propertyLookupRecipe(table, property, key string) ExpressionRecipe {
	return ExpressionRecipe{
		Kind: ExpressionRecipePropertyLookup,
		PropertyLookup: &PropertyLookupReferenceRecipe{
			Table: table, Property: property,
			Key: ptrExpressionRecipe(ExpressionRecipe{Kind: ExpressionRecipeLiteral, Literal: &TypedLiteral{TypeSignature: "varchar", Encoding: LiteralEncodingString, Value: key}}),
		},
	}
}

func argumentRecipe(argument ExpressionArgument) ExpressionRecipe {
	return ExpressionRecipe{
		Kind:              ExpressionRecipeArgumentReference,
		ArgumentReference: &ArgumentReferenceRecipe{Argument: argument},
	}
}

func scopedFieldRecipe(side RelationshipJoinSide, field string) ExpressionRecipe {
	return ExpressionRecipe{
		Kind:                 ExpressionRecipeScopedFieldReference,
		ScopedFieldReference: &ScopedFieldReferenceRecipe{Side: side, Field: field},
	}
}

func ptrExpressionRecipe(recipe ExpressionRecipe) *ExpressionRecipe {
	return &recipe
}
