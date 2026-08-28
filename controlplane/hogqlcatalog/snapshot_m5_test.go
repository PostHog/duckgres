package hogqlcatalog

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestSemanticMetadataJSONAndPublisherRoundTrip(t *testing.T) {
	snapshot := completeSemanticSnapshot(1)
	document, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	var manifest struct {
		Functions []map[string]json.RawMessage `json:"functions"`
	}
	if err := json.Unmarshal(document, &manifest); err != nil {
		t.Fatalf("inspect function contract: %v", err)
	}
	for index, rewrite := range []string{`"IS_NULL"`, `"IS_NOT_NULL"`} {
		if got := string(manifest.Functions[index+1]["rewrite"]); got != rewrite {
			t.Fatalf("function %d rewrite = %s, want %s", index+1, got, rewrite)
		}
	}
	if _, exists := manifest.Functions[0]["rewrite"]; exists {
		t.Fatal("stock function unexpectedly published rewrite")
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

	snapshot.ExpressionFields[0].Recipe.FunctionCall.Arguments[0].FieldReference.Field = "mutated"
	read.VirtualTables[0].Projections[0].Name = "mutated"
	again, err := store.Latest(context.Background(), testCatalog())
	if err != nil {
		t.Fatalf("reread snapshot: %v", err)
	}
	if got := again.ExpressionFields[0].Recipe.FunctionCall.Arguments[0].FieldReference.Field; got != "id" {
		t.Fatalf("published expression leaked caller mutation: field = %q", got)
	}
	if got := again.VirtualTables[0].Projections[0].Name; got != "id" {
		t.Fatalf("published projection leaked reader mutation: name = %q", got)
	}
}

func TestFunctionRewriteContractRoundTrip(t *testing.T) {
	rewrites := make([]FunctionRewriteIdentifier, 0, len(functionRewriteContracts))
	for rewrite := range functionRewriteContracts {
		rewrites = append(rewrites, rewrite)
	}
	sort.Slice(rewrites, func(left, right int) bool { return rewrites[left] < rewrites[right] })

	snapshot := testSnapshot(1)
	snapshot.Functions = make([]FunctionCapabilityDefinition, 0, len(rewrites))
	for _, rewrite := range rewrites {
		contract := functionRewriteContracts[rewrite]
		snapshot.Functions = append(snapshot.Functions, validRewriteFunction(rewrite, contract.Signature))
	}
	document, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	decoded, err := DecodeSnapshot(strings.NewReader(string(document)))
	if err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if !reflect.DeepEqual(decoded.Functions, snapshot.Functions) {
		t.Fatalf("rewrite contract changed during JSON round trip\n got: %#v\nwant: %#v", decoded.Functions, snapshot.Functions)
	}
}

func TestFunctionRewriteSignatureContract(t *testing.T) {
	tests := []struct {
		name           string
		rewrite        FunctionRewriteIdentifier
		kind           FunctionKind
		signature      FunctionSignature
		supportsWindow bool
		valid          bool
	}{
		{name: "zero argument scalar", rewrite: FunctionRewriteToday, kind: FunctionKindScalar, signature: rewriteSignature(0, false, "date"), valid: true},
		{name: "unary scalar", rewrite: FunctionRewriteCastBigint, kind: FunctionKindScalar, signature: rewriteSignature(1, false, "bigint"), valid: true},
		{name: "binary aggregate", rewrite: FunctionRewriteSumIf, kind: FunctionKindAggregate, signature: rewriteSignature(2, false, "any"), valid: true},
		{name: "ternary aggregate", rewrite: FunctionRewriteArgMaxIf, kind: FunctionKindAggregate, signature: rewriteSignature(3, false, "any"), valid: true},
		{name: "one or two arguments", rewrite: FunctionRewriteCastTimestamp, kind: FunctionKindScalar, signature: rewriteSignature(2, false, "timestamp"), valid: true},
		{name: "two or three arguments", rewrite: FunctionRewriteDateAdd, kind: FunctionKindScalar, signature: rewriteSignature(3, false, "any"), valid: true},
		{name: "fixed or variadic from one", rewrite: FunctionRewriteJSONLength, kind: FunctionKindScalar, signature: rewriteSignature(3, true, "bigint"), valid: true},
		{name: "fixed or variadic from two", rewrite: FunctionRewriteAnd, kind: FunctionKindScalar, signature: rewriteSignature(3, true, "boolean"), valid: true},
		{name: "variadic from one", rewrite: FunctionRewriteTuple, kind: FunctionKindScalar, signature: rewriteSignature(2, true, "row"), valid: true},
		{name: "variadic from two", rewrite: FunctionRewriteJSONExtractInt, kind: FunctionKindScalar, signature: rewriteSignature(3, true, "bigint"), valid: true},
		{name: "variadic from three", rewrite: FunctionRewriteMultiIf, kind: FunctionKindScalar, signature: rewriteSignature(4, true, "any"), valid: true},
		{name: "wrong kind", rewrite: FunctionRewriteCountIf, kind: FunctionKindScalar, signature: rewriteSignature(1, false, "bigint"), valid: false},
		{name: "wrong fixed arity", rewrite: FunctionRewriteRegexReplaceAll, kind: FunctionKindScalar, signature: rewriteSignature(2, false, "varchar"), valid: false},
		{name: "wrong variadic shape", rewrite: FunctionRewriteMultiIf, kind: FunctionKindScalar, signature: rewriteSignature(3, true, "any"), valid: false},
		{name: "unexpected variadic signature", rewrite: FunctionRewriteCastDate, kind: FunctionKindScalar, signature: rewriteSignature(2, true, "date"), valid: false},
		{name: "null predicate non-boolean result", rewrite: FunctionRewriteIsNull, kind: FunctionKindScalar, signature: rewriteSignature(1, false, "bigint"), valid: false},
		{name: "aggregate window invocation", rewrite: FunctionRewriteSumIf, kind: FunctionKindAggregate, signature: rewriteSignature(2, false, "any"), supportsWindow: true, valid: true},
		{name: "scalar window invocation", rewrite: FunctionRewriteCastDate, kind: FunctionKindScalar, signature: rewriteSignature(1, false, "date"), supportsWindow: true, valid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := testSnapshot(1)
			function := validRewriteFunction(test.rewrite, functionRewriteContracts[test.rewrite].Signature)
			function.Kind = test.kind
			function.Signatures = []FunctionSignature{test.signature}
			function.SupportsWindow = test.supportsWindow
			snapshot.Functions = []FunctionCapabilityDefinition{function}
			err := NewMemoryStore().Publish(context.Background(), snapshot)
			if test.valid && err != nil {
				t.Fatalf("publish valid rewrite: %v", err)
			}
			if !test.valid && !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("publish invalid rewrite error = %v, want ErrInvalidSnapshot", err)
			}
		})
	}
}

func TestHTTPSemanticMetadataRoundTrip(t *testing.T) {
	snapshot := completeSemanticSnapshot(1)
	router := testRouter(NewMemoryStore())
	publishSnapshot(t, router, snapshot, 204)
	read := getSnapshot(t, router, compatibilityPath("ducklake", false, 1, "1.0.0"), 200)
	if !reflect.DeepEqual(read, snapshot) {
		t.Fatalf("HTTP round trip changed snapshot\n got: %#v\nwant: %#v", read, snapshot)
	}
}

func TestDecimalLiteralValidationIsExact(t *testing.T) {
	valid := completeSemanticSnapshot(1)
	valid.ModifierDefaults = append(valid.ModifierDefaults, SemanticModifierDefault{
		Name: "large_decimal", Behavior: ModifierBehaviorCompiler,
		DefaultValue: TypedLiteral{TypeSignature: "decimal(38, 9)", Encoding: LiteralEncodingDecimal, Value: "12345678901234567890123456789.123456789"},
	})
	if err := NewMemoryStore().Publish(context.Background(), valid); err != nil {
		t.Fatalf("publish exact decimal: %v", err)
	}

	for _, value := range []string{"NaN", "Inf", "-Inf", "1e3", ".5", "01"} {
		t.Run(value, func(t *testing.T) {
			snapshot := completeSemanticSnapshot(1)
			snapshot.ModifierDefaults[0].DefaultValue = TypedLiteral{TypeSignature: "decimal(10, 2)", Encoding: LiteralEncodingDecimal, Value: value}
			if err := NewMemoryStore().Publish(context.Background(), snapshot); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("publish decimal %q error = %v, want ErrInvalidSnapshot", value, err)
			}
		})
	}
}

func TestPublishRejectsInvalidSemanticRecipeGraph(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*HogQLSemanticCatalogSnapshot)
	}{
		{
			name: "unknown virtual source",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.VirtualTables[0].Source.Name = "missing"
			},
		},
		{
			name: "unknown saved query target",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.SavedQueries[0].Target.Name = "missing"
			},
		},
		{
			name: "saved query and virtual table cycle",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.VirtualTables[0].Source = RelationReference{Kind: RelationKindSavedQuery, Name: "example_funnel"}
			},
		},
		{
			name: "virtual source cycle",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.VirtualTables = append(snapshot.VirtualTables, VirtualTableDefinition{
					Name: "cycle_a", Source: RelationReference{Kind: RelationKindVirtualTable, Name: "cycle_b"}, Projections: []VirtualProjection{{Name: "id", SourceField: "id", StarVisible: true}},
				}, VirtualTableDefinition{
					Name: "cycle_b", Source: RelationReference{Kind: RelationKindVirtualTable, Name: "cycle_a"}, Projections: []VirtualProjection{{Name: "id", SourceField: "id", StarVisible: true}},
				})
			},
		},
		{
			name: "semantic relation depth limit",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				for index := range maxSemanticRelationDepth + 1 {
					source := RelationReference{Kind: RelationKindLogicalTable, Name: "events"}
					if index < maxSemanticRelationDepth {
						source = RelationReference{Kind: RelationKindVirtualTable, Name: "deep_" + strconv.Itoa(index+1)}
					}
					snapshot.VirtualTables = append(snapshot.VirtualTables, VirtualTableDefinition{
						Name: "deep_" + strconv.Itoa(index), Source: source,
						Projections: []VirtualProjection{{Name: "id", SourceField: "id", StarVisible: true}},
					})
				}
			},
		},
		{
			name: "expression field cycle",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ExpressionFields = []ExpressionFieldDefinition{
					{Table: "events", Name: "a", TrinoTypeSignature: "varchar", LogicalType: LogicalTypeString, Recipe: fieldRecipe("events", "b")},
					{Table: "events", Name: "b", TrinoTypeSignature: "varchar", LogicalType: LogicalTypeString, Recipe: fieldRecipe("events", "a")},
				}
			},
		},
		{
			name: "expression field conflicts with property",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ExpressionFields[0].Name = "properties"
			},
		},
		{
			name: "expression field conflicts with relationship",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ExpressionFields[0].Name = "person"
			},
		},
		{
			name: "unknown expression field reference",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ExpressionFields[0].Recipe.FunctionCall.Arguments[0] = fieldRecipe("events", "missing")
			},
		},
		{
			name: "cross-table expression field reference",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ExpressionFields[0].Recipe.FunctionCall.Arguments[0] = fieldRecipe("persons", "id")
			},
		},
		{
			name: "lossy decimal literal",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ExpressionFields[0].Recipe.FunctionCall.Arguments[1].Literal.Encoding = LiteralEncodingDecimal
				snapshot.ExpressionFields[0].Recipe.FunctionCall.Arguments[1].Literal.Value = "1e309"
			},
		},
		{
			name: "mismatched recipe payload",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ExpressionFields[0].Recipe.FieldReference = &FieldReferenceRecipe{Table: "events", Field: "id"}
			},
		},
		{
			name: "binary operator with one argument",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ExpressionFields[0].Recipe = ExpressionRecipe{
					Kind: ExpressionRecipeOperator,
					Operator: &OperatorRecipe{
						Operator:  SemanticOperatorAdd,
						Arguments: []ExpressionRecipe{fieldRecipe("events", "id")},
					},
				}
			},
		},
		{
			name: "unary operator with two arguments",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ExpressionFields[0].Recipe = ExpressionRecipe{
					Kind: ExpressionRecipeOperator,
					Operator: &OperatorRecipe{
						Operator: SemanticOperatorNot,
						Arguments: []ExpressionRecipe{
							fieldRecipe("events", "id"),
							fieldRecipe("events", "id"),
						},
					},
				}
			},
		},
		{
			name: "expression recipe depth limit",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				recipe := fieldRecipe("events", "id")
				for range maxExpressionRecipeDepth {
					nested := recipe
					recipe = ExpressionRecipe{Kind: ExpressionRecipeCast, Cast: &CastRecipe{Expression: &nested, TargetTypeSignature: "varchar"}}
				}
				snapshot.ExpressionFields[0].Recipe = recipe
			},
		},
		{
			name: "session modifier missing property",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ModifierDefaults[1].SessionProperty = nil
			},
		},
		{
			name: "stock function missing Trino name",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[0].TrinoName = nil
			},
		},
		{
			name: "UDF missing Trino name",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[0].Implementation = FunctionImplementationUDF
				snapshot.Functions[0].TrinoName = []PhysicalIdentifier{}
			},
		},
		{
			name: "rewrite function missing rewrite identifier",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].Rewrite = ""
			},
		},
		{
			name: "rewrite function with unknown rewrite identifier",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].Rewrite = FunctionRewriteIdentifier("UNKNOWN")
			},
		},
		{
			name: "rewrite function with Trino name",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].TrinoName = []PhysicalIdentifier{{Value: "is_null"}}
			},
		},
		{
			name: "rewrite function with non-scalar kind",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].Kind = FunctionKindAggregate
			},
		},
		{
			name: "rewrite function is non-deterministic",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].Deterministic = false
			},
		},
		{
			name: "rewrite function has zero-argument signature",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].Signatures = append(snapshot.Functions[1].Signatures, FunctionSignature{ArgumentTypes: []string{}, ReturnType: "boolean"})
			},
		},
		{
			name: "rewrite function has two-argument signature",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].Signatures = append(snapshot.Functions[1].Signatures, FunctionSignature{ArgumentTypes: []string{"varchar", "varchar"}, ReturnType: "boolean"})
			},
		},
		{
			name: "rewrite function has variadic signature",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].Signatures = append(snapshot.Functions[1].Signatures, FunctionSignature{ArgumentTypes: []string{"varchar"}, ReturnType: "boolean", Variadic: true})
			},
		},
		{
			name: "rewrite function has non-boolean return type",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].Signatures = append(snapshot.Functions[1].Signatures, FunctionSignature{ArgumentTypes: []string{"varchar"}, ReturnType: "bigint"})
			},
		},
		{
			name: "rewrite function supports distinct",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].SupportsDistinct = true
			},
		},
		{
			name: "rewrite function supports order by",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].SupportsOrderBy = true
			},
		},
		{
			name: "rewrite function supports filter",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].SupportsFilter = true
			},
		},
		{
			name: "scalar rewrite function supports window",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[1].SupportsWindow = true
			},
		},
		{
			name: "stock function with rewrite identifier",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[0].Rewrite = FunctionRewriteIsNull
			},
		},
		{
			name: "UDF with rewrite identifier",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.Functions[0].Implementation = FunctionImplementationUDF
				snapshot.Functions[0].Rewrite = FunctionRewriteIsNotNull
			},
		},
		{
			name: "session property with too many name parts",
			mutate: func(snapshot *HogQLSemanticCatalogSnapshot) {
				snapshot.ModifierDefaults[1].SessionProperty = []PhysicalIdentifier{
					{Value: "catalog"},
					{Value: "namespace"},
					{Value: "property"},
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := completeSemanticSnapshot(1)
			test.mutate(snapshot)
			if err := NewMemoryStore().Publish(context.Background(), snapshot); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("publish error = %v, want ErrInvalidSnapshot", err)
			}
		})
	}
}

func completeSemanticSnapshot(generation int64) *HogQLSemanticCatalogSnapshot {
	snapshot := testSnapshot(generation)
	snapshot.ExpressionFields = []ExpressionFieldDefinition{
		{
			Table: "events", Name: "display_name", TrinoTypeSignature: "varchar", LogicalType: LogicalTypeString, Nullable: false, StarVisible: true,
			Recipe: ExpressionRecipe{Kind: ExpressionRecipeFunctionCall, FunctionCall: &FunctionCallRecipe{
				Name: "concat",
				Arguments: []ExpressionRecipe{
					fieldRecipe("events", "id"),
					{Kind: ExpressionRecipeLiteral, Literal: &TypedLiteral{TypeSignature: "varchar", Encoding: LiteralEncodingString, Value: "-synthetic"}},
				},
			}},
		},
	}
	snapshot.VirtualTables = []VirtualTableDefinition{{
		Name: "recent_events", Source: RelationReference{Kind: RelationKindLogicalTable, Name: "events"},
		Projections: []VirtualProjection{{Name: "id", SourceField: "id", StarVisible: true}, {Name: "display_name", SourceField: "display_name", StarVisible: true}},
	}}
	snapshot.SavedQueries = []SavedQueryReference{{
		Name: "example_funnel", QueryID: "saved_query_example", Target: RelationReference{Kind: RelationKindVirtualTable, Name: "recent_events"},
		Fields: []ReferencedField{
			{Name: "id", TrinoTypeSignature: "varchar", LogicalType: LogicalTypeString, Nullable: false, StarVisible: true},
			{Name: "display_name", TrinoTypeSignature: "varchar", LogicalType: LogicalTypeString, Nullable: false, StarVisible: true},
		},
	}}
	snapshot.MaterializedViews = []MaterializedViewReference{{
		Name:         "daily_events",
		PhysicalView: PhysicalQualifiedName{Catalog: testCatalog(), Schema: PhysicalIdentifier{Value: "default"}, Table: PhysicalIdentifier{Value: "daily_events"}},
		Fields:       []ReferencedField{{Name: "day", TrinoTypeSignature: "date", LogicalType: LogicalTypeDate, Nullable: false, StarVisible: true}},
	}}
	snapshot.Functions = []FunctionCapabilityDefinition{
		{
			Name: "concat", Kind: FunctionKindScalar, Implementation: FunctionImplementationStock,
			TrinoName: []PhysicalIdentifier{{Value: "concat"}}, Deterministic: true,
			Signatures: []FunctionSignature{{ArgumentTypes: []string{"varchar", "varchar"}, ReturnType: "varchar", Variadic: false}},
		},
		{
			Name: "isNull", Kind: FunctionKindScalar, Implementation: FunctionImplementationRewrite,
			TrinoName: []PhysicalIdentifier{}, Rewrite: FunctionRewriteIsNull, Deterministic: true,
			Signatures: []FunctionSignature{{ArgumentTypes: []string{"varchar"}, ReturnType: "boolean", Variadic: false}},
		},
		{
			Name: "isNotNull", Kind: FunctionKindScalar, Implementation: FunctionImplementationRewrite,
			TrinoName: []PhysicalIdentifier{}, Rewrite: FunctionRewriteIsNotNull, Deterministic: true,
			Signatures: []FunctionSignature{{ArgumentTypes: []string{"varchar"}, ReturnType: "boolean", Variadic: false}},
		},
	}
	snapshot.ModifierDefaults = []SemanticModifierDefault{
		{Name: "week_start", Behavior: ModifierBehaviorCompiler, DefaultValue: TypedLiteral{TypeSignature: "integer", Encoding: LiteralEncodingInteger, Value: "1"}},
		{Name: "optimize_metadata_queries", Behavior: ModifierBehaviorTrinoSessionProperty, DefaultValue: TypedLiteral{TypeSignature: "boolean", Encoding: LiteralEncodingBoolean, Value: "true"}, SessionProperty: []PhysicalIdentifier{{Value: "optimizer"}, {Value: "optimize_metadata_queries"}}},
	}
	return snapshot
}

func fieldRecipe(table, field string) ExpressionRecipe {
	return ExpressionRecipe{Kind: ExpressionRecipeFieldReference, FieldReference: &FieldReferenceRecipe{Table: table, Field: field}}
}

func validRewriteFunction(rewrite FunctionRewriteIdentifier, signatureContract rewriteSignatureContract) FunctionCapabilityDefinition {
	returnType := "any"
	if rewrite == FunctionRewriteIsNull || rewrite == FunctionRewriteIsNotNull {
		returnType = "boolean"
	}
	return FunctionCapabilityDefinition{
		Name:           "rewrite_" + strings.ToLower(string(rewrite)),
		Kind:           functionRewriteContracts[rewrite].Kind,
		Implementation: FunctionImplementationRewrite,
		TrinoName:      []PhysicalIdentifier{},
		Rewrite:        rewrite,
		Signatures:     []FunctionSignature{validRewriteSignature(signatureContract, returnType)},
		Deterministic:  true,
	}
}

func validRewriteSignature(contract rewriteSignatureContract, returnType string) FunctionSignature {
	switch contract {
	case rewriteSignatureFixedZero:
		return rewriteSignature(0, false, returnType)
	case rewriteSignatureFixedOne, rewriteSignatureFixedOneOrTwo, rewriteSignatureFixedOneOrVariadicMinimumTwo:
		return rewriteSignature(1, false, returnType)
	case rewriteSignatureFixedTwo, rewriteSignatureFixedTwoOrThree, rewriteSignatureFixedTwoOrVariadicMinimumTwo:
		return rewriteSignature(2, false, returnType)
	case rewriteSignatureFixedThree:
		return rewriteSignature(3, false, returnType)
	case rewriteSignatureVariadicMinimumOne:
		return rewriteSignature(2, true, returnType)
	case rewriteSignatureVariadicMinimumTwo:
		return rewriteSignature(3, true, returnType)
	case rewriteSignatureVariadicMinimumThree:
		return rewriteSignature(4, true, returnType)
	default:
		panic("unknown rewrite signature contract")
	}
}

func rewriteSignature(argumentCount int, variadic bool, returnType string) FunctionSignature {
	arguments := make([]string, argumentCount)
	for index := range arguments {
		arguments[index] = "any"
	}
	return FunctionSignature{ArgumentTypes: arguments, ReturnType: returnType, Variadic: variadic}
}
