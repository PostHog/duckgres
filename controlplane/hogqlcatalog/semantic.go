package hogqlcatalog

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	maxSemanticDefinitions   = 10_000
	maxExpressionRecipeDepth = 64
	maxExpressionRecipeNodes = 4_096
	maxSemanticRelationDepth = 64
)

type ExpressionFieldDefinition struct {
	Table              string           `json:"table"`
	Name               string           `json:"name"`
	TrinoTypeSignature string           `json:"trinoTypeSignature"`
	LogicalType        LogicalType      `json:"logicalType"`
	Nullable           bool             `json:"nullable"`
	StarVisible        bool             `json:"starVisible"`
	Recipe             ExpressionRecipe `json:"recipe"`
}

type ExpressionRecipe struct {
	Kind           ExpressionRecipeKind  `json:"kind"`
	FieldReference *FieldReferenceRecipe `json:"fieldReference,omitempty"`
	Literal        *TypedLiteral         `json:"literal,omitempty"`
	FunctionCall   *FunctionCallRecipe   `json:"functionCall,omitempty"`
	Operator       *OperatorRecipe       `json:"operator,omitempty"`
	Cast           *CastRecipe           `json:"cast,omitempty"`
}

type ExpressionRecipeKind string

const (
	ExpressionRecipeFieldReference ExpressionRecipeKind = "FIELD_REFERENCE"
	ExpressionRecipeLiteral        ExpressionRecipeKind = "LITERAL"
	ExpressionRecipeFunctionCall   ExpressionRecipeKind = "FUNCTION_CALL"
	ExpressionRecipeOperator       ExpressionRecipeKind = "OPERATOR"
	ExpressionRecipeCast           ExpressionRecipeKind = "CAST"
)

type FieldReferenceRecipe struct {
	Table string `json:"table"`
	Field string `json:"field"`
}

type FunctionCallRecipe struct {
	Name      string             `json:"name"`
	Arguments []ExpressionRecipe `json:"arguments"`
}

type OperatorRecipe struct {
	Operator  SemanticOperator   `json:"operator"`
	Arguments []ExpressionRecipe `json:"arguments"`
}

type SemanticOperator string

const (
	SemanticOperatorAdd                SemanticOperator = "ADD"
	SemanticOperatorSubtract           SemanticOperator = "SUBTRACT"
	SemanticOperatorMultiply           SemanticOperator = "MULTIPLY"
	SemanticOperatorDivide             SemanticOperator = "DIVIDE"
	SemanticOperatorModulus            SemanticOperator = "MODULUS"
	SemanticOperatorEqual              SemanticOperator = "EQUAL"
	SemanticOperatorNotEqual           SemanticOperator = "NOT_EQUAL"
	SemanticOperatorLessThan           SemanticOperator = "LESS_THAN"
	SemanticOperatorLessThanOrEqual    SemanticOperator = "LESS_THAN_OR_EQUAL"
	SemanticOperatorGreaterThan        SemanticOperator = "GREATER_THAN"
	SemanticOperatorGreaterThanOrEqual SemanticOperator = "GREATER_THAN_OR_EQUAL"
	SemanticOperatorAnd                SemanticOperator = "AND"
	SemanticOperatorOr                 SemanticOperator = "OR"
	SemanticOperatorNot                SemanticOperator = "NOT"
	SemanticOperatorNegate             SemanticOperator = "NEGATE"
	SemanticOperatorIsNull             SemanticOperator = "IS_NULL"
	SemanticOperatorIsNotNull          SemanticOperator = "IS_NOT_NULL"
)

type CastRecipe struct {
	Expression          *ExpressionRecipe `json:"expression"`
	TargetTypeSignature string            `json:"targetTypeSignature"`
}

type TypedLiteral struct {
	TypeSignature string          `json:"typeSignature"`
	Encoding      LiteralEncoding `json:"encoding"`
	Value         string          `json:"value"`
}

type LiteralEncoding string

const (
	LiteralEncodingNull    LiteralEncoding = "NULL"
	LiteralEncodingString  LiteralEncoding = "STRING"
	LiteralEncodingBoolean LiteralEncoding = "BOOLEAN"
	LiteralEncodingInteger LiteralEncoding = "INTEGER"
	LiteralEncodingDecimal LiteralEncoding = "DECIMAL"
	LiteralEncodingFloat   LiteralEncoding = "FLOAT"
	LiteralEncodingJSON    LiteralEncoding = "JSON"
	LiteralEncodingBase64  LiteralEncoding = "BASE64"
)

type VirtualTableDefinition struct {
	Name        string              `json:"name"`
	Source      RelationReference   `json:"source"`
	Projections []VirtualProjection `json:"projections"`
}

type RelationReference struct {
	Kind RelationKind `json:"kind"`
	Name string       `json:"name"`
}

type RelationKind string

const (
	RelationKindLogicalTable     RelationKind = "LOGICAL_TABLE"
	RelationKindVirtualTable     RelationKind = "VIRTUAL_TABLE"
	RelationKindSavedQuery       RelationKind = "SAVED_QUERY"
	RelationKindMaterializedView RelationKind = "MATERIALIZED_VIEW"
)

type VirtualProjection struct {
	Name        string `json:"name"`
	SourceField string `json:"sourceField"`
	StarVisible bool   `json:"starVisible"`
}

type SavedQueryReference struct {
	Name    string            `json:"name"`
	QueryID string            `json:"queryId"`
	Target  RelationReference `json:"target"`
	Fields  []ReferencedField `json:"fields"`
}

type MaterializedViewReference struct {
	Name         string                `json:"name"`
	PhysicalView PhysicalQualifiedName `json:"physicalView"`
	Fields       []ReferencedField     `json:"fields"`
}

type ReferencedField struct {
	Name               string      `json:"name"`
	TrinoTypeSignature string      `json:"trinoTypeSignature"`
	LogicalType        LogicalType `json:"logicalType"`
	Nullable           bool        `json:"nullable"`
	StarVisible        bool        `json:"starVisible"`
}

type FunctionCapabilityDefinition struct {
	Name             string                 `json:"name"`
	Kind             FunctionKind           `json:"kind"`
	Implementation   FunctionImplementation `json:"implementation"`
	TrinoName        []PhysicalIdentifier   `json:"trinoName"`
	Signatures       []FunctionSignature    `json:"signatures"`
	Deterministic    bool                   `json:"deterministic"`
	SupportsDistinct bool                   `json:"supportsDistinct"`
	SupportsOrderBy  bool                   `json:"supportsOrderBy"`
	SupportsFilter   bool                   `json:"supportsFilter"`
	SupportsWindow   bool                   `json:"supportsWindow"`
}

type FunctionKind string

const (
	FunctionKindScalar    FunctionKind = "SCALAR"
	FunctionKindAggregate FunctionKind = "AGGREGATE"
	FunctionKindWindow    FunctionKind = "WINDOW"
	FunctionKindTable     FunctionKind = "TABLE"
)

type FunctionImplementation string

const (
	FunctionImplementationStock   FunctionImplementation = "STOCK"
	FunctionImplementationUDF     FunctionImplementation = "UDF"
	FunctionImplementationRewrite FunctionImplementation = "REWRITE"
)

type FunctionSignature struct {
	ArgumentTypes []string `json:"argumentTypes"`
	ReturnType    string   `json:"returnType"`
	Variadic      bool     `json:"variadic"`
}

type SemanticModifierDefault struct {
	Name            string               `json:"name"`
	Behavior        ModifierBehavior     `json:"behavior"`
	DefaultValue    TypedLiteral         `json:"defaultValue"`
	SessionProperty []PhysicalIdentifier `json:"sessionProperty,omitempty"`
}

type ModifierBehavior string

const (
	ModifierBehaviorCompiler             ModifierBehavior = "COMPILER"
	ModifierBehaviorTrinoSessionProperty ModifierBehavior = "TRINO_SESSION_PROPERTY"
	ModifierBehaviorSafeNoop             ModifierBehavior = "SAFE_NOOP"
	ModifierBehaviorUnsupported          ModifierBehavior = "UNSUPPORTED"
)

type semanticRelation struct {
	kind         RelationKind
	fields       map[string]struct{}
	virtualTable *VirtualTableDefinition
	savedQuery   *SavedQueryReference
}

func validateSemanticMetadata(snapshot *HogQLSemanticCatalogSnapshot, logicalTables map[string]*LogicalTableDefinition) error {
	if snapshot.ExpressionFields == nil || snapshot.VirtualTables == nil || snapshot.SavedQueries == nil || snapshot.MaterializedViews == nil || snapshot.Functions == nil || snapshot.ModifierDefaults == nil {
		return invalidSnapshot("semantic metadata lists are required")
	}
	definitionCount := len(snapshot.ExpressionFields) + len(snapshot.VirtualTables) + len(snapshot.SavedQueries) + len(snapshot.MaterializedViews) + len(snapshot.Functions) + len(snapshot.ModifierDefaults)
	if definitionCount > maxSemanticDefinitions {
		return invalidSnapshot("semantic metadata exceeds definition limit")
	}

	functions, err := validateFunctions(snapshot.Functions)
	if err != nil {
		return err
	}
	expressionFields, err := validateExpressionFieldHeaders(snapshot.ExpressionFields, logicalTables)
	if err != nil {
		return err
	}
	if err := validateExpressionRecipes(snapshot.ExpressionFields, logicalTables, expressionFields, functions); err != nil {
		return err
	}
	if err := validateModifiers(snapshot.ModifierDefaults); err != nil {
		return err
	}
	return validateRelations(snapshot, logicalTables, expressionFields)
}

func validateSemanticShapeBounds(snapshot *HogQLSemanticCatalogSnapshot) error {
	definitionCount := len(snapshot.ExpressionFields) + len(snapshot.VirtualTables) + len(snapshot.SavedQueries) + len(snapshot.MaterializedViews) + len(snapshot.Functions) + len(snapshot.ModifierDefaults)
	if definitionCount > maxSemanticDefinitions {
		return invalidSnapshot("semantic metadata exceeds definition limit")
	}
	type pendingRecipe struct {
		recipe *ExpressionRecipe
		depth  int
	}
	pending := make([]pendingRecipe, 0, len(snapshot.ExpressionFields))
	for index := range snapshot.ExpressionFields {
		pending = append(pending, pendingRecipe{recipe: &snapshot.ExpressionFields[index].Recipe, depth: 1})
	}
	nodes := 0
	for len(pending) > 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current.depth > maxExpressionRecipeDepth {
			return invalidSnapshot("expression recipe exceeds depth limit")
		}
		nodes++
		if nodes > maxExpressionRecipeNodes {
			return invalidSnapshot("expression recipes exceed node limit")
		}
		if current.recipe.FunctionCall != nil {
			for index := range current.recipe.FunctionCall.Arguments {
				pending = append(pending, pendingRecipe{recipe: &current.recipe.FunctionCall.Arguments[index], depth: current.depth + 1})
			}
		}
		if current.recipe.Operator != nil {
			for index := range current.recipe.Operator.Arguments {
				pending = append(pending, pendingRecipe{recipe: &current.recipe.Operator.Arguments[index], depth: current.depth + 1})
			}
		}
		if current.recipe.Cast != nil && current.recipe.Cast.Expression != nil {
			pending = append(pending, pendingRecipe{recipe: current.recipe.Cast.Expression, depth: current.depth + 1})
		}
	}
	return nil
}

func validateExpressionFieldHeaders(definitions []ExpressionFieldDefinition, tables map[string]*LogicalTableDefinition) (map[string]*ExpressionFieldDefinition, error) {
	fields := make(map[string]*ExpressionFieldDefinition, len(definitions))
	for index := range definitions {
		definition := &definitions[index]
		if err := validateDefinitionText(definition.Table, "expression field table"); err != nil {
			return nil, err
		}
		table := tables[canonicalName(definition.Table)]
		if table == nil {
			return nil, invalidSnapshot("expression field %q references an unknown table", definition.Name)
		}
		if err := validateReferencedField(ReferencedField{Name: definition.Name, TrinoTypeSignature: definition.TrinoTypeSignature, LogicalType: definition.LogicalType}); err != nil {
			return nil, err
		}
		key := expressionFieldKey(definition.Table, definition.Name)
		if _, exists := logicalMemberNames(table)[canonicalName(definition.Name)]; exists {
			return nil, invalidSnapshot("expression field %q conflicts with an existing logical member", definition.Name)
		}
		if _, exists := fields[key]; exists {
			return nil, invalidSnapshot("duplicate expression field %q", definition.Name)
		}
		fields[key] = definition
	}
	return fields, nil
}

func logicalMemberNames(table *LogicalTableDefinition) map[string]struct{} {
	members := fieldNames(table)
	for _, property := range table.Properties {
		members[canonicalName(property.Name)] = struct{}{}
	}
	for _, relationship := range table.Relationships {
		members[canonicalName(relationship.Name)] = struct{}{}
	}
	return members
}

func validateExpressionRecipes(definitions []ExpressionFieldDefinition, tables map[string]*LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition, functions map[string]struct{}) error {
	dependencies := make(map[string][]string, len(definitions))
	nodes := 0
	for index := range definitions {
		definition := &definitions[index]
		key := expressionFieldKey(definition.Table, definition.Name)
		fieldDependencies := dependencies[key]
		if err := validateExpressionRecipe(&definition.Recipe, definition.Table, 1, &nodes, tables, expressionFields, functions, &fieldDependencies); err != nil {
			return fmt.Errorf("%w: expression field %q: %v", ErrInvalidSnapshot, definition.Name, err)
		}
		dependencies[key] = fieldDependencies
	}
	return validateDependencyCycles(dependencies, "expression field")
}

func validateExpressionRecipe(recipe *ExpressionRecipe, ownerTable string, depth int, nodes *int, tables map[string]*LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition, functions map[string]struct{}, dependencies *[]string) error {
	if depth > maxExpressionRecipeDepth {
		return invalidSnapshot("expression recipe exceeds depth limit")
	}
	*nodes++
	if *nodes > maxExpressionRecipeNodes {
		return invalidSnapshot("expression recipes exceed node limit")
	}
	payloads := 0
	for _, present := range []bool{recipe.FieldReference != nil, recipe.Literal != nil, recipe.FunctionCall != nil, recipe.Operator != nil, recipe.Cast != nil} {
		if present {
			payloads++
		}
	}
	if payloads != 1 {
		return invalidSnapshot("expression recipe must have exactly one payload")
	}
	switch recipe.Kind {
	case ExpressionRecipeFieldReference:
		if recipe.FieldReference == nil || payloads != 1 {
			return invalidSnapshot("FIELD_REFERENCE recipe has mismatched payload")
		}
		ref := recipe.FieldReference
		if err := validateDefinitionText(ref.Table, "field reference table"); err != nil {
			return err
		}
		if err := validateDefinitionText(ref.Field, "field reference field"); err != nil {
			return err
		}
		if canonicalName(ref.Table) != canonicalName(ownerTable) {
			return invalidSnapshot("field reference crosses tables without a relationship path")
		}
		table := tables[canonicalName(ref.Table)]
		if table == nil {
			return invalidSnapshot("field reference has unknown table")
		}
		key := expressionFieldKey(ref.Table, ref.Field)
		if _, exists := expressionFields[key]; exists {
			*dependencies = append(*dependencies, key)
			return nil
		}
		if _, exists := fieldNames(table)[canonicalName(ref.Field)]; !exists {
			return invalidSnapshot("field reference has unknown field")
		}
	case ExpressionRecipeLiteral:
		if recipe.Literal == nil || payloads != 1 {
			return invalidSnapshot("LITERAL recipe has mismatched payload")
		}
		return validateTypedLiteral(recipe.Literal)
	case ExpressionRecipeFunctionCall:
		if recipe.FunctionCall == nil || payloads != 1 {
			return invalidSnapshot("FUNCTION_CALL recipe has mismatched payload")
		}
		call := recipe.FunctionCall
		if err := validateDefinitionText(call.Name, "function name"); err != nil {
			return err
		}
		if _, exists := functions[canonicalName(call.Name)]; !exists {
			return invalidSnapshot("function call references an undeclared function")
		}
		if call.Arguments == nil {
			return invalidSnapshot("function arguments are required")
		}
		for index := range call.Arguments {
			if err := validateExpressionRecipe(&call.Arguments[index], ownerTable, depth+1, nodes, tables, expressionFields, functions, dependencies); err != nil {
				return err
			}
		}
	case ExpressionRecipeOperator:
		if recipe.Operator == nil || payloads != 1 || !slices.Contains(validSemanticOperators, recipe.Operator.Operator) || len(recipe.Operator.Arguments) == 0 {
			return invalidSnapshot("invalid OPERATOR recipe")
		}
		for index := range recipe.Operator.Arguments {
			if err := validateExpressionRecipe(&recipe.Operator.Arguments[index], ownerTable, depth+1, nodes, tables, expressionFields, functions, dependencies); err != nil {
				return err
			}
		}
	case ExpressionRecipeCast:
		if recipe.Cast == nil || payloads != 1 || recipe.Cast.Expression == nil {
			return invalidSnapshot("invalid CAST recipe")
		}
		if err := validateDefinitionText(recipe.Cast.TargetTypeSignature, "cast target type signature"); err != nil {
			return err
		}
		return validateExpressionRecipe(recipe.Cast.Expression, ownerTable, depth+1, nodes, tables, expressionFields, functions, dependencies)
	default:
		return invalidSnapshot("unknown expression recipe kind")
	}
	return nil
}

func validateTypedLiteral(literal *TypedLiteral) error {
	if err := validateDefinitionText(literal.TypeSignature, "literal type signature"); err != nil {
		return err
	}
	if strings.ContainsRune(literal.Value, '\x00') {
		return invalidSnapshot("literal value contains NUL")
	}
	switch literal.Encoding {
	case LiteralEncodingNull:
		if literal.Value != "" {
			return invalidSnapshot("NULL literal must have an empty value")
		}
	case LiteralEncodingString:
	case LiteralEncodingBoolean:
		if literal.Value != "true" && literal.Value != "false" {
			return invalidSnapshot("invalid BOOLEAN literal")
		}
	case LiteralEncodingInteger:
		if _, err := strconv.ParseInt(literal.Value, 10, 64); err != nil {
			return invalidSnapshot("invalid INTEGER literal")
		}
	case LiteralEncodingDecimal:
		if !decimalLiteralPattern.MatchString(literal.Value) {
			return invalidSnapshot("invalid DECIMAL literal")
		}
	case LiteralEncodingFloat:
		value, err := strconv.ParseFloat(literal.Value, 64)
		if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
			return invalidSnapshot("invalid FLOAT literal")
		}
	case LiteralEncodingJSON:
		if !json.Valid([]byte(literal.Value)) {
			return invalidSnapshot("invalid JSON literal")
		}
	case LiteralEncodingBase64:
		if _, err := base64.StdEncoding.DecodeString(literal.Value); err != nil {
			return invalidSnapshot("invalid BASE64 literal")
		}
	default:
		return invalidSnapshot("unknown literal encoding")
	}
	return nil
}

func validateFunctions(definitions []FunctionCapabilityDefinition) (map[string]struct{}, error) {
	functions := make(map[string]struct{}, len(definitions))
	for index := range definitions {
		definition := &definitions[index]
		if err := validateDefinitionText(definition.Name, "function name"); err != nil {
			return nil, err
		}
		name := canonicalName(definition.Name)
		if _, exists := functions[name]; exists {
			return nil, invalidSnapshot("duplicate function %q", definition.Name)
		}
		functions[name] = struct{}{}
		if !slices.Contains(validFunctionKinds, definition.Kind) || !slices.Contains(validFunctionImplementations, definition.Implementation) {
			return nil, invalidSnapshot("function %q has an unknown capability", definition.Name)
		}
		if definition.TrinoName == nil || definition.Signatures == nil || len(definition.Signatures) == 0 {
			return nil, invalidSnapshot("function %q must include Trino name and signatures", definition.Name)
		}
		if definition.Implementation == FunctionImplementationRewrite && len(definition.TrinoName) != 0 {
			return nil, invalidSnapshot("rewrite function %q cannot name a Trino function", definition.Name)
		}
		if definition.Implementation != FunctionImplementationRewrite && len(definition.TrinoName) == 0 {
			return nil, invalidSnapshot("function %q must name a Trino function", definition.Name)
		}
		for nameIndex := range definition.TrinoName {
			if err := normalizePhysicalIdentifier(&definition.TrinoName[nameIndex]); err != nil {
				return nil, err
			}
		}
		for _, signature := range definition.Signatures {
			if signature.ArgumentTypes == nil {
				return nil, invalidSnapshot("function %q signature arguments are required", definition.Name)
			}
			for _, argument := range signature.ArgumentTypes {
				if err := validateDefinitionText(argument, "function argument type"); err != nil {
					return nil, err
				}
			}
			if err := validateDefinitionText(signature.ReturnType, "function return type"); err != nil {
				return nil, err
			}
		}
	}
	return functions, nil
}

func validateModifiers(definitions []SemanticModifierDefault) error {
	seen := make(map[string]struct{}, len(definitions))
	for index := range definitions {
		definition := &definitions[index]
		if err := validateDefinitionText(definition.Name, "modifier name"); err != nil {
			return err
		}
		name := canonicalName(definition.Name)
		if _, exists := seen[name]; exists {
			return invalidSnapshot("duplicate modifier %q", definition.Name)
		}
		seen[name] = struct{}{}
		if !slices.Contains(validModifierBehaviors, definition.Behavior) {
			return invalidSnapshot("modifier %q has an unknown behavior", definition.Name)
		}
		if err := validateTypedLiteral(&definition.DefaultValue); err != nil {
			return err
		}
		if definition.Behavior == ModifierBehaviorTrinoSessionProperty {
			if len(definition.SessionProperty) == 0 {
				return invalidSnapshot("modifier %q must name a session property", definition.Name)
			}
		} else if len(definition.SessionProperty) != 0 {
			return invalidSnapshot("modifier %q cannot name a session property", definition.Name)
		}
		for propertyIndex := range definition.SessionProperty {
			if err := normalizePhysicalIdentifier(&definition.SessionProperty[propertyIndex]); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRelations(snapshot *HogQLSemanticCatalogSnapshot, logicalTables map[string]*LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition) error {
	relations := make(map[string]*semanticRelation, len(logicalTables)+len(snapshot.VirtualTables)+len(snapshot.SavedQueries)+len(snapshot.MaterializedViews))
	for name, table := range logicalTables {
		fields := fieldNames(table)
		for key, field := range expressionFields {
			if strings.HasPrefix(key, name+".") {
				fields[canonicalName(field.Name)] = struct{}{}
			}
		}
		relations[name] = &semanticRelation{kind: RelationKindLogicalTable, fields: fields}
	}
	for index := range snapshot.SavedQueries {
		definition := &snapshot.SavedQueries[index]
		fields, err := validateReferencedRelation(definition.Name, definition.Fields)
		if err != nil {
			return err
		}
		if err := validateDefinitionText(definition.QueryID, "saved query ID"); err != nil {
			return err
		}
		if err := addRelation(relations, definition.Name, RelationKindSavedQuery, fields, nil, definition); err != nil {
			return err
		}
	}
	for index := range snapshot.MaterializedViews {
		definition := &snapshot.MaterializedViews[index]
		fields, err := validateReferencedRelation(definition.Name, definition.Fields)
		if err != nil {
			return err
		}
		if err := normalizePhysicalQualifiedName(&definition.PhysicalView); err != nil {
			return err
		}
		if definition.PhysicalView.Catalog != snapshot.Catalog {
			return invalidSnapshot("materialized view %q physical catalog does not match snapshot catalog", definition.Name)
		}
		if err := addRelation(relations, definition.Name, RelationKindMaterializedView, fields, nil, nil); err != nil {
			return err
		}
	}
	for index := range snapshot.VirtualTables {
		definition := &snapshot.VirtualTables[index]
		if definition.Projections == nil {
			return invalidSnapshot("virtual table %q projections are required", definition.Name)
		}
		if err := addRelation(relations, definition.Name, RelationKindVirtualTable, nil, definition, nil); err != nil {
			return err
		}
	}
	states := make(map[string]uint8, len(snapshot.VirtualTables)+len(snapshot.SavedQueries))
	for index := range snapshot.VirtualTables {
		if _, err := resolveSemanticRelation(canonicalName(snapshot.VirtualTables[index].Name), relations, states, 1); err != nil {
			return err
		}
	}
	for index := range snapshot.SavedQueries {
		if _, err := resolveSemanticRelation(canonicalName(snapshot.SavedQueries[index].Name), relations, states, 1); err != nil {
			return err
		}
	}
	return nil
}

func validateReferencedRelation(name string, fields []ReferencedField) (map[string]struct{}, error) {
	if err := validateDefinitionText(name, "relation name"); err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, invalidSnapshot("relation %q fields are required", name)
	}
	result := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if err := validateReferencedField(field); err != nil {
			return nil, err
		}
		key := canonicalName(field.Name)
		if _, exists := result[key]; exists {
			return nil, invalidSnapshot("duplicate field %q on relation %q", field.Name, name)
		}
		result[key] = struct{}{}
	}
	return result, nil
}

func validateReferencedField(field ReferencedField) error {
	if err := validateDefinitionText(field.Name, "referenced field name"); err != nil {
		return err
	}
	if err := validateDefinitionText(field.TrinoTypeSignature, "referenced field type signature"); err != nil {
		return err
	}
	if !slices.Contains(validLogicalTypes, field.LogicalType) {
		return invalidSnapshot("referenced field %q has unknown logicalType", field.Name)
	}
	return nil
}

func addRelation(relations map[string]*semanticRelation, name string, kind RelationKind, fields map[string]struct{}, virtualTable *VirtualTableDefinition, savedQuery *SavedQueryReference) error {
	if err := validateDefinitionText(name, "relation name"); err != nil {
		return err
	}
	key := canonicalName(name)
	if _, exists := relations[key]; exists {
		return invalidSnapshot("duplicate relation %q", name)
	}
	relations[key] = &semanticRelation{kind: kind, fields: fields, virtualTable: virtualTable, savedQuery: savedQuery}
	return nil
}

func resolveSemanticRelation(name string, relations map[string]*semanticRelation, states map[string]uint8, depth int) (map[string]struct{}, error) {
	if depth > maxSemanticRelationDepth {
		return nil, invalidSnapshot("virtual table reference exceeds depth limit")
	}
	relation := relations[name]
	if relation == nil {
		return nil, invalidSnapshot("invalid semantic relation reference")
	}
	if relation.kind != RelationKindVirtualTable && relation.kind != RelationKindSavedQuery {
		return relation.fields, nil
	}
	if states[name] == 1 {
		return nil, invalidSnapshot("semantic relation reference cycle")
	}
	if states[name] == 2 {
		return relation.fields, nil
	}
	states[name] = 1
	var reference RelationReference
	if relation.kind == RelationKindVirtualTable {
		if relation.virtualTable == nil {
			return nil, invalidSnapshot("invalid virtual table definition")
		}
		reference = relation.virtualTable.Source
	} else {
		if relation.savedQuery == nil {
			return nil, invalidSnapshot("invalid saved query definition")
		}
		reference = relation.savedQuery.Target
		if reference.Kind == RelationKindSavedQuery {
			return nil, invalidSnapshot("saved query target must be logical, virtual, or materialized")
		}
	}
	sourceName := canonicalName(reference.Name)
	if err := validateDefinitionText(reference.Name, "semantic relation source"); err != nil {
		return nil, err
	}
	source := relations[sourceName]
	if source == nil || source.kind != reference.Kind {
		return nil, invalidSnapshot("semantic relation %q references an unknown or mismatched source", name)
	}
	sourceFields := source.fields
	if source.kind == RelationKindVirtualTable || source.kind == RelationKindSavedQuery {
		resolved, err := resolveSemanticRelation(sourceName, relations, states, depth+1)
		if err != nil {
			return nil, err
		}
		sourceFields = resolved
	}
	if relation.kind == RelationKindSavedQuery {
		for field := range relation.fields {
			if _, exists := sourceFields[field]; !exists {
				return nil, invalidSnapshot("saved query %q declares a field missing from its target", relation.savedQuery.Name)
			}
		}
		states[name] = 2
		return relation.fields, nil
	}

	fields := make(map[string]struct{}, len(relation.virtualTable.Projections))
	for _, projection := range relation.virtualTable.Projections {
		if err := validateDefinitionText(projection.Name, "virtual projection name"); err != nil {
			return nil, err
		}
		if err := validateDefinitionText(projection.SourceField, "virtual projection source field"); err != nil {
			return nil, err
		}
		if _, exists := sourceFields[canonicalName(projection.SourceField)]; !exists {
			return nil, invalidSnapshot("virtual table %q projection references an unknown source field", relation.virtualTable.Name)
		}
		fieldName := canonicalName(projection.Name)
		if _, exists := fields[fieldName]; exists {
			return nil, invalidSnapshot("duplicate projection %q on virtual table %q", projection.Name, relation.virtualTable.Name)
		}
		fields[fieldName] = struct{}{}
	}
	relation.fields = fields
	states[name] = 2
	return fields, nil
}

func validateDependencyCycles(dependencies map[string][]string, kind string) error {
	states := make(map[string]uint8, len(dependencies))
	var visit func(string, int) error
	visit = func(node string, depth int) error {
		if depth > maxExpressionRecipeDepth {
			return invalidSnapshot("%s dependency exceeds depth limit", kind)
		}
		if states[node] == 1 {
			return invalidSnapshot("%s dependency cycle", kind)
		}
		if states[node] == 2 {
			return nil
		}
		states[node] = 1
		for _, dependency := range dependencies[node] {
			if err := visit(dependency, depth+1); err != nil {
				return err
			}
		}
		states[node] = 2
		return nil
	}
	for node := range dependencies {
		if err := visit(node, 1); err != nil {
			return err
		}
	}
	return nil
}

func expressionFieldKey(table, field string) string {
	return canonicalName(table) + "." + canonicalName(field)
}

func cloneSemanticMetadata(clone, snapshot *HogQLSemanticCatalogSnapshot) {
	clone.ExpressionFields = slices.Clone(snapshot.ExpressionFields)
	for index := range clone.ExpressionFields {
		clone.ExpressionFields[index].Recipe = cloneExpressionRecipe(snapshot.ExpressionFields[index].Recipe)
	}
	clone.VirtualTables = slices.Clone(snapshot.VirtualTables)
	for index := range clone.VirtualTables {
		clone.VirtualTables[index].Projections = slices.Clone(snapshot.VirtualTables[index].Projections)
	}
	clone.SavedQueries = slices.Clone(snapshot.SavedQueries)
	for index := range clone.SavedQueries {
		clone.SavedQueries[index].Fields = slices.Clone(snapshot.SavedQueries[index].Fields)
	}
	clone.MaterializedViews = slices.Clone(snapshot.MaterializedViews)
	for index := range clone.MaterializedViews {
		clone.MaterializedViews[index].Fields = slices.Clone(snapshot.MaterializedViews[index].Fields)
	}
	clone.Functions = slices.Clone(snapshot.Functions)
	for index := range clone.Functions {
		clone.Functions[index].TrinoName = slices.Clone(snapshot.Functions[index].TrinoName)
		clone.Functions[index].Signatures = slices.Clone(snapshot.Functions[index].Signatures)
		for signatureIndex := range clone.Functions[index].Signatures {
			clone.Functions[index].Signatures[signatureIndex].ArgumentTypes = slices.Clone(snapshot.Functions[index].Signatures[signatureIndex].ArgumentTypes)
		}
	}
	clone.ModifierDefaults = slices.Clone(snapshot.ModifierDefaults)
	for index := range clone.ModifierDefaults {
		clone.ModifierDefaults[index].SessionProperty = slices.Clone(snapshot.ModifierDefaults[index].SessionProperty)
	}
}

func cloneExpressionRecipe(recipe ExpressionRecipe) ExpressionRecipe {
	clone := recipe
	if recipe.FieldReference != nil {
		value := *recipe.FieldReference
		clone.FieldReference = &value
	}
	if recipe.Literal != nil {
		value := *recipe.Literal
		clone.Literal = &value
	}
	if recipe.FunctionCall != nil {
		value := *recipe.FunctionCall
		value.Arguments = slices.Clone(recipe.FunctionCall.Arguments)
		for index := range value.Arguments {
			value.Arguments[index] = cloneExpressionRecipe(value.Arguments[index])
		}
		clone.FunctionCall = &value
	}
	if recipe.Operator != nil {
		value := *recipe.Operator
		value.Arguments = slices.Clone(recipe.Operator.Arguments)
		for index := range value.Arguments {
			value.Arguments[index] = cloneExpressionRecipe(value.Arguments[index])
		}
		clone.Operator = &value
	}
	if recipe.Cast != nil {
		value := *recipe.Cast
		if recipe.Cast.Expression != nil {
			expression := cloneExpressionRecipe(*recipe.Cast.Expression)
			value.Expression = &expression
		}
		clone.Cast = &value
	}
	return clone
}

var validSemanticOperators = []SemanticOperator{
	SemanticOperatorAdd, SemanticOperatorSubtract, SemanticOperatorMultiply, SemanticOperatorDivide, SemanticOperatorModulus,
	SemanticOperatorEqual, SemanticOperatorNotEqual, SemanticOperatorLessThan, SemanticOperatorLessThanOrEqual,
	SemanticOperatorGreaterThan, SemanticOperatorGreaterThanOrEqual, SemanticOperatorAnd, SemanticOperatorOr,
	SemanticOperatorNot, SemanticOperatorNegate, SemanticOperatorIsNull, SemanticOperatorIsNotNull,
}

var validFunctionKinds = []FunctionKind{FunctionKindScalar, FunctionKindAggregate, FunctionKindWindow, FunctionKindTable}

var validFunctionImplementations = []FunctionImplementation{FunctionImplementationStock, FunctionImplementationUDF, FunctionImplementationRewrite}

var validModifierBehaviors = []ModifierBehavior{ModifierBehaviorCompiler, ModifierBehaviorTrinoSessionProperty, ModifierBehaviorSafeNoop, ModifierBehaviorUnsupported}

var decimalLiteralPattern = regexp.MustCompile(`^[+-]?(0|[1-9][0-9]*)(\.[0-9]+)?$`)
