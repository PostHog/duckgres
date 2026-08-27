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
	Kind                 ExpressionRecipeKind           `json:"kind"`
	FieldReference       *FieldReferenceRecipe          `json:"fieldReference,omitempty"`
	Literal              *TypedLiteral                  `json:"literal,omitempty"`
	FunctionCall         *FunctionCallRecipe            `json:"functionCall,omitempty"`
	Operator             *OperatorRecipe                `json:"operator,omitempty"`
	Cast                 *CastRecipe                    `json:"cast,omitempty"`
	ArgumentReference    *ArgumentReferenceRecipe       `json:"argumentReference,omitempty"`
	ScopedFieldReference *ScopedFieldReferenceRecipe    `json:"scopedFieldReference,omitempty"`
	PropertyLookup       *PropertyLookupReferenceRecipe `json:"propertyLookup,omitempty"`
}

type ExpressionRecipeKind string

const (
	ExpressionRecipeFieldReference       ExpressionRecipeKind = "FIELD_REFERENCE"
	ExpressionRecipeLiteral              ExpressionRecipeKind = "LITERAL"
	ExpressionRecipeFunctionCall         ExpressionRecipeKind = "FUNCTION_CALL"
	ExpressionRecipeOperator             ExpressionRecipeKind = "OPERATOR"
	ExpressionRecipeCast                 ExpressionRecipeKind = "CAST"
	ExpressionRecipeArgumentReference    ExpressionRecipeKind = "ARGUMENT_REFERENCE"
	ExpressionRecipeScopedFieldReference ExpressionRecipeKind = "SCOPED_FIELD_REFERENCE"
	ExpressionRecipePropertyLookup       ExpressionRecipeKind = "PROPERTY_LOOKUP"
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
	SemanticOperatorSubscript          SemanticOperator = "SUBSCRIPT"
)

type ArgumentReferenceRecipe struct {
	Argument ExpressionArgument `json:"argument"`
}

type ExpressionArgument string

const (
	ExpressionArgumentPropertySource ExpressionArgument = "PROPERTY_SOURCE"
	ExpressionArgumentPropertyKey    ExpressionArgument = "PROPERTY_KEY"
)

type ScopedFieldReferenceRecipe struct {
	Side  RelationshipJoinSide `json:"side"`
	Field string               `json:"field"`
}

type RelationshipJoinSide string

const (
	RelationshipJoinSource RelationshipJoinSide = "SOURCE"
	RelationshipJoinTarget RelationshipJoinSide = "TARGET"
)

type PropertyLookupReferenceRecipe struct {
	Table    string            `json:"table"`
	Property string            `json:"property"`
	Key      *ExpressionRecipe `json:"key"`
}

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
	definitionCount := semanticDefinitionCount(snapshot)
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
	if err := validatePropertyRecipes(logicalTables, expressionFields, functions); err != nil {
		return err
	}
	if err := validateExpressionRecipes(snapshot.ExpressionFields, logicalTables, expressionFields, functions); err != nil {
		return err
	}
	if err := validateModifiers(snapshot.ModifierDefaults); err != nil {
		return err
	}
	if err := validateRelationshipPredicates(logicalTables, expressionFields, functions); err != nil {
		return err
	}
	relations, err := validateRelations(snapshot, logicalTables, expressionFields)
	if err != nil {
		return err
	}
	if err := validateLazyTables(snapshot.LazyTables, logicalTables, expressionFields, functions); err != nil {
		return err
	}
	return validateSemanticEntities(snapshot, logicalTables, expressionFields, functions, relations)
}

func validateSemanticShapeBounds(snapshot *HogQLSemanticCatalogSnapshot) error {
	definitionCount := semanticDefinitionCount(snapshot)
	if definitionCount > maxSemanticDefinitions {
		return invalidSnapshot("semantic metadata exceeds definition limit")
	}
	type pendingRecipe struct {
		recipe *ExpressionRecipe
		depth  int
	}
	pending := make([]pendingRecipe, 0, len(snapshot.ExpressionFields))
	appendRecipe := func(recipe *ExpressionRecipe) {
		if recipe != nil {
			pending = append(pending, pendingRecipe{recipe: recipe, depth: 1})
		}
	}
	for index := range snapshot.ExpressionFields {
		appendRecipe(&snapshot.ExpressionFields[index].Recipe)
	}
	for tableIndex := range snapshot.LogicalTables {
		for propertyIndex := range snapshot.LogicalTables[tableIndex].Properties {
			appendRecipe(snapshot.LogicalTables[tableIndex].Properties[propertyIndex].LookupRecipe)
		}
		for relationshipIndex := range snapshot.LogicalTables[tableIndex].Relationships {
			appendRecipe(snapshot.LogicalTables[tableIndex].Relationships[relationshipIndex].JoinPredicate)
		}
	}
	for lazyTableIndex := range snapshot.LazyTables {
		for projectionIndex := range snapshot.LazyTables[lazyTableIndex].Projections {
			appendRecipe(&snapshot.LazyTables[lazyTableIndex].Projections[projectionIndex].Recipe)
		}
	}
	for index := range snapshot.Actions {
		appendRecipe(snapshot.Actions[index].Representation.Predicate)
	}
	for index := range snapshot.Cohorts {
		appendRecipe(snapshot.Cohorts[index].Representation.Predicate)
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
		if current.recipe.PropertyLookup != nil && current.recipe.PropertyLookup.Key != nil {
			pending = append(pending, pendingRecipe{recipe: current.recipe.PropertyLookup.Key, depth: current.depth + 1})
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

func validateExpressionRecipes(definitions []ExpressionFieldDefinition, tables map[string]*LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition, functions map[string]*FunctionCapabilityDefinition) error {
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

func validateExpressionRecipe(recipe *ExpressionRecipe, ownerTable string, depth int, nodes *int, tables map[string]*LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition, functions map[string]*FunctionCapabilityDefinition, dependencies *[]string) error {
	context := expressionRecipeValidationContext{
		ownerTable:           ownerTable,
		tables:               tables,
		expressionFields:     expressionFields,
		functions:            functions,
		dependencies:         dependencies,
		allowFieldReferences: true,
		allowPropertyLookups: true,
	}
	return validateExpressionRecipeWithContext(recipe, depth, nodes, context)
}

type expressionRecipeValidationContext struct {
	ownerTable           string
	tables               map[string]*LogicalTableDefinition
	expressionFields     map[string]*ExpressionFieldDefinition
	functions            map[string]*FunctionCapabilityDefinition
	dependencies         *[]string
	allowFieldReferences bool
	allowPropertyLookups bool
	arguments            map[ExpressionArgument]int
	scopedTables         map[RelationshipJoinSide]string
}

func validateExpressionRecipeWithContext(recipe *ExpressionRecipe, depth int, nodes *int, context expressionRecipeValidationContext) error {
	if depth > maxExpressionRecipeDepth {
		return invalidSnapshot("expression recipe exceeds depth limit")
	}
	*nodes++
	if *nodes > maxExpressionRecipeNodes {
		return invalidSnapshot("expression recipes exceed node limit")
	}
	payloads := 0
	for _, present := range []bool{
		recipe.FieldReference != nil,
		recipe.Literal != nil,
		recipe.FunctionCall != nil,
		recipe.Operator != nil,
		recipe.Cast != nil,
		recipe.ArgumentReference != nil,
		recipe.ScopedFieldReference != nil,
		recipe.PropertyLookup != nil,
	} {
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
		if !context.allowFieldReferences {
			return invalidSnapshot("field reference is not valid in this recipe")
		}
		if canonicalName(ref.Table) != canonicalName(context.ownerTable) {
			return invalidSnapshot("field reference crosses tables without a relationship path")
		}
		table := context.tables[canonicalName(ref.Table)]
		if table == nil {
			return invalidSnapshot("field reference has unknown table")
		}
		key := expressionFieldKey(ref.Table, ref.Field)
		if _, exists := context.expressionFields[key]; exists {
			if context.dependencies != nil {
				*context.dependencies = append(*context.dependencies, key)
			}
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
		function := context.functions[canonicalName(call.Name)]
		if function == nil {
			return invalidSnapshot("function call references an undeclared function")
		}
		if call.Arguments == nil {
			return invalidSnapshot("function arguments are required")
		}
		if !functionAcceptsArity(function, len(call.Arguments)) {
			return invalidSnapshot("function call has an unsupported argument count")
		}
		for index := range call.Arguments {
			if err := validateExpressionRecipeWithContext(&call.Arguments[index], depth+1, nodes, context); err != nil {
				return err
			}
		}
	case ExpressionRecipeOperator:
		if recipe.Operator == nil || payloads != 1 || !slices.Contains(validSemanticOperators, recipe.Operator.Operator) {
			return invalidSnapshot("invalid OPERATOR recipe")
		}
		if len(recipe.Operator.Arguments) != semanticOperatorArity(recipe.Operator.Operator) {
			return invalidSnapshot("invalid OPERATOR recipe argument count")
		}
		for index := range recipe.Operator.Arguments {
			if err := validateExpressionRecipeWithContext(&recipe.Operator.Arguments[index], depth+1, nodes, context); err != nil {
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
		return validateExpressionRecipeWithContext(recipe.Cast.Expression, depth+1, nodes, context)
	case ExpressionRecipeArgumentReference:
		if recipe.ArgumentReference == nil || payloads != 1 || context.arguments == nil {
			return invalidSnapshot("invalid ARGUMENT_REFERENCE recipe")
		}
		argument := recipe.ArgumentReference.Argument
		if argument != ExpressionArgumentPropertySource && argument != ExpressionArgumentPropertyKey {
			return invalidSnapshot("unknown recipe argument")
		}
		context.arguments[argument]++
	case ExpressionRecipeScopedFieldReference:
		if recipe.ScopedFieldReference == nil || payloads != 1 || context.scopedTables == nil {
			return invalidSnapshot("invalid SCOPED_FIELD_REFERENCE recipe")
		}
		ref := recipe.ScopedFieldReference
		if err := validateDefinitionText(ref.Field, "scoped field reference"); err != nil {
			return err
		}
		tableName := context.scopedTables[ref.Side]
		if tableName == "" {
			return invalidSnapshot("unknown scoped field side")
		}
		if _, exists := semanticFieldNames(context.tables[canonicalName(tableName)], context.expressionFields)[canonicalName(ref.Field)]; !exists {
			return invalidSnapshot("scoped field reference has unknown field")
		}
	case ExpressionRecipePropertyLookup:
		if recipe.PropertyLookup == nil || payloads != 1 || !context.allowPropertyLookups {
			return invalidSnapshot("invalid PROPERTY_LOOKUP recipe")
		}
		lookup := recipe.PropertyLookup
		if lookup.Key == nil {
			return invalidSnapshot("property lookup key is required")
		}
		if err := validateDefinitionText(lookup.Table, "property lookup table"); err != nil {
			return err
		}
		if err := validateDefinitionText(lookup.Property, "property lookup name"); err != nil {
			return err
		}
		if !recipeContextAllowsTable(context, lookup.Table) {
			return invalidSnapshot("property lookup crosses tables without a declared scope")
		}
		table := context.tables[canonicalName(lookup.Table)]
		if table == nil || findProperty(table, lookup.Property) == nil {
			return invalidSnapshot("property lookup references an unknown property")
		}
		return validateExpressionRecipeWithContext(lookup.Key, depth+1, nodes, context)
	default:
		return invalidSnapshot("unknown expression recipe kind")
	}
	return nil
}

func recipeContextAllowsTable(context expressionRecipeValidationContext, table string) bool {
	if canonicalName(table) == canonicalName(context.ownerTable) {
		return true
	}
	for _, scopedTable := range context.scopedTables {
		if canonicalName(table) == canonicalName(scopedTable) {
			return true
		}
	}
	return false
}

func semanticFieldNames(table *LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition) map[string]struct{} {
	if table == nil {
		return nil
	}
	fields := fieldNames(table)
	for key, field := range expressionFields {
		if strings.HasPrefix(key, canonicalName(table.Name)+".") {
			fields[canonicalName(field.Name)] = struct{}{}
		}
	}
	return fields
}

func findProperty(table *LogicalTableDefinition, name string) *PropertyDefinition {
	for index := range table.Properties {
		if canonicalName(table.Properties[index].Name) == canonicalName(name) {
			return &table.Properties[index]
		}
	}
	return nil
}

func semanticOperatorArity(operator SemanticOperator) int {
	switch operator {
	case SemanticOperatorAdd,
		SemanticOperatorSubtract,
		SemanticOperatorMultiply,
		SemanticOperatorDivide,
		SemanticOperatorModulus,
		SemanticOperatorEqual,
		SemanticOperatorNotEqual,
		SemanticOperatorLessThan,
		SemanticOperatorLessThanOrEqual,
		SemanticOperatorGreaterThan,
		SemanticOperatorGreaterThanOrEqual,
		SemanticOperatorAnd,
		SemanticOperatorOr,
		SemanticOperatorSubscript:
		return 2
	case SemanticOperatorNot,
		SemanticOperatorNegate,
		SemanticOperatorIsNull,
		SemanticOperatorIsNotNull:
		return 1
	default:
		return 0
	}
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

func validateFunctions(definitions []FunctionCapabilityDefinition) (map[string]*FunctionCapabilityDefinition, error) {
	functions := make(map[string]*FunctionCapabilityDefinition, len(definitions))
	for index := range definitions {
		definition := &definitions[index]
		if err := validateDefinitionText(definition.Name, "function name"); err != nil {
			return nil, err
		}
		name := canonicalName(definition.Name)
		if _, exists := functions[name]; exists {
			return nil, invalidSnapshot("duplicate function %q", definition.Name)
		}
		functions[name] = definition
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
			if signature.Variadic && len(signature.ArgumentTypes) == 0 {
				return nil, invalidSnapshot("function %q variadic signature must declare an argument", definition.Name)
			}
			if err := validateDefinitionText(signature.ReturnType, "function return type"); err != nil {
				return nil, err
			}
		}
	}
	return functions, nil
}

func functionAcceptsArity(function *FunctionCapabilityDefinition, arity int) bool {
	for _, signature := range function.Signatures {
		if signature.Variadic {
			if arity >= len(signature.ArgumentTypes)-1 {
				return true
			}
			continue
		}
		if arity == len(signature.ArgumentTypes) {
			return true
		}
	}
	return false
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

func validateRelations(snapshot *HogQLSemanticCatalogSnapshot, logicalTables map[string]*LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition) (map[string]*semanticRelation, error) {
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
			return nil, err
		}
		if err := validateDefinitionText(definition.QueryID, "saved query ID"); err != nil {
			return nil, err
		}
		if err := addRelation(relations, definition.Name, RelationKindSavedQuery, fields, nil, definition); err != nil {
			return nil, err
		}
	}
	for index := range snapshot.MaterializedViews {
		definition := &snapshot.MaterializedViews[index]
		fields, err := validateReferencedRelation(definition.Name, definition.Fields)
		if err != nil {
			return nil, err
		}
		if err := normalizePhysicalQualifiedName(&definition.PhysicalView); err != nil {
			return nil, err
		}
		if definition.PhysicalView.Catalog != snapshot.Catalog {
			return nil, invalidSnapshot("materialized view %q physical catalog does not match snapshot catalog", definition.Name)
		}
		if err := addRelation(relations, definition.Name, RelationKindMaterializedView, fields, nil, nil); err != nil {
			return nil, err
		}
	}
	for index := range snapshot.VirtualTables {
		definition := &snapshot.VirtualTables[index]
		if definition.Projections == nil {
			return nil, invalidSnapshot("virtual table %q projections are required", definition.Name)
		}
		if err := addRelation(relations, definition.Name, RelationKindVirtualTable, nil, definition, nil); err != nil {
			return nil, err
		}
	}
	states := make(map[string]uint8, len(snapshot.VirtualTables)+len(snapshot.SavedQueries))
	for index := range snapshot.VirtualTables {
		if _, err := resolveSemanticRelation(canonicalName(snapshot.VirtualTables[index].Name), relations, states, 1); err != nil {
			return nil, err
		}
	}
	for index := range snapshot.SavedQueries {
		if _, err := resolveSemanticRelation(canonicalName(snapshot.SavedQueries[index].Name), relations, states, 1); err != nil {
			return nil, err
		}
	}
	return relations, nil
}

func semanticDefinitionCount(snapshot *HogQLSemanticCatalogSnapshot) int {
	return len(snapshot.ExpressionFields) + len(snapshot.VirtualTables) + len(snapshot.SavedQueries) + len(snapshot.MaterializedViews) + len(snapshot.Functions) + len(snapshot.ModifierDefaults) + len(snapshot.LazyTables) + len(snapshot.Actions) + len(snapshot.Cohorts)
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
	clone.LazyTables = slices.Clone(snapshot.LazyTables)
	for index := range clone.LazyTables {
		clone.LazyTables[index].RelationshipPath = slices.Clone(snapshot.LazyTables[index].RelationshipPath)
		clone.LazyTables[index].Projections = slices.Clone(snapshot.LazyTables[index].Projections)
		for projectionIndex := range clone.LazyTables[index].Projections {
			clone.LazyTables[index].Projections[projectionIndex].Recipe = cloneExpressionRecipe(snapshot.LazyTables[index].Projections[projectionIndex].Recipe)
		}
	}
	clone.Actions = slices.Clone(snapshot.Actions)
	for index := range clone.Actions {
		clone.Actions[index].Representation = cloneSemanticEntityRepresentation(snapshot.Actions[index].Representation)
	}
	clone.Cohorts = slices.Clone(snapshot.Cohorts)
	for index := range clone.Cohorts {
		clone.Cohorts[index].Representation = cloneSemanticEntityRepresentation(snapshot.Cohorts[index].Representation)
	}
}

func cloneSemanticEntityRepresentation(representation SemanticEntityRepresentation) SemanticEntityRepresentation {
	clone := representation
	if representation.Predicate != nil {
		predicate := cloneExpressionRecipe(*representation.Predicate)
		clone.Predicate = &predicate
	}
	if representation.Relation != nil {
		relation := *representation.Relation
		clone.Relation = &relation
	}
	return clone
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
	if recipe.ArgumentReference != nil {
		value := *recipe.ArgumentReference
		clone.ArgumentReference = &value
	}
	if recipe.ScopedFieldReference != nil {
		value := *recipe.ScopedFieldReference
		clone.ScopedFieldReference = &value
	}
	if recipe.PropertyLookup != nil {
		value := *recipe.PropertyLookup
		if recipe.PropertyLookup.Key != nil {
			key := cloneExpressionRecipe(*recipe.PropertyLookup.Key)
			value.Key = &key
		}
		clone.PropertyLookup = &value
	}
	return clone
}

var validSemanticOperators = []SemanticOperator{
	SemanticOperatorAdd, SemanticOperatorSubtract, SemanticOperatorMultiply, SemanticOperatorDivide, SemanticOperatorModulus,
	SemanticOperatorEqual, SemanticOperatorNotEqual, SemanticOperatorLessThan, SemanticOperatorLessThanOrEqual,
	SemanticOperatorGreaterThan, SemanticOperatorGreaterThanOrEqual, SemanticOperatorAnd, SemanticOperatorOr,
	SemanticOperatorNot, SemanticOperatorNegate, SemanticOperatorIsNull, SemanticOperatorIsNotNull,
	SemanticOperatorSubscript,
}

var validFunctionKinds = []FunctionKind{FunctionKindScalar, FunctionKindAggregate, FunctionKindWindow, FunctionKindTable}

var validFunctionImplementations = []FunctionImplementation{FunctionImplementationStock, FunctionImplementationUDF, FunctionImplementationRewrite}

var validModifierBehaviors = []ModifierBehavior{ModifierBehaviorCompiler, ModifierBehaviorTrinoSessionProperty, ModifierBehaviorSafeNoop, ModifierBehaviorUnsupported}

var decimalLiteralPattern = regexp.MustCompile(`^[+-]?(0|[1-9][0-9]*)(\.[0-9]+)?$`)
