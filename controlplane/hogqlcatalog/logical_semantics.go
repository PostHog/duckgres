package hogqlcatalog

type LazyTableDefinition struct {
	Table            string                     `json:"table"`
	Name             string                     `json:"name"`
	RelationshipPath []string                   `json:"relationshipPath"`
	Projections      []LazyProjectionDefinition `json:"projections"`
}

type LazyProjectionDefinition struct {
	Name               string           `json:"name"`
	TrinoTypeSignature string           `json:"trinoTypeSignature"`
	LogicalType        LogicalType      `json:"logicalType"`
	Nullable           bool             `json:"nullable"`
	StarVisible        bool             `json:"starVisible"`
	Recipe             ExpressionRecipe `json:"recipe"`
}

type ActionReference struct {
	Name           string                       `json:"name"`
	ActionID       string                       `json:"actionId"`
	Table          string                       `json:"table"`
	Representation SemanticEntityRepresentation `json:"representation"`
}

type CohortReference struct {
	Name           string                       `json:"name"`
	CohortID       string                       `json:"cohortId"`
	Table          string                       `json:"table"`
	Representation SemanticEntityRepresentation `json:"representation"`
}

type SemanticEntityRepresentation struct {
	Kind      SemanticEntityKind        `json:"kind"`
	Predicate *ExpressionRecipe         `json:"predicate,omitempty"`
	Relation  *RelationMembershipRecipe `json:"relation,omitempty"`
}

type SemanticEntityKind string

const (
	SemanticEntityPredicate SemanticEntityKind = "PREDICATE"
	SemanticEntityRelation  SemanticEntityKind = "RELATION"
)

type RelationMembershipRecipe struct {
	Relation    RelationReference `json:"relation"`
	SourceField string            `json:"sourceField"`
	TargetField string            `json:"targetField"`
}

func validatePropertyRecipes(tables map[string]*LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition, functions map[string]*FunctionCapabilityDefinition) error {
	for _, table := range tables {
		for index := range table.Properties {
			property := &table.Properties[index]
			if property.LookupRecipe == nil {
				if property.KeyTypeSignature != "" || property.ValueTypeSignature != "" {
					return invalidSnapshot("property %q must include a lookup recipe with its type signatures", property.Name)
				}
				continue
			}
			if err := validateDefinitionText(property.KeyTypeSignature, "property key type signature"); err != nil {
				return err
			}
			if err := validateDefinitionText(property.ValueTypeSignature, "property value type signature"); err != nil {
				return err
			}
			arguments := make(map[ExpressionArgument]int, 2)
			context := expressionRecipeValidationContext{
				ownerTable:           table.Name,
				tables:               tables,
				expressionFields:     expressionFields,
				functions:            functions,
				allowFieldReferences: false,
				allowPropertyLookups: false,
				arguments:            arguments,
			}
			nodes := 0
			if err := validateExpressionRecipeWithContext(property.LookupRecipe, 1, &nodes, context); err != nil {
				return invalidSnapshot("property %q lookup recipe is invalid: %v", property.Name, err)
			}
			if arguments[ExpressionArgumentPropertySource] == 0 || arguments[ExpressionArgumentPropertyKey] == 0 {
				return invalidSnapshot("property %q lookup recipe must reference source and key arguments", property.Name)
			}
		}
	}
	return nil
}

func validateRelationshipPredicates(tables map[string]*LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition, functions map[string]*FunctionCapabilityDefinition) error {
	for _, sourceTable := range tables {
		for index := range sourceTable.Relationships {
			relationship := &sourceTable.Relationships[index]
			if relationship.JoinPredicate == nil {
				continue
			}
			targetTable := tables[canonicalName(relationship.TargetTable)]
			context := expressionRecipeValidationContext{
				ownerTable:           sourceTable.Name,
				tables:               tables,
				expressionFields:     expressionFields,
				functions:            functions,
				allowFieldReferences: false,
				allowPropertyLookups: true,
				scopedTables: map[RelationshipJoinSide]string{
					RelationshipJoinSource: sourceTable.Name,
					RelationshipJoinTarget: targetTable.Name,
				},
			}
			nodes := 0
			if err := validateExpressionRecipeWithContext(relationship.JoinPredicate, 1, &nodes, context); err != nil {
				return invalidSnapshot("relationship %q join predicate is invalid: %v", relationship.Name, err)
			}
		}
	}
	return nil
}

func validateLazyTables(definitions []LazyTableDefinition, tables map[string]*LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition, functions map[string]*FunctionCapabilityDefinition) error {
	members := make(map[string]map[string]struct{}, len(tables))
	for tableName, table := range tables {
		members[tableName] = logicalMemberNames(table)
		for _, field := range expressionFields {
			if canonicalName(field.Table) == tableName {
				members[tableName][canonicalName(field.Name)] = struct{}{}
			}
		}
	}
	for index := range definitions {
		definition := &definitions[index]
		if err := validateDefinitionText(definition.Table, "lazy table owner"); err != nil {
			return err
		}
		if err := validateDefinitionText(definition.Name, "lazy table name"); err != nil {
			return err
		}
		owner := tables[canonicalName(definition.Table)]
		if owner == nil {
			return invalidSnapshot("lazy table %q references an unknown owner table", definition.Name)
		}
		memberName := canonicalName(definition.Name)
		if _, exists := members[canonicalName(owner.Name)][memberName]; exists {
			return invalidSnapshot("lazy table %q conflicts with an existing logical member", definition.Name)
		}
		members[canonicalName(owner.Name)][memberName] = struct{}{}
		if len(definition.RelationshipPath) == 0 {
			return invalidSnapshot("lazy table %q must include a relationship path", definition.Name)
		}
		if len(definition.RelationshipPath) > maxSemanticRelationDepth {
			return invalidSnapshot("lazy table %q relationship path exceeds depth limit", definition.Name)
		}
		terminal := owner
		for _, relationshipName := range definition.RelationshipPath {
			if err := validateDefinitionText(relationshipName, "lazy table relationship path"); err != nil {
				return err
			}
			relationship := findRelationship(terminal, relationshipName)
			if relationship == nil {
				return invalidSnapshot("lazy table %q references an unknown relationship", definition.Name)
			}
			terminal = tables[canonicalName(relationship.TargetTable)]
			if terminal == nil {
				return invalidSnapshot("lazy table %q relationship has an unknown target", definition.Name)
			}
		}
		if len(definition.Projections) == 0 {
			return invalidSnapshot("lazy table %q must include projections", definition.Name)
		}
		projectionNames := make(map[string]struct{}, len(definition.Projections))
		for projectionIndex := range definition.Projections {
			projection := &definition.Projections[projectionIndex]
			if err := validateReferencedField(ReferencedField{
				Name: projection.Name, TrinoTypeSignature: projection.TrinoTypeSignature, LogicalType: projection.LogicalType,
			}); err != nil {
				return err
			}
			projectionName := canonicalName(projection.Name)
			if _, exists := projectionNames[projectionName]; exists {
				return invalidSnapshot("duplicate projection %q on lazy table %q", projection.Name, definition.Name)
			}
			projectionNames[projectionName] = struct{}{}
			dependencies := make([]string, 0)
			context := expressionRecipeValidationContext{
				ownerTable: terminal.Name, tables: tables, expressionFields: expressionFields, functions: functions,
				dependencies: &dependencies, allowFieldReferences: true, allowPropertyLookups: true,
			}
			nodes := 0
			if err := validateExpressionRecipeWithContext(&projection.Recipe, 1, &nodes, context); err != nil {
				return invalidSnapshot("lazy table %q projection %q is invalid: %v", definition.Name, projection.Name, err)
			}
		}
	}
	return nil
}

func findRelationship(table *LogicalTableDefinition, name string) *RelationshipDefinition {
	for index := range table.Relationships {
		if canonicalName(table.Relationships[index].Name) == canonicalName(name) {
			return &table.Relationships[index]
		}
	}
	return nil
}

func validateSemanticEntities(snapshot *HogQLSemanticCatalogSnapshot, tables map[string]*LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition, functions map[string]*FunctionCapabilityDefinition, relations map[string]*semanticRelation) error {
	actionNames := make(map[string]struct{}, len(snapshot.Actions))
	for index := range snapshot.Actions {
		action := &snapshot.Actions[index]
		if err := validateDefinitionText(action.ActionID, "action ID"); err != nil {
			return err
		}
		if err := validateEntityReference(action.Name, action.Table, &action.Representation, "action", actionNames, tables, expressionFields, functions, relations); err != nil {
			return err
		}
	}
	cohortNames := make(map[string]struct{}, len(snapshot.Cohorts))
	for index := range snapshot.Cohorts {
		cohort := &snapshot.Cohorts[index]
		if err := validateDefinitionText(cohort.CohortID, "cohort ID"); err != nil {
			return err
		}
		if err := validateEntityReference(cohort.Name, cohort.Table, &cohort.Representation, "cohort", cohortNames, tables, expressionFields, functions, relations); err != nil {
			return err
		}
	}
	return nil
}

func validateEntityReference(name, tableName string, representation *SemanticEntityRepresentation, kind string, seen map[string]struct{}, tables map[string]*LogicalTableDefinition, expressionFields map[string]*ExpressionFieldDefinition, functions map[string]*FunctionCapabilityDefinition, relations map[string]*semanticRelation) error {
	if err := validateDefinitionText(name, kind+" name"); err != nil {
		return err
	}
	canonical := canonicalName(name)
	if _, exists := seen[canonical]; exists {
		return invalidSnapshot("duplicate %s %q", kind, name)
	}
	seen[canonical] = struct{}{}
	if err := validateDefinitionText(tableName, kind+" table"); err != nil {
		return err
	}
	owner := tables[canonicalName(tableName)]
	if owner == nil {
		return invalidSnapshot("%s %q references an unknown table", kind, name)
	}
	payloads := 0
	if representation.Predicate != nil {
		payloads++
	}
	if representation.Relation != nil {
		payloads++
	}
	if payloads != 1 {
		return invalidSnapshot("%s %q representation must have exactly one payload", kind, name)
	}
	switch representation.Kind {
	case SemanticEntityPredicate:
		if representation.Predicate == nil {
			return invalidSnapshot("%s %q has a mismatched predicate representation", kind, name)
		}
		dependencies := make([]string, 0)
		context := expressionRecipeValidationContext{
			ownerTable: owner.Name, tables: tables, expressionFields: expressionFields, functions: functions,
			dependencies: &dependencies, allowFieldReferences: true, allowPropertyLookups: true,
		}
		nodes := 0
		if err := validateExpressionRecipeWithContext(representation.Predicate, 1, &nodes, context); err != nil {
			return invalidSnapshot("%s %q predicate is invalid: %v", kind, name, err)
		}
	case SemanticEntityRelation:
		if representation.Relation == nil {
			return invalidSnapshot("%s %q has a mismatched relation representation", kind, name)
		}
		membership := representation.Relation
		if err := validateDefinitionText(membership.Relation.Name, kind+" relation name"); err != nil {
			return err
		}
		target := relations[canonicalName(membership.Relation.Name)]
		if target == nil || target.kind != membership.Relation.Kind {
			return invalidSnapshot("%s %q references an unknown or mismatched relation", kind, name)
		}
		if err := validateDefinitionText(membership.SourceField, kind+" source field"); err != nil {
			return err
		}
		if _, exists := semanticFieldNames(owner, expressionFields)[canonicalName(membership.SourceField)]; !exists {
			return invalidSnapshot("%s %q references an unknown source field", kind, name)
		}
		if err := validateDefinitionText(membership.TargetField, kind+" target field"); err != nil {
			return err
		}
		if _, exists := target.fields[canonicalName(membership.TargetField)]; !exists {
			return invalidSnapshot("%s %q references an unknown target field", kind, name)
		}
	default:
		return invalidSnapshot("%s %q has an unknown representation kind", kind, name)
	}
	return nil
}
