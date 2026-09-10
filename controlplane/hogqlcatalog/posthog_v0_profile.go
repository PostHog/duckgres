package hogqlcatalog

import "strings"

const (
	postHogEventsTableName   = "events"
	postHogPersonsTableName  = "persons"
	postHogPropertiesName    = "properties"
	postHogEventPersonIDName = "person_id"
	postHogPersonIDName      = "id"
)

func applyPostHogV0Profile(snapshot *HogQLSemanticCatalogSnapshot) *HogQLSemanticCatalogSnapshot {
	profiled := cloneSnapshot(snapshot)
	events, persons, ok := postHogV0Tables(profiled)
	if !ok {
		return profiled
	}

	eventProperties := physicalField(events, postHogPropertiesName)
	personProperties := physicalField(persons, postHogPropertiesName)
	eventPersonID := physicalField(events, postHogEventPersonIDName)
	personID := physicalField(persons, postHogPersonIDName)
	if !postHogV0FieldsCompatible(eventProperties, personProperties, eventPersonID, personID) {
		return profiled
	}

	addPostHogV0Property(events, eventProperties)
	addPostHogV0Property(persons, personProperties)
	addPostHogV0PersonRelationship(events, persons, eventPersonID, personID)
	return profiled
}

func postHogV0Tables(snapshot *HogQLSemanticCatalogSnapshot) (*LogicalTableDefinition, *LogicalTableDefinition, bool) {
	var events []*LogicalTableDefinition
	var persons []*LogicalTableDefinition
	for index := range snapshot.LogicalTables {
		table := &snapshot.LogicalTables[index]
		switch table.PhysicalTable.Table {
		case PhysicalIdentifier{Value: postHogEventsTableName}:
			events = append(events, table)
		case PhysicalIdentifier{Value: postHogPersonsTableName}:
			persons = append(persons, table)
		}
	}
	if len(events) != 1 || len(persons) != 1 || events[0].PhysicalTable.Schema != persons[0].PhysicalTable.Schema {
		return nil, nil, false
	}
	return events[0], persons[0], true
}

func physicalField(table *LogicalTableDefinition, column string) *LogicalFieldDefinition {
	var match *LogicalFieldDefinition
	for index := range table.Fields {
		if table.Fields[index].PhysicalColumn != (PhysicalIdentifier{Value: column}) {
			continue
		}
		if match != nil {
			return nil
		}
		match = &table.Fields[index]
	}
	return match
}

func postHogV0FieldsCompatible(eventProperties, personProperties, eventPersonID, personID *LogicalFieldDefinition) bool {
	if eventProperties == nil || personProperties == nil || eventPersonID == nil || personID == nil {
		return false
	}
	if !isVarcharType(eventProperties.TrinoTypeSignature) || !isVarcharType(personProperties.TrinoTypeSignature) {
		return false
	}
	if !strings.EqualFold(eventPersonID.TrinoTypeSignature, personID.TrinoTypeSignature) || eventPersonID.LogicalType != personID.LogicalType {
		return false
	}
	return eventPersonID.LogicalType == LogicalTypeString || eventPersonID.LogicalType == LogicalTypeUUID
}

func isVarcharType(signature string) bool {
	lower := strings.ToLower(signature)
	return lower == "varchar" || strings.HasPrefix(lower, "varchar(") && strings.HasSuffix(lower, ")")
}

func addPostHogV0Property(table *LogicalTableDefinition, source *LogicalFieldDefinition) {
	if hasLogicalMember(table, "properties", source.Name) {
		return
	}
	table.Properties = append(table.Properties, PropertyDefinition{
		Name:               "properties",
		SourceField:        source.Name,
		Storage:            PropertyStorageJSONObject,
		LogicalType:        LogicalTypeString,
		Nullable:           source.Nullable,
		KeyTypeSignature:   "varchar",
		ValueTypeSignature: "varchar",
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
	})
}

func addPostHogV0PersonRelationship(events, persons *LogicalTableDefinition, eventPersonID, personID *LogicalFieldDefinition) {
	if hasLogicalMember(events, "person", "") {
		return
	}
	events.Relationships = append(events.Relationships, RelationshipDefinition{
		Name:        "person",
		TargetTable: persons.Name,
		Cardinality: RelationshipCardinalityManyToOne,
		JoinKeys: []JoinKey{{
			SourceField: eventPersonID.Name,
			TargetField: personID.Name,
		}},
	})
}

func hasLogicalMember(table *LogicalTableDefinition, name, allowedField string) bool {
	canonical := canonicalName(name)
	for _, field := range table.Fields {
		if canonicalName(field.Name) == canonical && canonicalName(field.Name) != canonicalName(allowedField) {
			return true
		}
	}
	for _, property := range table.Properties {
		if canonicalName(property.Name) == canonical {
			return true
		}
	}
	for _, relationship := range table.Relationships {
		if canonicalName(relationship.Name) == canonical {
			return true
		}
	}
	return false
}

func argumentReferenceRecipe(argument ExpressionArgument) ExpressionRecipe {
	return ExpressionRecipe{
		Kind:              ExpressionRecipeArgumentReference,
		ArgumentReference: &ArgumentReferenceRecipe{Argument: argument},
	}
}
