package hogqlcatalog

import (
	"context"
	"fmt"
	"reflect"
	"slices"
)

func mergePhysicalCatalog(ctx context.Context, metadata *PhysicalCatalogMetadata, latest *HogQLSemanticCatalogSnapshot, catalog PhysicalIdentifier, languageVersion string, generation int64) (*HogQLSemanticCatalogSnapshot, error) {
	physical, err := buildPhysicalSnapshot(ctx, metadata, catalog, languageVersion, generation)
	if err != nil {
		return nil, err
	}
	if latest == nil {
		return physical, nil
	}
	current, err := normalizeAndValidateSnapshot(latest)
	if err != nil {
		return nil, err
	}
	if current.Catalog != physical.Catalog {
		return nil, invalidSnapshot("physical refresh catalog does not match latest snapshot")
	}
	if current.LanguageVersion != languageVersion {
		return nil, invalidSnapshot("physical refresh languageVersion does not match latest snapshot")
	}

	currentTables := make(map[string][]LogicalTableDefinition, len(current.LogicalTables))
	for _, table := range current.LogicalTables {
		key := physicalQualifiedNameKey(table.PhysicalTable)
		currentTables[key] = append(currentTables[key], table)
	}
	refreshedTables := make([]LogicalTableDefinition, 0, len(physical.LogicalTables))
	for _, physicalTable := range physical.LogicalTables {
		currentDefinitions := currentTables[physicalQualifiedNameKey(physicalTable.PhysicalTable)]
		if len(currentDefinitions) == 0 {
			refreshedTables = append(refreshedTables, physicalTable)
			continue
		}
		for _, currentTable := range currentDefinitions {
			refreshedTables = append(refreshedTables, refreshLogicalTable(physicalTable, currentTable))
		}
	}
	physical.LogicalTables = refreshedTables
	cloneSemanticMetadata(physical, current)
	normalized, err := normalizeAndValidateSnapshot(physical)
	if err != nil {
		return nil, fmt.Errorf("merge HogQL physical catalog metadata: %w", err)
	}
	return normalized, nil
}

func refreshLogicalTable(physicalTable, currentTable LogicalTableDefinition) LogicalTableDefinition {
	refreshed := physicalTable
	refreshed.Fields = slices.Clone(physicalTable.Fields)
	refreshed.Name = currentTable.Name
	refreshed.Properties = currentTable.Properties
	refreshed.Relationships = currentTable.Relationships
	currentFields := make(map[string]LogicalFieldDefinition, len(currentTable.Fields))
	for _, field := range currentTable.Fields {
		currentFields[physicalIdentifierKey(field.PhysicalColumn)] = field
	}
	for fieldIndex := range refreshed.Fields {
		field := &refreshed.Fields[fieldIndex]
		if currentField, exists := currentFields[physicalIdentifierKey(field.PhysicalColumn)]; exists {
			field.Name = currentField.Name
		}
	}
	return refreshed
}

func physicalQualifiedNameKey(name PhysicalQualifiedName) string {
	return physicalIdentifierKey(name.Catalog) + "\x00" + physicalIdentifierKey(name.Schema) + "\x00" + physicalIdentifierKey(name.Table)
}

func physicalInventoriesEqual(left, right *HogQLSemanticCatalogSnapshot) bool {
	return reflect.DeepEqual(physicalInventory(left), physicalInventory(right))
}

type physicalInventoryTable struct {
	name   PhysicalQualifiedName
	fields []physicalInventoryField
}

type physicalInventoryField struct {
	column             PhysicalIdentifier
	trinoTypeSignature string
	logicalType        LogicalType
	nullable           bool
	starVisible        bool
}

func physicalInventory(snapshot *HogQLSemanticCatalogSnapshot) []physicalInventoryTable {
	tables := make([]physicalInventoryTable, 0, len(snapshot.LogicalTables))
	for _, table := range snapshot.LogicalTables {
		fields := make([]physicalInventoryField, 0, len(table.Fields))
		for _, field := range table.Fields {
			fields = append(fields, physicalInventoryField{
				column:             field.PhysicalColumn,
				trinoTypeSignature: field.TrinoTypeSignature,
				logicalType:        field.LogicalType,
				nullable:           field.Nullable,
				starVisible:        field.StarVisible,
			})
		}
		tables = append(tables, physicalInventoryTable{name: table.PhysicalTable, fields: fields})
	}
	return tables
}
