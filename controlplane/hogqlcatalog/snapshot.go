package hogqlcatalog

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

const (
	SnapshotProtocolVersion = 1
	SnapshotSchemaVersion   = 2
)

var (
	ErrInvalidSnapshot      = errors.New("invalid HogQL semantic catalog snapshot")
	ErrCatalogNotFound      = errors.New("HogQL semantic catalog not found")
	ErrGenerationNotFound   = errors.New("HogQL semantic catalog generation not found")
	ErrGenerationRegression = errors.New("HogQL semantic catalog generation regressed")
	ErrGenerationConflict   = errors.New("HogQL semantic catalog generation conflicts with published content")

	unquotedIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	languageVersionPattern    = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
)

type HogQLSemanticCatalogSnapshot struct {
	ProtocolVersion   int                            `json:"protocolVersion"`
	SchemaVersion     int                            `json:"schemaVersion"`
	LanguageVersion   string                         `json:"languageVersion"`
	Catalog           PhysicalIdentifier             `json:"catalog"`
	Generation        int64                          `json:"generation"`
	LogicalTables     []LogicalTableDefinition       `json:"logicalTables"`
	ExpressionFields  []ExpressionFieldDefinition    `json:"expressionFields"`
	VirtualTables     []VirtualTableDefinition       `json:"virtualTables"`
	SavedQueries      []SavedQueryReference          `json:"savedQueries"`
	MaterializedViews []MaterializedViewReference    `json:"materializedViews"`
	Functions         []FunctionCapabilityDefinition `json:"functions"`
	ModifierDefaults  []SemanticModifierDefault      `json:"modifierDefaults"`
	LazyTables        []LazyTableDefinition          `json:"lazyTables,omitempty"`
	Actions           []ActionReference              `json:"actions,omitempty"`
	Cohorts           []CohortReference              `json:"cohorts,omitempty"`
}

type LogicalTableDefinition struct {
	Name          string                   `json:"name"`
	PhysicalTable PhysicalQualifiedName    `json:"physicalTable"`
	Fields        []LogicalFieldDefinition `json:"fields"`
	Properties    []PropertyDefinition     `json:"properties"`
	Relationships []RelationshipDefinition `json:"relationships"`
}

type LogicalFieldDefinition struct {
	Name               string             `json:"name"`
	PhysicalColumn     PhysicalIdentifier `json:"physicalColumn"`
	TrinoTypeSignature string             `json:"trinoTypeSignature"`
	LogicalType        LogicalType        `json:"logicalType"`
	Nullable           bool               `json:"nullable"`
	StarVisible        bool               `json:"starVisible"`
}

type PropertyDefinition struct {
	Name               string            `json:"name"`
	SourceField        string            `json:"sourceField"`
	Storage            PropertyStorage   `json:"storage"`
	LogicalType        LogicalType       `json:"logicalType"`
	Nullable           bool              `json:"nullable"`
	KeyTypeSignature   string            `json:"keyTypeSignature,omitempty"`
	ValueTypeSignature string            `json:"valueTypeSignature,omitempty"`
	LookupRecipe       *ExpressionRecipe `json:"lookupRecipe,omitempty"`
}

type RelationshipDefinition struct {
	Name          string                  `json:"name"`
	TargetTable   string                  `json:"targetTable"`
	Cardinality   RelationshipCardinality `json:"cardinality"`
	JoinKeys      []JoinKey               `json:"joinKeys"`
	JoinPredicate *ExpressionRecipe       `json:"joinPredicate,omitempty"`
}

type JoinKey struct {
	SourceField string `json:"sourceField"`
	TargetField string `json:"targetField"`
}

type PhysicalQualifiedName struct {
	Catalog PhysicalIdentifier `json:"catalog"`
	Schema  PhysicalIdentifier `json:"schema"`
	Table   PhysicalIdentifier `json:"table"`
}

type PhysicalIdentifier struct {
	Value     string `json:"value"`
	Delimited bool   `json:"delimited"`
}

type LogicalType string

const (
	LogicalTypeUnknown   LogicalType = "UNKNOWN"
	LogicalTypeBoolean   LogicalType = "BOOLEAN"
	LogicalTypeInteger   LogicalType = "INTEGER"
	LogicalTypeFloat     LogicalType = "FLOAT"
	LogicalTypeDecimal   LogicalType = "DECIMAL"
	LogicalTypeString    LogicalType = "STRING"
	LogicalTypeDate      LogicalType = "DATE"
	LogicalTypeTimestamp LogicalType = "TIMESTAMP"
	LogicalTypeInterval  LogicalType = "INTERVAL"
	LogicalTypeUUID      LogicalType = "UUID"
	LogicalTypeJSON      LogicalType = "JSON"
	LogicalTypeArray     LogicalType = "ARRAY"
	LogicalTypeMap       LogicalType = "MAP"
	LogicalTypeRow       LogicalType = "ROW"
)

type PropertyStorage string

const (
	PropertyStorageJSONObject PropertyStorage = "JSON_OBJECT"
	PropertyStorageMap        PropertyStorage = "MAP"
)

type RelationshipCardinality string

const (
	RelationshipCardinalityOneToOne   RelationshipCardinality = "ONE_TO_ONE"
	RelationshipCardinalityOneToMany  RelationshipCardinality = "ONE_TO_MANY"
	RelationshipCardinalityManyToOne  RelationshipCardinality = "MANY_TO_ONE"
	RelationshipCardinalityManyToMany RelationshipCardinality = "MANY_TO_MANY"
)

func normalizeAndValidateSnapshot(snapshot *HogQLSemanticCatalogSnapshot) (*HogQLSemanticCatalogSnapshot, error) {
	if snapshot == nil {
		return nil, invalidSnapshot("snapshot is null")
	}
	if err := validateSemanticShapeBounds(snapshot); err != nil {
		return nil, err
	}
	normalized := cloneSnapshot(snapshot)
	if len(normalized.LazyTables) == 0 {
		normalized.LazyTables = nil
	}
	if len(normalized.Actions) == 0 {
		normalized.Actions = nil
	}
	if len(normalized.Cohorts) == 0 {
		normalized.Cohorts = nil
	}
	if normalized.ProtocolVersion != SnapshotProtocolVersion {
		return nil, invalidSnapshot("unsupported protocolVersion %d", normalized.ProtocolVersion)
	}
	if normalized.SchemaVersion != SnapshotSchemaVersion {
		return nil, invalidSnapshot("unsupported schemaVersion %d", normalized.SchemaVersion)
	}
	if !languageVersionPattern.MatchString(normalized.LanguageVersion) {
		return nil, invalidSnapshot("invalid languageVersion")
	}
	if normalized.Generation <= 0 {
		return nil, invalidSnapshot("generation must be positive")
	}
	if normalized.LogicalTables == nil {
		return nil, invalidSnapshot("logicalTables is required")
	}
	if err := normalizePhysicalIdentifier(&normalized.Catalog); err != nil {
		return nil, err
	}

	tables := make(map[string]*LogicalTableDefinition, len(normalized.LogicalTables))
	for index := range normalized.LogicalTables {
		table := &normalized.LogicalTables[index]
		if err := validateDefinitionText(table.Name, "logical table name"); err != nil {
			return nil, err
		}
		canonicalTableName := canonicalName(table.Name)
		if _, exists := tables[canonicalTableName]; exists {
			return nil, invalidSnapshot("duplicate logical table %q", table.Name)
		}
		tables[canonicalTableName] = table
		if table.Fields == nil || table.Properties == nil || table.Relationships == nil {
			return nil, invalidSnapshot("logical table %q must include fields, properties, and relationships", table.Name)
		}
		if err := normalizePhysicalQualifiedName(&table.PhysicalTable); err != nil {
			return nil, err
		}
		if table.PhysicalTable.Catalog != normalized.Catalog {
			return nil, invalidSnapshot("logical table %q physical catalog does not match snapshot catalog", table.Name)
		}
		if err := validateTableMembers(table); err != nil {
			return nil, err
		}
	}
	if err := validateReferences(tables); err != nil {
		return nil, err
	}
	if err := validateSemanticMetadata(normalized, tables); err != nil {
		return nil, err
	}
	return normalized, nil
}

func validateTableMembers(table *LogicalTableDefinition) error {
	members := make(map[string]struct{}, len(table.Fields)+len(table.Properties)+len(table.Relationships))
	for index := range table.Fields {
		field := &table.Fields[index]
		if err := addMember(members, table.Name, field.Name); err != nil {
			return err
		}
		if err := normalizePhysicalIdentifier(&field.PhysicalColumn); err != nil {
			return err
		}
		if err := validateDefinitionText(field.TrinoTypeSignature, "Trino type signature"); err != nil {
			return err
		}
		if !slices.Contains(validLogicalTypes, field.LogicalType) {
			return invalidSnapshot("logical field %q has unknown logicalType", field.Name)
		}
	}
	for _, property := range table.Properties {
		if err := addMember(members, table.Name, property.Name); err != nil {
			return err
		}
		if err := validateDefinitionText(property.SourceField, "property source field"); err != nil {
			return err
		}
		if !slices.Contains(validPropertyStorages, property.Storage) {
			return invalidSnapshot("property %q has unknown storage", property.Name)
		}
		if !slices.Contains(validLogicalTypes, property.LogicalType) {
			return invalidSnapshot("property %q has unknown logicalType", property.Name)
		}
	}
	for _, relationship := range table.Relationships {
		if err := addMember(members, table.Name, relationship.Name); err != nil {
			return err
		}
		if err := validateDefinitionText(relationship.TargetTable, "relationship target table"); err != nil {
			return err
		}
		if !slices.Contains(validRelationshipCardinalities, relationship.Cardinality) {
			return invalidSnapshot("relationship %q has unknown cardinality", relationship.Name)
		}
		if len(relationship.JoinKeys) == 0 {
			return invalidSnapshot("relationship %q must have at least one join key", relationship.Name)
		}
		for _, joinKey := range relationship.JoinKeys {
			if err := validateDefinitionText(joinKey.SourceField, "relationship source field"); err != nil {
				return err
			}
			if err := validateDefinitionText(joinKey.TargetField, "relationship target field"); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateReferences(tables map[string]*LogicalTableDefinition) error {
	for _, table := range tables {
		fields := fieldNames(table)
		for _, property := range table.Properties {
			if _, exists := fields[canonicalName(property.SourceField)]; !exists {
				return invalidSnapshot("property %q on table %q references an unknown source field", property.Name, table.Name)
			}
		}
		for _, relationship := range table.Relationships {
			target := tables[canonicalName(relationship.TargetTable)]
			if target == nil {
				return invalidSnapshot("relationship %q on table %q references an unknown target table", relationship.Name, table.Name)
			}
			targetFields := fieldNames(target)
			for _, joinKey := range relationship.JoinKeys {
				if _, exists := fields[canonicalName(joinKey.SourceField)]; !exists {
					return invalidSnapshot("relationship %q on table %q references an unknown source field", relationship.Name, table.Name)
				}
				if _, exists := targetFields[canonicalName(joinKey.TargetField)]; !exists {
					return invalidSnapshot("relationship %q on table %q references an unknown target field", relationship.Name, table.Name)
				}
			}
		}
	}
	return nil
}

func fieldNames(table *LogicalTableDefinition) map[string]struct{} {
	fields := make(map[string]struct{}, len(table.Fields))
	for _, field := range table.Fields {
		fields[canonicalName(field.Name)] = struct{}{}
	}
	return fields
}

func addMember(members map[string]struct{}, tableName, memberName string) error {
	if err := validateDefinitionText(memberName, "logical member name"); err != nil {
		return err
	}
	canonical := canonicalName(memberName)
	if _, exists := members[canonical]; exists {
		return invalidSnapshot("duplicate logical member %q on table %q", memberName, tableName)
	}
	members[canonical] = struct{}{}
	return nil
}

func normalizePhysicalQualifiedName(name *PhysicalQualifiedName) error {
	if err := normalizePhysicalIdentifier(&name.Catalog); err != nil {
		return err
	}
	if err := normalizePhysicalIdentifier(&name.Schema); err != nil {
		return err
	}
	return normalizePhysicalIdentifier(&name.Table)
}

func normalizePhysicalIdentifier(identifier *PhysicalIdentifier) error {
	if err := validateDefinitionText(identifier.Value, "physical identifier"); err != nil {
		return err
	}
	if !identifier.Delimited {
		if !unquotedIdentifierPattern.MatchString(identifier.Value) {
			return invalidSnapshot("invalid physical identifier %q", identifier.Value)
		}
		identifier.Value = strings.ToLower(identifier.Value)
	}
	return nil
}

func validateDefinitionText(value, kind string) error {
	if strings.TrimSpace(value) == "" || containsExecutableDelimiter(value) {
		return invalidSnapshot("invalid %s", kind)
	}
	return nil
}

func containsExecutableDelimiter(value string) bool {
	return strings.ContainsAny(value, ";\x00\n\r") || strings.Contains(value, "--") || strings.Contains(value, "/*") || strings.Contains(value, "*/")
}

func canonicalName(value string) string {
	return strings.ToLower(value)
}

func invalidSnapshot(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSnapshot, fmt.Sprintf(format, args...))
}

func cloneSnapshot(snapshot *HogQLSemanticCatalogSnapshot) *HogQLSemanticCatalogSnapshot {
	if snapshot == nil {
		return nil
	}
	clone := *snapshot
	clone.LogicalTables = slices.Clone(snapshot.LogicalTables)
	for tableIndex, table := range clone.LogicalTables {
		clone.LogicalTables[tableIndex].Fields = slices.Clone(table.Fields)
		clone.LogicalTables[tableIndex].Properties = slices.Clone(table.Properties)
		clone.LogicalTables[tableIndex].Relationships = slices.Clone(table.Relationships)
		for propertyIndex, property := range clone.LogicalTables[tableIndex].Properties {
			if property.LookupRecipe != nil {
				recipe := cloneExpressionRecipe(*property.LookupRecipe)
				clone.LogicalTables[tableIndex].Properties[propertyIndex].LookupRecipe = &recipe
			}
		}
		for relationshipIndex, relationship := range clone.LogicalTables[tableIndex].Relationships {
			clone.LogicalTables[tableIndex].Relationships[relationshipIndex].JoinKeys = slices.Clone(relationship.JoinKeys)
			if relationship.JoinPredicate != nil {
				recipe := cloneExpressionRecipe(*relationship.JoinPredicate)
				clone.LogicalTables[tableIndex].Relationships[relationshipIndex].JoinPredicate = &recipe
			}
		}
	}
	cloneSemanticMetadata(&clone, snapshot)
	return &clone
}

var validLogicalTypes = []LogicalType{
	LogicalTypeUnknown,
	LogicalTypeBoolean,
	LogicalTypeInteger,
	LogicalTypeFloat,
	LogicalTypeDecimal,
	LogicalTypeString,
	LogicalTypeDate,
	LogicalTypeTimestamp,
	LogicalTypeInterval,
	LogicalTypeUUID,
	LogicalTypeJSON,
	LogicalTypeArray,
	LogicalTypeMap,
	LogicalTypeRow,
}

var validPropertyStorages = []PropertyStorage{
	PropertyStorageJSONObject,
	PropertyStorageMap,
}

var validRelationshipCardinalities = []RelationshipCardinality{
	RelationshipCardinalityOneToOne,
	RelationshipCardinalityOneToMany,
	RelationshipCardinalityManyToOne,
	RelationshipCardinalityManyToMany,
}
