package hogqlcatalog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

var ErrInvalidPhysicalMetadata = errors.New("invalid HogQL physical catalog metadata")

type PhysicalMetadataProvider interface {
	PhysicalCatalog(ctx context.Context, catalog PhysicalIdentifier) (*PhysicalCatalogMetadata, error)
}

type PhysicalCatalogMetadata struct {
	Catalog PhysicalIdentifier
	Tables  []PhysicalTableMetadata
}

type PhysicalTableMetadata struct {
	Schema  PhysicalIdentifier
	Table   PhysicalIdentifier
	Columns []PhysicalColumnMetadata
}

type PhysicalColumnMetadata struct {
	Name               PhysicalIdentifier
	Ordinal            int
	TrinoTypeSignature string
	Nullability        ColumnNullability
	StarVisibility     ColumnStarVisibility
}

type ColumnNullability string

const (
	ColumnNullable ColumnNullability = "NULLABLE"
	ColumnNotNull  ColumnNullability = "NOT_NULL"
)

type ColumnStarVisibility string

const (
	ColumnStarVisible ColumnStarVisibility = "VISIBLE"
	ColumnStarHidden  ColumnStarVisibility = "HIDDEN"
)

func BuildPhysicalSnapshot(ctx context.Context, provider PhysicalMetadataProvider, catalog PhysicalIdentifier, languageVersion string, generation int64) (*HogQLSemanticCatalogSnapshot, error) {
	if provider == nil {
		return nil, invalidPhysicalMetadata("provider is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !languageVersionPattern.MatchString(languageVersion) {
		return nil, invalidSnapshot("invalid languageVersion")
	}
	if generation <= 0 {
		return nil, invalidSnapshot("generation must be positive")
	}
	normalizedCatalog, err := normalizedCatalog(catalog)
	if err != nil {
		return nil, invalidPhysicalMetadata("requested catalog is invalid: %v", err)
	}
	metadata, err := provider.PhysicalCatalog(ctx, normalizedCatalog)
	if err != nil {
		return nil, fmt.Errorf("load HogQL physical catalog metadata: %w", err)
	}
	return buildPhysicalSnapshot(ctx, metadata, normalizedCatalog, languageVersion, generation)
}

func buildPhysicalSnapshot(ctx context.Context, metadata *PhysicalCatalogMetadata, normalizedCatalog PhysicalIdentifier, languageVersion string, generation int64) (*HogQLSemanticCatalogSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if metadata == nil {
		return nil, invalidPhysicalMetadata("provider returned no catalog")
	}
	if err := requireCanonicalPhysicalIdentifier(metadata.Catalog, "catalog"); err != nil {
		return nil, err
	}
	if metadata.Catalog != normalizedCatalog {
		return nil, invalidPhysicalMetadata("provider catalog does not match requested catalog")
	}
	if metadata.Tables == nil {
		return nil, invalidPhysicalMetadata("table inventory is missing")
	}

	tables := make([]LogicalTableDefinition, 0, len(metadata.Tables))
	physicalTables := make(map[string]struct{}, len(metadata.Tables))
	for _, physicalTable := range metadata.Tables {
		if err := requireCanonicalPhysicalIdentifier(physicalTable.Schema, "schema"); err != nil {
			return nil, err
		}
		if err := requireCanonicalPhysicalIdentifier(physicalTable.Table, "table"); err != nil {
			return nil, err
		}
		if physicalTable.Columns == nil {
			return nil, invalidPhysicalMetadata("column inventory is missing for table %q", physicalTable.Table.Value)
		}
		tableKey := physicalIdentifierKey(physicalTable.Schema) + "\x00" + physicalIdentifierKey(physicalTable.Table)
		if _, exists := physicalTables[tableKey]; exists {
			return nil, invalidPhysicalMetadata("duplicate physical table %q.%q", physicalTable.Schema.Value, physicalTable.Table.Value)
		}
		physicalTables[tableKey] = struct{}{}

		columns := append([]PhysicalColumnMetadata(nil), physicalTable.Columns...)
		sort.Slice(columns, func(left, right int) bool {
			return columns[left].Ordinal < columns[right].Ordinal
		})
		fields := make([]LogicalFieldDefinition, 0, len(columns))
		ordinals := make(map[int]struct{}, len(columns))
		physicalColumns := make(map[string]struct{}, len(columns))
		for _, column := range columns {
			if err := requireCanonicalPhysicalIdentifier(column.Name, "column"); err != nil {
				return nil, err
			}
			if column.Ordinal <= 0 {
				return nil, invalidPhysicalMetadata("column %q has a nonpositive ordinal", column.Name.Value)
			}
			if _, exists := ordinals[column.Ordinal]; exists {
				return nil, invalidPhysicalMetadata("table %q has duplicate column ordinal %d", physicalTable.Table.Value, column.Ordinal)
			}
			ordinals[column.Ordinal] = struct{}{}
			columnKey := physicalIdentifierKey(column.Name)
			if _, exists := physicalColumns[columnKey]; exists {
				return nil, invalidPhysicalMetadata("table %q has duplicate physical column %q", physicalTable.Table.Value, column.Name.Value)
			}
			physicalColumns[columnKey] = struct{}{}
			if column.TrinoTypeSignature != strings.TrimSpace(column.TrinoTypeSignature) {
				return nil, invalidPhysicalMetadata("column %q has a noncanonical Trino type signature", column.Name.Value)
			}
			if err := validateDefinitionText(column.TrinoTypeSignature, "Trino type signature"); err != nil {
				return nil, invalidPhysicalMetadata("column %q has an invalid Trino type signature: %v", column.Name.Value, err)
			}
			nullable, err := columnNullability(column)
			if err != nil {
				return nil, err
			}
			starVisible, err := columnStarVisibility(column)
			if err != nil {
				return nil, err
			}
			fields = append(fields, LogicalFieldDefinition{
				Name:               column.Name.Value,
				PhysicalColumn:     column.Name,
				TrinoTypeSignature: column.TrinoTypeSignature,
				LogicalType:        logicalTypeForTrinoSignature(column.TrinoTypeSignature),
				Nullable:           nullable,
				StarVisible:        starVisible,
			})
		}
		tables = append(tables, LogicalTableDefinition{
			Name: physicalTable.Table.Value,
			PhysicalTable: PhysicalQualifiedName{
				Catalog: metadata.Catalog,
				Schema:  physicalTable.Schema,
				Table:   physicalTable.Table,
			},
			Fields:        fields,
			Properties:    []PropertyDefinition{},
			Relationships: []RelationshipDefinition{},
		})
	}
	sort.Slice(tables, func(left, right int) bool {
		leftName := tables[left].PhysicalTable
		rightName := tables[right].PhysicalTable
		leftKey := physicalIdentifierKey(leftName.Schema) + "\x00" + physicalIdentifierKey(leftName.Table)
		rightKey := physicalIdentifierKey(rightName.Schema) + "\x00" + physicalIdentifierKey(rightName.Table)
		return leftKey < rightKey
	})

	snapshot := &HogQLSemanticCatalogSnapshot{
		ProtocolVersion:   SnapshotProtocolVersion,
		SchemaVersion:     SnapshotSchemaVersion,
		LanguageVersion:   languageVersion,
		Catalog:           metadata.Catalog,
		Generation:        generation,
		LogicalTables:     tables,
		ExpressionFields:  []ExpressionFieldDefinition{},
		VirtualTables:     []VirtualTableDefinition{},
		SavedQueries:      []SavedQueryReference{},
		MaterializedViews: []MaterializedViewReference{},
		Functions:         []FunctionCapabilityDefinition{},
		ModifierDefaults:  []SemanticModifierDefault{},
	}
	normalized, err := normalizeAndValidateSnapshot(snapshot)
	if err != nil {
		return nil, invalidPhysicalMetadata("translated snapshot is invalid: %v", err)
	}
	return normalized, nil
}

func columnNullability(column PhysicalColumnMetadata) (bool, error) {
	switch column.Nullability {
	case ColumnNullable:
		return true, nil
	case ColumnNotNull:
		return false, nil
	default:
		return false, invalidPhysicalMetadata("column %q has unknown nullability", column.Name.Value)
	}
}

func columnStarVisibility(column PhysicalColumnMetadata) (bool, error) {
	switch column.StarVisibility {
	case ColumnStarVisible:
		return true, nil
	case ColumnStarHidden:
		return false, nil
	default:
		return false, invalidPhysicalMetadata("column %q has unknown star visibility", column.Name.Value)
	}
}

func requireCanonicalPhysicalIdentifier(identifier PhysicalIdentifier, kind string) error {
	normalized := identifier
	if err := normalizePhysicalIdentifier(&normalized); err != nil {
		return invalidPhysicalMetadata("invalid %s identifier: %v", kind, err)
	}
	if normalized != identifier {
		return invalidPhysicalMetadata("%s identifier %q is not canonical", kind, identifier.Value)
	}
	return nil
}

func physicalIdentifierKey(identifier PhysicalIdentifier) string {
	if identifier.Delimited {
		return "1" + identifier.Value
	}
	return "0" + identifier.Value
}

func logicalTypeForTrinoSignature(signature string) LogicalType {
	lower := strings.ToLower(signature)
	base := lower
	if separator := strings.IndexAny(base, "( "); separator >= 0 {
		base = base[:separator]
	}
	switch base {
	case "boolean":
		return LogicalTypeBoolean
	case "tinyint", "smallint", "integer", "bigint":
		return LogicalTypeInteger
	case "real", "double":
		return LogicalTypeFloat
	case "decimal":
		return LogicalTypeDecimal
	case "varchar", "char":
		return LogicalTypeString
	case "date":
		return LogicalTypeDate
	case "timestamp":
		return LogicalTypeTimestamp
	case "interval":
		return LogicalTypeInterval
	case "uuid":
		return LogicalTypeUUID
	case "json":
		return LogicalTypeJSON
	case "array":
		return LogicalTypeArray
	case "map":
		return LogicalTypeMap
	case "row":
		return LogicalTypeRow
	default:
		return LogicalTypeUnknown
	}
}

func invalidPhysicalMetadata(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidPhysicalMetadata, fmt.Sprintf(format, args...))
}
