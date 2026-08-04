package backend

type Name string

const (
	Memory   Name = "memory"
	DuckLake Name = "ducklake"
)

type ConstraintHandling string
type UnsupportedDDLHandling string
type ConflictHandling string

const (
	PreserveConstraints ConstraintHandling = "preserve"
	StripConstraints    ConstraintHandling = "strip"

	ExecuteUnsupportedDDL UnsupportedDDLHandling = "execute"
	NoOpUnsupportedDDL    UnsupportedDDLHandling = "noop"

	UseInsertOnConflict ConflictHandling = "insert_on_conflict"
	RewriteToMerge      ConflictHandling = "rewrite_to_merge"
)

type CatalogPolicy struct {
	PhysicalName    string
	MapPublicToMain bool
	QualifyMacros   bool
}

type DDLPolicy struct {
	ConstraintHandling    ConstraintHandling
	RewriteSerial         bool
	StripVolatileDefaults bool
	UnsupportedDDL        UnsupportedDDLHandling
	RewriteCascadeDrop    bool
	SplitMultiAlter       bool
}

func (p DDLPolicy) NeedsTransform() bool {
	return p.ConstraintHandling == StripConstraints ||
		p.RewriteSerial ||
		p.StripVolatileDefaults ||
		p.UnsupportedDDL == NoOpUnsupportedDDL ||
		p.RewriteCascadeDrop ||
		p.SplitMultiAlter
}

type DMLPolicy struct {
	ConflictHandling ConflictHandling
}

type MetadataPolicy struct {
	InterceptShowCreate bool
}

type Profile struct {
	name     Name
	catalog  CatalogPolicy
	ddl      DDLPolicy
	dml      DMLPolicy
	metadata MetadataPolicy
}

func (p Profile) Name() Name {
	return p.name
}

func (p Profile) Catalog() CatalogPolicy {
	return p.catalog
}

func (p Profile) DDL() DDLPolicy {
	return p.ddl
}

func (p Profile) DML() DMLPolicy {
	return p.dml
}

func (p Profile) Metadata() MetadataPolicy {
	return p.metadata
}

func ForName(name Name) Profile {
	switch name {
	case DuckLake:
		// DuckLake keeps the historical silent-strip behavior (sqlmesh/dbt issue
		// PK/serial/DEFAULT now() DDL and rely on it succeeding).
		return lakeProfile(DuckLake, "ducklake", true)
	default:
		return Profile{
			name:    Memory,
			catalog: CatalogPolicy{MapPublicToMain: true},
			ddl: DDLPolicy{
				ConstraintHandling: PreserveConstraints,
				UnsupportedDDL:     ExecuteUnsupportedDDL,
			},
			dml: DMLPolicy{ConflictHandling: UseInsertOnConflict},
		}
	}
}

func lakeProfile(name Name, physical string, mapPublicToMain bool) Profile {
	return Profile{
		name: name,
		catalog: CatalogPolicy{
			PhysicalName:    physical,
			MapPublicToMain: mapPublicToMain,
			QualifyMacros:   true,
		},
		ddl: DDLPolicy{
			ConstraintHandling:    StripConstraints,
			RewriteSerial:         true,
			StripVolatileDefaults: true,
			UnsupportedDDL:        NoOpUnsupportedDDL,
			RewriteCascadeDrop:    true,
			SplitMultiAlter:       true,
		},
		dml:      DMLPolicy{ConflictHandling: RewriteToMerge},
		metadata: MetadataPolicy{InterceptShowCreate: true},
	}
}

func WithPhysicalCatalog(profile Profile, physicalCatalog string) Profile {
	if physicalCatalog != "" {
		profile.catalog.PhysicalName = physicalCatalog
	}
	return profile
}
