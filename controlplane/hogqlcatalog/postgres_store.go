package hogqlcatalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"gorm.io/gorm"
)

const postgresSnapshotTable = "duckgres_hogql_semantic_catalog_snapshots"

type PostgresStore struct {
	db *gorm.DB
}

type postgresSnapshotRow struct {
	CatalogValue     string
	CatalogDelimited bool
	Generation       int64
	ProtocolVersion  int
	SchemaVersion    int
	LanguageVersion  string
	Manifest         []byte
}

func NewPostgresStore(db *gorm.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) Publish(ctx context.Context, snapshot *HogQLSemanticCatalogSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	normalized, err := normalizeAndValidateSnapshot(snapshot)
	if err != nil {
		return err
	}
	manifest, err := json.Marshal(normalized)
	if err != nil {
		return fmt.Errorf("encode HogQL semantic catalog snapshot: %w", err)
	}

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lockName := fmt.Sprintf("hogql-semantic-catalog:%t:%s", normalized.Catalog.Delimited, normalized.Catalog.Value)
		if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", lockName).Error; err != nil {
			return fmt.Errorf("lock HogQL semantic catalog: %w", err)
		}

		latest, err := latestPostgresRow(tx, normalized.Catalog)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil {
			switch {
			case normalized.Generation < latest.Generation:
				return fmt.Errorf("%w: latest=%d received=%d", ErrGenerationRegression, latest.Generation, normalized.Generation)
			case normalized.Generation == latest.Generation:
				published, decodeErr := decodePostgresRow(latest)
				if decodeErr != nil {
					return decodeErr
				}
				if reflect.DeepEqual(published, normalized) {
					return nil
				}
				return fmt.Errorf("%w: generation=%d", ErrGenerationConflict, normalized.Generation)
			}
		}

		row := postgresSnapshotRow{
			CatalogValue:     normalized.Catalog.Value,
			CatalogDelimited: normalized.Catalog.Delimited,
			Generation:       normalized.Generation,
			ProtocolVersion:  normalized.ProtocolVersion,
			SchemaVersion:    normalized.SchemaVersion,
			LanguageVersion:  normalized.LanguageVersion,
			Manifest:         manifest,
		}
		if err := tx.Table(postgresSnapshotTable).Create(&row).Error; err != nil {
			return fmt.Errorf("publish HogQL semantic catalog snapshot: %w", err)
		}
		return nil
	})
}

func (s *PostgresStore) Latest(ctx context.Context, catalog PhysicalIdentifier) (*HogQLSemanticCatalogSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	normalized, err := normalizedCatalog(catalog)
	if err != nil {
		return nil, err
	}
	row, err := latestPostgresRow(s.db.WithContext(ctx), normalized)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrCatalogNotFound
	}
	if err != nil {
		return nil, err
	}
	return decodePostgresRow(row)
}

func (s *PostgresStore) Generation(ctx context.Context, catalog PhysicalIdentifier, generation int64) (*HogQLSemanticCatalogSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if generation <= 0 {
		return nil, ErrGenerationNotFound
	}
	normalized, err := normalizedCatalog(catalog)
	if err != nil {
		return nil, err
	}
	db := s.db.WithContext(ctx)
	var row postgresSnapshotRow
	err = db.Table(postgresSnapshotTable).
		Where("catalog_value = ? AND catalog_delimited = ? AND generation = ?", normalized.Value, normalized.Delimited, generation).
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		if _, latestErr := latestPostgresRow(db, normalized); errors.Is(latestErr, gorm.ErrRecordNotFound) {
			return nil, ErrCatalogNotFound
		} else if latestErr != nil {
			return nil, latestErr
		}
		return nil, ErrGenerationNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read HogQL semantic catalog generation: %w", err)
	}
	return decodePostgresRow(&row)
}

func latestPostgresRow(db *gorm.DB, catalog PhysicalIdentifier) (*postgresSnapshotRow, error) {
	var row postgresSnapshotRow
	err := db.Table(postgresSnapshotTable).
		Where("catalog_value = ? AND catalog_delimited = ?", catalog.Value, catalog.Delimited).
		Order("generation DESC").
		Take(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("read latest HogQL semantic catalog snapshot: %w", err)
	}
	return &row, nil
}

func decodePostgresRow(row *postgresSnapshotRow) (*HogQLSemanticCatalogSnapshot, error) {
	snapshot, err := DecodeSnapshot(bytes.NewReader(row.Manifest))
	if err != nil {
		return nil, fmt.Errorf("decode persisted HogQL semantic catalog snapshot: %w", err)
	}
	if snapshot.Catalog.Value != row.CatalogValue ||
		snapshot.Catalog.Delimited != row.CatalogDelimited ||
		snapshot.Generation != row.Generation ||
		snapshot.ProtocolVersion != row.ProtocolVersion ||
		snapshot.SchemaVersion != row.SchemaVersion ||
		snapshot.LanguageVersion != row.LanguageVersion {
		return nil, fmt.Errorf("%w: persisted snapshot columns do not match manifest", ErrInvalidSnapshot)
	}
	return snapshot, nil
}
