package hogqlcatalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	postgresSnapshotTable             = "duckgres_hogql_semantic_catalog_snapshots"
	postgresPhysicalRefreshLeaseTable = "duckgres_hogql_physical_catalog_refresh_leases"
)

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

func (s *PostgresStore) AcquirePhysicalRefresh(ctx context.Context, catalog PhysicalIdentifier, ttl time.Duration, force bool) (*PhysicalRefreshLease, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if ttl <= 0 {
		return nil, false, fmt.Errorf("physical refresh lease TTL must be positive")
	}
	normalized, err := normalizedCatalog(catalog)
	if err != nil {
		return nil, false, err
	}
	token := uuid.NewString()
	ttlMilliseconds := max(ttl.Milliseconds(), 1)
	var epoch int64
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := tx.Raw(
			`INSERT INTO `+postgresPhysicalRefreshLeaseTable+` (catalog_value, catalog_delimited, epoch, lease_token, lease_expires_at, next_refresh_at)
			 VALUES (?, ?, 1, ?, CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond'), '-infinity')
			 ON CONFLICT (catalog_value, catalog_delimited) DO UPDATE
			 SET epoch = `+postgresPhysicalRefreshLeaseTable+`.epoch + 1,
			     lease_token = EXCLUDED.lease_token,
			     lease_expires_at = EXCLUDED.lease_expires_at
			 WHERE (`+postgresPhysicalRefreshLeaseTable+`.lease_token IS NULL
			    OR `+postgresPhysicalRefreshLeaseTable+`.lease_expires_at <= CURRENT_TIMESTAMP)
			   AND (? OR `+postgresPhysicalRefreshLeaseTable+`.next_refresh_at <= CURRENT_TIMESTAMP)
			 RETURNING epoch`,
			normalized.Value,
			normalized.Delimited,
			token,
			ttlMilliseconds,
			force,
		).Row()
		if err := row.Scan(&epoch); err != nil {
			return err
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("acquire HogQL physical catalog refresh lease: %w", err)
	}
	return &PhysicalRefreshLease{catalog: normalized, epoch: epoch, token: token}, true, nil
}

func (s *PostgresStore) PublishPhysicalRefresh(ctx context.Context, lease *PhysicalRefreshLease, metadata *PhysicalCatalogMetadata, languageVersion string, refreshAfter time.Duration) (*HogQLSemanticCatalogSnapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if lease == nil {
		return nil, false, ErrPhysicalRefreshLeaseLost
	}
	if refreshAfter <= 0 {
		return nil, false, fmt.Errorf("physical refresh interval must be positive")
	}
	refreshMilliseconds := max(refreshAfter.Milliseconds(), 1)

	var published *HogQLSemanticCatalogSnapshot
	var changed bool
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockPostgresCatalog(tx, lease.catalog); err != nil {
			return err
		}
		var active int
		if err := tx.Raw(
			`SELECT 1 FROM `+postgresPhysicalRefreshLeaseTable+`
			 WHERE catalog_value = ? AND catalog_delimited = ? AND epoch = ? AND lease_token = ?
			   AND lease_expires_at > CURRENT_TIMESTAMP
			 FOR UPDATE`,
			lease.catalog.Value,
			lease.catalog.Delimited,
			lease.epoch,
			lease.token,
		).Row().Scan(&active); errors.Is(err, sql.ErrNoRows) {
			return ErrPhysicalRefreshLeaseLost
		} else if err != nil {
			return fmt.Errorf("verify HogQL physical catalog refresh lease: %w", err)
		}

		latestRow, err := latestPostgresRow(tx, lease.catalog)
		var latest *HogQLSemanticCatalogSnapshot
		generation := int64(1)
		if err == nil {
			latest, err = decodePostgresRow(latestRow)
			if err != nil {
				return err
			}
			generation = latest.Generation + 1
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		merged, err := mergePhysicalCatalog(ctx, metadata, latest, lease.catalog, languageVersion, generation)
		if err != nil {
			return err
		}
		if latest != nil && physicalInventoriesEqual(latest, merged) {
			published = latest
		} else {
			if err := insertPostgresSnapshot(tx, merged); err != nil {
				return err
			}
			published = merged
			changed = true
		}
		result := tx.Exec(
			`UPDATE `+postgresPhysicalRefreshLeaseTable+`
			 SET lease_token = NULL,
			     lease_expires_at = NULL,
			     next_refresh_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond'),
			     last_success_at = CURRENT_TIMESTAMP
			 WHERE catalog_value = ? AND catalog_delimited = ? AND epoch = ? AND lease_token = ?`,
			refreshMilliseconds,
			lease.catalog.Value,
			lease.catalog.Delimited,
			lease.epoch,
			lease.token,
		)
		if result.Error != nil {
			return fmt.Errorf("release HogQL physical catalog refresh lease: %w", result.Error)
		}
		if result.RowsAffected != 1 {
			return ErrPhysicalRefreshLeaseLost
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return cloneSnapshot(published), changed, nil
}

func (s *PostgresStore) ReleasePhysicalRefresh(ctx context.Context, lease *PhysicalRefreshLease, retryAfter time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if lease == nil {
		return ErrPhysicalRefreshLeaseLost
	}
	if retryAfter < 0 {
		return fmt.Errorf("physical refresh retry interval cannot be negative")
	}
	retryMilliseconds := max(retryAfter.Milliseconds(), 0)
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Exec(
			`UPDATE `+postgresPhysicalRefreshLeaseTable+`
			 SET lease_token = NULL,
			     lease_expires_at = NULL,
			     next_refresh_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')
			 WHERE catalog_value = ? AND catalog_delimited = ? AND epoch = ? AND lease_token = ?`,
			retryMilliseconds,
			lease.catalog.Value,
			lease.catalog.Delimited,
			lease.epoch,
			lease.token,
		)
		if result.Error != nil {
			return fmt.Errorf("release HogQL physical catalog refresh lease: %w", result.Error)
		}
		if result.RowsAffected == 1 {
			return nil
		}
		var epoch int64
		var token *string
		err := tx.Table(postgresPhysicalRefreshLeaseTable).
			Select("epoch", "lease_token").
			Where("catalog_value = ? AND catalog_delimited = ?", lease.catalog.Value, lease.catalog.Delimited).
			Row().Scan(&epoch, &token)
		if err == nil && epoch == lease.epoch && token == nil {
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read HogQL physical catalog refresh lease: %w", err)
		}
		return ErrPhysicalRefreshLeaseLost
	})
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
		if err := lockPostgresCatalog(tx, normalized.Catalog); err != nil {
			return err
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

		return insertPostgresSnapshotManifest(tx, normalized, manifest)
	})
}

func lockPostgresCatalog(tx *gorm.DB, catalog PhysicalIdentifier) error {
	lockName := fmt.Sprintf("hogql-semantic-catalog:%t:%s", catalog.Delimited, catalog.Value)
	if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", lockName).Error; err != nil {
		return fmt.Errorf("lock HogQL semantic catalog: %w", err)
	}
	return nil
}

func insertPostgresSnapshot(tx *gorm.DB, snapshot *HogQLSemanticCatalogSnapshot) error {
	manifest, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode HogQL semantic catalog snapshot: %w", err)
	}
	return insertPostgresSnapshotManifest(tx, snapshot, manifest)
}

func insertPostgresSnapshotManifest(tx *gorm.DB, snapshot *HogQLSemanticCatalogSnapshot, manifest []byte) error {
	row := postgresSnapshotRow{
		CatalogValue:     snapshot.Catalog.Value,
		CatalogDelimited: snapshot.Catalog.Delimited,
		Generation:       snapshot.Generation,
		ProtocolVersion:  snapshot.ProtocolVersion,
		SchemaVersion:    snapshot.SchemaVersion,
		LanguageVersion:  snapshot.LanguageVersion,
		Manifest:         manifest,
	}
	if err := tx.Table(postgresSnapshotTable).Create(&row).Error; err != nil {
		return fmt.Errorf("publish HogQL semantic catalog snapshot: %w", err)
	}
	return nil
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
