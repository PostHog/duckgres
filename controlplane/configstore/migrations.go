package configstore

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

//go:embed migrations/*.sql
var configStoreMigrationFS embed.FS

type configStoreMigration struct {
	Name     string
	SQL      string
	Checksum string
}

func runConfigStoreMigrations(db *gorm.DB) error {
	migrations, err := loadConfigStoreMigrations()
	if err != nil {
		return err
	}

	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", advisoryLockKey("duckgres:configstore:migrations")).Error; err != nil {
			return fmt.Errorf("acquire configstore migration lock: %w", err)
		}
		if err := ensureConfigStoreMigrationTable(tx); err != nil {
			return err
		}
		for _, migration := range migrations {
			applied, err := readAppliedConfigStoreMigration(tx, migration.Name)
			if err != nil {
				return err
			}
			if applied != nil {
				if applied.Checksum == "" {
					if err := tx.Exec(
						`UPDATE duckgres_schema_migrations SET checksum = ? WHERE name = ?`,
						migration.Checksum, migration.Name,
					).Error; err != nil {
						return fmt.Errorf("backfill checksum for configstore migration %s: %w", migration.Name, err)
					}
					continue
				}
				if applied.Checksum != migration.Checksum {
					return fmt.Errorf("configstore migration %s checksum mismatch: database has %s, binary has %s",
						migration.Name, applied.Checksum, migration.Checksum)
				}
				continue
			}

			if err := tx.Exec(migration.SQL).Error; err != nil {
				return fmt.Errorf("apply configstore migration %s: %w", migration.Name, err)
			}
			if err := tx.Create(&SchemaMigration{
				Name:      migration.Name,
				Checksum:  migration.Checksum,
				AppliedAt: time.Now().UTC(),
			}).Error; err != nil {
				return fmt.Errorf("record configstore migration %s: %w", migration.Name, err)
			}
		}
		return nil
	})
}

func loadConfigStoreMigrations() ([]configStoreMigration, error) {
	entries, err := fs.ReadDir(configStoreMigrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read configstore migrations: %w", err)
	}

	migrations := make([]configStoreMigration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		path := "migrations/" + entry.Name()
		contents, err := configStoreMigrationFS.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read configstore migration %s: %w", entry.Name(), err)
		}
		sum := sha256.Sum256(contents)
		migrations = append(migrations, configStoreMigration{
			Name:     strings.TrimSuffix(entry.Name(), ".sql"),
			SQL:      string(contents),
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Name < migrations[j].Name
	})
	return migrations, nil
}

func ensureConfigStoreMigrationTable(tx *gorm.DB) error {
	if err := tx.Exec(`
		CREATE TABLE IF NOT EXISTS duckgres_schema_migrations (
			name VARCHAR(128) PRIMARY KEY,
			checksum TEXT NOT NULL DEFAULT '',
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`).Error; err != nil {
		return fmt.Errorf("ensure configstore migration table: %w", err)
	}
	if err := tx.Exec(`
		ALTER TABLE duckgres_schema_migrations
			ADD COLUMN IF NOT EXISTS checksum TEXT NOT NULL DEFAULT ''
	`).Error; err != nil {
		return fmt.Errorf("ensure configstore migration checksum column: %w", err)
	}
	if err := tx.Exec(`
		ALTER TABLE duckgres_schema_migrations
			ADD COLUMN IF NOT EXISTS applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	`).Error; err != nil {
		return fmt.Errorf("ensure configstore migration applied_at column: %w", err)
	}
	return nil
}

func readAppliedConfigStoreMigration(tx *gorm.DB, name string) (*SchemaMigration, error) {
	var migration SchemaMigration
	err := tx.First(&migration, "name = ?", name).Error
	if err == nil {
		return &migration, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return nil, fmt.Errorf("read configstore migration %s: %w", name, err)
}
