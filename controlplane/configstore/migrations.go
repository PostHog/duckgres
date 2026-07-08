package configstore

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"

	"github.com/pressly/goose/v3"
	gooselock "github.com/pressly/goose/v3/lock"
	"gorm.io/gorm"
)

//go:embed migrations/*.sql
var configStoreMigrationFS embed.FS

func runConfigStoreMigrations(db *gorm.DB) error {
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("configstore migrations sql db: %w", err)
	}

	migrationFS, err := fs.Sub(configStoreMigrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("configstore migrations fs: %w", err)
	}

	locker, err := gooselock.NewPostgresSessionLocker(
		gooselock.WithLockID(advisoryLockKey("duckgres:configstore:migrations")),
	)
	if err != nil {
		return fmt.Errorf("configstore migration locker: %w", err)
	}
	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		sqlDB,
		migrationFS,
		goose.WithSessionLocker(locker),
		goose.WithSlog(slog.Default()),
	)
	if err != nil {
		return fmt.Errorf("configstore migration provider: %w", err)
	}

	results, err := provider.Up(context.Background())
	if err != nil {
		return fmt.Errorf("apply configstore migrations: %w", err)
	}
	for _, result := range results {
		slog.Info("Applied config store migration.",
			"version", result.Source.Version,
			"path", result.Source.Path,
			"duration", result.Duration,
		)
	}
	return nil
}
