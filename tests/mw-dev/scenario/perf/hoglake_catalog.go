package perf

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	trinodriver "github.com/posthog/duckgres/tests/perf/drivers/trino"
)

// selectHoglakeCatalog switches only the backend dataset of the disposable
// tenant catalog. Its name, principal, credentials, cache mode and OPA grant
// remain identical. Callers select the original dataset explicitly on retries.
func (f defaultDriverFactory) selectHoglakeCatalog(ctx context.Context, connection trinodriver.ConnectionConfig) error {
	if f.trinoCatalogStoreDSN == "" || connection.CatalogStoreCellID == "" {
		return errors.New("dataset selection requires an explicit isolated benchmark catalog store and cell")
	}
	if !benchmarkCatalogName.MatchString(connection.Catalog) || !regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`).MatchString(connection.HoglakeCatalog) {
		return errors.New("invalid disposable benchmark catalog selection")
	}
	timeout := connection.Startup.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, f.trinoCatalogStoreDSN)
	if err != nil {
		return errors.New("connect to isolated benchmark catalog store")
	}
	defer func() { _ = conn.Close(context.Background()) }()
	original, err := readBenchmarkCatalog(ctx, conn, connection.CatalogStoreCellID, connection.Catalog)
	if err != nil {
		return err
	}
	if original["connector.name"] != "hoglake" || original["hoglake.catalog"] == "" || original["hoglake.uri"] == "" || original["fs.cache.enabled"] != "false" {
		return errors.New("dataset selection requires an existing uncached Hoglake benchmark catalog")
	}
	desired := maps.Clone(original)
	desired["hoglake.catalog"] = connection.HoglakeCatalog
	if maps.Equal(original, desired) {
		return waitHoglakeSchema(ctx, connection)
	}
	if f.trinoAdminPasswordFile == "" {
		return errors.New("dataset selection requires the existing benchmark Trino admin password file")
	}
	password, err := os.ReadFile(f.trinoAdminPasswordFile)
	if err != nil || strings.TrimSpace(string(password)) == "" {
		return errors.New("read benchmark Trino admin password file")
	}
	admin := connection
	admin.Username = "__admin_provisioner"
	admin.Password = strings.TrimSpace(string(password))
	admin.Catalog = "system"
	admin.Schema = "runtime"
	admin.Source = "duckgres-perf-dataset-selection"
	dsn, err := admin.DSN()
	if err != nil {
		return errors.New("configure benchmark Trino admin connection")
	}
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return errors.New("open benchmark Trino admin connection")
	}
	defer func() { _ = db.Close() }()
	// Serialize with scenario steps: callers must finish the original perf run
	// before changing this catalog. Never mutate its persistent row directly.
	quote := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
	keys := make([]string, 0, len(desired))
	for k := range desired {
		if k != "connector.name" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	properties := make([]string, 0, len(keys))
	for _, k := range keys {
		properties = append(properties, quote(k)+" = '"+strings.ReplaceAll(desired[k], "'", "''")+"'")
	}
	create := "CREATE CATALOG " + quote(connection.Catalog) + " USING " + quote(desired["connector.name"]) + " WITH (" + strings.Join(properties, ", ") + ")"
	interval := connection.Startup.PollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	for {
		current, readErr := readBenchmarkCatalog(ctx, conn, connection.CatalogStoreCellID, connection.Catalog)
		if readErr != nil {
			return readErr
		}
		if maps.Equal(current, desired) {
			return waitHoglakeSchema(ctx, connection)
		}
		if current != nil {
			if !maps.Equal(current, original) {
				return errors.New("benchmark catalog changed unexpectedly during dataset selection")
			}

			// A transport error can follow a committed DROP. Inspect persisted
			// state on the next pass before deciding whether DROP or CREATE is needed.
			_, _ = db.ExecContext(ctx, "DROP CATALOG "+quote(connection.Catalog))
		} else {
			_, _ = db.ExecContext(ctx, create)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("benchmark dataset selection did not complete within startup timeout; recreate the benchmark namespace")
		case <-time.After(interval):
		}
	}
}

// Metadata access establishes that the connector loaded the selected dataset;
// SELECT 1 only establishes that the coordinator accepts SQL.
func waitHoglakeSchema(ctx context.Context, connection trinodriver.ConnectionConfig) error {
	dsn, err := connection.DSN()
	if err != nil {
		return errors.New("configure benchmark tenant metadata connection")
	}
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return errors.New("open benchmark tenant metadata connection")
	}
	defer func() { _ = db.Close() }()
	interval := connection.Startup.PollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	query := "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '" + strings.ReplaceAll(connection.Schema, "'", "''") + "'"
	for {
		var schema string
		if err := db.QueryRowContext(ctx, query).Scan(&schema); err == nil && schema == connection.Schema {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("selected Hoglake schema did not become readable within startup timeout")
		case <-time.After(interval):
		}
	}
}
