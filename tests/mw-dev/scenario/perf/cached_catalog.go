package perf

import (
	"context"
	"database/sql"
	"encoding/json"
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

var benchmarkCatalogName = regexp.MustCompile(`^org_[a-z0-9_]+$`)

func cachedCatalogStatement(catalog string, baseline map[string]string) (string, error) {
	if !benchmarkCatalogName.MatchString(catalog) || baseline["connector.name"] == "" || baseline["fs.cache.enabled"] != "false" {
		return "", errors.New("cached benchmark requires a managed baseline catalog with caching explicitly disabled")
	}
	quoteIdentifier := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
	keys := make([]string, 0, len(baseline))
	for key := range baseline {
		if key != "connector.name" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	properties := make([]string, 0, len(keys))
	for _, key := range keys {
		value := baseline[key]
		if key == "fs.cache.enabled" {
			value = "true"
		}
		properties = append(properties, quoteIdentifier(key)+" = '"+strings.ReplaceAll(value, "'", "''")+"'")
	}
	return "CREATE CATALOG " + quoteIdentifier(catalog) + " USING " + quoteIdentifier(baseline["connector.name"]) + " WITH (" + strings.Join(properties, ", ") + ")", nil
}

func checkCachedCatalogProperties(baseline, cached map[string]string) error {
	expected := maps.Clone(baseline)
	expected["fs.cache.enabled"] = "true"
	if !maps.Equal(expected, cached) {
		return errors.New("cached Trino catalog differs from the baseline dataset or cache configuration; recreate the benchmark namespace")
	}
	return nil
}

// The store belongs to the disposable benchmark namespace. Always scope reads
// by cell and catalog: both deployments deliberately use the same tenant name.
func readBenchmarkCatalog(ctx context.Context, db catalogCacheQuery, cell, catalog string) (map[string]string, error) {
	var connector string
	var raw []byte
	err := db.QueryRow(ctx, `SELECT connector_name, properties FROM trino_catalogs WHERE cell_id = $1 AND catalog_name = $2`, cell, catalog).Scan(&connector, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.New("read isolated benchmark catalog properties")
	}
	var props map[string]string
	if json.Unmarshal(raw, &props) != nil || props == nil || connector == "" {
		return nil, errors.New("invalid isolated benchmark catalog properties")
	}
	props["connector.name"] = connector
	return props, nil
}

func (f defaultDriverFactory) prepareCachedCatalog(ctx context.Context, baseline trinodriver.ConnectionConfig) error {
	if f.trinoCachedURL == "" || f.trinoCachedCellID == "" || f.trinoAdminPasswordFile == "" {
		return errors.New("trino_cached requires DUCKGRES_SCENARIO_TRINO_CACHED_URL, DUCKGRES_SCENARIO_TRINO_CACHED_CELL_ID and DUCKGRES_SCENARIO_TRINO_ADMIN_PASSWORD_FILE")
	}
	if baseline.ServerURL == f.trinoCachedURL || baseline.CatalogStoreCellID == f.trinoCachedCellID {
		return errors.New("cached Trino must use a separate coordinator and catalog store cell")
	}
	timeout := baseline.Startup.Timeout
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
	return f.initializeCachedCatalog(ctx, conn, baseline)
}

func (f defaultDriverFactory) initializeCachedCatalog(ctx context.Context, conn catalogCacheQuery, baseline trinodriver.ConnectionConfig) error {
	props, err := readBenchmarkCatalog(ctx, conn, baseline.CatalogStoreCellID, baseline.Catalog)
	if err != nil {
		return err
	}
	statement, err := cachedCatalogStatement(baseline.Catalog, props)
	if err != nil {
		return err
	}
	password, err := os.ReadFile(f.trinoAdminPasswordFile)
	if err != nil || len(strings.TrimSpace(string(password))) == 0 {
		return errors.New("read benchmark Trino admin password file")
	}
	admin := baseline
	admin.ServerURL = f.trinoCachedURL
	admin.Username = "__admin_provisioner"
	admin.Password = strings.TrimSpace(string(password))
	admin.Catalog = "system"
	admin.Schema = "runtime"
	admin.Source = "duckgres-perf-catalog-setup"
	dsn, err := admin.DSN()
	if err != nil {
		return errors.New("configure verified cached Trino admin connection")
	}
	adminDB, err := sql.Open("trino", dsn)
	if err != nil {
		return errors.New("open cached Trino admin connection")
	}
	defer func() { _ = adminDB.Close() }()
	tenant := baseline
	tenant.ServerURL = f.trinoCachedURL
	tenantDSN, err := tenant.DSN()
	if err != nil {
		return errors.New("configure cached Trino tenant connection")
	}
	tenantDB, err := sql.Open("trino", tenantDSN)
	if err != nil {
		return errors.New("open cached Trino tenant connection")
	}
	defer func() { _ = tenantDB.Close() }()
	interval := baseline.Startup.PollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	createFailure, readinessFailure := "none", "none"
	createAttempts, readinessAttempts := 0, 0
	timeoutError := func() error {
		return fmt.Errorf("cached Trino catalog did not become ready within startup timeout: create_attempts=%d last_create_failure=[%s]; readiness_attempts=%d last_readiness_failure=[%s]", createAttempts, createFailure, readinessAttempts, readinessFailure)
	}
	for {
		cached, readErr := readBenchmarkCatalog(ctx, conn, f.trinoCachedCellID, baseline.Catalog)
		if readErr != nil {
			if ctx.Err() != nil {
				return timeoutError()
			}
			return readErr
		}
		if cached == nil {
			// Secret projections and OPA can become ready after coordinator health.
			// A retry first checks persistence, so an uncertain CREATE is not repeated
			// after its catalog has actually been committed.
			createAttempts++
			_, createErr := adminDB.ExecContext(ctx, statement)
			retainTrinoFailure("catalog creation", createErr, &createFailure)
		} else {
			if err := checkCachedCatalogProperties(props, cached); err != nil {
				return err
			}
			// SELECT 1 alone does not resolve the catalog. Check tenant access to its
			// real metadata before timed queries use the independently started cluster.
			readinessAttempts++
			rows, queryErr := tenantDB.QueryContext(ctx, `SELECT schema_name FROM information_schema.schemata LIMIT 1`)
			if queryErr == nil {
				for rows.Next() {
					var name string
					if err := rows.Scan(&name); err != nil {
						queryErr = err
						break
					}
				}
				if err := rows.Err(); err != nil {
					queryErr = err
				}
				_ = rows.Close()
				if queryErr == nil {
					return nil
				}
			}
			retainTrinoFailure("tenant metadata readiness", queryErr, &readinessFailure)
		}
		select {
		case <-ctx.Done():
			return timeoutError()
		case <-time.After(interval):
		}
	}
}
