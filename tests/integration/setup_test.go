package integration

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"
)

var (
	// Global harness for all tests
	testHarness *TestHarness
	// Skip PostgreSQL comparison tests
	skipPostgresCompare bool
)

// TestMain sets up and tears down the test environment
func TestMain(m *testing.M) {
	cfg := DefaultConfig()

	// Check if PostgreSQL (for comparison) is running
	pgPort := cfg.PostgresPort
	if !IsPostgresRunning(pgPort) {
		fmt.Println("PostgreSQL container not running. Starting it...")
		if err := StartPostgresContainer(); err != nil {
			fmt.Printf("Failed to start PostgreSQL: %v\n", err)
			fmt.Println("Running tests without PostgreSQL comparison (Duckgres-only mode)")
			skipPostgresCompare = true
		}
	}

	// Check and wait for DuckLake infrastructure if DuckLake mode is enabled
	if cfg.UseDuckLake {
		if !IsDuckLakeInfraRunning(cfg.DuckLakeMetadataPort, cfg.MinIOPort) {
			fmt.Println("DuckLake infrastructure not running. Waiting for it...")
			if err := WaitForDuckLakeInfra(cfg.DuckLakeMetadataPort, cfg.MinIOPort, 30*time.Second); err != nil {
				fmt.Printf("DuckLake infrastructure not available: %v\n", err)
				fmt.Println("Falling back to vanilla DuckDB mode (set DUCKGRES_TEST_NO_DUCKLAKE=1 to suppress this)")
				cfg.UseDuckLake = false
			} else {
				fmt.Println("DuckLake infrastructure is ready")
			}
		}
	}

	cfg.SkipPostgres = skipPostgresCompare

	var err error
	testHarness, err = NewTestHarness(cfg)
	if err != nil {
		fmt.Printf("Failed to create test harness: %v\n", err)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	if testHarness != nil {
		testHarness.Close()
	}

	os.Exit(code)
}

// runQueryTest runs a single query test
func runQueryTest(t *testing.T, test QueryTest) {
	t.Helper()

	if test.Skip != "" {
		t.Skipf("Skipping: %s", test.Skip)
		return
	}

	if test.DuckgresOnly || skipPostgresCompare {
		// Only test against Duckgres
		result, err := ExecuteQuery(testHarness.DuckgresDB, test.Query)
		if err != nil {
			t.Fatalf("Failed to execute query: %v", err)
		}
		if test.ExpectError {
			if result.Error == nil {
				t.Errorf("Expected error but query succeeded")
			}
		} else {
			if result.Error != nil {
				t.Errorf("Query failed: %v", result.Error)
			}
		}
		return
	}

	// Compare PostgreSQL and Duckgres
	opts := test.Options
	if opts.FloatTolerance == 0 {
		opts = DefaultCompareOptions()
	}

	result := CompareQueries(testHarness.PostgresDB, testHarness.DuckgresDB, test.Query, opts)

	if test.ExpectError {
		if result.PGError == nil && result.DGError == nil {
			t.Errorf("Expected error but both queries succeeded")
		}
		return
	}

	if !result.Match {
		t.Errorf("Query results do not match:\n  PostgreSQL rows: %d\n  Duckgres rows: %d\n  Differences:\n    %s",
			result.PGRowCount, result.DGRowCount, formatDifferences(result.Differences))
	}
}

// runQueryTests runs multiple query tests
func runQueryTests(t *testing.T, tests []QueryTest) {
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			runQueryTest(t, test)
		})
	}
}

// formatDifferences formats a list of differences for display
func formatDifferences(diffs []string) string {
	if len(diffs) == 0 {
		return "(none)"
	}
	result := ""
	for i, diff := range diffs {
		if i > 0 {
			result += "\n    "
		}
		result += diff
		if i >= 5 {
			result += fmt.Sprintf("\n    ... and %d more differences", len(diffs)-5)
			break
		}
	}
	return result
}

// mustExec executes a statement and fails the test on error
func mustExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("Failed to execute %q: %v", truncateQuery(query), err)
	}
}

// mustExecBoth executes a statement on both databases
func mustExecBoth(t *testing.T, query string) {
	t.Helper()
	if testHarness.PostgresDB != nil && !skipPostgresCompare {
		mustExec(t, testHarness.PostgresDB, query)
	}
	mustExec(t, testHarness.DuckgresDB, query)
}

// truncateQuery truncates a query for display
func truncateQuery(q string) string {
	if len(q) > 80 {
		return q[:80] + "..."
	}
	return q
}

// pgOnly returns the PostgreSQL database, or skips if not available
func pgOnly(t *testing.T) *sql.DB {
	t.Helper()
	if testHarness.PostgresDB == nil || skipPostgresCompare {
		t.Skip("PostgreSQL not available")
	}
	return testHarness.PostgresDB
}

// dgOnly returns the Duckgres database
func dgOnly(t *testing.T) *sql.DB {
	t.Helper()
	return testHarness.DuckgresDB
}
