//go:build k8s_integration

package k8s_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestK8sTenantIsolation_DifferentTenantsSeeDistinctCatalogs(t *testing.T) {
	analyticsTable := fmt.Sprintf("analytics_isolation_%d", time.Now().UnixNano())
	billingTable := fmt.Sprintf("billing_isolation_%d", time.Now().UnixNano())
	analyticsSessionStart := time.Now().UTC()

	analyticsDB, err := openDBConnAs("analytics", "postgres")
	if err != nil {
		t.Fatalf("open analytics DB: %v", err)
	}
	if _, err := execDBWithTimeout(analyticsDB, "CREATE OR REPLACE TABLE "+analyticsTable+" AS SELECT 7 AS value"); err != nil {
		_ = analyticsDB.Close()
		t.Fatalf("create analytics table: %v", err)
	}
	analyticsVisible, err := queryIntWithTimeout(analyticsDB, "SELECT COUNT(*) FROM "+analyticsTable)
	if err != nil {
		_ = analyticsDB.Close()
		t.Fatalf("count analytics table rows: %v", err)
	}
	if analyticsVisible != 1 {
		_ = analyticsDB.Close()
		t.Fatalf("expected analytics table to contain one row, got %d", analyticsVisible)
	}
	analyticsWorkerPod, err := findActiveOrgWorkerPodSince("analytics", analyticsSessionStart, 30*time.Second)
	if err != nil {
		_ = analyticsDB.Close()
		t.Fatalf("find analytics worker pod from runtime state: %v", err)
	}
	if err := analyticsDB.Close(); err != nil {
		t.Fatalf("close analytics DB: %v", err)
	}

	if err := waitForWorkerRelease(analyticsWorkerPod, 30*time.Second); err != nil {
		t.Fatalf("wait for analytics worker release: %v", err)
	}

	billingDB, err := openDBConnAs("billing", "postgres")
	if err != nil {
		t.Fatalf("open billing DB: %v", err)
	}
	billingSeesAnalytics, err := queryIntWithTimeout(billingDB, "SELECT COUNT(*) FROM "+analyticsTable)
	if err == nil {
		_ = billingDB.Close()
		t.Fatalf("expected billing not to read analytics table, got %d rows", billingSeesAnalytics)
	}
	if !isMissingTableError(err) {
		_ = billingDB.Close()
		t.Fatalf("expected missing-table error when billing reads analytics table, got %v", err)
	}
	if _, err := execDBWithTimeout(billingDB, "CREATE OR REPLACE TABLE "+billingTable+" AS SELECT 11 AS value"); err != nil {
		_ = billingDB.Close()
		t.Fatalf("create billing table: %v", err)
	}
	if err := billingDB.Close(); err != nil {
		t.Fatalf("close billing DB: %v", err)
	}

	analyticsDB, err = openDBConnAs("analytics", "postgres")
	if err != nil {
		t.Fatalf("reopen analytics DB: %v", err)
	}
	analyticsSeesBilling, err := queryIntWithTimeout(analyticsDB, "SELECT COUNT(*) FROM "+billingTable)
	if err == nil {
		_ = analyticsDB.Close()
		t.Fatalf("expected analytics not to read billing table, got %d rows", analyticsSeesBilling)
	}
	if !isMissingTableError(err) {
		_ = analyticsDB.Close()
		t.Fatalf("expected missing-table error when analytics reads billing table, got %v", err)
	}
	if err := analyticsDB.Close(); err != nil {
		t.Fatalf("close analytics DB after verification: %v", err)
	}
}

func TestK8sTenantIsolation_WritesStayInOwnObjectStorePrefix(t *testing.T) {
	analyticsTable := fmt.Sprintf("analytics_prefix_%d", time.Now().UnixNano())
	billingTable := fmt.Sprintf("billing_prefix_%d", time.Now().UnixNano())

	analyticsPrefix := "orgs/analytics"
	billingPrefix := "orgs/billing"

	analyticsBefore, err := minioPrefixFileCount(analyticsPrefix)
	if err != nil {
		t.Fatalf("count analytics prefix before write: %v", err)
	}
	billingBefore, err := minioPrefixFileCount(billingPrefix)
	if err != nil {
		t.Fatalf("count billing prefix before write: %v", err)
	}

	analyticsDB, err := openDBConnAs("analytics", "postgres")
	if err != nil {
		t.Fatalf("open analytics DB: %v", err)
	}
	if _, err := execDBWithTimeout(analyticsDB, "CREATE OR REPLACE TABLE "+analyticsTable+" AS SELECT i AS value, repeat('x', 4096) AS payload FROM generate_series(1, 2048) AS t(i)"); err != nil {
		_ = analyticsDB.Close()
		t.Fatalf("create analytics table: %v", err)
	}
	if err := analyticsDB.Close(); err != nil {
		t.Fatalf("close analytics DB: %v", err)
	}

	if err := waitForMinioPrefixFileCountAtLeast(analyticsPrefix, analyticsBefore+1, 60*time.Second); err != nil {
		t.Fatalf("wait for analytics prefix growth: %v", err)
	}
	if err := waitForMinioPrefixFileCountToStayAtMost(billingPrefix, billingBefore, 8*time.Second); err != nil {
		t.Fatalf("billing prefix changed during analytics write: %v", err)
	}

	billingDB, err := openDBConnAs("billing", "postgres")
	if err != nil {
		t.Fatalf("open billing DB: %v", err)
	}
	if _, err := execDBWithTimeout(billingDB, "CREATE OR REPLACE TABLE "+billingTable+" AS SELECT i AS value, repeat('x', 4096) AS payload FROM generate_series(1, 2048) AS t(i)"); err != nil {
		_ = billingDB.Close()
		t.Fatalf("create billing table: %v", err)
	}
	if err := billingDB.Close(); err != nil {
		t.Fatalf("close billing DB: %v", err)
	}

	if err := waitForMinioPrefixFileCountAtLeast(billingPrefix, billingBefore+1, 60*time.Second); err != nil {
		t.Fatalf("wait for billing prefix growth: %v", err)
	}
}

func TestK8sWorkerPodsDoNotMountServiceAccountTokens(t *testing.T) {
	if err := retryQueryWithReconnect("SELECT 1", 30*time.Second); err != nil {
		t.Fatalf("query failed: %v", err)
	}

	pod := latestWorkerPod(t)
	if err := ensureWorkerPodLacksServiceAccountToken(pod.Name); err != nil {
		t.Fatalf("worker pod %s has ambient service account token: %v", pod.Name, err)
	}
}

func execDBWithTimeout(db *sql.DB, query string, args ...any) (sql.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dbAttemptTimeout)
	defer cancel()
	return db.ExecContext(ctx, query, args...)
}

func queryIntWithTimeout(db *sql.DB, query string, args ...any) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dbAttemptTimeout)
	defer cancel()

	var value int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func isMissingTableError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") || strings.Contains(msg, "not found")
}
