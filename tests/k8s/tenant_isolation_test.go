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

	if err := retryDBOperationWithReconnectAs("analytics", "postgres", 30*time.Second, "create analytics table", func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, "CREATE OR REPLACE TABLE "+analyticsTable+" AS SELECT 7 AS value")
		return err
	}); err != nil {
		t.Fatalf("create analytics table: %v", err)
	}
	analyticsVisible, err := queryIntWithReconnectAs("analytics", "postgres", "SELECT COUNT(*) FROM "+analyticsTable, 30*time.Second)
	if err != nil {
		t.Fatalf("count analytics table rows: %v", err)
	}
	if analyticsVisible != 1 {
		t.Fatalf("expected analytics table to contain one row, got %d", analyticsVisible)
	}
	analyticsWorkerPod, err := findActiveOrgWorkerPodSince("analytics", analyticsSessionStart, 30*time.Second)
	if err != nil {
		t.Fatalf("find analytics worker pod from runtime state: %v", err)
	}

	if err := waitForWorkerRelease(analyticsWorkerPod, 30*time.Second); err != nil {
		t.Fatalf("wait for analytics worker release: %v", err)
	}

	var billingSeesAnalytics int
	var billingMissingErr error
	err = retryDBOperationWithReconnectAs("billing", "postgres", 30*time.Second, "billing reads analytics table", func(ctx context.Context, db *sql.DB) error {
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+analyticsTable).Scan(&billingSeesAnalytics)
		if err == nil {
			return nil
		}
		if isMissingTableError(err) {
			billingMissingErr = err
			return nil
		}
		return err
	})
	if err != nil {
		t.Fatalf("billing reads analytics table: %v", err)
	}
	if billingMissingErr == nil {
		t.Fatalf("expected billing not to read analytics table, got %d rows", billingSeesAnalytics)
		return
	}
	if err := retryDBOperationWithReconnectAs("billing", "postgres", 30*time.Second, "create billing table", func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, "CREATE OR REPLACE TABLE "+billingTable+" AS SELECT 11 AS value")
		return err
	}); err != nil {
		t.Fatalf("create billing table: %v", err)
	}

	var analyticsSeesBilling int
	var analyticsMissingErr error
	err = retryDBOperationWithReconnectAs("analytics", "postgres", 30*time.Second, "analytics reads billing table", func(ctx context.Context, db *sql.DB) error {
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+billingTable).Scan(&analyticsSeesBilling)
		if err == nil {
			return nil
		}
		if isMissingTableError(err) {
			analyticsMissingErr = err
			return nil
		}
		return err
	})
	if err != nil {
		t.Fatalf("analytics reads billing table: %v", err)
	}
	if analyticsMissingErr == nil {
		t.Fatalf("expected analytics not to read billing table, got %d rows", analyticsSeesBilling)
		return
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

	if err := retryDBOperationWithReconnectAs("analytics", "postgres", 45*time.Second, "create analytics table", func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, "CREATE OR REPLACE TABLE "+analyticsTable+" AS SELECT i AS value, repeat('x', 4096) AS payload FROM generate_series(1, 2048) AS t(i)")
		return err
	}); err != nil {
		t.Fatalf("create analytics table: %v", err)
	}

	if err := waitForMinioPrefixFileCountAtLeast(analyticsPrefix, analyticsBefore+1, 60*time.Second); err != nil {
		t.Fatalf("wait for analytics prefix growth: %v", err)
	}
	if err := waitForMinioPrefixFileCountToStayAtMost(billingPrefix, billingBefore, 8*time.Second); err != nil {
		t.Fatalf("billing prefix changed during analytics write: %v", err)
	}

	if err := retryDBOperationWithReconnectAs("billing", "postgres", 45*time.Second, "create billing table", func(ctx context.Context, db *sql.DB) error {
		_, err := db.ExecContext(ctx, "CREATE OR REPLACE TABLE "+billingTable+" AS SELECT i AS value, repeat('x', 4096) AS payload FROM generate_series(1, 2048) AS t(i)")
		return err
	}); err != nil {
		t.Fatalf("create billing table: %v", err)
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

func queryIntWithReconnectAs(username, password, query string, timeout time.Duration) (int, error) {
	var value int
	err := retryDBOperationWithReconnectAs(username, password, timeout, fmt.Sprintf("query %q", query), func(ctx context.Context, db *sql.DB) error {
		return db.QueryRowContext(ctx, query).Scan(&value)
	})
	return value, err
}

func isMissingTableError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "does not exist") || strings.Contains(msg, "not found")
}
