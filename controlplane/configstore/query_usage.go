package configstore

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// QueryUsageEvent is the normalized subset of a Trino completion event retained
// for billing. Source is diagnostic, never an authorization or pricing input.
type QueryUsageEvent struct {
	ClusterID           string
	QueryID             string
	Principal           string
	Source              string
	State               string
	ErrorCode           string
	CompletedAt         time.Time
	PhysicalInputBytes  int64
	ProcessedInputBytes int64
	StatisticsComplete  bool
	TrinoVersion        string
}

// RememberTrinoUsagePrincipals retains tenant identities after disablement or
// deletion. Reusing a historical identity for another org is rejected rather
// than silently reattributing late events. Team IDs remain informational.
func (cs *ConfigStore) RememberTrinoUsagePrincipals(orgs []TrinoEnabledOrg) error {
	return cs.db.Transaction(func(tx *gorm.DB) error {
		for _, org := range orgs {
			if org.OrgID == "" || org.TrinoPrincipal() == "" {
				return fmt.Errorf("empty Trino usage principal or organization")
			}
			result := tx.Exec(`INSERT INTO duckgres_trino_usage_principals (principal, org_id, team_id)
VALUES (?, ?, COALESCE((SELECT team_id FROM duckgres_org_teams WHERE org_id = ? ORDER BY created_at, team_id LIMIT 1), 0))
ON CONFLICT (principal) DO UPDATE SET team_id = EXCLUDED.team_id
WHERE duckgres_trino_usage_principals.org_id = EXCLUDED.org_id`, org.TrinoPrincipal(), org.OrgID, org.OrgID)
			if result.Error != nil {
				return fmt.Errorf("remember Trino usage principal: %w", result.Error)
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("trino usage principal is already assigned to another organization")
			}
		}
		return nil
	})
}

// RecordTrinoQueryUsage commits before acknowledging delivery. All outcomes and
// incomplete statistics are billable at the reported byte count. Unknown tenant
// identities are retained and resolved when a subsequent batch can attribute them.
// A retry never overwrites the first accepted event or its ownership snapshot.
func (cs *ConfigStore) RecordTrinoQueryUsage(ctx context.Context, event QueryUsageEvent) (bool, error) {
	if event.ClusterID == "" || event.QueryID == "" || event.Principal == "" || event.CompletedAt.IsZero() || event.State == "" {
		return false, fmt.Errorf("query usage requires cluster, query, principal, state and completion time")
	}
	if event.PhysicalInputBytes < 0 || event.ProcessedInputBytes < 0 {
		return false, fmt.Errorf("query usage counters must be nonnegative")
	}
	result := cs.db.WithContext(ctx).Exec(`INSERT INTO duckgres_trino_query_usage
(cluster_id, query_id, principal, org_id, team_id, source, state, error_code, completed_at,
 physical_input_bytes, processed_input_bytes, statistics_complete, trino_version)
SELECT ?, ?, ?, p.org_id, COALESCE(p.team_id, 0), ?, ?, ?, ?, ?, ?, ?, ?
FROM (SELECT 1) singleton LEFT JOIN duckgres_trino_usage_principals p ON p.principal = ?
ON CONFLICT (cluster_id, query_id) DO NOTHING`,
		event.ClusterID, event.QueryID, event.Principal, event.Source, event.State, event.ErrorCode, event.CompletedAt.UTC(),
		event.PhysicalInputBytes, event.ProcessedInputBytes, event.StatisticsComplete, event.TrinoVersion, event.Principal)
	if result.Error != nil {
		return false, fmt.Errorf("record Trino query usage: %w", result.Error)
	}
	return result.RowsAffected == 1, nil
}
