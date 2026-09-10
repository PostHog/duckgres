//go:build kubernetes

package controlplane

import (
	"context"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/internal/analytics"
)

// Trino's query endpoint retains recently finished queries only in coordinator
// memory. Poll it from the janitor leader so every terminal query becomes one
// product-analytics usage event without multiplying events by CP replicas.
const (
	trinoUsagePollInterval = 10 * time.Second
	trinoUsageSeenTTL      = time.Hour
)

type trinoUsageTeamResolver func(orgID, username string) int64

type trinoUsageCollector struct {
	coordinator admin.TrinoCoordinatorClient
	orgs        admin.TrinoOrgStore
	teamID      trinoUsageTeamResolver
	seen        map[string]time.Time
	now         func() time.Time
	interval    time.Duration
}

func newTrinoUsageCollector(coordinator admin.TrinoCoordinatorClient, orgs admin.TrinoOrgStore, teamID trinoUsageTeamResolver) *trinoUsageCollector {
	return &trinoUsageCollector{
		coordinator: coordinator,
		orgs:        orgs,
		teamID:      teamID,
		seen:        make(map[string]time.Time),
		now:         time.Now,
		interval:    trinoUsagePollInterval,
	}
}

// Run is a leader-only loop. A new leader may see recently completed queries
// after failover; the query_id property lets downstream consumers deduplicate
// that bounded overlap without ever capturing customer SQL.
func (c *trinoUsageCollector) Run(ctx context.Context) {
	if c == nil || c.coordinator == nil || c.orgs == nil {
		return
	}
	c.collect(ctx)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collect(ctx)
		}
	}
}

func (c *trinoUsageCollector) collect(ctx context.Context) {
	if c == nil || c.coordinator == nil || c.orgs == nil {
		return
	}
	orgs, err := c.orgs.ListTrinoEnabledOrgs()
	if err != nil {
		slog.Warn("Trino usage collection skipped: list enabled orgs failed", "error", err)
		return
	}
	orgByPrincipal := make(map[string]string, len(orgs))
	for _, org := range orgs {
		orgByPrincipal[org.TrinoPrincipal()] = org.OrgID
	}
	queries, err := c.coordinator.Queries(ctx)
	if err != nil {
		slog.Warn("Trino usage collection skipped: list queries failed", "error", err)
		return
	}
	now := c.now()
	for id, recordedAt := range c.seen {
		if now.Sub(recordedAt) > trinoUsageSeenTTL {
			delete(c.seen, id)
		}
	}
	for _, query := range queries {
		if query.State != "FINISHED" && query.State != "FAILED" {
			continue
		}
		if query.QueryID == "" {
			continue
		}
		if _, ok := c.seen[query.QueryID]; ok {
			continue
		}
		orgID := orgByPrincipal[query.Principal]
		if orgID == "" {
			continue // operator queries do not represent tenant usage
		}
		teamID := int64(0)
		if c.teamID != nil {
			teamID = c.teamID(orgID, query.Principal)
		}
		props := trinoUsageProperties(query, teamID)
		if query.State == "FINISHED" {
			analytics.Default().Capture("query_completed", orgID, props)
		} else {
			analytics.Default().Capture("query_failed", orgID, props)
		}
		c.seen[query.QueryID] = now
	}
}

func trinoUsageProperties(query admin.TrinoQuery, teamID int64) map[string]any {
	props := map[string]any{
		"execution_engine":       "trino",
		"query_id":               query.QueryID,
		"user":                   query.Principal,
		"team_id":                teamID,
		"duration_ms":            query.ElapsedMS,
		"queued_ms":              query.QueuedMS,
		"cpu_seconds":            float64(query.CPUMS) / 1000,
		"physical_input_bytes":   query.PhysicalInputBytes,
		"internal_network_bytes": query.InternalNetworkBytes,
		"peak_memory_bytes":      query.PeakMemoryBytes,
		"spilled_bytes":          query.SpilledBytes,
		"processed_input_rows":   query.ProcessedInputRows,
		"total_drivers":          query.TotalDrivers,
		"queued_drivers":         query.QueuedDrivers,
		"running_drivers":        query.RunningDrivers,
		"completed_drivers":      query.CompletedDrivers,
		"source":                 query.Source,
		"resource_group":         query.ResourceGroup,
	}
	if query.State == "FAILED" {
		props["error_type"] = query.ErrorType
		props["error_code"] = query.ErrorCode
	}
	return props
}
