//go:build kubernetes

package controlplane

import (
	"context"
	"log/slog"
	"time"

	"github.com/posthog/duckgres/controlplane/admin"
	"github.com/posthog/duckgres/controlplane/configstore"
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

	// Per-org Prometheus metrics. nil leaves the collector emitting only
	// product-analytics events.
	metrics *trinoOrgMetricSet
	// cellID is the stored id of the cell this collector observes. One
	// collector runs per cell, and each may only write the in-flight gauges
	// of its own cell's orgs: another cell's collector sees none of their
	// queries and would overwrite the gauges with zero.
	cellID string
	// gauged is the set of orgs whose in-flight gauges this collector set
	// on its last successful poll.
	gauged map[string]struct{}
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

// withOrgMetrics makes the collector export per-org query metrics for the
// orgs assigned to cellID.
func (c *trinoUsageCollector) withOrgMetrics(metrics *trinoOrgMetricSet, cellID string) *trinoUsageCollector {
	c.metrics = metrics
	c.cellID = cellID
	return c
}

// Run is a leader-only loop. A new leader may see recently completed queries
// after failover; the query_id property lets downstream consumers deduplicate
// that bounded overlap without ever capturing customer SQL.
func (c *trinoUsageCollector) Run(ctx context.Context) {
	if c == nil || c.coordinator == nil || c.orgs == nil {
		return
	}
	// A gauge is a statement about now. Once this process stops polling it
	// must stop exporting the last value it saw.
	defer c.clearInFlight()
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
		c.clearInFlight()
		return
	}
	owners := configstore.NewTrinoPrincipalOwners(orgs)
	queries, err := c.coordinator.Queries(ctx)
	if err != nil {
		slog.Warn("Trino usage collection skipped: list queries failed", "error", err)
		c.clearInFlight()
		return
	}
	now := c.now()
	for id, recordedAt := range c.seen {
		if now.Sub(recordedAt) > trinoUsageSeenTTL {
			delete(c.seen, id)
		}
	}
	inFlight := make(map[string]map[string]int)
	for _, query := range queries {
		if state, active := admin.TrinoInFlightState(query); active {
			if owner, ok := owners.Resolve(query.Principal); ok {
				if inFlight[owner.OrgID] == nil {
					inFlight[owner.OrgID] = make(map[string]int)
				}
				inFlight[owner.OrgID][state]++
			}
			continue
		}
		if query.QueryID == "" {
			continue
		}
		if _, ok := c.seen[query.QueryID]; ok {
			continue
		}
		owner, ok := owners.Resolve(query.Principal)
		if !ok {
			continue // operator queries do not represent tenant usage
		}
		orgID := owner.OrgID
		teamID := int64(0)
		if c.teamID != nil {
			// The duckgres username, not the Trino principal: a project login's
			// team is keyed on the login it authenticated as.
			teamID = c.teamID(orgID, owner.Username)
		}
		props := trinoUsageProperties(query, teamID)
		if query.State == "FINISHED" {
			analytics.Default().Capture("query_completed", orgID, props)
		} else {
			analytics.Default().Capture("query_failed", orgID, props)
		}
		c.metrics.observeFinished(orgID, query)
		c.seen[query.QueryID] = now
	}
	c.recordInFlight(orgs, inFlight)
}

func (c *trinoUsageCollector) recordInFlight(orgs []configstore.TrinoEnabledOrg, inFlight map[string]map[string]int) {
	if c.metrics == nil {
		return
	}
	current := make(map[string]struct{}, len(orgs))
	for _, org := range orgs {
		if c.cellID != "" && org.CellID != c.cellID {
			continue
		}
		current[org.OrgID] = struct{}{}
		c.metrics.setInFlight(org.OrgID, inFlight[org.OrgID])
	}
	for orgID := range c.gauged {
		if _, ok := current[orgID]; !ok {
			c.metrics.clearInFlight(orgID)
		}
	}
	c.gauged = current
}

func (c *trinoUsageCollector) clearInFlight() {
	if c == nil || c.metrics == nil {
		return
	}
	for orgID := range c.gauged {
		c.metrics.clearInFlight(orgID)
	}
	c.gauged = nil
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
