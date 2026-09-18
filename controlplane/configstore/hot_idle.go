package configstore

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

// HotIdleOrgStat is one org's hot-idle pool footprint for the admin
// dashboard's hot-idle reporting: how many workers the org is holding parked,
// their summed shape (vCPU / bytes), and when the longest-parked one went
// idle. Read from the durable runtime store (worker_records), the same source
// the fleet rollup uses — a parked worker holds no session, so the in-memory
// session map cannot see it.
type HotIdleOrgStat struct {
	OrgID              string     `json:"org_id"`
	Count              int64      `json:"count"`
	CPUCores           float64    `json:"cpu_cores"`
	MemoryBytes        int64      `json:"memory_bytes"`
	OldestHotIdleSince *time.Time `json:"oldest_hot_idle_since"`
}

// ListHotIdleByOrg aggregates hot_idle worker rows per org for the admin
// dashboard. Worker shape resolution: the worker's explicit profile wins,
// else the org's default worker profile, else the CP-global default worker
// shape passed by the caller (defaultCPU/defaultMemory — the pod requests the
// worker was actually spawned with; pass "" to keep the legacy
// zero-contribution). Unparseable quantities contribute 0. Orgs with no
// hot-idle workers do not appear.
func (cs *ConfigStore) ListHotIdleByOrg(defaultCPU, defaultMemory string) ([]HotIdleOrgStat, error) {
	workerTable := cs.runtimeTable((&WorkerRecord{}).TableName())
	orgTable := (&Org{}).TableName()
	type workerRow struct {
		OrgID        string
		CPU          string
		Memory       string
		HotIdleSince *time.Time
	}
	var rows []workerRow
	err := cs.db.Table(workerTable+" AS w").
		Select("w.org_id AS org_id, "+
			"COALESCE(NULLIF(w.profile_cpu, ''), NULLIF(o.default_worker_cpu, ''), ?) AS cpu, "+
			"COALESCE(NULLIF(w.profile_memory, ''), NULLIF(o.default_worker_memory, ''), ?) AS memory, "+
			"w.hot_idle_since AS hot_idle_since",
			defaultCPU, defaultMemory).
		Joins("LEFT JOIN " + orgTable + " AS o ON o.name = w.org_id").
		Where("w.state = ? AND w.org_id <> ''", WorkerStateHotIdle).
		Order("w.org_id ASC").
		Scan(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("list hot-idle workers by org: %w", err)
	}

	index := make(map[string]int)
	var out []HotIdleOrgStat
	for _, r := range rows {
		i, ok := index[r.OrgID]
		if !ok {
			i = len(out)
			index[r.OrgID] = i
			out = append(out, HotIdleOrgStat{OrgID: r.OrgID})
		}
		out[i].Count++
		if r.CPU != "" {
			if q, err := resource.ParseQuantity(r.CPU); err == nil {
				out[i].CPUCores += q.AsApproximateFloat64()
			}
		}
		if r.Memory != "" {
			if q, err := resource.ParseQuantity(r.Memory); err == nil {
				out[i].MemoryBytes += q.Value()
			}
		}
		if r.HotIdleSince != nil && (out[i].OldestHotIdleSince == nil || r.HotIdleSince.Before(*out[i].OldestHotIdleSince)) {
			since := *r.HotIdleSince
			out[i].OldestHotIdleSince = &since
		}
	}
	return out, nil
}

// ListOrgHotIdleWorkers returns one org's hot_idle worker rows ordered by
// idle age, OLDEST first (COALESCE(hot_idle_since, updated_at) ascending —
// the same clock the TTL reaper uses). The janitor's hot-idle cap sweep
// retires the (count − cap) PREFIX of this list: the longest-parked workers
// go first, keeping the freshest warm capacity.
func (cs *ConfigStore) ListOrgHotIdleWorkers(orgID string) ([]WorkerRecord, error) {
	var workers []WorkerRecord
	err := cs.db.Table(cs.runtimeTable((&WorkerRecord{}).TableName())+" AS w").
		Where("w.state = ? AND w.org_id = ?", WorkerStateHotIdle, orgID).
		Order("COALESCE(w.hot_idle_since, w.updated_at) ASC, w.worker_id ASC").
		Find(&workers).Error
	if err != nil {
		return nil, fmt.Errorf("list org hot-idle workers (org=%s): %w", orgID, err)
	}
	return workers, nil
}
