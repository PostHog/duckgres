//go:build kubernetes

package controlplane

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// --- Per-org metrics (multi-tenant mode) ---

var orgWorkersActiveGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_org_workers_active",
	Help: "Number of active workers per org",
}, []string{"org"})

var orgWorkersIdleGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_org_workers_idle",
	Help: "Number of idle workers per org",
}, []string{"org"})

var orgSessionsActiveGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_org_sessions_active",
	Help: "Number of active sessions per org",
}, []string{"org"})

var orgWorkerSpawnsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_org_worker_spawns_total",
	Help: "Total worker spawns per org",
}, []string{"org"})

var orgWorkerCrashesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_org_worker_crashes_total",
	Help: "Total worker crashes per org",
}, []string{"org"})

// orgPgSessionsAcceptedCounter tracks every PG session that completes auth and
// is dispatched to a worker, partitioned by org and passthrough mode. Useful
// for spotting unexpected passthrough usage spikes (a misconfigured client) or
// validating that a newly onboarded org is actually getting traffic.
var orgPgSessionsAcceptedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_org_pg_sessions_accepted_total",
	Help: "Total PG sessions accepted by the control plane, partitioned by org and passthrough mode",
}, []string{"org", "passthrough"})

// sniRoutingResolutionsCounter counts how SNI hostname prefixes resolve to a
// database_name: via hostname_alias (translated) vs. as-is (alias absent or
// prefix already equals dbname). Use to spot operators relying on the alias
// path and to validate alias rollouts. Labelled by protocol so PG and Flight
// can be analyzed separately.
var sniRoutingResolutionsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_sni_routing_resolutions_total",
	Help: "SNI hostname prefix resolutions, partitioned by whether a hostname_alias was used",
}, []string{"protocol", "alias_used"})

func observeOrgWorkersActive(org string, count int) {
	orgWorkersActiveGauge.WithLabelValues(org).Set(float64(count))
}

func observeOrgWorkersIdle(org string, count int) {
	orgWorkersIdleGauge.WithLabelValues(org).Set(float64(count))
}

func observeOrgSessionsActive(org string, count int) {
	orgSessionsActiveGauge.WithLabelValues(org).Set(float64(count))
}

func observeOrgWorkerSpawn(org string) {
	orgWorkerSpawnsCounter.WithLabelValues(org).Inc()
}

func observeOrgWorkerCrash(org string) {
	orgWorkerCrashesCounter.WithLabelValues(org).Inc()
}

func observeOrgPgSessionAccepted(org string, passthrough bool) {
	mode := "false"
	if passthrough {
		mode = "true"
	}
	orgPgSessionsAcceptedCounter.WithLabelValues(org, mode).Inc()
}

func observeSNIRoutingResolution(protocol string, aliasUsed bool) {
	used := "false"
	if aliasUsed {
		used = "true"
	}
	sniRoutingResolutionsCounter.WithLabelValues(protocol, used).Inc()
}
