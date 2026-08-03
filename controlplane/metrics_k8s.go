//go:build kubernetes

package controlplane

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var orgSessionsActiveGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "duckgres_org_sessions_active",
	Help: "Number of active sessions per org",
}, []string{"org"})

var orgWorkerCrashesCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_org_worker_crashes_total",
	Help: "Total worker crashes per org",
}, []string{"org"})

var orgPgSessionsAcceptedCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_org_pg_sessions_accepted_total",
	Help: "Total PG sessions accepted by the control plane, partitioned by org and passthrough mode",
}, []string{"org", "passthrough"})

var sniRoutingResolutionsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "duckgres_sni_routing_resolutions_total",
	Help: "SNI hostname prefix resolutions, partitioned by whether a hostname_alias was used",
}, []string{"protocol", "alias_used"})

func observeOrgSessionsActive(org string, count int) {
	orgSessionsActiveGauge.WithLabelValues(org).Set(float64(count))
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
