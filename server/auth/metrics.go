package auth

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Auth-related Prometheus metrics. Defined here (rather than in the larger
// server package's metrics block) so this package is self-contained — it can
// be imported and built without pulling in the rest of server/.

// AuthFailuresCounter is exported so the wire-protocol code in server can
// also bump it when sending FATAL/Class-28 error responses to the client.
var AuthFailuresCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_auth_failures_total",
	Help: "Total number of authentication failures",
})

// RateLimitRejectsCounter is exported so the connection-handling code in
// server can also bump it when rejecting a connection at the rate-limit
// gate (before any auth attempt happens).
var RateLimitRejectsCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "duckgres_rate_limit_rejects_total",
	Help: "Total number of connections rejected due to rate limiting",
})

var rateLimitedIPsGauge = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "duckgres_rate_limited_ips",
	Help: "Number of currently rate-limited IP addresses",
})
