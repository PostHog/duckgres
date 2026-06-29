//go:build kubernetes

package admin

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// MetricsProxy forwards a small, fixed set of named panels to the in-cluster
// Prometheus. It is deliberately NOT an open PromQL relay: the client passes a
// panel KEY (+ optional org/window), and the PromQL is built server-side from
// the allow-list below. This keeps org-scoping enforced and prevents arbitrary
// queries against the cluster's metrics.
type MetricsProxy struct {
	promURL string
	client  *http.Client
}

// NewMetricsProxy returns a proxy to the given Prometheus base URL (e.g.
// http://prometheus-server.monitoring.svc:80). An empty URL disables the
// endpoints (they return 503 so the UI can show "metrics not configured").
func NewMetricsProxy(promURL string) *MetricsProxy {
	return &MetricsProxy{
		promURL: promURL,
		client:  &http.Client{Timeout: 20 * time.Second},
	}
}

// panel maps a stable key to a PromQL template. %[1]s = org label selector
// (already including braces when org is set, empty otherwise), %[2]s = rate
// window. Templates that don't need org/window simply ignore the verbs.
var rangePanels = map[string]string{
	"query_rate":      `sum by (outcome) (rate(duckgres_query_total%[1]s[%[2]s]))`,
	"error_ratio":     `sum(rate(duckgres_query_total%[3]s[%[2]s])) / clamp_min(sum(rate(duckgres_query_total%[1]s[%[2]s])), 1)`,
	"duration_p95":    `histogram_quantile(0.95, sum by (le) (rate(duckgres_query_duration_seconds_bucket%[1]s[%[2]s])))`,
	"duration_p50":    `histogram_quantile(0.50, sum by (le) (rate(duckgres_query_duration_seconds_bucket%[1]s[%[2]s])))`,
	"sessions_active": `sum(duckgres_org_sessions_active%[1]s)`,
	"s3_bytes_rate":   `sum(rate(duckgres_s3_bytes_read_total%[1]s[%[2]s]))`,
	"worker_states":   `sum by (state) (duckgres_worker_lifecycle_count)`,
	"queue_depth":     `sum(duckgres_control_plane_worker_queue_depth)`,
}

// RegisterMetricsProxy wires the metrics endpoints onto the group.
func (m *MetricsProxy) RegisterMetricsProxy(r *gin.RouterGroup) {
	r.GET("/metrics/panels", func(c *gin.Context) {
		keys := make([]string, 0, len(rangePanels))
		for k := range rangePanels {
			keys = append(keys, k)
		}
		c.JSON(http.StatusOK, gin.H{"panels": keys, "configured": m.promURL != ""})
	})
	r.GET("/metrics/query_range", m.queryRange)
}

func (m *MetricsProxy) queryRange(c *gin.Context) {
	if m.promURL == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "metrics not configured (DUCKGRES_PROMETHEUS_URL unset)"})
		return
	}
	tmpl, ok := rangePanels[c.Query("expr")]
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unknown panel; GET /metrics/panels for the allow-list"})
		return
	}

	// Org selector: an exact-match label selector, empty when no org given.
	orgSel := ""
	if org := c.Query("org"); org != "" {
		orgSel = fmt.Sprintf(`{org=%q}`, org)
	}
	rateWindow := c.DefaultQuery("rate_window", "5m")
	if _, err := time.ParseDuration(rateWindow); err != nil {
		rateWindow = "5m"
	}
	promql := fmt.Sprintf(tmpl, orgSel, rateWindow, orgSel)

	// Time window → [start, end, step]. Cap at ~250 points.
	window, err := time.ParseDuration(c.DefaultQuery("window", "1h"))
	if err != nil || window <= 0 {
		window = time.Hour
	}
	end := time.Now()
	start := end.Add(-window)
	step := window / 240
	if step < 15*time.Second {
		step = 15 * time.Second
	}

	q := url.Values{}
	q.Set("query", promql)
	q.Set("start", strconv.FormatInt(start.Unix(), 10))
	q.Set("end", strconv.FormatInt(end.Unix(), 10))
	q.Set("step", strconv.Itoa(int(step.Seconds()))+"s")

	m.forward(c, "/api/v1/query_range", q)
}

// forward proxies to Prometheus and streams the JSON response through verbatim.
func (m *MetricsProxy) forward(c *gin.Context, path string, q url.Values) {
	target := m.promURL + path + "?" + q.Encode()
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, target, nil)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	resp, err := m.client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "prometheus unreachable: " + err.Error()})
		return
	}
	defer resp.Body.Close()
	c.Header("Content-Type", resp.Header.Get("Content-Type"))
	c.Status(resp.StatusCode)
	_, _ = io.Copy(c.Writer, resp.Body)
}
