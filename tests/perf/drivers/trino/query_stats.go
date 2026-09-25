package trino

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/posthog/duckgres/tests/perf/core"
)

// Trino query statistics are read after each measured statement, outside its
// timed window. The trino-go-client does not expose the query ID through
// database/sql, so a wrapping HTTP transport records it (and the final
// client-protocol statistics) from the statement responses of the request
// whose context carries a queryCapture. The coordinator's query info is then
// fetched with the same credentials; the statement statistics are the
// fallback when the coordinator does not answer.

const (
	defaultQueryStatsAttempts      = 10
	defaultQueryStatsRetryInterval = 200 * time.Millisecond
	defaultQueryStatsTimeout       = 10 * time.Second
	// A pruned query info omits task and operator detail; it stays far below this.
	maxQueryInfoBytes = 64 << 20
)

// QueryStatsOptions bounds the post-query statistics fetch. The zero value
// uses the defaults.
type QueryStatsOptions struct {
	// Attempts bounds query info reads while the coordinator finalizes it.
	Attempts int
	// RetryInterval separates query info reads.
	RetryInterval time.Duration
	// Timeout bounds each query info request.
	Timeout time.Duration
}

// QueryInfo is the subset of Trino's GET /v1/query/{queryId} response the
// perf harness records.
type QueryInfo struct {
	QueryID string
	State   string
	// Final reports the coordinator's finalQueryInfo flag: the statistics
	// will not change any more.
	Final   bool
	Metrics core.ServiceMetrics
}

type queryInfoDocument struct {
	QueryID        string              `json:"queryId"`
	State          string              `json:"state"`
	FinalQueryInfo bool                `json:"finalQueryInfo"`
	QueryStats     *queryStatsDocument `json:"queryStats"`
}

// Durations and data sizes are airlift units serialized as strings such as
// "1.21ms" and "2.18MB"; counts are JSON numbers.
type queryStatsDocument struct {
	QueuedTime                string `json:"queuedTime"`
	AnalysisTime              string `json:"analysisTime"`
	PlanningTime              string `json:"planningTime"`
	ExecutionTime             string `json:"executionTime"`
	ElapsedTime               string `json:"elapsedTime"`
	TotalCPUTime              string `json:"totalCpuTime"`
	TotalDrivers              int64  `json:"totalDrivers"`
	CompletedDrivers          int64  `json:"completedDrivers"`
	PhysicalInputDataSize     string `json:"physicalInputDataSize"`
	PhysicalInputPositions    int64  `json:"physicalInputPositions"`
	PeakUserMemoryReservation string `json:"peakUserMemoryReservation"`
}

// ParseQueryInfo reads a coordinator query info document. Trino's client
// protocol and web UI call drivers "splits"; totalDrivers is what the
// statement protocol reports as totalSplits. Planning time includes analysis,
// which Trino reports separately.
func ParseQueryInfo(raw []byte) (QueryInfo, error) {
	var document queryInfoDocument
	if err := json.Unmarshal(raw, &document); err != nil {
		return QueryInfo{}, fmt.Errorf("decode Trino query info: %w", err)
	}
	if document.QueryID == "" {
		return QueryInfo{}, errors.New("trino query info has no queryId")
	}
	stats := document.QueryStats
	if stats == nil {
		return QueryInfo{}, fmt.Errorf("trino query info %s has no queryStats", document.QueryID)
	}
	if stats.TotalDrivers < 0 || stats.CompletedDrivers < 0 || stats.PhysicalInputPositions < 0 {
		return QueryInfo{}, fmt.Errorf("trino query info %s has negative counts", document.QueryID)
	}
	var err error
	duration := func(field, value string) time.Duration {
		if err != nil || value == "" {
			return 0
		}
		var parsed time.Duration
		if parsed, err = parseAirliftDuration(value); err != nil {
			err = fmt.Errorf("trino query info %s %s: %w", document.QueryID, field, err)
		}
		return parsed
	}
	size := func(field, value string) int64 {
		if err != nil || value == "" {
			return 0
		}
		var parsed int64
		if parsed, err = parseAirliftDataSize(value); err != nil {
			err = fmt.Errorf("trino query info %s %s: %w", document.QueryID, field, err)
		}
		return parsed
	}
	physicalInputRows := stats.PhysicalInputPositions
	metrics := core.ServiceMetrics{
		QueueDuration:    duration("queuedTime", stats.QueuedTime),
		PlanningDuration: duration("analysisTime", stats.AnalysisTime) + duration("planningTime", stats.PlanningTime),
		EngineDuration:   duration("executionTime", stats.ExecutionTime),
		ServiceDuration:  duration("elapsedTime", stats.ElapsedTime),
		BytesScanned:     size("physicalInputDataSize", stats.PhysicalInputDataSize),
		Trino: &core.TrinoQueryStats{
			QueryID:           document.QueryID,
			Source:            core.TrinoStatsSourceQueryInfo,
			TotalSplits:       stats.TotalDrivers,
			CompletedSplits:   stats.CompletedDrivers,
			PhysicalInputRows: &physicalInputRows,
			CPUDuration:       duration("totalCpuTime", stats.TotalCPUTime),
			PeakMemoryBytes:   size("peakUserMemoryReservation", stats.PeakUserMemoryReservation),
		},
	}
	if err != nil {
		return QueryInfo{}, err
	}
	return QueryInfo{QueryID: document.QueryID, State: document.State, Final: document.FinalQueryInfo, Metrics: metrics}, nil
}

func (q QueryInfo) terminal() bool {
	return q.State == "FINISHED" || q.State == "FAILED"
}

var airliftUnitPattern = regexp.MustCompile(`^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)\s*$`)

var airliftDurationUnits = map[string]time.Duration{
	"ns": time.Nanosecond,
	"us": time.Microsecond,
	"ms": time.Millisecond,
	"s":  time.Second,
	"m":  time.Minute,
	"h":  time.Hour,
	"d":  24 * time.Hour,
}

// airlift DataSize units are powers of 1024.
var airliftDataSizeUnits = map[string]float64{
	"B":  1,
	"kB": 1 << 10,
	"MB": 1 << 20,
	"GB": 1 << 30,
	"TB": 1 << 40,
	"PB": 1 << 50,
}

func parseAirliftDuration(text string) (time.Duration, error) {
	value, unit, err := splitAirliftUnit(text)
	if err != nil {
		return 0, err
	}
	multiplier, ok := airliftDurationUnits[unit]
	if !ok {
		return 0, fmt.Errorf("unknown duration unit in %q", text)
	}
	nanos := math.Round(value * float64(multiplier))
	if nanos > math.MaxInt64 {
		return 0, fmt.Errorf("duration %q overflows", text)
	}
	return time.Duration(nanos), nil
}

func parseAirliftDataSize(text string) (int64, error) {
	value, unit, err := splitAirliftUnit(text)
	if err != nil {
		return 0, err
	}
	multiplier, ok := airliftDataSizeUnits[unit]
	if !ok {
		return 0, fmt.Errorf("unknown data size unit in %q", text)
	}
	bytes := math.Round(value * multiplier)
	if bytes > math.MaxInt64 {
		return 0, fmt.Errorf("data size %q overflows", text)
	}
	return int64(bytes), nil
}

func splitAirliftUnit(text string) (float64, string, error) {
	match := airliftUnitPattern.FindStringSubmatch(text)
	if match == nil {
		return 0, "", fmt.Errorf("invalid airlift unit value %q", text)
	}
	value, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, "", fmt.Errorf("invalid airlift unit value %q", text)
	}
	return value, match[2], nil
}

// statementStats is the stats object of a client-protocol statement response.
type statementStats struct {
	State              string `json:"state"`
	TotalSplits        int64  `json:"totalSplits"`
	CompletedSplits    int64  `json:"completedSplits"`
	CPUTimeMillis      int64  `json:"cpuTimeMillis"`
	QueuedTimeMillis   int64  `json:"queuedTimeMillis"`
	ElapsedTimeMillis  int64  `json:"elapsedTimeMillis"`
	AnalysisTimeMillis int64  `json:"analysisTimeMillis"`
	PlanningTimeMillis int64  `json:"planningTimeMillis"`
	PhysicalInputBytes int64  `json:"physicalInputBytes"`
	PeakMemoryBytes    int64  `json:"peakMemoryBytes"`
}

func (s statementStats) metrics(queryID string) *core.ServiceMetrics {
	return &core.ServiceMetrics{
		QueueDuration:    time.Duration(s.QueuedTimeMillis) * time.Millisecond,
		PlanningDuration: time.Duration(s.AnalysisTimeMillis+s.PlanningTimeMillis) * time.Millisecond,
		ServiceDuration:  time.Duration(s.ElapsedTimeMillis) * time.Millisecond,
		BytesScanned:     s.PhysicalInputBytes,
		Trino: &core.TrinoQueryStats{
			QueryID:         queryID,
			Source:          core.TrinoStatsSourceStatement,
			TotalSplits:     s.TotalSplits,
			CompletedSplits: s.CompletedSplits,
			CPUDuration:     time.Duration(s.CPUTimeMillis) * time.Millisecond,
			PeakMemoryBytes: s.PeakMemoryBytes,
		},
	}
}

// queryCapture collects what the statement responses of one query reveal.
type queryCapture struct {
	mu      sync.Mutex
	queryID string
	stats   *statementStats
}

func (c *queryCapture) observe(queryID string, stats *statementStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.queryID == "" {
		c.queryID = queryID
	}
	if stats != nil && queryID == c.queryID {
		c.stats = stats
	}
}

func (c *queryCapture) snapshot() (string, *statementStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queryID, c.stats
}

type queryCaptureKey struct{}

func withQueryCapture(ctx context.Context, capture *queryCapture) context.Context {
	return context.WithValue(ctx, queryCaptureKey{}, capture)
}

func queryCaptureFrom(ctx context.Context) *queryCapture {
	capture, _ := ctx.Value(queryCaptureKey{}).(*queryCapture)
	return capture
}

// captureTransport observes statement responses for requests whose context
// carries a queryCapture. Other requests (readiness, spooled segments, query
// info, cancellation) pass through untouched.
type captureTransport struct {
	base http.RoundTripper
}

func (t captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	capture := queryCaptureFrom(req.Context())
	if err != nil || capture == nil || resp.StatusCode != http.StatusOK ||
		(req.Method != http.MethodPost && req.Method != http.MethodGet) ||
		// A gateway may prefix continuation paths, so match the segment anywhere.
		!strings.Contains(req.URL.Path, "/v1/statement") {
		return resp, err
	}
	body, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	var statement struct {
		ID    string          `json:"id"`
		Stats *statementStats `json:"stats"`
	}
	if json.Unmarshal(body, &statement) == nil && statement.ID != "" {
		capture.observe(statement.ID, statement.Stats)
	}
	return resp, nil
}

// newCoordinatorTransport mirrors the trino-go-client TLS setup: the
// configured CA bundle when one is given, otherwise the system roots.
func newCoordinatorTransport(caCertFile string) (*http.Transport, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if caCertFile == "" {
		return transport, nil
	}
	pemBytes, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, fmt.Errorf("read Trino CA certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, errors.New("trino CA certificate file contains no PEM certificates")
	}
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return transport, nil
}

// statsCollector turns a captured query into service metrics.
type statsCollector struct {
	baseURL  string
	username string
	password string
	client   *http.Client
	options  QueryStatsOptions
	sleep    SleepFunc

	mu            sync.Mutex
	lastDiagnosis string
}

func newStatsCollector(baseURL, username, password string, transport http.RoundTripper, options QueryStatsOptions) *statsCollector {
	if options.Attempts <= 0 {
		options.Attempts = defaultQueryStatsAttempts
	}
	if options.RetryInterval <= 0 {
		options.RetryInterval = defaultQueryStatsRetryInterval
	}
	if options.Timeout <= 0 {
		options.Timeout = defaultQueryStatsTimeout
	}
	return &statsCollector{
		baseURL:  strings.TrimRight(baseURL, "/"),
		username: username,
		password: password,
		client:   &http.Client{Transport: transport},
		options:  options,
		sleep:    waitSleep,
	}
}

// collect never fails the measured query: statistics that cannot be read
// leave the metrics nil, and the perf gate reports any bound it could not
// check.
func (c *statsCollector) collect(ctx context.Context, capture *queryCapture) *core.ServiceMetrics {
	queryID, stats := capture.snapshot()
	if queryID == "" {
		c.diagnose("no query ID was observed in the statement responses")
		return nil
	}
	info, err := c.fetchQueryInfo(ctx, queryID)
	if err == nil {
		metrics := info.Metrics
		return &metrics
	}
	if stats == nil {
		c.diagnose("query info unavailable (" + err.Error() + ") and no statement statistics were observed")
		return nil
	}
	c.diagnose("query info unavailable (" + err.Error() + "); recording statement statistics")
	return stats.metrics(queryID)
}

// diagnose logs a statistics failure once per distinct reason. Reasons are
// fixed text plus HTTP status codes or Go error types, never URLs or bodies.
func (c *statsCollector) diagnose(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reason != c.lastDiagnosis {
		fmt.Printf("Trino query statistics: %s\n", reason)
		c.lastDiagnosis = reason
	}
}

type queryInfoError struct {
	reason    string
	retryable bool
}

func (e queryInfoError) Error() string { return e.reason }

func (c *statsCollector) fetchQueryInfo(ctx context.Context, queryID string) (QueryInfo, error) {
	var last QueryInfo
	var lastErr error
	for attempt := 1; attempt <= c.options.Attempts; attempt++ {
		if attempt > 1 {
			if err := c.sleep(ctx, c.options.RetryInterval); err != nil {
				break
			}
		}
		info, err := c.readQueryInfo(ctx, queryID)
		if err != nil {
			lastErr = err
			var infoErr queryInfoError
			if errors.As(err, &infoErr) && !infoErr.retryable {
				return QueryInfo{}, err
			}
			continue
		}
		if info.Final {
			return info, nil
		}
		last, lastErr = info, nil
	}
	// The coordinator had not frozen the final statistics in time. A terminal
	// query's counters are complete apart from late bookkeeping.
	if lastErr == nil && last.terminal() {
		return last, nil
	}
	if lastErr == nil {
		lastErr = queryInfoError{reason: "query info not final after " + strconv.Itoa(c.options.Attempts) + " attempts (state " + last.State + ")"}
	}
	return QueryInfo{}, lastErr
}

func (c *statsCollector) readQueryInfo(ctx context.Context, queryID string) (QueryInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()
	diagnostic := os.Getenv("DUCKGRES_TRINO_DIAGNOSTIC_PROFILE") == "1"
	endpoint := c.baseURL + "/v1/query/" + url.PathEscape(queryID) + "?pruned=" + strconv.FormatBool(!diagnostic)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return QueryInfo{}, queryInfoError{reason: "build query info request"}
	}
	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("X-Trino-User", c.username)
	resp, err := c.client.Do(req)
	if err != nil {
		return QueryInfo{}, queryInfoError{reason: fmt.Sprintf("query info request failed: %T", errors.Unwrap(err)), retryable: true}
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return QueryInfo{}, queryInfoError{
			reason:    "query info http_status=" + strconv.Itoa(resp.StatusCode),
			retryable: resp.StatusCode >= http.StatusInternalServerError,
		}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxQueryInfoBytes+1))
	if err != nil {
		return QueryInfo{}, queryInfoError{reason: "read query info body", retryable: true}
	}
	if len(body) > maxQueryInfoBytes {
		return QueryInfo{}, queryInfoError{reason: "query info body exceeds size limit"}
	}
	info, err := ParseQueryInfo(body)
	if err != nil {
		return QueryInfo{}, queryInfoError{reason: "parse query info"}
	}
	if info.QueryID != queryID {
		return QueryInfo{}, queryInfoError{reason: "query info answered for a different query"}
	}
	if diagnostic && info.Final {
		printDiagnosticProfile(body)
	}
	return info, nil
}
