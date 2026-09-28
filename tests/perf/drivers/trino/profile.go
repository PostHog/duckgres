package trino

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/posthog/duckgres/tests/perf/core"
)

// Profile executes a separate instrumented statement and immediately retains
// unpruned coordinator details. Callers must protect these private documents.
// It does not record a benchmark sample or alter ordinary statistics collection.
func (d *Driver) Profile(ctx context.Context, query core.Query) ([][]*string, []byte, error) {
	if d.stats == nil {
		return nil, nil, fmt.Errorf("profiling requires a coordinator connection")
	}
	sqlText, err := query.SQLFor(d.Protocol())
	if err != nil {
		return nil, nil, err
	}
	query.PGWireSQL = "EXPLAIN ANALYZE VERBOSE " + sqlText
	capture := &queryCapture{}
	plan, err := d.ReadResults(withQueryCapture(ctx, capture), query, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("instrumented Trino execution failed: %w", err)
	}
	id, _ := capture.snapshot()
	if id == "" {
		return plan, nil, fmt.Errorf("instrumented Trino execution returned no query ID")
	}
	var raw []byte
	for attempt := 0; attempt < d.stats.options.Attempts; attempt++ {
		if attempt > 0 {
			if err := d.stats.sleep(ctx, d.stats.options.RetryInterval); err != nil {
				return plan, raw, err
			}
		}
		raw, err = d.stats.readFullQueryInfo(ctx, id)
		if err != nil {
			continue
		}
		info, parseErr := ParseQueryInfo(raw)
		if parseErr != nil {
			return plan, raw, fmt.Errorf("parse full coordinator query info: %w", parseErr)
		}
		if info.QueryID != id {
			return plan, raw, fmt.Errorf("full coordinator query info answered for a different query")
		}
		if info.Final {
			return plan, raw, nil
		}
	}
	if err != nil {
		return plan, raw, fmt.Errorf("full coordinator query info unavailable: %w", err)
	}
	return plan, raw, fmt.Errorf("full coordinator query info did not finalize")
}

func (c *statsCollector) readFullQueryInfo(ctx context.Context, id string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.options.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/query/"+url.PathEscape(id), nil)
	if err != nil {
		return nil, fmt.Errorf("build full query info request")
	}
	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("X-Trino-User", c.username)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("full query info request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("full query info http_status=%d", resp.StatusCode)
	}
	const limit = 256 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read full query info: %w", err)
	}
	if len(raw) > limit {
		return nil, fmt.Errorf("full query info exceeds 256 MiB")
	}
	return raw, nil
}
