package perf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

const maxDistinctTraceBytes = 32 << 20

// captureDistinctTrace returns private diagnostic data for encrypted storage only.
// Export can lag query completion, so searches missing expected stage spans are
// retried for up to a minute. Task and split span completeness is not guaranteed.
func captureDistinctTrace(ctx context.Context, rawQueryInfo []byte) (json.RawMessage, error) {
	endpoint := os.Getenv("DUCKGRES_SCENARIO_TRINO_OTLP_ENDPOINT")
	if endpoint == "" {
		return nil, errors.New("trace capture requires endpoint configuration")
	}
	return captureDistinctTraceFrom(ctx, endpoint, rawQueryInfo, 3*time.Second)
}

func captureDistinctTraceFrom(ctx context.Context, endpoint string, rawQueryInfo []byte, retryDelay time.Duration) (json.RawMessage, error) {
	var info struct {
		QueryID string `json:"queryId"`
		Stages  struct {
			Stages []struct {
				StageID string `json:"stageId"`
			} `json:"stages"`
		} `json:"stages"`
		QueryStats struct {
			CreateTime time.Time `json:"createTime"`
		} `json:"queryStats"`
	}
	if json.Unmarshal(rawQueryInfo, &info) != nil || info.QueryID == "" {
		return nil, errors.New("trace capture requires query identity")
	}
	expectedStages := make(map[string]bool)
	for _, stage := range info.Stages.Stages {
		if stage.StageID == "" {
			return nil, errors.New("trace capture requires valid stage identities")
		}
		expectedStages[stage.StageID] = true
	}
	target, err := url.Parse(endpoint)
	if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") || target.User != nil {
		return nil, errors.New("invalid trace endpoint configuration")
	}
	target.Path, target.RawPath, target.RawQuery, target.Fragment = "/select/jaeger/api/traces", "", "", ""
	now := time.Now()
	start := info.QueryStats.CreateTime
	if start.IsZero() {
		start = now.Add(-time.Hour)
	} else {
		start = start.Add(-5 * time.Second)
	}
	tags, _ := json.Marshal(map[string]string{"trino.query_id": info.QueryID})
	query := url.Values{"service": {"trino"}, "tags": {string(tags)}, "start": {strconv.FormatInt(start.UnixMicro(), 10)}, "end": {strconv.FormatInt(now.Add(time.Minute).UnixMicro(), 10)}, "limit": {"10"}}
	target.RawQuery = query.Encode()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	var lastRaw json.RawMessage
	lastSeen := 0
	timeoutError := func() error {
		return fmt.Errorf("trace capture timed out or was canceled: %d/%d stage spans received", lastSeen, len(expectedStages))
	}
	for {
		raw, found, seen, err := fetchDistinctTrace(ctx, client, target.String(), info.QueryID, expectedStages)
		if err != nil {
			if ctx.Err() != nil {
				return lastRaw, timeoutError()
			}
			return lastRaw, err
		}
		if len(raw) > 0 {
			lastRaw = raw
			lastSeen = seen
		}
		if found {
			return raw, nil
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return lastRaw, timeoutError()
		case <-timer.C:
		}
	}
}

func fetchDistinctTrace(ctx context.Context, client *http.Client, endpoint, queryID string, expectedStages map[string]bool) (json.RawMessage, bool, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, false, 0, errors.New("trace request creation failed")
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, false, 0, errors.New("trace request failed")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, false, 0, errors.New("trace service returned unsuccessful status")
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxDistinctTraceBytes+1))
	if err != nil {
		return nil, false, 0, errors.New("trace response read failed")
	}
	if len(raw) > maxDistinctTraceBytes {
		return nil, false, 0, errors.New("trace response exceeds size limit")
	}
	var result struct {
		Data []struct {
			Spans []struct {
				OperationName string `json:"operationName"`
				Duration      int64  `json:"duration"`
				Tags          []struct {
					Key   string          `json:"key"`
					Value json.RawMessage `json:"value"`
				} `json:"tags"`
			} `json:"spans"`
		} `json:"data"`
		Errors json.RawMessage `json:"errors"`
	}
	if json.Unmarshal(raw, &result) != nil {
		return nil, false, 0, errors.New("invalid trace response")
	}
	if len(result.Errors) > 0 && string(result.Errors) != "null" && string(result.Errors) != "[]" {
		return nil, false, 0, errors.New("trace response contains errors")
	}
	if len(result.Data) == 0 {
		return json.RawMessage(raw), false, 0, nil
	}
	seenStages := make(map[string]bool)
	for _, trace := range result.Data {
		matched := false
		for _, span := range trace.Spans {
			for _, tag := range span.Tags {
				if span.OperationName == "stage" && span.Duration > 0 && tag.Key == "trino.stage_id" {
					var value string
					if json.Unmarshal(tag.Value, &value) == nil {
						seenStages[value] = true
					}
				}
				if tag.Key == "trino.query_id" {
					var value string
					if json.Unmarshal(tag.Value, &value) != nil || value != queryID {
						return nil, false, 0, errors.New("trace query identity mismatch")
					}
					matched = true
				}
			}
		}
		if !matched {
			return nil, false, 0, errors.New("trace query identity missing")
		}
	}
	seen := 0
	for stageID := range expectedStages {
		if seenStages[stageID] {
			seen++
		}
	}
	return json.RawMessage(raw), seen == len(expectedStages), seen, nil
}
