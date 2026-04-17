package duckdbservice

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// validateGRPCMetadataValue checks that a string is safe for use as a gRPC
// metadata value: ASCII-only, no newlines, no carriage returns, no NUL bytes.
func validateGRPCMetadataValue(s string) error {
	for i, c := range s {
		if c == '\n' || c == '\r' || c == 0 {
			return fmt.Errorf("invalid character %q at position %d", c, i)
		}
		if c > 127 {
			return fmt.Errorf("non-ASCII character %q at position %d", c, i)
		}
	}
	return nil
}

func TestProfilingOutputCompaction(t *testing.T) {
	// Simulate pretty-printed DuckDB profiling JSON with newlines
	prettyJSON := `{
    "latency": 0.5,
    "cpu_time": 0.3,
    "rows_returned": 42,
    "children": [
        {
            "operator_name": "SEQ_SCAN",
            "operator_timing": 0.2
        }
    ]
}`

	// Compact it like sendProfilingMetadata does
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(prettyJSON)); err != nil {
		t.Fatalf("json.Compact: %v", err)
	}

	result := compact.String()

	// Must not contain newlines (gRPC metadata requirement)
	if strings.Contains(result, "\n") {
		t.Errorf("compacted JSON still contains newlines: %s", result)
	}

	// Must be valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Errorf("compacted JSON is not valid: %v", err)
	}

	// Must be safe for gRPC metadata
	if err := validateGRPCMetadataValue(result); err != nil {
		t.Errorf("compacted JSON is not gRPC-safe: %v", err)
	}

	// Key fields must survive compaction
	if parsed["latency"] != 0.5 {
		t.Errorf("expected latency 0.5, got %v", parsed["latency"])
	}
	if parsed["rows_returned"] != float64(42) {
		t.Errorf("expected rows_returned 42, got %v", parsed["rows_returned"])
	}
}

func TestProfilingOutputGRPCSafety(t *testing.T) {
	// Realistic DuckDB profiling output with deeply nested operators,
	// string values containing special characters, and large numbers.
	input := `{
    "latency": 6.812345678,
    "cpu_time": 5.123456789,
    "query_name": "SELECT * FROM \"my_table\" WHERE name = 'O''Brien'",
    "rows_returned": 1000000,
    "result_set_size": 8388608,
    "total_memory_allocated": 134217728,
    "system_peak_buffer_memory": 67108864,
    "children": [
        {
            "operator_name": "DUCKLAKE_SCAN",
            "operator_timing": 4.5,
            "extra_info": {
                "Table": "ducklake.main.events",
                "Filters": "id > 0 AND name IS NOT NULL"
            },
            "children": []
        }
    ]
}`

	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(input)); err != nil {
		t.Fatalf("json.Compact: %v", err)
	}

	result := compact.String()

	if err := validateGRPCMetadataValue(result); err != nil {
		t.Fatalf("compacted JSON is not gRPC-safe: %v", err)
	}

	// Verify round-trip
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(result), &parsed); err != nil {
		t.Fatalf("compacted JSON is not valid: %v", err)
	}
	if parsed["query_name"] != "SELECT * FROM \"my_table\" WHERE name = 'O''Brien'" {
		t.Errorf("query_name mangled: %v", parsed["query_name"])
	}
}
