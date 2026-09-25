package trino

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Temporary diagnostic branch only. Whitelist numeric counters and engine
// duration/type fields; never emit SQL, plans, sessions, paths, or raw QueryInfo.
func printDiagnosticProfile(body []byte) {
	var document map[string]any
	if json.Unmarshal(body, &document) != nil {
		return
	}
	keys := []string{"elapsedTime", "executionTime", "totalCpuTime", "totalScheduledTime", "totalBlockedTime", "physicalInputDataSize", "physicalInputPositions", "physicalInputReadTime", "processedInputDataSize", "processedInputPositions", "internalNetworkInputDataSize", "internalNetworkInputPositions", "outputDataSize", "outputPositions", "spilledDataSize", "peakUserMemoryReservation", "totalDrivers", "fullGcCount", "fullGcTime"}
	pick := func(value any, fields []string) map[string]any {
		result := map[string]any{}
		values, _ := value.(map[string]any)
		for _, key := range fields {
			switch item := values[key].(type) {
			case string:
				result[key] = item
			case float64:
				result[key] = item
			}
		}
		return result
	}
	operatorKeys := []string{"pipelineId", "operatorId", "operatorType", "totalDrivers", "addInputCalls", "addInputCpu", "addInputWall", "getOutputCalls", "getOutputCpu", "getOutputWall", "finishCpu", "finishWall", "blockedWall", "physicalInputReadTime", "physicalInputPositions", "physicalInputDataSize", "inputPositions", "inputDataSize", "outputPositions", "outputDataSize", "spilledDataSize", "peakUserMemoryReservation"}
	var stages []any
	var walk func(any)
	walk = func(value any) {
		stage, ok := value.(map[string]any)
		if !ok {
			return
		}
		stats, _ := stage["stageStats"].(map[string]any)
		record := pick(stats, keys)
		var operators []any
		if entries, ok := stats["operatorSummaries"].([]any); ok {
			for _, op := range entries {
				operators = append(operators, pick(op, operatorKeys))
			}
		}
		record["operators"] = operators
		var tasks []any
		if entries, ok := stage["tasks"].([]any); ok {
			for _, entry := range entries {
				task, _ := entry.(map[string]any)
				tasks = append(tasks, pick(task["stats"], keys))
			}
		}
		record["tasks"] = tasks
		stages = append(stages, record)
		if entries, ok := stage["subStages"].([]any); ok {
			for _, child := range entries {
				walk(child)
			}
		}
	}
	walk(document["outputStage"])
	output := map[string]any{"queryId": document["queryId"], "totals": pick(document["queryStats"], keys), "stages": stages}
	encoded, err := json.Marshal(output)
	if err != nil {
		return
	}
	// Successful non-verbose go tests suppress stdout, so retain the sanitized
	// profile directly alongside the scenario artifacts.
	directory := os.Getenv("DUCKGRES_SCENARIO_OUTPUT_BASE")
	if directory == "" {
		return
	}
	file, err := os.OpenFile(filepath.Join(directory, "distinct-profiles.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Println("Diagnostic profile file unavailable")
		return
	}
	_, writeErr := file.Write(append(encoded, '\n'))
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		fmt.Println("Diagnostic profile write failed")
	}
}
