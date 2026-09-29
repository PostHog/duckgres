package e2emwdev_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDistinctDiagnosticRenderer(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(raw), "render_trino_template() {")
	end := strings.Index(string(raw)[start:], "\n}\n") + start + 3
	here, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)[start:end] + "\nrender_trino_template \"$MODE\"\n"
	for _, tc := range []struct {
		name, mode, enabled, endpoint, memory, dictionary string
		valid                                             bool
	}{
		{"baseline", "perf", "true", "http://collector.example:4318", "16MB", "false", true},
		{"capacity", "perf", "true", "http://collector.example:4318", "64MB", "false", true},
		{"dictionary", "perf", "true", "http://collector.example:4318", "16MB", "true", true},
		{"normal", "perf", "false", "", "16MB", "false", true},
		{"bootstrap", "", "true", "", "16MB", "false", true},
		{"missing_collector", "perf", "true", "", "16MB", "false", false},
		{"invalid_budget", "perf", "true", "http://collector.example:4318", "4GB", "false", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("bash", "-c", script)
			cmd.Env = append(os.Environ(), "HERE="+here, "MODE="+tc.mode, "DUCKGRES_SCENARIO_PROFILE_DISTINCT="+tc.enabled, "DUCKGRES_SCENARIO_TRINO_OTLP_ENDPOINT="+tc.endpoint, "DUCKGRES_SCENARIO_DISTINCT_PARTIAL_MEMORY="+tc.memory, "DUCKGRES_SCENARIO_DISTINCT_DICTIONARY="+tc.dictionary)
			out, err := cmd.CombinedOutput()
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			if !tc.valid {
				return
			}
			diagnostic := tc.enabled == "true" && tc.mode == "perf"
			if strings.Contains(string(out), "tracing.enabled=true") != diagnostic {
				t.Fatal("wrong tracing activation")
			}
			if diagnostic {
				if strings.Count(string(out), "otel.exporter.endpoint="+tc.endpoint) != 2 {
					t.Fatal("missing tracing on coordinator or workers")
				}
				if !strings.Contains(string(out), "task.max-partial-aggregation-memory="+tc.memory) || !strings.Contains(string(out), "optimizer.dictionary-aggregation="+tc.dictionary) {
					t.Fatal("missing experiment configuration")
				}
			} else if strings.Contains(string(out), "task.max-partial-aggregation-memory=") || strings.Contains(string(out), "optimizer.dictionary-aggregation=") {
				t.Fatal("diagnostic changes leaked into normal configuration")
			}
		})
	}
}

func TestDistinctTracingReusesDevConfiguration(t *testing.T) {
	raw, err := os.ReadFile("run.sh")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(raw), "resolve_distinct_tracing() {")
	if start < 0 {
		t.Fatal("missing runtime tracing resolver")
	}
	end := strings.Index(string(raw)[start:], "\n}\n") + start + 3
	resolver := string(raw)[start:end]
	for _, tc := range []struct {
		name, enabled, workerConfig string
		valid                       bool
	}{
		{"matching", "true", "tracing.enabled=true\notel.exporter.protocol=http/protobuf\notel.exporter.endpoint=http://collector.example:4318\n", true},
		{"mismatch", "true", "tracing.enabled=true\notel.exporter.protocol=http/protobuf\notel.exporter.endpoint=http://other.example:4318\n", false},
		{"disabled", "true", "tracing.enabled=false\notel.exporter.protocol=http/protobuf\notel.exporter.endpoint=http://collector.example:4318\n", false},
		{"ordinary", "false", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			fixture := "tracing.enabled=true\notel.exporter.protocol=http/protobuf\notel.exporter.endpoint=http://collector.example:4318\n"
			for name, contents := range map[string]string{"coordinator": fixture, "worker": tc.workerConfig} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			script := "#!/bin/bash\nprintf '%s\\n' \"$*\" >> \"$FIXTURES/calls\"\ncase \"$*\" in *trino-coordinator*) cat \"$FIXTURES/coordinator\";; *trino-worker*) cat \"$FIXTURES/worker\";; *) exit 1;; esac\n"
			if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			command := "set -euo pipefail\nKUBECTL=(\"$FIXTURES/kubectl\")\n" + resolver + "\nresolve_distinct_tracing\nif [ \"$DUCKGRES_SCENARIO_PROFILE_DISTINCT\" = true ]; then test \"$DUCKGRES_SCENARIO_TRINO_OTLP_ENDPOINT\" = http://collector.example:4318; fi\n"
			cmd := exec.Command("bash", "-c", command)
			cmd.Env = append(os.Environ(), "FIXTURES="+dir, "DUCKGRES_SCENARIO_PROFILE_DISTINCT="+tc.enabled, "GITHUB_ACTIONS=true")
			out, err := cmd.CombinedOutput()
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v output=%s", tc.valid, err, out)
			}
			if tc.enabled == "false" {
				if _, err := os.Stat(filepath.Join(dir, "calls")); !os.IsNotExist(err) {
					t.Fatal("ordinary run queried dev config")
				}
				return
			}
			if tc.valid && !strings.Contains(string(out), "::add-mask::http://collector.example:4318") {
				t.Fatal("endpoint not masked")
			}
			calls, err := os.ReadFile(filepath.Join(dir, "calls"))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(string(calls), "get configmap") != 2 || !strings.Contains(string(calls), "-n trino") || !strings.Contains(string(calls), `config\.properties`) {
				t.Fatal("expected only two config.properties reads")
			}
		})
	}
}
