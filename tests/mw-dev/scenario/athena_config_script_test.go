package scenario

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestAthenaConfigScriptLoadsAndValidatesSSMConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name      string
		change    func(map[string]any)
		raw       string
		awsError  bool
		wantError string
	}{
		{name: "valid"},
		{name: "missing role", change: func(c map[string]any) { delete(c, "pod_identity_role_arn") }, wantError: "Invalid Athena perf configuration"},
		{name: "empty workgroup", change: func(c map[string]any) { c["workgroup_name"] = "" }, wantError: "Invalid Athena perf configuration"},
		{name: "wrong database type", change: func(c map[string]any) { c["glue_database_name"] = 42 }, wantError: "Invalid Athena perf configuration"},
		{name: "newline injection", change: func(c map[string]any) { c["results_s3_uri"] = "s3://example/results/\nUNEXPECTED=value" }, wantError: "Invalid Athena perf configuration"},
		{name: "carriage return", change: func(c map[string]any) { c["workgroup_name"] = "benchmark\rOTHER=value" }, wantError: "Invalid Athena perf configuration"},
		{name: "invalid JSON", raw: "not-json", wantError: "Invalid Athena perf configuration"},
		{name: "multiple documents", raw: "{}\n{}", wantError: "Invalid Athena perf configuration"},
		{name: "AWS failure", awsError: true, wantError: "Could not load Athena perf configuration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := map[string]any{
				"pod_identity_role_arn": "arn:aws:iam::123456789012:role/example-benchmark",
				"workgroup_name":        "benchmark",
				"glue_database_name":    "benchmark_frozen",
				"results_s3_uri":        "s3://example/results/",
			}
			if tc.change != nil {
				tc.change(config)
			}
			raw, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			if tc.raw != "" {
				raw = []byte(tc.raw)
			}
			binDir := t.TempDir()
			argsPath := filepath.Join(binDir, "args")
			fakeAWS := "#!/bin/sh\nprintf '%s\\n' \"$*\" > \"$ATHENA_TEST_ARGS\"\nif [ \"$ATHENA_TEST_AWS_ERROR\" = 1 ]; then exit 1; fi\nprintf '%s\\n' \"$ATHENA_TEST_CONFIG\"\n"
			if err := os.WriteFile(filepath.Join(binDir, "aws"), []byte(fakeAWS), 0o700); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", filepath.Join("..", "..", "..", "scripts", "scenario_athena_config.sh"))
			cmd.Env = []string{
				"PATH=" + binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
				"AWS_REGION=us-east-1", "ATHENA_TEST_ARGS=" + argsPath,
				"ATHENA_TEST_CONFIG=" + string(raw),
			}
			if tc.awsError {
				cmd.Env = append(cmd.Env, "ATHENA_TEST_AWS_ERROR=1")
			}
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err = cmd.Run()
			if tc.wantError != "" {
				if err == nil || !strings.Contains(stderr.String(), tc.wantError) {
					t.Fatalf("err=%v stderr=%q, want %q", err, stderr.String(), tc.wantError)
				}
				if stdout.Len() != 0 {
					t.Fatalf("failed validation emitted partial environment: %q", stdout.String())
				}
				return
			}
			if err != nil {
				t.Fatalf("script failed: %v: %s", err, stderr.String())
			}
			want := "SCENARIO_POD_IDENTITY_ROLE=arn:aws:iam::123456789012:role/example-benchmark\n" +
				"DUCKGRES_SCENARIO_ATHENA_WORKGROUP=benchmark\n" +
				"DUCKGRES_SCENARIO_ATHENA_DATABASE=benchmark_frozen\n" +
				"DUCKGRES_SCENARIO_ATHENA_RESULTS_S3_URI=s3://example/results/\n"
			if stdout.String() != want {
				t.Fatalf("output=%q, want %q", stdout.String(), want)
			}
			args, err := os.ReadFile(argsPath)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(args), "ssm get-parameter --name /duckgres/perf/athena --region us-east-1 --query Parameter.Value --output text") {
				t.Fatalf("unexpected AWS request: %s", args)
			}
		})
	}
}
