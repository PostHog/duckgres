package e2emwdev_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTrinoQueryFailureDiagnostics(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Fatal("jq is required for the Trino harness fixture")
	}
	raw, err := os.ReadFile("e2e/trino.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(raw)
	start := strings.Index(script, "trino_query() {")
	end := strings.Index(script, "\nscalar() {")
	if start < 0 || end <= start {
		t.Fatal("Trino statement helper is missing")
	}
	for _, paged := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial", true: "paged"}[paged], func(t *testing.T) {
			response := `{"id":"20260101_000000_00001_abcde","infoUri":"https://private.example.test/query","error":{"message":"Failed to finish Hoglake Parquet file","errorName":"HOGLAKE_WRITE_ERROR","errorType":"EXTERNAL","errorCode":123,"failureInfo":{"type":"io.trino.spi.TrinoException","message":"password=fixture-secret","stack":["private.example.test"],"cause":{"type":"java.io.IOException","message":"s3://fixture-private-bucket/key","cause":{"type":"software.amazon.awssdk.services.s3.model.S3Exception","message":"token=fixture-secret","cause":{"type":"invalid type with private.example.test"}}}}}}`
			code := `set -eu
CA=fixture-ca
TRINO=https://coordinator.example.test
curl() {
  printf 'request\n' >> "$TEST_CALLS"
  if [ "$TEST_PAGED" = true ] && [ "$(wc -l < "$TEST_CALLS" | tr -d ' ')" = 1 ]; then
    printf '%s\n' '{"nextUri":"https://coordinator.example.test/next"}'
  else
    printf '%s\n' "$TEST_RESPONSE"
  fi
}
` + script[start:end] + "\ntrino_query fixture-user fixture-password 'INSERT INTO fixture VALUES 7'\n"
			calls := filepath.Join(t.TempDir(), "calls")
			cmd := exec.Command("sh", "-c", code)
			cmd.Env = append(os.Environ(), "TEST_RESPONSE="+response, "TEST_CALLS="+calls, "TEST_PAGED="+map[bool]string{false: "false", true: "true"}[paged])
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			if err := cmd.Run(); err == nil {
				t.Fatal("failed query incorrectly succeeded")
			}
			if stdout.Len() != 0 {
				t.Fatalf("failed query emitted result rows: %s", stdout.String())
			}
			for _, expected := range []string{
				"Failed to finish Hoglake Parquet file", "20260101_000000_00001_abcde",
				"HOGLAKE_WRITE_ERROR", "EXTERNAL", "123", "io.trino.spi.TrinoException",
				"java.io.IOException", "software.amazon.awssdk.services.s3.model.S3Exception",
			} {
				if !strings.Contains(stderr.String(), expected) {
					t.Errorf("missing diagnostic %q: %s", expected, stderr.String())
				}
			}
			for _, sensitive := range []string{"private.example.test", "fixture-secret", "fixture-private-bucket", "fixture-password", "INSERT INTO"} {
				if strings.Contains(stderr.String(), sensitive) {
					t.Errorf("diagnostics exposed %q", sensitive)
				}
			}
			requests, err := os.ReadFile(calls)
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if paged {
				want = 2
			}
			if got := strings.Count(string(requests), "request\n"); got != want {
				t.Fatalf("failed statement was retried: got %d requests, want %d", got, want)
			}
		})
	}
	for _, failureInfo := range []string{"null", `"malformed"`} {
		t.Run("incomplete_"+failureInfo, func(t *testing.T) {
			cmd := exec.Command("sh", "-c", `set -eu
CA=fixture-ca
TRINO=https://coordinator.example.test
curl() { printf '%s\n' "$TEST_RESPONSE"; }
`+script[start:end]+"\ntrino_query fixture fixture 'SELECT 1'\n")
			cmd.Env = append(os.Environ(), `TEST_RESPONSE={"error":{"message":"original error","failureInfo":`+failureInfo+`}}`)
			out, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(out), "original error") {
				t.Fatalf("incomplete diagnostics hid the original failure: %v %s", err, out)
			}
		})
	}
}
