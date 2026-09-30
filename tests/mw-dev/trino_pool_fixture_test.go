package e2emwdev_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestTrinoFixturePinsSharedCatalogSyncImage(t *testing.T) {
	// This reviewed build includes catalog.sync.enabled and read-only catalog stores.
	const supportedImage = "ghcr.io/posthog/trino:0ed6ee0cbaed3daf124304a421407ea395ae7697@sha256:28392ff10e5502ca1e468031c29fc50800005427beb96a363cfd331c434a3462"
	imagePattern := regexp.MustCompile(`ghcr\.io/posthog/trino:[a-f0-9]{40}@sha256:[a-f0-9]{64}`)
	for _, path := range []string{"run.sh", "../../.github/workflows/e2e-mw-dev.yml"} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		images := imagePattern.FindAllString(string(body), -1)
		if len(images) != 1 || images[0] != supportedImage {
			t.Errorf("%s must use the reviewed shared-catalog sync image %s; got %v", path, supportedImage, images)
		}
	}
}

func TestTrinoFixtureUsesOnlySharedPools(t *testing.T) {
	for _, path := range []string{"run.sh", "trino-controlplane-patch.tmpl.json", "e2e/trino.sh"} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, removed := range []string{"DUCKGRES_TRINO_COORDINATOR_URL", "DUCKGRES_TRINO_REGISTRY_ONLY", "TRINO_MULTICELL_ENABLED", "TRINO_SHARED_CATALOGS_ENABLED", "--arg cell legacy"} {
			if strings.Contains(string(body), removed) {
				t.Errorf("%s retains retired fixture contract %s", path, removed)
			}
		}
	}
	patch, err := os.ReadFile("trino-controlplane-patch.tmpl.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"DUCKGRES_TRINO_CELLS_FILE", "DUCKGRES_TRINO_POOL_OPERATOR_ENABLED", "DUCKGRES_TRINO_POOL_CATALOG_WRITER_ENABLED", "DUCKGRES_TRINO_POOL_PUBLISHER_IMAGE"} {
		if !strings.Contains(string(patch), required) {
			t.Errorf("fixture does not enable %s", required)
		}
	}
}

func TestTrinoGatewayImageResolutionIsReadOnlyAndPinned(t *testing.T) {
	for _, tc := range []struct {
		name, image string
		wantError   bool
		override    bool
	}{
		{"pinned", "example.invalid/gateway@sha256:" + strings.Repeat("c", 64), false, false},
		{"mutable", "example.invalid/gateway:latest", true, false},
		{"missing", "", true, false},
		{"override", "example.invalid/gateway@sha256:" + strings.Repeat("d", 64), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakes := newRunSHFakes(t)
			writeFake(t, fakes.binDir, "kubectl", `#!/usr/bin/env bash
printf 'kubectl %s\n' "$*" >> "$RUN_SH_TEST_CALLS"
if [[ "$*" == "--context test-context -n trino-gateway get deployment trino-gateway -o json" ]]; then
  jq -n --arg image "$FIXTURE_GATEWAY_IMAGE" '{spec:{template:{spec:{containers:[{name:"trino-gateway",image:$image}]}}}}'
  exit 0
fi
exit 1
`)
			body, err := os.ReadFile("run.sh")
			if err != nil {
				t.Fatal(err)
			}
			start := strings.Index(string(body), "resolve_gateway_image() {")
			if start < 0 {
				t.Fatal("image resolution helper missing")
			}
			end := strings.Index(string(body)[start:], "\nwait_trino_pool() {") + start
			if end < start {
				t.Fatal("image preflight helpers missing")
			}
			script := `#!/usr/bin/env bash
set -euo pipefail
TRINO_GATEWAY_IMAGE="${FIXTURE_OVERRIDE:-}"
CONTROLPLANE_IMAGE="example.invalid/controlplane@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
KUBECTL=(kubectl --context test-context)
` + string(body)[start:end] + "\nresolve_gateway_image\nrequire_pool_images\n"
			writeFake(t, fakes.binDir, "resolve-image", script)
			cmd := runSHCommand(t, fakes.binDir, "unused", "FIXTURE_GATEWAY_IMAGE="+tc.image, "GITHUB_ACTIONS=true")
			if tc.override {
				cmd.Env = append(cmd.Env, "FIXTURE_OVERRIDE="+tc.image)
			}
			cmd.Path = filepath.Join(fakes.binDir, "resolve-image")
			cmd.Args = []string{cmd.Path}
			out, err := cmd.CombinedOutput()
			if (err != nil) != tc.wantError {
				t.Fatalf("preflight result: %v %s", err, out)
			}
			if strings.Contains(fakes.calls(t), "delete") || strings.Contains(fakes.calls(t), "patch") {
				t.Fatal("image resolution mutated cluster")
			}
			if tc.override && strings.Contains(fakes.calls(t), "kubectl") {
				t.Fatal("explicit override queried the cluster")
			}
			if !tc.wantError && !tc.override && string(out) != "::add-mask::"+tc.image+"\n" {
				t.Fatalf("unexpected image output: %q", out)
			}
		})
	}
}
