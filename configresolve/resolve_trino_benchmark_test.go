package configresolve

import "testing"

// The Trino benchmark lifecycle is a dev-only comparison harness. It must be
// OFF unless a deployment explicitly turns it on AND pins an image, so an
// ordinary environment can never spin up benchmark clusters.
func TestResolveEffectiveDisablesTrinoBenchmarkByDefault(t *testing.T) {
	resolved := ResolveEffective(nil, CLIInputs{}, nil, nil)

	if resolved.TrinoBenchmark.Enabled {
		t.Fatal("Trino benchmark lifecycle must default to disabled")
	}
	if resolved.TrinoBenchmark.Image != "" {
		t.Fatalf("Trino benchmark image = %q, want empty by default", resolved.TrinoBenchmark.Image)
	}
	if resolved.TrinoBenchmark.Workers != 4 {
		t.Fatalf("Trino benchmark workers = %d, want the documented default of 4", resolved.TrinoBenchmark.Workers)
	}
	if resolved.TrinoBenchmark.CoordinatorCPU != "2" || resolved.TrinoBenchmark.CoordinatorMemory != "8Gi" {
		t.Fatalf("coordinator shape = %s/%s, want the documented 2/8Gi default",
			resolved.TrinoBenchmark.CoordinatorCPU, resolved.TrinoBenchmark.CoordinatorMemory)
	}
	if resolved.TrinoBenchmark.WorkerCPU != "2" || resolved.TrinoBenchmark.WorkerMemory != "8Gi" {
		t.Fatalf("worker shape = %s/%s, want the documented 2/8Gi default",
			resolved.TrinoBenchmark.WorkerCPU, resolved.TrinoBenchmark.WorkerMemory)
	}
	if resolved.TrinoBenchmark.ImagePullPolicy != "IfNotPresent" {
		t.Fatalf("image pull policy = %q, want IfNotPresent", resolved.TrinoBenchmark.ImagePullPolicy)
	}
}

func TestResolveEffectiveReadsTrinoBenchmarkEnvironment(t *testing.T) {
	env := map[string]string{
		"DUCKGRES_TRINO_BENCHMARK_ENABLED":            "true",
		"DUCKGRES_TRINO_BENCHMARK_IMAGE":              "registry.example/trino-brikk@sha256:abc",
		"DUCKGRES_TRINO_BENCHMARK_IMAGE_PULL_POLICY":  "Always",
		"DUCKGRES_TRINO_BENCHMARK_SERVICE_ACCOUNT":    "duckgres-trino-benchmark",
		"DUCKGRES_TRINO_BENCHMARK_WORKERS":            "6",
		"DUCKGRES_TRINO_BENCHMARK_COORDINATOR_CPU":    "4",
		"DUCKGRES_TRINO_BENCHMARK_COORDINATOR_MEMORY": "16Gi",
		"DUCKGRES_TRINO_BENCHMARK_WORKER_CPU":         "3",
		"DUCKGRES_TRINO_BENCHMARK_WORKER_MEMORY":      "12Gi",
	}
	resolved := ResolveEffective(nil, CLIInputs{}, func(key string) string { return env[key] }, nil)

	settings := resolved.TrinoBenchmark
	if !settings.Enabled {
		t.Fatal("Trino benchmark lifecycle should be enabled")
	}
	if settings.Image != "registry.example/trino-brikk@sha256:abc" {
		t.Fatalf("image = %q", settings.Image)
	}
	if settings.ImagePullPolicy != "Always" {
		t.Fatalf("image pull policy = %q", settings.ImagePullPolicy)
	}
	if settings.ServiceAccount != "duckgres-trino-benchmark" {
		t.Fatalf("service account = %q", settings.ServiceAccount)
	}
	if settings.Workers != 6 {
		t.Fatalf("workers = %d", settings.Workers)
	}
	if settings.CoordinatorCPU != "4" || settings.CoordinatorMemory != "16Gi" {
		t.Fatalf("coordinator shape = %s/%s", settings.CoordinatorCPU, settings.CoordinatorMemory)
	}
	if settings.WorkerCPU != "3" || settings.WorkerMemory != "12Gi" {
		t.Fatalf("worker shape = %s/%s", settings.WorkerCPU, settings.WorkerMemory)
	}
}

func TestResolveEffectiveWarnsOnInvalidTrinoBenchmarkValues(t *testing.T) {
	env := map[string]string{
		"DUCKGRES_TRINO_BENCHMARK_ENABLED": "yes-please",
		"DUCKGRES_TRINO_BENCHMARK_WORKERS": "not-a-number",
	}
	var warnings []string
	resolved := ResolveEffective(nil, CLIInputs{},
		func(key string) string { return env[key] },
		func(message string) { warnings = append(warnings, message) })

	if resolved.TrinoBenchmark.Enabled {
		t.Fatal("an unparseable enable flag must leave the feature disabled")
	}
	if resolved.TrinoBenchmark.Workers != 4 {
		t.Fatalf("workers = %d, want the default after an unparseable value", resolved.TrinoBenchmark.Workers)
	}
	if len(warnings) != 2 {
		t.Fatalf("warnings = %v, want one per invalid value", warnings)
	}
}
