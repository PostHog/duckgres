package main

import (
	"strings"
	"testing"
	"time"
)

func envFromMap(values map[string]string) func(string) string {
	return func(key string) string {
		return values[key]
	}
}

func TestResolveEffectiveConfigPrecedence(t *testing.T) {
	fileCfg := &FileConfig{
		Host: "file-host",
		Port: 5000,
		Flight: FlightFileConfig{
			Port: 7777,
		},
		DataDir: "/tmp/file-data",
		TLS: TLSConfig{
			Cert: "/tmp/file.crt",
			Key:  "/tmp/file.key",
		},
		ProcessIsolation: true,
		IdleTimeout:      "1h",
	}

	env := map[string]string{
		"DUCKGRES_HOST":              "env-host",
		"DUCKGRES_PORT":              "6000",
		"DUCKGRES_DATA_DIR":          "/tmp/env-data",
		"DUCKGRES_CERT":              "/tmp/env.crt",
		"DUCKGRES_KEY":               "/tmp/env.key",
		"DUCKGRES_FLIGHT_PORT":       "8888",
		"DUCKGRES_PROCESS_ISOLATION": "true",
		"DUCKGRES_IDLE_TIMEOUT":      "2h",
	}

	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{
		Set: map[string]bool{
			"host":              true,
			"port":              true,
			"data-dir":          true,
			"cert":              true,
			"key":               true,
			"process-isolation": true,
			"idle-timeout":      true,
			"flight-port":       true,
		},
		Host:             "cli-host",
		Port:             7000,
		DataDir:          "/tmp/cli-data",
		CertFile:         "/tmp/cli.crt",
		KeyFile:          "/tmp/cli.key",
		ProcessIsolation: false,
		IdleTimeout:      "3h",
		FlightPort:       9999,
	}, envFromMap(env), nil)

	if resolved.Server.Host != "cli-host" {
		t.Fatalf("host precedence mismatch: got %q", resolved.Server.Host)
	}
	if resolved.Server.Port != 7000 {
		t.Fatalf("port precedence mismatch: got %d", resolved.Server.Port)
	}
	if resolved.Server.DataDir != "/tmp/cli-data" {
		t.Fatalf("data dir precedence mismatch: got %q", resolved.Server.DataDir)
	}
	if resolved.Server.TLSCertFile != "/tmp/cli.crt" {
		t.Fatalf("cert precedence mismatch: got %q", resolved.Server.TLSCertFile)
	}
	if resolved.Server.TLSKeyFile != "/tmp/cli.key" {
		t.Fatalf("key precedence mismatch: got %q", resolved.Server.TLSKeyFile)
	}
	if resolved.Server.ProcessIsolation {
		t.Fatalf("process isolation precedence mismatch: expected false")
	}
	if resolved.Server.IdleTimeout != 3*time.Hour {
		t.Fatalf("idle timeout precedence mismatch: got %s", resolved.Server.IdleTimeout)
	}
	if resolved.FlightPort != 9999 {
		t.Fatalf("flight port precedence mismatch: got %d", resolved.FlightPort)
	}
}

func TestResolveEffectiveConfigEnvOverridesFile(t *testing.T) {
	fileCfg := &FileConfig{
		Host: "file-host",
		Port: 5000,
		Flight: FlightFileConfig{
			Port: 7777,
		},
	}

	env := map[string]string{
		"DUCKGRES_HOST":        "env-host",
		"DUCKGRES_PORT":        "6000",
		"DUCKGRES_FLIGHT_PORT": "8888",
	}

	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(env), nil)

	if resolved.Server.Host != "env-host" {
		t.Fatalf("expected env host, got %q", resolved.Server.Host)
	}
	if resolved.Server.Port != 6000 {
		t.Fatalf("expected env port, got %d", resolved.Server.Port)
	}
	if resolved.FlightPort != 8888 {
		t.Fatalf("expected env flight port, got %d", resolved.FlightPort)
	}
}

func TestResolveEffectiveConfigInvalidEnvValues(t *testing.T) {
	fileCfg := &FileConfig{
		Flight: FlightFileConfig{
			Port: 7777,
		},
		ProcessIsolation: true,
		IdleTimeout:      "45m",
		DuckLake: DuckLakeFileConfig{
			S3UseSSL: true,
		},
	}

	env := map[string]string{
		"DUCKGRES_FLIGHT_PORT":         "bad-port",
		"DUCKGRES_PROCESS_ISOLATION":   "not-a-bool",
		"DUCKGRES_DUCKLAKE_S3_USE_SSL": "not-a-bool",
		"DUCKGRES_IDLE_TIMEOUT":        "bad-duration",
	}

	var warns []string
	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(env), func(msg string) {
		warns = append(warns, msg)
	})

	if resolved.FlightPort != 7777 {
		t.Fatalf("invalid env flight port should not override valid file value, got %d", resolved.FlightPort)
	}
	if !resolved.Server.ProcessIsolation {
		t.Fatalf("invalid env process isolation should not override valid file value")
	}
	if !resolved.Server.DuckLake.S3UseSSL {
		t.Fatalf("invalid env S3_USE_SSL should not override valid file value")
	}
	if resolved.Server.IdleTimeout != 45*time.Minute {
		t.Fatalf("invalid env idle timeout should not override valid file value, got %s", resolved.Server.IdleTimeout)
	}

	wantWarnings := []string{
		"Invalid DUCKGRES_FLIGHT_PORT",
		"Invalid DUCKGRES_PROCESS_ISOLATION",
		"Invalid DUCKGRES_DUCKLAKE_S3_USE_SSL",
		"Invalid DUCKGRES_IDLE_TIMEOUT duration",
	}
	for _, w := range wantWarnings {
		found := false
		for _, got := range warns {
			if strings.Contains(got, w) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected warning containing %q, warnings: %v", w, warns)
		}
	}
}

func TestResolveEffectiveConfigMemoryLimitAndThreads(t *testing.T) {
	// Test YAML → env → CLI precedence for memory_limit and threads

	// YAML only
	fileCfg := &FileConfig{
		MemoryLimit: "2GB",
		Threads:     2,
	}
	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(nil), nil)
	if resolved.Server.MemoryLimit != "2GB" {
		t.Fatalf("expected memory_limit from file, got %q", resolved.Server.MemoryLimit)
	}
	if resolved.Server.Threads != 2 {
		t.Fatalf("expected threads from file, got %d", resolved.Server.Threads)
	}

	// Env overrides file
	env := map[string]string{
		"DUCKGRES_MEMORY_LIMIT": "8GB",
		"DUCKGRES_THREADS":      "8",
	}
	resolved = resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(env), nil)
	if resolved.Server.MemoryLimit != "8GB" {
		t.Fatalf("expected memory_limit from env, got %q", resolved.Server.MemoryLimit)
	}
	if resolved.Server.Threads != 8 {
		t.Fatalf("expected threads from env, got %d", resolved.Server.Threads)
	}

	// CLI overrides env
	resolved = resolveEffectiveConfig(fileCfg, configCLIInputs{
		Set:         map[string]bool{"memory-limit": true, "threads": true},
		MemoryLimit: "16GB",
		Threads:     16,
	}, envFromMap(env), nil)
	if resolved.Server.MemoryLimit != "16GB" {
		t.Fatalf("expected memory_limit from CLI, got %q", resolved.Server.MemoryLimit)
	}
	if resolved.Server.Threads != 16 {
		t.Fatalf("expected threads from CLI, got %d", resolved.Server.Threads)
	}
}

func TestResolveEffectiveConfigInvalidThreadsEnv(t *testing.T) {
	env := map[string]string{
		"DUCKGRES_THREADS": "not-a-number",
	}

	var warns []string
	resolved := resolveEffectiveConfig(nil, configCLIInputs{}, envFromMap(env), func(msg string) {
		warns = append(warns, msg)
	})

	if resolved.Server.Threads != 0 {
		t.Fatalf("expected default threads, got %d", resolved.Server.Threads)
	}

	found := false
	for _, w := range warns {
		if strings.Contains(w, "Invalid DUCKGRES_THREADS") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected warning about invalid DUCKGRES_THREADS, warnings: %v", warns)
	}
}

func TestEffectiveFlightConfigDefaults(t *testing.T) {
	cfg := defaultServerConfig()
	port := effectiveFlightConfig(cfg, 0)
	if port != 8815 {
		t.Fatalf("expected default flight port 8815, got %d", port)
	}
}
