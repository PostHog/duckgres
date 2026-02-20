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
		Host:       "file-host",
		Port:       5000,
		FlightPort: 5001,
		DataDir:    "/tmp/file-data",
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
		"DUCKGRES_FLIGHT_PORT":       "6001",
		"DUCKGRES_DATA_DIR":          "/tmp/env-data",
		"DUCKGRES_CERT":              "/tmp/env.crt",
		"DUCKGRES_KEY":               "/tmp/env.key",
		"DUCKGRES_PROCESS_ISOLATION": "true",
		"DUCKGRES_IDLE_TIMEOUT":      "2h",
	}

	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{
		Set: map[string]bool{
			"host":              true,
			"port":              true,
			"flight-port":       true,
			"data-dir":          true,
			"cert":              true,
			"key":               true,
			"process-isolation": true,
			"idle-timeout":      true,
		},
		Host:             "cli-host",
		Port:             7000,
		FlightPort:       7001,
		DataDir:          "/tmp/cli-data",
		CertFile:         "/tmp/cli.crt",
		KeyFile:          "/tmp/cli.key",
		ProcessIsolation: false,
		IdleTimeout:      "3h",
	}, envFromMap(env), nil)

	if resolved.Server.Host != "cli-host" {
		t.Fatalf("host precedence mismatch: got %q", resolved.Server.Host)
	}
	if resolved.Server.Port != 7000 {
		t.Fatalf("port precedence mismatch: got %d", resolved.Server.Port)
	}
	if resolved.Server.FlightPort != 7001 {
		t.Fatalf("flight port precedence mismatch: got %d", resolved.Server.FlightPort)
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
}

func TestResolveEffectiveConfigEnvOverridesFile(t *testing.T) {
	fileCfg := &FileConfig{
		Host:       "file-host",
		Port:       5000,
		FlightPort: 5001,
	}

	env := map[string]string{
		"DUCKGRES_HOST":        "env-host",
		"DUCKGRES_PORT":        "6000",
		"DUCKGRES_FLIGHT_PORT": "6001",
	}

	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(env), nil)

	if resolved.Server.Host != "env-host" {
		t.Fatalf("expected env host, got %q", resolved.Server.Host)
	}
	if resolved.Server.Port != 6000 {
		t.Fatalf("expected env port, got %d", resolved.Server.Port)
	}
	if resolved.Server.FlightPort != 6001 {
		t.Fatalf("expected env flight port, got %d", resolved.Server.FlightPort)
	}
}

func TestResolveEffectiveConfigInvalidEnvValues(t *testing.T) {
	fileCfg := &FileConfig{
		ProcessIsolation: true,
		IdleTimeout:      "45m",
		DuckLake: DuckLakeFileConfig{
			S3UseSSL: true,
		},
	}

	env := map[string]string{
		"DUCKGRES_PROCESS_ISOLATION":   "not-a-bool",
		"DUCKGRES_DUCKLAKE_S3_USE_SSL": "not-a-bool",
		"DUCKGRES_IDLE_TIMEOUT":        "bad-duration",
	}

	var warns []string
	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(env), func(msg string) {
		warns = append(warns, msg)
	})

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

func TestResolveEffectiveConfigInvalidMemoryLimit(t *testing.T) {
	env := map[string]string{
		"DUCKGRES_MEMORY_LIMIT": "lots-of-memory",
	}

	var warns []string
	resolved := resolveEffectiveConfig(nil, configCLIInputs{}, envFromMap(env), func(msg string) {
		warns = append(warns, msg)
	})

	// Invalid format should be cleared (falls back to auto-detection)
	if resolved.Server.MemoryLimit != "" {
		t.Fatalf("expected empty memory_limit after invalid input, got %q", resolved.Server.MemoryLimit)
	}

	found := false
	for _, w := range warns {
		if strings.Contains(w, "Invalid memory_limit format") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected warning about invalid memory_limit format, warnings: %v", warns)
	}
}

func TestResolveEffectiveConfigMemoryBudgetAndWorkers(t *testing.T) {
	// YAML only
	fileCfg := &FileConfig{
		MemoryBudget: "24GB",
		MinWorkers:   2,
		MaxWorkers:   10,
	}
	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(nil), nil)
	if resolved.Server.MemoryBudget != "24GB" {
		t.Fatalf("expected memory_budget from file, got %q", resolved.Server.MemoryBudget)
	}
	if resolved.Server.MinWorkers != 2 {
		t.Fatalf("expected min_workers from file, got %d", resolved.Server.MinWorkers)
	}
	if resolved.Server.MaxWorkers != 10 {
		t.Fatalf("expected max_workers from file, got %d", resolved.Server.MaxWorkers)
	}

	// Env overrides file
	env := map[string]string{
		"DUCKGRES_MEMORY_BUDGET": "32GB",
		"DUCKGRES_MIN_WORKERS":   "4",
		"DUCKGRES_MAX_WORKERS":   "20",
	}
	resolved = resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(env), nil)
	if resolved.Server.MemoryBudget != "32GB" {
		t.Fatalf("expected memory_budget from env, got %q", resolved.Server.MemoryBudget)
	}
	if resolved.Server.MinWorkers != 4 {
		t.Fatalf("expected min_workers from env, got %d", resolved.Server.MinWorkers)
	}
	if resolved.Server.MaxWorkers != 20 {
		t.Fatalf("expected max_workers from env, got %d", resolved.Server.MaxWorkers)
	}

	// CLI overrides env
	resolved = resolveEffectiveConfig(fileCfg, configCLIInputs{
		Set:          map[string]bool{"memory-budget": true, "min-workers": true, "max-workers": true},
		MemoryBudget: "48GB",
		MinWorkers:   8,
		MaxWorkers:   50,
	}, envFromMap(env), nil)
	if resolved.Server.MemoryBudget != "48GB" {
		t.Fatalf("expected memory_budget from CLI, got %q", resolved.Server.MemoryBudget)
	}
	if resolved.Server.MinWorkers != 8 {
		t.Fatalf("expected min_workers from CLI, got %d", resolved.Server.MinWorkers)
	}
	if resolved.Server.MaxWorkers != 50 {
		t.Fatalf("expected max_workers from CLI, got %d", resolved.Server.MaxWorkers)
	}
}

func TestResolveEffectiveConfigInvalidMemoryBudget(t *testing.T) {
	env := map[string]string{
		"DUCKGRES_MEMORY_BUDGET": "lots-of-memory",
	}

	var warns []string
	resolved := resolveEffectiveConfig(nil, configCLIInputs{}, envFromMap(env), func(msg string) {
		warns = append(warns, msg)
	})

	if resolved.Server.MemoryBudget != "" {
		t.Fatalf("expected empty memory_budget after invalid input, got %q", resolved.Server.MemoryBudget)
	}

	found := false
	for _, w := range warns {
		if strings.Contains(w, "Invalid memory_budget format") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected warning about invalid memory_budget format, warnings: %v", warns)
	}
}

func TestResolveEffectiveConfigInvalidWorkerEnvVars(t *testing.T) {
	env := map[string]string{
		"DUCKGRES_MIN_WORKERS": "not-a-number",
		"DUCKGRES_MAX_WORKERS": "also-bad",
	}

	var warns []string
	resolved := resolveEffectiveConfig(nil, configCLIInputs{}, envFromMap(env), func(msg string) {
		warns = append(warns, msg)
	})

	if resolved.Server.MinWorkers != 0 {
		t.Fatalf("expected default min_workers, got %d", resolved.Server.MinWorkers)
	}
	if resolved.Server.MaxWorkers != 0 {
		t.Fatalf("expected default max_workers, got %d", resolved.Server.MaxWorkers)
	}

	wantWarnings := []string{
		"Invalid DUCKGRES_MIN_WORKERS",
		"Invalid DUCKGRES_MAX_WORKERS",
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

func TestResolveEffectiveConfigFlightIngressDurations(t *testing.T) {
	fileCfg := &FileConfig{
		FlightSessionIdleTTL:      "7m",
		FlightSessionReapInterval: "45s",
		FlightHandleIdleTTL:       "3m",
		FlightSessionTokenTTL:     "2h",
	}

	env := map[string]string{
		"DUCKGRES_FLIGHT_SESSION_IDLE_TTL":      "9m",
		"DUCKGRES_FLIGHT_SESSION_REAP_INTERVAL": "30s",
		"DUCKGRES_FLIGHT_HANDLE_IDLE_TTL":       "4m",
		"DUCKGRES_FLIGHT_SESSION_TOKEN_TTL":     "90m",
	}

	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{
		Set: map[string]bool{
			"flight-session-idle-ttl":      true,
			"flight-session-reap-interval": true,
			"flight-handle-idle-ttl":       true,
			"flight-session-token-ttl":     true,
		},
		FlightSessionIdleTTL:      "11m",
		FlightSessionReapInterval: "15s",
		FlightHandleIdleTTL:       "5m",
		FlightSessionTokenTTL:     "75m",
	}, envFromMap(env), nil)

	if resolved.Server.FlightSessionIdleTTL != 11*time.Minute {
		t.Fatalf("expected CLI flight_session_idle_ttl, got %s", resolved.Server.FlightSessionIdleTTL)
	}
	if resolved.Server.FlightSessionReapInterval != 15*time.Second {
		t.Fatalf("expected CLI flight_session_reap_interval, got %s", resolved.Server.FlightSessionReapInterval)
	}
	if resolved.Server.FlightHandleIdleTTL != 5*time.Minute {
		t.Fatalf("expected CLI flight_handle_idle_ttl, got %s", resolved.Server.FlightHandleIdleTTL)
	}
	if resolved.Server.FlightSessionTokenTTL != 75*time.Minute {
		t.Fatalf("expected CLI flight_session_token_ttl, got %s", resolved.Server.FlightSessionTokenTTL)
	}
}

func TestResolveEffectiveConfigFlightIngressDurationsFromFile(t *testing.T) {
	fileCfg := &FileConfig{
		FlightSessionIdleTTL:      "7m",
		FlightSessionReapInterval: "45s",
		FlightHandleIdleTTL:       "3m",
		FlightSessionTokenTTL:     "2h",
	}

	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(nil), nil)

	if resolved.Server.FlightSessionIdleTTL != 7*time.Minute {
		t.Fatalf("expected file flight_session_idle_ttl, got %s", resolved.Server.FlightSessionIdleTTL)
	}
	if resolved.Server.FlightSessionReapInterval != 45*time.Second {
		t.Fatalf("expected file flight_session_reap_interval, got %s", resolved.Server.FlightSessionReapInterval)
	}
	if resolved.Server.FlightHandleIdleTTL != 3*time.Minute {
		t.Fatalf("expected file flight_handle_idle_ttl, got %s", resolved.Server.FlightHandleIdleTTL)
	}
	if resolved.Server.FlightSessionTokenTTL != 2*time.Hour {
		t.Fatalf("expected file flight_session_token_ttl, got %s", resolved.Server.FlightSessionTokenTTL)
	}
}

func TestResolveEffectiveConfigFlightIngressDurationsFromEnv(t *testing.T) {
	env := map[string]string{
		"DUCKGRES_FLIGHT_SESSION_IDLE_TTL":      "9m",
		"DUCKGRES_FLIGHT_SESSION_REAP_INTERVAL": "30s",
		"DUCKGRES_FLIGHT_HANDLE_IDLE_TTL":       "4m",
		"DUCKGRES_FLIGHT_SESSION_TOKEN_TTL":     "30m",
	}

	resolved := resolveEffectiveConfig(nil, configCLIInputs{}, envFromMap(env), nil)

	if resolved.Server.FlightSessionIdleTTL != 9*time.Minute {
		t.Fatalf("expected env flight_session_idle_ttl, got %s", resolved.Server.FlightSessionIdleTTL)
	}
	if resolved.Server.FlightSessionReapInterval != 30*time.Second {
		t.Fatalf("expected env flight_session_reap_interval, got %s", resolved.Server.FlightSessionReapInterval)
	}
	if resolved.Server.FlightHandleIdleTTL != 4*time.Minute {
		t.Fatalf("expected env flight_handle_idle_ttl, got %s", resolved.Server.FlightHandleIdleTTL)
	}
	if resolved.Server.FlightSessionTokenTTL != 30*time.Minute {
		t.Fatalf("expected env flight_session_token_ttl, got %s", resolved.Server.FlightSessionTokenTTL)
	}
}

func TestResolveEffectiveConfigInvalidFlightPortEnv(t *testing.T) {
	fileCfg := &FileConfig{
		FlightPort: 8815,
	}
	env := map[string]string{
		"DUCKGRES_FLIGHT_PORT": "not-a-number",
	}

	var warns []string
	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(env), func(msg string) {
		warns = append(warns, msg)
	})

	if resolved.Server.FlightPort != 8815 {
		t.Fatalf("invalid env flight port should not override valid file value, got %d", resolved.Server.FlightPort)
	}

	found := false
	for _, w := range warns {
		if strings.Contains(w, "Invalid DUCKGRES_FLIGHT_PORT") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected warning about invalid DUCKGRES_FLIGHT_PORT, warnings: %v", warns)
	}
}

func TestResolveEffectiveConfigPassthroughUsers(t *testing.T) {
	fileCfg := &FileConfig{
		PassthroughUsers: []string{"alice", "bob"},
	}
	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(nil), nil)

	if len(resolved.Server.PassthroughUsers) != 2 {
		t.Fatalf("expected 2 passthrough users, got %d", len(resolved.Server.PassthroughUsers))
	}
	if !resolved.Server.PassthroughUsers["alice"] {
		t.Fatalf("expected alice to be a passthrough user")
	}
	if !resolved.Server.PassthroughUsers["bob"] {
		t.Fatalf("expected bob to be a passthrough user")
	}

	// Empty list should not set the map
	resolved = resolveEffectiveConfig(&FileConfig{}, configCLIInputs{}, envFromMap(nil), nil)
	if resolved.Server.PassthroughUsers != nil {
		t.Fatalf("expected nil passthrough users for empty config, got %v", resolved.Server.PassthroughUsers)
	}
}

func TestResolveEffectiveConfigACME(t *testing.T) {
	// Test YAML config
	fileCfg := &FileConfig{
		TLS: TLSConfig{
			ACME: ACMEConfig{
				Domain:   "test.us.duckgres.com",
				Email:    "infra@posthog.com",
				CacheDir: "/var/lib/duckgres/acme",
			},
		},
	}
	resolved := resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(nil), nil)
	if resolved.Server.ACMEDomain != "test.us.duckgres.com" {
		t.Fatalf("expected ACME domain from file, got %q", resolved.Server.ACMEDomain)
	}
	if resolved.Server.ACMEEmail != "infra@posthog.com" {
		t.Fatalf("expected ACME email from file, got %q", resolved.Server.ACMEEmail)
	}
	if resolved.Server.ACMECacheDir != "/var/lib/duckgres/acme" {
		t.Fatalf("expected ACME cache dir from file, got %q", resolved.Server.ACMECacheDir)
	}

	// Env overrides file
	env := map[string]string{
		"DUCKGRES_ACME_DOMAIN":    "env.us.duckgres.com",
		"DUCKGRES_ACME_EMAIL":     "ops@posthog.com",
		"DUCKGRES_ACME_CACHE_DIR": "/tmp/acme-cache",
	}
	resolved = resolveEffectiveConfig(fileCfg, configCLIInputs{}, envFromMap(env), nil)
	if resolved.Server.ACMEDomain != "env.us.duckgres.com" {
		t.Fatalf("expected ACME domain from env, got %q", resolved.Server.ACMEDomain)
	}
	if resolved.Server.ACMEEmail != "ops@posthog.com" {
		t.Fatalf("expected ACME email from env, got %q", resolved.Server.ACMEEmail)
	}
	if resolved.Server.ACMECacheDir != "/tmp/acme-cache" {
		t.Fatalf("expected ACME cache dir from env, got %q", resolved.Server.ACMECacheDir)
	}

	// CLI overrides env
	resolved = resolveEffectiveConfig(fileCfg, configCLIInputs{
		Set:          map[string]bool{"acme-domain": true, "acme-email": true, "acme-cache-dir": true},
		ACMEDomain:   "cli.us.duckgres.com",
		ACMEEmail:    "cli@posthog.com",
		ACMECacheDir: "/cli/acme-cache",
	}, envFromMap(env), nil)
	if resolved.Server.ACMEDomain != "cli.us.duckgres.com" {
		t.Fatalf("expected ACME domain from CLI, got %q", resolved.Server.ACMEDomain)
	}
	if resolved.Server.ACMEEmail != "cli@posthog.com" {
		t.Fatalf("expected ACME email from CLI, got %q", resolved.Server.ACMEEmail)
	}
	if resolved.Server.ACMECacheDir != "/cli/acme-cache" {
		t.Fatalf("expected ACME cache dir from CLI, got %q", resolved.Server.ACMECacheDir)
	}
}

func TestResolveEffectiveConfigACMEEnvOnly(t *testing.T) {
	// No file config, just env vars
	env := map[string]string{
		"DUCKGRES_ACME_DOMAIN": "envonly.us.duckgres.com",
		"DUCKGRES_ACME_EMAIL":  "test@example.com",
	}
	resolved := resolveEffectiveConfig(nil, configCLIInputs{}, envFromMap(env), nil)
	if resolved.Server.ACMEDomain != "envonly.us.duckgres.com" {
		t.Fatalf("expected ACME domain from env, got %q", resolved.Server.ACMEDomain)
	}
	if resolved.Server.ACMEEmail != "test@example.com" {
		t.Fatalf("expected ACME email from env, got %q", resolved.Server.ACMEEmail)
	}
}
