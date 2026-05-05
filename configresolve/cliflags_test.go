package configresolve

import (
	"flag"
	"reflect"
	"strings"
	"testing"
)

// envOnlyCLIInputsFields lists CLIInputs struct fields that are intentionally
// not registered as CLI flags — they are populated from env vars only by
// design. CLAUDE.md ("K8s pod scheduling knobs are env-only") owns this list.
// If you add a flag for one of these, remove it from the set; if you add a
// new env-only field to CLIInputs, add it here.
var envOnlyCLIInputsFields = map[string]bool{
	"K8sWorkerCPURequest":      true,
	"K8sWorkerMemoryRequest":   true,
	"K8sWorkerNodeSelector":    true,
	"K8sWorkerTolerationKey":   true,
	"K8sWorkerTolerationValue": true,
	"K8sWorkerExclusiveNode":   true,
}

// fieldNameToFlagName converts a CLIInputs Go field name (PascalCase) into
// the kebab-case flag name. The mapping follows the convention used in
// root main.go and cliflags.go: insert hyphens between word boundaries,
// lowercase everything, and apply specific rewrites for acronyms (TTL,
// ACME, DNS, K8s, SNI, AWS, ID) so the generated names round-trip.
func fieldNameToFlagName(name string) string {
	// Acronym splits — the order matters: longer acronyms first so we
	// don't accidentally split inside a longer match.
	rewrites := []struct{ from, to string }{
		{"DuckLakeDeltaCatalogEnabled", "ducklake-delta-catalog-enabled"},
		{"DuckLakeDeltaCatalogPath", "ducklake-delta-catalog-path"},
		{"DuckLakeDefaultSpecVersion", "ducklake-default-spec-version"},
		{"FlightSessionIdleTTL", "flight-session-idle-ttl"},
		{"FlightSessionReapInterval", "flight-session-reap-interval"},
		{"FlightHandleIdleTTL", "flight-handle-idle-ttl"},
		{"FlightSessionTokenTTL", "flight-session-token-ttl"},
		{"FlightPort", "flight-port"},
		{"K8sWorkerImagePullPolicy", "k8s-worker-image-pull-policy"},
		{"K8sWorkerServiceAccount", "k8s-worker-service-account"},
		{"K8sControlPlaneID", "k8s-control-plane-id"},
		{"K8sWorkerNamespace", "k8s-worker-namespace"},
		{"K8sWorkerConfigMap", "k8s-worker-configmap"},
		{"K8sSharedWarmTarget", "k8s-shared-warm-target"},
		{"K8sWorkerImage", "k8s-worker-image"},
		{"K8sWorkerSecret", "k8s-worker-secret"},
		{"K8sWorkerPort", "k8s-worker-port"},
		{"K8sMaxWorkers", "k8s-max-workers"},
		{"ACMEDNSProvider", "acme-dns-provider"},
		{"ACMEDNSZoneID", "acme-dns-zone-id"},
		{"ACMECacheDir", "acme-cache-dir"},
		{"ACMEDomain", "acme-domain"},
		{"ACMEEmail", "acme-email"},
		{"AWSRegion", "aws-region"},
		{"SNIRoutingMode", "sni-routing-mode"},
		{"ManagedHostnameSuffixes", "managed-hostname-suffixes"},
		{"ConfigStoreConn", "config-store"},
		{"ConfigPollInterval", "config-poll-interval"},
		{"InternalSecret", "internal-secret"},
		{"WorkerBackend", "worker-backend"},
		{"WorkerQueueTimeout", "worker-queue-timeout"},
		{"WorkerIdleTimeout", "worker-idle-timeout"},
		{"HandoverDrainTimeout", "handover-drain-timeout"},
		{"ProcessMinWorkers", "process-min-workers"},
		{"ProcessMaxWorkers", "process-max-workers"},
		{"ProcessRetireOnSessionEnd", "process-retire-on-session-end"},
		{"ProcessIsolation", "process-isolation"},
		{"FilePersistence", "file-persistence"},
		{"IdleTimeout", "idle-timeout"},
		{"SessionInitTimeout", "session-init-timeout"},
		{"MemoryLimit", "memory-limit"},
		{"MemoryBudget", "memory-budget"},
		{"MemoryRebalance", "memory-rebalance"},
		{"MaxConnections", "max-connections"},
		{"DataDir", "data-dir"},
		{"CertFile", "cert"},
		{"KeyFile", "key"},
		{"QueryLog", "query-log"},
		{"Threads", "threads"},
		{"Host", "host"},
		{"Port", "port"},
	}
	for _, rw := range rewrites {
		if name == rw.from {
			return rw.to
		}
	}
	return strings.ToLower(name) // fallback; the test will fail if a new field misses a rewrite
}

// TestRegisterCLIInputsFlagsCoversEveryCLIBackedField guards against drift
// between the CLIInputs struct and the flag registrar. Every CLI-backed
// CLIInputs field must have a corresponding flag registered; every flag
// registered by RegisterCLIInputsFlags must map to a CLIInputs field.
func TestRegisterCLIInputsFlagsCoversEveryCLIBackedField(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	harvest := RegisterCLIInputsFlags(fs)

	// Build the set of flag names that were registered.
	registered := map[string]bool{}
	fs.VisitAll(func(f *flag.Flag) {
		registered[f.Name] = true
	})

	// Walk CLIInputs; every non-Set, non-env-only field must be registered.
	cliType := reflect.TypeOf(CLIInputs{})
	for i := 0; i < cliType.NumField(); i++ {
		field := cliType.Field(i)
		if field.Name == "Set" {
			continue
		}
		if envOnlyCLIInputsFields[field.Name] {
			continue
		}
		flagName := fieldNameToFlagName(field.Name)
		if !registered[flagName] {
			t.Errorf("CLIInputs field %s expects flag --%s but it was not registered by RegisterCLIInputsFlags", field.Name, flagName)
		}
	}

	// Every registered flag name must map to a CLIInputs field.
	cliFieldsByFlagName := map[string]string{}
	for i := 0; i < cliType.NumField(); i++ {
		field := cliType.Field(i)
		if field.Name == "Set" {
			continue
		}
		cliFieldsByFlagName[fieldNameToFlagName(field.Name)] = field.Name
	}
	for name := range registered {
		if _, ok := cliFieldsByFlagName[name]; !ok {
			t.Errorf("flag --%s is registered but no CLIInputs field maps to it", name)
		}
	}

	// Harvest closure should not panic with an empty parse.
	if err := fs.Parse([]string{}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	cli := harvest()
	if cli.Set == nil {
		t.Error("harvested CLIInputs.Set is nil; expected initialized empty map")
	}
	if len(cli.Set) != 0 {
		t.Errorf("harvested CLIInputs.Set should be empty after no-arg parse; got %v", cli.Set)
	}
}

// TestRegisterCLIInputsFlagsHarvestPropagatesValuesAndSet exercises the
// happy path: parse a few flags, confirm both the value field and the Set
// map reflect what the CLI provided.
func TestRegisterCLIInputsFlagsHarvestPropagatesValuesAndSet(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	harvest := RegisterCLIInputsFlags(fs)
	if err := fs.Parse([]string{
		"--port", "5433",
		"--data-dir", "/tmp/duckgres-test",
		"--memory-limit", "2GB",
		"--query-log=false",
	}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	cli := harvest()
	if cli.Port != 5433 {
		t.Errorf("Port: want 5433, got %d", cli.Port)
	}
	if cli.DataDir != "/tmp/duckgres-test" {
		t.Errorf("DataDir: want /tmp/duckgres-test, got %q", cli.DataDir)
	}
	if cli.MemoryLimit != "2GB" {
		t.Errorf("MemoryLimit: want 2GB, got %q", cli.MemoryLimit)
	}
	if cli.QueryLog != false {
		t.Errorf("QueryLog: want false, got %v", cli.QueryLog)
	}
	for _, name := range []string{"port", "data-dir", "memory-limit", "query-log"} {
		if !cli.Set[name] {
			t.Errorf("Set[%q] should be true after explicit --%s", name, name)
		}
	}
	if cli.Set["host"] {
		t.Error("Set[host] should be false; --host was not provided")
	}
	// Default values should land on un-set fields.
	if cli.Host != "" {
		t.Errorf("Host should default to empty string; got %q", cli.Host)
	}
	if cli.Threads != 0 {
		t.Errorf("Threads should default to 0; got %d", cli.Threads)
	}
}
