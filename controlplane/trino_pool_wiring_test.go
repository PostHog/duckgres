//go:build kubernetes

package controlplane

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/configstore"

	"github.com/posthog/duckgres/controlplane/provisioner"
	"k8s.io/client-go/kubernetes/fake"
)

// testPoolFleet wires one cell whose stored id matches the pooled registry
// fixture, so the operator can find ITS OWN credentials rather than whichever
// map entry came first.
func testPoolFleet() trinoFleet {
	return trinoFleet{{
		Cell:        trinoCell{ID: registeredTrinoCellPrefix + "cell-001", PublicID: "cell-001", Mode: trinoPoolModeShared},
		Kubernetes:  fake.NewClientset(),
		Provisioner: &provisioner.TrinoProvisioner{},
	}}
}

// With no pooled cell in the registry the wiring produces nothing. This is what
// "ships disabled" has to mean at the startup boundary: the code path is
// constructed on every boot, and on a fleet that has not opted in it resolves
// to zero operators and zero side effects.
func TestPoolWiringIsInertWithoutAPooledCell(t *testing.T) {
	t.Setenv(envTrinoCellsFile, "")
	t.Setenv(envTrinoPoolEnabled, "")
	t.Setenv(envTrinoPoolOperatorEnabled, "")

	operators, err := buildTrinoPoolOperators(nil, nil, "cp-test")
	if err != nil {
		t.Fatalf("wiring failed on a fleet without pools: %v", err)
	}
	if len(operators) != 0 {
		t.Fatalf("built %d operators", len(operators))
	}
}

// A configured pool with the operator enabled but no Gateway would create
// compute it can never admit, drain or retire. Refusing to start is the
// smaller failure.
func TestPoolWiringRefusesAnOperatorWithoutAGateway(t *testing.T) {
	blueprint := filepath.Join(t.TempDir(), "blueprint.json")
	if err := os.WriteFile(blueprint, testBlueprintJSON(t), 0o600); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}
	t.Setenv(envTrinoCellsFile, sharedPoolRegistry(t, blueprint))
	t.Setenv(envTrinoRegistryOnly, "true")
	t.Setenv(envTrinoPoolEnabled, "true")
	t.Setenv(envTrinoPoolOperatorEnabled, "true")
	t.Setenv(envTrinoPoolConfigMap, "duckgres-trino-pool")
	t.Setenv(envTrinoPoolGatewayURL, "")
	t.Setenv("DUCKGRES_TRINO_MANAGED_GATEWAY_URL", "")

	_, err := buildTrinoPoolOperators(nil, testPoolFleet(), "cp-test")
	if err == nil {
		t.Fatal("an enabled operator without a Gateway was accepted")
	}
}

// A pooled cell with the operator disabled still wires: the durable desired
// state is kept in sync and nothing external is touched.
func TestPoolWiringBuildsAReadOnlyOperator(t *testing.T) {
	blueprint := filepath.Join(t.TempDir(), "blueprint.json")
	if err := os.WriteFile(blueprint, testBlueprintJSON(t), 0o600); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}
	t.Setenv(envTrinoCellsFile, sharedPoolRegistry(t, blueprint))
	t.Setenv(envTrinoRegistryOnly, "true")
	t.Setenv(envTrinoPoolEnabled, "true")
	t.Setenv(envTrinoPoolOperatorEnabled, "false")
	// Desired state is published from the API object, so the pool has to be
	// told which one.
	t.Setenv(envTrinoPoolConfigMap, "duckgres-trino-pool")
	t.Setenv(envTrinoPoolGatewayURL, "")
	t.Setenv("DUCKGRES_TRINO_MANAGED_GATEWAY_URL", "")

	operators, err := buildTrinoPoolOperators(nil, testPoolFleet(), "cp-test")
	if err != nil {
		t.Fatalf("wiring failed: %v", err)
	}
	if len(operators) != 1 {
		t.Fatalf("built %d operators", len(operators))
	}
	if operators[0].operatorEnabled {
		t.Fatal("the operator was built enabled")
	}
	// The owner is the control-plane id plus a per-PROCESS suffix: two
	// processes of the same control plane must not both satisfy the fence's
	// owner check, or the epoch is the only thing telling them apart.
	if !strings.HasPrefix(operators[0].owner, "cp-test.") || operators[0].owner == "cp-test." {
		t.Fatalf("owner = %q, want a per-process identity under cp-test", operators[0].owner)
	}
	second, err := buildTrinoPoolOperators(nil, testPoolFleet(), "cp-test")
	if err != nil {
		t.Fatalf("second wiring: %v", err)
	}
	if second[0].owner == operators[0].owner {
		t.Fatal("two processes of the same control plane received the same owner identity")
	}
}

// A pooled cell whose desired-state source is not named refuses to start.
//
// The alternative would be publishing desired state from this pod's mounted
// copy of the configuration, which lags per pod and never updates at all under
// a subPath mount - so two replicas could drive one pool from two different
// configurations, indefinitely, with nothing saying which. A refusal at startup
// is the smaller failure, and it is the same rule the rest of the Trino branch
// follows: asking for the feature and getting a silently different shape is
// worse than not starting.
func TestPoolWiringRefusesWithoutADesiredStateSource(t *testing.T) {
	blueprint := filepath.Join(t.TempDir(), "blueprint.json")
	if err := os.WriteFile(blueprint, testBlueprintJSON(t), 0o600); err != nil {
		t.Fatalf("write blueprint: %v", err)
	}
	t.Setenv(envTrinoCellsFile, sharedPoolRegistry(t, blueprint))
	t.Setenv(envTrinoRegistryOnly, "true")
	t.Setenv(envTrinoPoolEnabled, "true")
	t.Setenv(envTrinoPoolOperatorEnabled, "false")
	t.Setenv(envTrinoPoolConfigMap, "")

	if _, err := buildTrinoPoolOperators(nil, testPoolFleet(), "cp-test"); err == nil {
		t.Fatal("a pooled cell was wired with no authoritative desired-state source")
	}
}

// The Ready gate asks whether this warehouse has ever been admitted and still
// is - not whether a barrier happens to be open right now.
//
// Adding a login opens a new attempt, and the publication's state leaves
// `admitted` while that attempt runs. Reading the state alone would flap a
// warehouse that has been serving for weeks back to Provisioning because
// somebody created a user.
func TestReadyGateFollowsAdmissionNotTheAttemptInFlight(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		publication configstore.TrinoPoolPublication
		ready       bool
	}{
		{
			name: "serving while a new attempt is in flight",
			publication: configstore.TrinoPoolPublication{
				State:                  configstore.TrinoPublicationAdmitting,
				AdmittedTargetRevision: "b0123456789a.a1",
			},
			ready: true,
		},
		{
			name: "admitted and idle",
			publication: configstore.TrinoPoolPublication{
				State:                  configstore.TrinoPublicationAdmitted,
				AdmittedTargetRevision: "b0123456789a.a1",
			},
			ready: true,
		},
		{
			name:        "never admitted",
			publication: configstore.TrinoPoolPublication{State: configstore.TrinoPublicationPublished},
			ready:       false,
		},
		{
			name: "revoked",
			publication: configstore.TrinoPoolPublication{
				State:                  configstore.TrinoPublicationRevoked,
				AdmittedTargetRevision: "",
			},
			ready: false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ready, reason := trinoPoolTenantIsAdmitted(&testCase.publication)
			if ready != testCase.ready {
				t.Fatalf("ready = %v (%q), want %v", ready, reason, testCase.ready)
			}
			if !ready && reason == "" {
				t.Fatal("a warehouse held at Provisioning must say why")
			}
		})
	}

	// An unreadable record is not a reason to report a warehouse ready.
	if ready, reason := trinoPoolTenantIsAdmitted(nil); ready || reason == "" {
		t.Fatalf("a missing publication reported ready=%v (%q)", ready, reason)
	}
}
