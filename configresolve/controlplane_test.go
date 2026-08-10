package configresolve

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/posthog/duckgres/server"
)

// The control-plane config used to be assembled by a hand-maintained literal in
// EACH binary: the all-in-one `duckgres --mode control-plane` and the
// production `cmd/duckgres-controlplane`. Nothing forced the two to agree, and
// two knobs had already drifted out of the production one — every
// DUCKGRES_TRINO_BENCHMARK_* variable and DUCKGRES_USER_SECRET_KEY were parsed
// and then discarded, so the feature they configure was dead in production
// while the mw-dev scenario (which runs the all-in-one binary) passed.
//
// ControlPlaneConfig is now the single assembly site, and these tests are the
// tripwire for that bug class. They check MAPPING COVERAGE in both directions
// and deliberately assert nothing about runtime values:
//
//   - every field of the produced config can be moved by some input, so a field
//     nothing wires is caught; and
//   - every field of Resolved changes the output, so a knob that is parsed and
//     then discarded is caught.
//
// Zero is a legitimate runtime value for many of these fields (an unset TTL, an
// empty PriorityClass meaning "headroom disabled", a false feature gate).
// Requiring non-zero values would turn this into an assertion about production
// configuration, which is not what it is for — the sentinels below are probes,
// not expected values.

// sentinelOverrides is the non-zero probe for the per-binary values that
// legitimately do NOT come from Resolved.
func sentinelOverrides() ControlPlaneOverrides {
	return ControlPlaneOverrides{
		Server:        server.Config{Host: "127.0.0.1", Port: 5432},
		SocketDir:     "/tmp/duckgres-sockets",
		ConfigPath:    "/etc/duckgres/duckgres.yaml",
		MetricsServer: &http.Server{Addr: ":9090"},
	}
}

// resolvedFieldsFromOverrides are Resolved fields that intentionally do NOT
// feed the control-plane config directly. Structural facts, not exemptions:
//
//   - Server: each binary adjusts its own copy (TLS/ACME) and passes it in via
//     the overrides, so the constructor must not read resolved.Server.
//   - SessionInitTimeout: a convenience mirror of Server.SessionInitTimeout
//     (see ResolveEffective); it reaches the control plane inside the embedded
//     server.Config, not as a top-level control-plane field.
var resolvedFieldsFromOverrides = map[string]string{
	"Server":             "supplied through ControlPlaneOverrides.Server",
	"SessionInitTimeout": "reaches the control plane inside the embedded server.Config",
}

// unconfiguredDestinationFields are control-plane config fields with NO
// configuration source anywhere — no flag, no env var, no YAML key — so nothing
// could move them and their absence here is not a dropped knob. Each entry
// names where the value actually comes from; if someone later adds a knob for
// one, this exemption becomes wrong and should be deleted rather than kept.
var unconfiguredDestinationFields = map[string]string{
	"HealthCheckInterval": "defaulted inside controlplane.RunControlPlane (2s); not configurable",
}

// fullyPopulatedResolved fills every Resolved field the constructor reads with a
// distinctive non-zero probe value.
func fullyPopulatedResolved(t *testing.T) Resolved {
	t.Helper()
	var resolved Resolved
	value := reflect.ValueOf(&resolved).Elem()
	for i := 0; i < value.NumField(); i++ {
		name := value.Type().Field(i).Name
		if _, skip := resolvedFieldsFromOverrides[name]; skip {
			continue
		}
		if !setSentinel(value.Field(i)) {
			t.Fatalf("Resolved.%s has unsupported kind %s; teach setSentinel about it "+
				"so the control-plane wiring tripwire keeps covering it", name, value.Field(i).Kind())
		}
	}
	return resolved
}

// setSentinel writes a non-zero value of the field's type. It returns false for
// a kind it does not know how to fill, which fails the test loudly rather than
// silently dropping a field from coverage.
func setSentinel(field reflect.Value) bool {
	switch field.Kind() {
	case reflect.String:
		field.SetString("sentinel")
	case reflect.Bool:
		field.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// time.Duration is an int64 kind; 7 is non-zero either way.
		field.SetInt(7)
	case reflect.Slice:
		slice := reflect.MakeSlice(field.Type(), 1, 1)
		if !setSentinel(slice.Index(0)) {
			return false
		}
		field.Set(slice)
	case reflect.Struct:
		filled := false
		for i := 0; i < field.NumField(); i++ {
			if field.Field(i).CanSet() && setSentinel(field.Field(i)) {
				filled = true
			}
		}
		return filled
	default:
		return false
	}
	return true
}

// A destination field is WIRED iff some input can move it. This says nothing
// about what its value should be — only that the assembly reads a source for
// it. A field that is identical whether every input is zero or every input is a
// sentinel has no source at all: whatever configures it is discarded.
func TestControlPlaneConfigMapsEveryDestinationField(t *testing.T) {
	unwired := ControlPlaneConfig(Resolved{}, ControlPlaneOverrides{})
	wired := ControlPlaneConfig(fullyPopulatedResolved(t), sentinelOverrides())

	assertEveryFieldInfluenced(t, reflect.ValueOf(unwired), reflect.ValueOf(wired), "ControlPlaneConfig")
}

// assertEveryFieldInfluenced compares two builds field by field, recursing into
// nested CONFIG structs (Process, K8s, TrinoBenchmark) so a single unwired
// nested field is caught rather than masked by its siblings.
func assertEveryFieldInfluenced(t *testing.T, unwired, wired reflect.Value, path string) {
	t.Helper()
	for i := 0; i < wired.NumField(); i++ {
		name := wired.Type().Field(i).Name
		qualified := path + "." + name
		unwiredField, wiredField := unwired.Field(i), wired.Field(i)
		if reason, skip := unconfiguredDestinationFields[name]; skip {
			t.Logf("%s: %s", qualified, reason)
			continue
		}

		// server.Config arrives wholesale from the overrides and has its own
		// resolution/defaulting tests; compare it as one unit.
		if name == "Config" {
			if reflect.DeepEqual(unwiredField.Interface(), wiredField.Interface()) {
				t.Errorf("%s is not wired: the assembly does not read the overrides' server config", qualified)
			}
			continue
		}
		if wiredField.Kind() == reflect.Struct {
			assertEveryFieldInfluenced(t, unwiredField, wiredField, qualified)
			continue
		}
		if reflect.DeepEqual(unwiredField.Interface(), wiredField.Interface()) {
			t.Errorf("%s is not wired: no input moves it, so whatever configures it "+
				"is parsed and then discarded", qualified)
		}
	}
}

// The other direction: a knob can be added to Resolved and simply never read.
// Setting exactly one Resolved field at a time must change the result.
func TestControlPlaneConfigConsumesEveryResolvedField(t *testing.T) {
	baseline := ControlPlaneConfig(Resolved{}, sentinelOverrides())

	var probe Resolved
	value := reflect.ValueOf(&probe).Elem()
	for i := 0; i < value.NumField(); i++ {
		name := value.Type().Field(i).Name
		if reason, skip := resolvedFieldsFromOverrides[name]; skip {
			t.Logf("Resolved.%s: %s", name, reason)
			continue
		}
		t.Run(name, func(t *testing.T) {
			var one Resolved
			field := reflect.ValueOf(&one).Elem().Field(i)
			if !setSentinel(field) {
				t.Fatalf("Resolved.%s has unsupported kind %s; teach setSentinel about it", name, field.Kind())
			}
			if reflect.DeepEqual(ControlPlaneConfig(one, sentinelOverrides()), baseline) {
				t.Fatalf("setting Resolved.%s does not change the control-plane config: "+
					"whatever env/flag populates it is parsed and then discarded", name)
			}
		})
	}
}

// Regression guards for the two knobs that had actually drifted out of the
// production binary's literal.
func TestControlPlaneConfigWiresTrinoBenchmarkAndUserSecretKey(t *testing.T) {
	resolved := Resolved{UserSecretKey: "base64-aes-key"}
	resolved.TrinoBenchmark.Enabled = true
	resolved.TrinoBenchmark.Image = "registry.example/trino-brikk@sha256:abc"
	resolved.TrinoBenchmark.Workers = 4

	cfg := ControlPlaneConfig(resolved, sentinelOverrides())

	if cfg.UserSecretKey != "base64-aes-key" {
		t.Fatal("UserSecretKey is not wired: CREATE PERSISTENT SECRET would be rejected in production")
	}
	if !cfg.K8s.TrinoBenchmark.Enabled || cfg.K8s.TrinoBenchmark.Image != "registry.example/trino-brikk@sha256:abc" {
		t.Fatalf("TrinoBenchmark is not wired: %+v", cfg.K8s.TrinoBenchmark)
	}
	if cfg.K8s.TrinoBenchmark.Workers != 4 {
		t.Fatalf("TrinoBenchmark worker count = %d", cfg.K8s.TrinoBenchmark.Workers)
	}
}

// The disabled default must survive the assembly: a deployment that sets no
// Trino benchmark variables gets a lifecycle that cannot be built.
func TestControlPlaneConfigPreservesDisabledTrinoBenchmarkDefault(t *testing.T) {
	cfg := ControlPlaneConfig(ResolveEffective(nil, CLIInputs{}, nil, nil), sentinelOverrides())

	if cfg.K8s.TrinoBenchmark.Enabled {
		t.Fatal("Trino benchmark lifecycle must stay disabled with no configuration")
	}
	if cfg.K8s.TrinoBenchmark.Image != "" {
		t.Fatalf("Trino benchmark image = %q, want empty", cfg.K8s.TrinoBenchmark.Image)
	}
}
