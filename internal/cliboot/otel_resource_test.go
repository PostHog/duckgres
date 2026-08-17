package cliboot

import (
	"testing"

	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func TestOTelServiceNameFromMode(t *testing.T) {
	tests := []struct {
		name       string
		mode       string
		identifier string
		override   string
		want       string
	}{
		{name: "control-plane", mode: "control-plane", want: "duckgres-control-plane"},
		{name: "worker", mode: "duckdb-service", want: "duckgres-worker"},
		{name: "reshard", mode: "reshard-runner", want: "duckgres-reshard"},
		{name: "standalone", mode: "standalone", want: "duckgres"},
		{name: "unset", want: "duckgres"},
		{name: "identifier does not suffix", mode: "control-plane", identifier: "acme", want: "duckgres-control-plane"},
		{name: "OTEL_SERVICE_NAME wins", mode: "control-plane", override: "custom", want: "custom"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DUCKGRES_MODE", tt.mode)
			t.Setenv("DUCKGRES_IDENTIFIER", tt.identifier)
			t.Setenv("OTEL_SERVICE_NAME", tt.override)
			if got := resolveServiceName(); got != tt.want {
				t.Fatalf("resolveServiceName() = %q, want %q", got, tt.want)
			}
			res := otelResource(BuildInfo{Version: "1.2.3"})
			var gotName, gotDeploy, gotVersion string
			for _, a := range res.Attributes() {
				switch a.Key {
				case semconv.ServiceNameKey:
					gotName = a.Value.AsString()
				case "duckgres.deployment":
					gotDeploy = a.Value.AsString()
				case semconv.ServiceVersionKey:
					gotVersion = a.Value.AsString()
				}
			}
			if gotName != tt.want {
				t.Fatalf("service.name = %q, want %q", gotName, tt.want)
			}
			if tt.identifier != "" && gotDeploy != tt.identifier {
				t.Fatalf("duckgres.deployment = %q, want %q", gotDeploy, tt.identifier)
			}
			if gotVersion != "1.2.3" {
				t.Fatalf("service.version = %q, want 1.2.3", gotVersion)
			}
		})
	}
}
