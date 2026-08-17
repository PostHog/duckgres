package cliboot

import (
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// otelResource returns the shared OTEL resource used by both the log and
// trace providers. service.name is the process role; DUCKGRES_IDENTIFIER
// is duckgres.deployment and no longer suffixes the service name.
func otelResource(bi BuildInfo) *resource.Resource {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(resolveServiceName()),
	}
	if bi.Version != "" && bi.Version != "unknown" {
		attrs = append(attrs, semconv.ServiceVersion(bi.Version))
	}
	if id := os.Getenv("DUCKGRES_IDENTIFIER"); id != "" {
		attrs = append(attrs, attribute.String("duckgres.deployment", id))
		switch id {
		case "dev", "staging", "production":
			attrs = append(attrs, semconv.DeploymentEnvironment(id))
		}
	}
	if pod := os.Getenv("POD_NAME"); pod != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(pod), semconv.K8SPodName(pod))
	}
	if node := os.Getenv("NODE_NAME"); node != "" {
		attrs = append(attrs, semconv.K8SNodeName(node))
	}
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		attrs = append(attrs, semconv.K8SNamespaceName(ns))
	}
	return resource.NewWithAttributes(semconv.SchemaURL, attrs...)
}

func resolveServiceName() string {
	if name := os.Getenv("OTEL_SERVICE_NAME"); name != "" {
		return name
	}
	switch os.Getenv("DUCKGRES_MODE") {
	case "control-plane":
		return "duckgres-control-plane"
	case "duckdb-service":
		return "duckgres-worker"
	case "reshard-runner":
		return "duckgres-reshard"
	default:
		return "duckgres"
	}
}
