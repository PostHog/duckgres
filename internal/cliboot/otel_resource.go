package cliboot

import (
	"os"

	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// otelResource returns the shared OTEL resource used by both the log and
// trace providers. It sets service.name to "duckgres" (or
// "duckgres-<DUCKGRES_IDENTIFIER>" when the env var is set).
func otelResource() *resource.Resource {
	serviceName := "duckgres"
	if id := os.Getenv("DUCKGRES_IDENTIFIER"); id != "" {
		serviceName = serviceName + "-" + id
	}
	return resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
	)
}
