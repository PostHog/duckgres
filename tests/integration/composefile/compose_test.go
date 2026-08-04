package composefile

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestDockerComposeCoreStack guards the shape of the integration compose
// stack: the comparison Postgres, the DuckLake metadata Postgres, and the
// MinIO object store the DuckLake tests run against.
func TestDockerComposeCoreStack(t *testing.T) {
	compose := readCompose(t)

	postgres := serviceNamed(t, compose, "postgres")
	if postgres.Image != "postgres:16-alpine" {
		t.Fatalf("postgres image = %q, want postgres:16-alpine", postgres.Image)
	}
	if !containsString(postgres.Ports, "35432:5432") {
		t.Fatalf("postgres ports = %#v, want 35432:5432", postgres.Ports)
	}

	metadata := serviceNamed(t, compose, "ducklake-metadata")
	if metadata.Image != "postgres:16-alpine" {
		t.Fatalf("ducklake-metadata image = %q, want postgres:16-alpine", metadata.Image)
	}
	if got := envString(metadata.Environment, "POSTGRES_DB"); got != "ducklake" {
		t.Fatalf("ducklake-metadata POSTGRES_DB = %q, want ducklake", got)
	}
	if !containsString(metadata.Ports, "35433:5432") {
		t.Fatalf("ducklake-metadata ports = %#v, want 35433:5432", metadata.Ports)
	}

	minio := serviceNamed(t, compose, "minio")
	if !containsString(minio.Ports, "39000:9000") {
		t.Fatalf("minio ports = %#v, want 39000:9000", minio.Ports)
	}

	minioInit := serviceNamed(t, compose, "minio-init")
	if _, ok := minioInit.DependsOn["minio"]; !ok {
		t.Fatalf("minio-init depends_on = %#v, want minio", minioInit.DependsOn)
	}
}

type composeFile struct {
	Services map[string]service `yaml:"services"`
}

type service struct {
	Image       string         `yaml:"image"`
	Ports       []string       `yaml:"ports"`
	Environment map[string]any `yaml:"environment"`
	DependsOn   map[string]any `yaml:"depends_on"`
}

func readCompose(t *testing.T) composeFile {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join("..", "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read docker-compose.yml: %v", err)
	}
	var compose composeFile
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatalf("parse docker-compose.yml: %v", err)
	}
	return compose
}

func serviceNamed(t *testing.T, compose composeFile, name string) service {
	t.Helper()

	svc, ok := compose.Services[name]
	if !ok {
		t.Fatalf("%s service missing", name)
	}
	return svc
}

func envString(env map[string]any, key string) string {
	value, ok := env[key]
	if !ok {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
