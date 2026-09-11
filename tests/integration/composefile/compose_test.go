package composefile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestDockerComposeCoreStack guards the shape of the integration compose
// stack: the comparison Postgres, the DuckLake metadata Postgres, and the
// Silo object store the DuckLake tests run against.
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

// All local stacks use the same server and bundled admin client, so health and
// initialization exercise the exact release used by credential-rotation tests.
func TestDockerComposeSilo(t *testing.T) {
	const image = "docker.io/pgsty/silo:RELEASE.2026-09-03T13-18-01Z@sha256:b616a0cf8cb281e7e6bb3c9b1fb53875b4016a2878223925541c18f82d6c5ca3"
	for _, path := range []string{
		filepath.Join("..", "docker-compose.yml"),
		filepath.Join("..", "..", "..", "docker-compose.yaml"),
		filepath.Join("..", "..", "..", "k8s", "local-config-store.compose.yaml"),
	} {
		t.Run(path, func(t *testing.T) {
			compose := readComposePath(t, path)
			storage := serviceNamed(t, compose, "minio")
			if storage.Image != image {
				t.Errorf("storage image = %q, want pinned Silo %q", storage.Image, image)
			}
			if got := strings.Join(storage.Healthcheck.Test, " "); got != "CMD mcli ready local" {
				t.Errorf("healthcheck = %q, want bundled mcli readiness check", got)
			}
			if init, ok := compose.Services["minio-init"]; ok {
				if init.Image != image {
					t.Errorf("init image = %q, want same bundled client release as server", init.Image)
				}
				entrypoint, _ := init.Entrypoint.(string)
				if !strings.Contains(entrypoint, "/bin/sh") || !strings.Contains(entrypoint, "mcli mb minio/ducklake --ignore-existing") {
					t.Errorf("init must override server entrypoint and create existing DuckLake bucket with mcli: %q", init.Entrypoint)
				}
			}
		})
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
	Entrypoint  any            `yaml:"entrypoint"`
	Healthcheck struct {
		Test []string `yaml:"test"`
	} `yaml:"healthcheck"`
}

func readCompose(t *testing.T) composeFile {
	t.Helper()
	return readComposePath(t, filepath.Join("..", "docker-compose.yml"))
}

func readComposePath(t *testing.T, path string) composeFile {
	t.Helper()
	raw, err := os.ReadFile(path)
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
