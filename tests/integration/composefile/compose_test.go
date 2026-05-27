package composefile

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDockerComposeIncludesLakekeeperStack(t *testing.T) {
	compose := readCompose(t)

	metadata := serviceNamed(t, compose, "lakekeeper-metadata")
	if metadata.Image != "postgres:16-alpine" {
		t.Fatalf("lakekeeper-metadata image = %q, want postgres:16-alpine", metadata.Image)
	}
	if got := envString(metadata.Environment, "POSTGRES_DB"); got != "lakekeeper" {
		t.Fatalf("lakekeeper-metadata POSTGRES_DB = %q, want lakekeeper", got)
	}
	if !containsString(metadata.Ports, "35434:5432") {
		t.Fatalf("lakekeeper-metadata ports = %#v, want 35434:5432", metadata.Ports)
	}

	migrate := serviceNamed(t, compose, "lakekeeper-migrate")
	if migrate.Image != lakekeeperImage {
		t.Fatalf("lakekeeper-migrate image = %q, want %s", migrate.Image, lakekeeperImage)
	}
	if !commandContains(migrate.Command, "migrate") {
		t.Fatalf("lakekeeper-migrate command = %#v, want migrate", migrate.Command)
	}
	if _, ok := migrate.DependsOn["lakekeeper-metadata"]; !ok {
		t.Fatalf("lakekeeper-migrate depends_on = %#v, want lakekeeper-metadata", migrate.DependsOn)
	}

	lakekeeper := serviceNamed(t, compose, "lakekeeper")
	if lakekeeper.Image != lakekeeperImage {
		t.Fatalf("lakekeeper image = %q, want %s", lakekeeper.Image, lakekeeperImage)
	}
	if !commandContains(lakekeeper.Command, "serve") {
		t.Fatalf("lakekeeper command = %#v, want serve", lakekeeper.Command)
	}
	if !containsString(lakekeeper.Ports, "38181:8181") {
		t.Fatalf("lakekeeper ports = %#v, want 38181:8181", lakekeeper.Ports)
	}
	if _, ok := lakekeeper.DependsOn["lakekeeper-migrate"]; !ok {
		t.Fatalf("lakekeeper depends_on = %#v, want lakekeeper-migrate", lakekeeper.DependsOn)
	}

	wantEnv := map[string]string{
		"LAKEKEEPER__PG_DATABASE_URL_READ":   "postgres://lakekeeper:lakekeeper@lakekeeper-metadata:5432/lakekeeper",
		"LAKEKEEPER__PG_DATABASE_URL_WRITE":  "postgres://lakekeeper:lakekeeper@lakekeeper-metadata:5432/lakekeeper",
		"LAKEKEEPER__PG_ENCRYPTION_KEY":      "duckgres-local-lakekeeper-key-32",
		"LAKEKEEPER__BASE_URI":               "http://localhost:38181",
		"LAKEKEEPER__ENABLE_DEFAULT_PROJECT": "true",
		"LAKEKEEPER__AUTHZ_BACKEND":          "allowall",
	}
	for key, want := range wantEnv {
		if got := envString(lakekeeper.Environment, key); got != want {
			t.Fatalf("lakekeeper %s = %q, want %q", key, got, want)
		}
		if got := envString(migrate.Environment, key); got != want {
			t.Fatalf("lakekeeper-migrate %s = %q, want %q", key, got, want)
		}
	}
}

const lakekeeperImage = "quay.io/lakekeeper/catalog:v0.12.2"

type composeFile struct {
	Services map[string]service `yaml:"services"`
}

type service struct {
	Image       string         `yaml:"image"`
	Command     any            `yaml:"command"`
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

func commandContains(command any, want string) bool {
	switch v := command.(type) {
	case string:
		return v == want
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}
