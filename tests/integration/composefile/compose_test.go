package composefile

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDockerComposeExcludesLakekeeperStack(t *testing.T) {
	compose := readCompose(t)

	for _, name := range []string{"lakekeeper-metadata", "lakekeeper-migrate", "lakekeeper"} {
		if _, ok := compose.Services[name]; ok {
			t.Fatalf("%s service should not be present", name)
		}
	}
}

type composeFile struct {
	Services map[string]service `yaml:"services"`
}

type service struct {
	Image string `yaml:"image"`
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
