package integration

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestDockerComposeArgsStartsOnlyRequestedServices(t *testing.T) {
	composeFile := filepath.Join("/tmp", "docker-compose.yml")

	got := dockerComposeArgs(composeFile, "up", "-d", "postgres")
	want := []string{"-f", composeFile, "up", "-d", "postgres"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dockerComposeArgs() = %#v, want %#v", got, want)
	}
}

func TestDockerComposeArgsAllowsMultipleServices(t *testing.T) {
	composeFile := filepath.Join("/tmp", "docker-compose.yml")

	got := dockerComposeArgs(composeFile, "up", "-d", "ducklake-metadata", "minio", "minio-init")
	want := []string{"-f", composeFile, "up", "-d", "ducklake-metadata", "minio", "minio-init"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dockerComposeArgs() = %#v, want %#v", got, want)
	}
}

func TestDockerComposeArgsWithoutServices(t *testing.T) {
	composeFile := filepath.Join("/tmp", "docker-compose.yml")

	got := dockerComposeArgs(composeFile, "down", "-v")
	want := []string{"-f", composeFile, "down", "-v"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dockerComposeArgs() = %#v, want %#v", got, want)
	}
}
