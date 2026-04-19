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

func TestDockerComposeCommandUsesDockerCLIPlugin(t *testing.T) {
	composeFile := filepath.Join("/tmp", "docker-compose.yml")

	gotName, gotArgs := dockerComposeCommand(composeFile, "up", "-d", "ducklake-metadata")
	wantArgs := []string{"compose", "-f", composeFile, "up", "-d", "ducklake-metadata"}
	if gotName != "docker" {
		t.Fatalf("dockerComposeCommand() name = %q, want %q", gotName, "docker")
	}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Fatalf("dockerComposeCommand() args = %#v, want %#v", gotArgs, wantArgs)
	}
}

func TestIsEphemeralPostgresRuntimeSchema(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   bool
	}{
		{name: "control plane runtime", schema: "cp_runtime", want: true},
		{name: "managed warehouse runtime", schema: "managed_warehouse_1775493609032883000_runtime", want: true},
		{name: "fixture schema", schema: "test_schema", want: false},
		{name: "public schema", schema: "public", want: false},
		{name: "base managed warehouse schema", schema: "managed_warehouse_1775493609032883000", want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := isEphemeralPostgresRuntimeSchema(tt.schema); got != tt.want {
				t.Fatalf("isEphemeralPostgresRuntimeSchema(%q) = %v, want %v", tt.schema, got, tt.want)
			}
		})
	}
}
