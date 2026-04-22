package server

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestSeedBundledExtensionsCopiesMissingFiles(t *testing.T) {
	srcRoot := t.TempDir()
	dstRoot := t.TempDir()

	srcDir := filepath.Join(srcRoot, "v1.5.2", "linux_arm64")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	srcExt := filepath.Join(srcDir, "httpfs.duckdb_extension")
	srcInfo := filepath.Join(srcDir, "httpfs.duckdb_extension.info")
	if err := os.WriteFile(srcExt, []byte("extension-bytes"), 0o644); err != nil {
		t.Fatalf("write src extension: %v", err)
	}
	if err := os.WriteFile(srcInfo, []byte("extension-info"), 0o644); err != nil {
		t.Fatalf("write src info: %v", err)
	}

	if err := seedBundledExtensions(srcRoot, dstRoot); err != nil {
		t.Fatalf("seedBundledExtensions: %v", err)
	}

	for _, rel := range []string{
		filepath.Join("v1.5.2", "linux_arm64", "httpfs.duckdb_extension"),
		filepath.Join("v1.5.2", "linux_arm64", "httpfs.duckdb_extension.info"),
	} {
		got, err := os.ReadFile(filepath.Join(dstRoot, rel))
		if err != nil {
			t.Fatalf("read copied %s: %v", rel, err)
		}
		if len(got) == 0 {
			t.Fatalf("expected copied %s to be non-empty", rel)
		}
	}
}

func TestSeedBundledExtensionsPreservesExistingFilesWithMatchingContents(t *testing.T) {
	srcRoot := t.TempDir()
	dstRoot := t.TempDir()

	srcDir := filepath.Join(srcRoot, "v1.5.2", "linux_arm64")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "httpfs.duckdb_extension"), []byte("existing"), 0o644); err != nil {
		t.Fatalf("write src extension: %v", err)
	}

	dstDir := filepath.Join(dstRoot, "v1.5.2", "linux_arm64")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}
	dstExt := filepath.Join(dstDir, "httpfs.duckdb_extension")
	if err := os.WriteFile(dstExt, []byte("existing"), 0o644); err != nil {
		t.Fatalf("write dst extension: %v", err)
	}

	if err := seedBundledExtensions(srcRoot, dstRoot); err != nil {
		t.Fatalf("seedBundledExtensions: %v", err)
	}

	got, err := os.ReadFile(dstExt)
	if err != nil {
		t.Fatalf("read dst extension: %v", err)
	}
	if string(got) != "existing" {
		t.Fatalf("expected existing extension to be preserved, got %q", string(got))
	}
}

func TestSeedBundledExtensionsReplacesExistingFilesWithUpdatedContents(t *testing.T) {
	srcRoot := t.TempDir()
	dstRoot := t.TempDir()

	srcDir := filepath.Join(srcRoot, "v1.5.2", "linux_arm64")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	srcExt := filepath.Join(srcDir, "postgres_scanner.duckdb_extension")
	if err := os.WriteFile(srcExt, []byte("nightly"), 0o644); err != nil {
		t.Fatalf("write src extension: %v", err)
	}

	dstDir := filepath.Join(dstRoot, "v1.5.2", "linux_arm64")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}
	dstExt := filepath.Join(dstDir, "postgres_scanner.duckdb_extension")
	if err := os.WriteFile(dstExt, []byte("stable"), 0o644); err != nil {
		t.Fatalf("write dst extension: %v", err)
	}

	if err := seedBundledExtensions(srcRoot, dstRoot); err != nil {
		t.Fatalf("seedBundledExtensions: %v", err)
	}

	got, err := os.ReadFile(dstExt)
	if err != nil {
		t.Fatalf("read dst extension: %v", err)
	}
	if string(got) != "nightly" {
		t.Fatalf("expected existing extension to be replaced, got %q", string(got))
	}
}

func TestSeedBundledExtensionsPreservesNonTargetedChangedFiles(t *testing.T) {
	srcRoot := t.TempDir()
	dstRoot := t.TempDir()

	srcDir := filepath.Join(srcRoot, "v1.5.2", "linux_arm64")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	srcExt := filepath.Join(srcDir, "httpfs.duckdb_extension")
	if err := os.WriteFile(srcExt, []byte("new-httpfs"), 0o644); err != nil {
		t.Fatalf("write src extension: %v", err)
	}

	dstDir := filepath.Join(dstRoot, "v1.5.2", "linux_arm64")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}
	dstExt := filepath.Join(dstDir, "httpfs.duckdb_extension")
	if err := os.WriteFile(dstExt, []byte("existing-httpfs"), 0o644); err != nil {
		t.Fatalf("write dst extension: %v", err)
	}

	if err := seedBundledExtensions(srcRoot, dstRoot); err != nil {
		t.Fatalf("seedBundledExtensions: %v", err)
	}

	got, err := os.ReadFile(dstExt)
	if err != nil {
		t.Fatalf("read dst extension: %v", err)
	}
	if string(got) != "existing-httpfs" {
		t.Fatalf("expected non-targeted extension to be preserved, got %q", string(got))
	}
}

func TestConfigureExtensionDirectorySeedsBundledExtensions(t *testing.T) {
	bundledRoot := t.TempDir()
	extDir := t.TempDir()

	srcDir := filepath.Join(bundledRoot, "v1.5.2", "linux_arm64")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	srcExt := filepath.Join(srcDir, "postgres_scanner.duckdb_extension")
	if err := os.WriteFile(srcExt, []byte("nightly"), 0o644); err != nil {
		t.Fatalf("write src extension: %v", err)
	}

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := configureExtensionDirectory(db, bundledRoot, extDir, "test"); err != nil {
		t.Fatalf("configureExtensionDirectory: %v", err)
	}

	dstExt := filepath.Join(extDir, "v1.5.2", "linux_arm64", "postgres_scanner.duckdb_extension")
	got, err := os.ReadFile(dstExt)
	if err != nil {
		t.Fatalf("read dst extension: %v", err)
	}
	if string(got) != "nightly" {
		t.Fatalf("expected seeded extension to match bundled contents, got %q", string(got))
	}
}

func TestConfigureExtensionDirectorySetsDuckDBExtensionDirectory(t *testing.T) {
	bundledRoot := t.TempDir()
	extDir := t.TempDir()

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := configureExtensionDirectory(db, bundledRoot, extDir, "test"); err != nil {
		t.Fatalf("configureExtensionDirectory: %v", err)
	}

	var got string
	row := db.QueryRow("SELECT current_setting('extension_directory')")
	if err := row.Scan(&got); err != nil {
		t.Fatalf("scan extension_directory: %v", err)
	}
	if got != extDir {
		t.Fatalf("extension_directory = %q, want %q", got, extDir)
	}
}
