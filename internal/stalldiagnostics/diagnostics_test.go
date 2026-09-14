package stalldiagnostics

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteDumpPrivateAndRejectExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stack.txt")
	if err := writeDump(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("permissions: %v", info.Mode())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "goroutine") {
		t.Fatal("missing goroutine stack")
	}
	if err := writeDump(path); err == nil {
		t.Fatal("must reject existing files, including symlinks")
	}
}
