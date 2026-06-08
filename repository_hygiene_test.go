package main

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestRepositoryDoesNotContainForbiddenToken(t *testing.T) {
	token := os.Getenv("DUCKGRES_FORBIDDEN_REPO_TOKEN")
	if token == "" {
		t.Skip("DUCKGRES_FORBIDDEN_REPO_TOKEN is unset")
	}

	var matches []string
	files, err := exec.Command("git", "ls-files", "-z").Output()
	if err != nil {
		t.Fatalf("list tracked files: %v", err)
	}

	for _, file := range bytes.Split(files, []byte{0}) {
		if len(file) == 0 {
			continue
		}
		path := string(file)
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(bytes.ToLower(content), []byte(strings.ToLower(token))) {
			matches = append(matches, path)
		}
	}
	if len(matches) > 0 {
		t.Fatalf("forbidden repository token found in %s", strings.Join(matches, ", "))
	}
}
