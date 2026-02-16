package controlplane

import (
	"encoding/base64"
	"testing"
)

func TestParseBasicCredentials(t *testing.T) {
	token := base64.StdEncoding.EncodeToString([]byte("postgres:postgres"))
	user, pass, err := parseBasicCredentials("Basic " + token)
	if err != nil {
		t.Fatalf("parseBasicCredentials returned error: %v", err)
	}
	if user != "postgres" {
		t.Fatalf("expected username postgres, got %q", user)
	}
	if pass != "postgres" {
		t.Fatalf("expected password postgres, got %q", pass)
	}
}

func TestParseBasicCredentialsInvalid(t *testing.T) {
	tests := []string{
		"",
		"Bearer token",
		"Basic !!!",
		"Basic " + base64.StdEncoding.EncodeToString([]byte("nousersep")),
	}

	for _, input := range tests {
		if _, _, err := parseBasicCredentials(input); err == nil {
			t.Fatalf("expected parseBasicCredentials(%q) to fail", input)
		}
	}
}

func TestSupportsLimit(t *testing.T) {
	if !supportsLimit("SELECT 1") {
		t.Fatalf("expected SELECT to support LIMIT")
	}
	if supportsLimit("SHOW TABLES") {
		t.Fatalf("expected SHOW to not support LIMIT")
	}
}
