package controlplane

import (
	"os"
	"strings"
	"testing"
)

func TestValidateTLSFiles(t *testing.T) {
	if err := validateTLSFiles("cert.pem", "key.pem"); err != nil {
		t.Fatalf("expected valid TLS files, got error: %v", err)
	}

	if err := validateTLSFiles("", "key.pem"); err == nil {
		t.Fatalf("expected error for missing cert file")
	}

	if err := validateTLSFiles("cert.pem", ""); err == nil {
		t.Fatalf("expected error for missing key file")
	}
}

func TestValidateUsers(t *testing.T) {
	tests := []struct {
		name    string
		users   map[string]string
		wantErr bool
	}{
		{
			name: "valid single user",
			users: map[string]string{
				"postgres": "postgres",
			},
			wantErr: false,
		},
		{
			name:    "empty users",
			users:   map[string]string{},
			wantErr: true,
		},
		{
			name: "empty username",
			users: map[string]string{
				"": "secret",
			},
			wantErr: true,
		},
		{
			name: "whitespace username",
			users: map[string]string{
				"   ": "secret",
			},
			wantErr: true,
		},
		{
			name: "empty password",
			users: map[string]string{
				"postgres": "",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUsers(tt.users)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
		})
	}
}

func TestValidateControlPlaneSecurity(t *testing.T) {
	baseCfg := ControlPlaneConfig{}
	baseCfg.TLSCertFile = "cert.pem"
	baseCfg.TLSKeyFile = "key.pem"
	baseCfg.Users = map[string]string{"postgres": "postgres"}

	if err := validateControlPlaneSecurity(baseCfg); err != nil {
		t.Fatalf("expected valid control-plane security config, got: %v", err)
	}

	noTLS := baseCfg
	noTLS.TLSCertFile = ""
	if err := validateControlPlaneSecurity(noTLS); err == nil {
		t.Fatalf("expected error for missing TLS cert")
	}

	noUsers := baseCfg
	noUsers.Users = map[string]string{}
	if err := validateControlPlaneSecurity(noUsers); err == nil {
		t.Fatalf("expected error for missing users")
	}
}

func TestCheckSocketDirWritable(t *testing.T) {
	// Happy path: writable directory
	tmpDir := t.TempDir()
	// Unix socket paths have a max length (~104 bytes on macOS). t.TempDir()
	// paths can be very long, so use a short filename for the check.
	if err := checkSocketDirWritable(tmpDir); err != nil {
		// If t.TempDir is still too long, fallback to /tmp
		if strings.Contains(err.Error(), "invalid argument") || strings.Contains(err.Error(), "too long") {
			shortDir, err2 := os.MkdirTemp("/tmp", "dg-test-*")
			if err2 == nil {
				defer func() { _ = os.RemoveAll(shortDir) }()
				if err3 := checkSocketDirWritable(shortDir); err3 != nil {
					t.Fatalf("expected short directory %s to be writable: %v", shortDir, err3)
				}
				return
			}
		}
		t.Fatalf("expected directory %s to be writable: %v", tmpDir, err)
	}

	// Failure path: non-existent directory
	if err := checkSocketDirWritable("/non/existent/path/for/duckgres/test"); err == nil {
		t.Fatalf("expected error for non-existent directory")
	}

	// Failure path: read-only directory (if we can create one)
	roDir := t.TempDir()
	if err := os.Chmod(roDir, 0555); err != nil {
		t.Fatalf("failed to chmod directory to read-only: %v", err)
	}
	defer func() { _ = os.Chmod(roDir, 0755) }() // Restore for cleanup

	if err := checkSocketDirWritable(roDir); err == nil {
		t.Fatalf("expected error for read-only directory")
	}
}
