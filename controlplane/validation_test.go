package controlplane

import "testing"

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
