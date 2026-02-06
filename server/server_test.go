package server

import "testing"

func TestParseExtensionName(t *testing.T) {
	tests := []struct {
		input       string
		wantName    string
		wantInstall string
	}{
		{"ducklake", "ducklake", "ducklake"},
		{"httpfs", "httpfs", "httpfs"},
		{"cache_httpfs FROM community", "cache_httpfs", "cache_httpfs FROM community"},
		{"cache_httpfs from community", "cache_httpfs", "cache_httpfs from community"},
		{"cache_httpfs FROM COMMUNITY", "cache_httpfs", "cache_httpfs FROM COMMUNITY"},
		{"my_ext FROM my_repo", "my_ext", "my_ext FROM my_repo"},
		{"ext  FROM  source", "ext", "ext  FROM  source"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			name, installCmd := parseExtensionName(tt.input)
			if name != tt.wantName {
				t.Errorf("parseExtensionName(%q) name = %q, want %q", tt.input, name, tt.wantName)
			}
			if installCmd != tt.wantInstall {
				t.Errorf("parseExtensionName(%q) installCmd = %q, want %q", tt.input, installCmd, tt.wantInstall)
			}
		})
	}
}

func TestHasCacheHTTPFS(t *testing.T) {
	tests := []struct {
		name       string
		extensions []string
		want       bool
	}{
		{"present bare", []string{"ducklake", "cache_httpfs"}, true},
		{"present with source", []string{"ducklake", "cache_httpfs FROM community"}, true},
		{"absent", []string{"ducklake", "httpfs"}, false},
		{"empty list", []string{}, false},
		{"nil list", nil, false},
		{"similar name", []string{"not_cache_httpfs"}, false},
		{"substring match", []string{"cache_httpfs_v2 FROM community"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasCacheHTTPFS(tt.extensions)
			if got != tt.want {
				t.Errorf("hasCacheHTTPFS(%v) = %v, want %v", tt.extensions, got, tt.want)
			}
		})
	}
}
