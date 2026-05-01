package configloader

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadFile reads a duckgres.yaml file from disk and parses it. Returns the
// caller-friendly error from os.ReadFile (so a missing file is recognizable
// via os.IsNotExist) or a wrapped yaml-parsing error.
func LoadFile(path string) (*FileConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg FileConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	return &cfg, nil
}

// Env returns the value of the named environment variable, falling back to
// defaultVal when unset or empty. Shared by every duckgres binary so the
// override semantics are identical.
func Env(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
