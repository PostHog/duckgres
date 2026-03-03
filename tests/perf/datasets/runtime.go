package datasets

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DatasetVersionEnv = "DUCKGRES_PERF_DATASET_VERSION"
	ManifestPathEnv   = "DUCKGRES_PERF_DATASET_MANIFEST"
)

type Manifest struct {
	Versions []ManifestVersion `yaml:"versions"`
}

type ManifestVersion struct {
	DatasetVersion string `yaml:"dataset_version"`
}

type RuntimeContract struct {
	Frozen         bool
	DatasetVersion string
	ManifestPath   string
}

func ResolveRuntimeContract(defaultManifestPath string) (RuntimeContract, error) {
	version := strings.TrimSpace(os.Getenv(DatasetVersionEnv))
	if version == "" {
		return RuntimeContract{}, nil
	}
	manifestPath := strings.TrimSpace(os.Getenv(ManifestPathEnv))
	if manifestPath == "" {
		manifestPath = defaultManifestPath
	}
	if manifestPath == "" {
		return RuntimeContract{}, fmt.Errorf("%s is required when %s is set", ManifestPathEnv, DatasetVersionEnv)
	}
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		return RuntimeContract{}, err
	}
	if !manifest.HasVersion(version) {
		return RuntimeContract{}, fmt.Errorf("dataset version %q not found in manifest %s", version, manifestPath)
	}
	return RuntimeContract{
		Frozen:         true,
		DatasetVersion: version,
		ManifestPath:   manifestPath,
	}, nil
}

func LoadManifest(path string) (Manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("read dataset manifest %s: %w", path, err)
	}
	var manifest Manifest
	if err := yaml.Unmarshal(b, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("parse dataset manifest %s: %w", path, err)
	}
	if len(manifest.Versions) == 0 {
		return Manifest{}, fmt.Errorf("dataset manifest %s has no versions", path)
	}
	for i, version := range manifest.Versions {
		manifest.Versions[i].DatasetVersion = strings.TrimSpace(version.DatasetVersion)
		if manifest.Versions[i].DatasetVersion == "" {
			return Manifest{}, fmt.Errorf("dataset manifest %s has empty dataset_version entry", path)
		}
	}
	return manifest, nil
}

func (m Manifest) HasVersion(version string) bool {
	version = strings.TrimSpace(version)
	for _, entry := range m.Versions {
		if entry.DatasetVersion == version {
			return true
		}
	}
	return false
}
