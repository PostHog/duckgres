package transpiler

import "github.com/posthog/duckgres/transpiler/transform"

// StorageBackend re-exports transform.StorageBackend so callers outside the
// transpiler tree (e.g. the server package) can select a backend without
// importing the internal transform package.
type StorageBackend = transform.StorageBackend

const (
	BackendMemory   = transform.BackendMemory
	BackendDuckLake = transform.BackendDuckLake
	BackendIceberg  = transform.BackendIceberg
)
