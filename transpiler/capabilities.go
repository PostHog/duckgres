package transpiler

import "github.com/posthog/duckgres/transpiler/backend"

// StorageBackend is kept as a compatibility alias for callers selecting the
// transpiler backend.
type StorageBackend = backend.Name

const (
	BackendMemory   = backend.Memory
	BackendDuckLake = backend.DuckLake
)
