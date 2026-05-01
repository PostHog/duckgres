// Package wire holds duckgres wire-level types and helpers shared between
// the PG protocol layer and the control plane / worker RPC paths. The
// package has no dependency on github.com/duckdb/duckdb-go so callers
// (notably the control plane) can use it without linking libduckdb.
package wire

import (
	"crypto/rand"
	"math/big"
	"time"
)

// BackendKey identifies a backend connection for PostgreSQL cancel requests.
// Pid is the backend process id surfaced to the client; SecretKey is the
// matching secret a cancel request must present.
type BackendKey struct {
	Pid       int32
	SecretKey int32
}

// GenerateSecretKey returns a cryptographically random secret key suitable
// for embedding in a BackendKey. Falls back to a time-based key if
// crypto/rand fails.
func GenerateSecretKey() int32 {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<31))
	if err != nil {
		return int32(time.Now().UnixNano() & 0x7FFFFFFF)
	}
	return int32(n.Int64())
}
