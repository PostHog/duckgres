package server

import "github.com/posthog/duckgres/server/wire"

// Aliases retained so existing references to server.WorkerControlMetadata,
// server.WorkerCreateSessionPayload, server.WorkerDestroySessionPayload and
// server.WorkerHealthCheckPayload continue to compile after the types
// moved to server/wire. New code should import server/wire and use
// wire.X directly.

type (
	WorkerControlMetadata       = wire.WorkerControlMetadata
	WorkerCreateSessionPayload  = wire.WorkerCreateSessionPayload
	WorkerDestroySessionPayload = wire.WorkerDestroySessionPayload
	WorkerHealthCheckPayload    = wire.WorkerHealthCheckPayload
)
