package server

import "github.com/posthog/duckgres/server/wire"

// WorkerActivationPayload moved to server/wire so the control plane can use
// it without importing the rest of server. The alias preserves the existing
// server.WorkerActivationPayload spelling for current call sites.
type WorkerActivationPayload = wire.WorkerActivationPayload
