package server

import "github.com/posthog/duckgres/server/sqlcore"

// Re-exports kept here so existing references to server.IsEmptyQuery and
// server.OTELGRPCClientHandler continue to compile after the helpers
// moved into server/sqlcore. The sqlcore package is duckdb-free; new code
// (notably the Flight client) should import server/sqlcore directly.

var (
	IsEmptyQuery          = sqlcore.IsEmptyQuery
	OTELGRPCClientHandler = sqlcore.OTELGRPCClientHandler
)
