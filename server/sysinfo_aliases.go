package server

import "github.com/posthog/duckgres/server/sysinfo"

// Re-exports kept here so existing references to server.SystemMemoryBytes,
// server.ValidateMemoryLimit, and server.ParseMemoryBytes continue to compile
// after the helpers moved into server/sysinfo. New code should import
// server/sysinfo and use sysinfo.X directly.

var (
	SystemMemoryBytes   = sysinfo.SystemMemoryBytes
	ValidateMemoryLimit = sysinfo.ValidateMemoryLimit
	ParseMemoryBytes    = sysinfo.ParseMemoryBytes
)
