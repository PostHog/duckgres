package server

import "github.com/posthog/duckgres/server/observe"

// Tracer re-exports observe.Tracer for callers that previously imported it
// from this package. The tracing helpers, profiling-output enrichment, and
// connection counters all live in server/observe; new code should import
// that package directly.
var Tracer = observe.Tracer
