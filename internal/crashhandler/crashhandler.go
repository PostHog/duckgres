// Package crashhandler makes native (C/C++) crashes loud instead of silent.
//
// Motivation: duckgres links DuckDB via cgo. A SIGSEGV on a thread the Go
// runtime does not own (e.g. a DuckDB TaskScheduler thread crashing inside an
// extension) is routed through the Go runtime's badsignal path, which can
// leave the process alive but wedged: the crashed thread never releases its
// locks, and every thread that later touches them blocks forever. In
// production this turned a postgres_scanner segfault into a query that ran
// for 30+ hours pinning a worker pod, with no log line anywhere.
//
// Importing this package installs (via a C constructor, before the Go runtime
// initializes its own signal handlers) a native handler for fatal signals that
//
//  1. writes a recognizable marker plus a native backtrace to stderr
//     (async-signal-safe: write(2)/backtrace_symbols_fd only), and
//  2. restores the default disposition and re-raises, so the process dies
//     with the original signal instead of wedging.
//
// Because the handler is registered before the Go runtime's, the runtime
// saves it as the "previous" handler and forwards non-Go faults to it
// (runtime.sigfwdgo). Faults raised by Go code keep producing ordinary Go
// panics — the handler never sees them.
package crashhandler

// Marker is the first bytes of the crash report written to stderr. Log
// pipelines can alert on it verbatim.
const Marker = "duckgres: fatal native signal"

// Installed reports whether the native crash handler is active.
func Installed() bool { return installed() }
