// Native fatal-signal handler: print a marker + native backtrace to stderr,
// then die with the original signal. Installed from a C constructor so it runs
// BEFORE the Go runtime registers its own handlers — the runtime then saves it
// as the pre-existing handler and forwards non-Go faults to it (sigfwdgo),
// while faults in Go code keep producing ordinary Go panics.
//
// Everything in the handler is async-signal-safe: write(2), backtrace(3),
// backtrace_symbols_fd(3), sigaction(2), raise(3).

#include <execinfo.h>
#include <signal.h>
#include <stdbool.h>
#include <string.h>
#include <unistd.h>

static volatile bool g_installed = false;

static void crash_write(const char *s) {
	ssize_t unused = write(STDERR_FILENO, s, strlen(s));
	(void)unused;
}

static const char *crash_signame(int sig) {
	switch (sig) {
	case SIGSEGV: return "SIGSEGV";
	case SIGBUS:  return "SIGBUS";
	case SIGILL:  return "SIGILL";
	case SIGFPE:  return "SIGFPE";
	case SIGABRT: return "SIGABRT";
	default:      return "signal";
	}
}

static void duckgres_crash_handler(int sig, siginfo_t *info, void *uctx) {
	(void)info;
	(void)uctx;

	// Keep this prefix in sync with crashhandler.Marker.
	crash_write("\nduckgres: fatal native signal ");
	crash_write(crash_signame(sig));
	crash_write(" on native thread - backtrace of crashed thread:\n");

	void *bt[64];
	int n = backtrace(bt, 64);
	backtrace_symbols_fd(bt, n, STDERR_FILENO);

	crash_write("duckgres: terminating with default signal disposition\n");

	// Restore the default disposition and re-raise so the process dies with
	// the original signal (and dumps core where enabled) instead of wedging.
	struct sigaction dfl;
	memset(&dfl, 0, sizeof(dfl));
	dfl.sa_handler = SIG_DFL;
	sigemptyset(&dfl.sa_mask);
	sigaction(sig, &dfl, NULL);
	raise(sig);
	// If the signal is blocked during handler execution, returning re-executes
	// the faulting instruction and the (now default) action fires then.
}

__attribute__((constructor)) static void duckgres_crashhandler_install(void) {
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_sigaction = duckgres_crash_handler;
	// SA_ONSTACK: the Go runtime forwards signals while running on the
	// per-thread alternate signal stack; the handler must be willing to run
	// there.
	sa.sa_flags = SA_SIGINFO | SA_ONSTACK;
	sigemptyset(&sa.sa_mask);

	const int sigs[] = {SIGSEGV, SIGBUS, SIGILL, SIGFPE, SIGABRT};
	for (size_t i = 0; i < sizeof(sigs) / sizeof(sigs[0]); i++) {
		sigaction(sigs[i], &sa, NULL);
	}
	g_installed = true;
}

bool duckgres_crashhandler_installed(void) { return g_installed; }
