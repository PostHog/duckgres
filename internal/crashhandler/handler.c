// Native fatal-signal handler: print a marker + native backtrace to stderr,
// then die with the original signal. Installed from a C constructor so it runs
// BEFORE the Go runtime registers its own handlers — the runtime then saves it
// as the pre-existing handler and forwards non-Go faults to it (sigfwdgo),
// while faults in Go code keep producing ordinary Go panics.
//
// Everything in the handler is async-signal-safe: write(2), backtrace(3),
// backtrace_symbols_fd(3), sigaction(2), raise(3).

#ifdef __linux__
#define _GNU_SOURCE // REG_RIP in <ucontext.h>
#endif
#ifdef __APPLE__
#define _XOPEN_SOURCE 700 // ucontext_t register access
#define _DARWIN_C_SOURCE  // keep SA_ONSTACK etc visible alongside _XOPEN_SOURCE
#endif

#include <execinfo.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <ucontext.h>
#include <unistd.h>

static volatile bool g_installed = false;

static void crash_write(const char *s) {
	ssize_t unused = write(STDERR_FILENO, s, strlen(s));
	(void)unused;
}

static void crash_write_hex(uintptr_t v) {
	char buf[2 + 2 * sizeof(v)];
	size_t n = 0;
	buf[n++] = '0';
	buf[n++] = 'x';
	bool started = false;
	for (int shift = 8 * (int)sizeof(v) - 4; shift >= 0; shift -= 4) {
		unsigned d = (unsigned)((v >> shift) & 0xf);
		if (!started && d == 0 && shift != 0) {
			continue;
		}
		started = true;
		buf[n++] = "0123456789abcdef"[d];
	}
	ssize_t unused = write(STDERR_FILENO, buf, n);
	(void)unused;
}

// crash_pc extracts the faulting instruction pointer from the signal context
// where we know the layout; 0 elsewhere. Even a shallow backtrace plus this PC
// is enough to addr2line the crash site.
static uintptr_t crash_pc(void *uctx) {
	if (uctx == NULL) {
		return 0;
	}
	ucontext_t *uc = (ucontext_t *)uctx;
#if defined(__linux__) && defined(__x86_64__)
	return (uintptr_t)uc->uc_mcontext.gregs[REG_RIP];
#elif defined(__linux__) && defined(__aarch64__)
	return (uintptr_t)uc->uc_mcontext.pc;
#elif defined(__APPLE__) && defined(__aarch64__)
	return (uintptr_t)uc->uc_mcontext->__ss.__pc;
#elif defined(__APPLE__) && defined(__x86_64__)
	return (uintptr_t)uc->uc_mcontext->__ss.__rip;
#else
	(void)uc;
	return 0;
#endif
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
	// Keep this prefix in sync with crashhandler.Marker.
	crash_write("\nduckgres: fatal native signal ");
	crash_write(crash_signame(sig));
	crash_write(" pc ");
	crash_write_hex(crash_pc(uctx));
	crash_write(" fault addr ");
	crash_write_hex(info != NULL ? (uintptr_t)info->si_addr : 0);
	crash_write(" - backtrace of crashed thread:\n");

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
