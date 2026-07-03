//go:build cgo && (linux || darwin)

package crashhandler

// Test-only crash triggers. They live in the package (not _test.go files)
// because cgo is not allowed in test files. Nothing in production code calls
// them.

/*
#include <pthread.h>
#include <stddef.h>

static void *duckgres_crashtrigger_thread(void *arg) {
	volatile int *p = NULL;
	*p = 42; // SIGSEGV on a C-created thread
	return NULL;
}

static void duckgres_crashtrigger_c_thread(void) {
	pthread_t t;
	pthread_create(&t, NULL, duckgres_crashtrigger_thread, NULL);
	pthread_join(t, NULL);
}

static void duckgres_crashtrigger_direct(void) {
	volatile int *p = NULL;
	*p = 42; // SIGSEGV inside a cgo call on a Go-created thread
}
*/
import "C"

// TriggerCThreadSegfault crashes with SIGSEGV on a thread created by C code
// (never returns).
func TriggerCThreadSegfault() {
	C.duckgres_crashtrigger_c_thread()
}

// TriggerCgoCallSegfault crashes with SIGSEGV inside a cgo call (never
// returns).
func TriggerCgoCallSegfault() {
	C.duckgres_crashtrigger_direct()
}
