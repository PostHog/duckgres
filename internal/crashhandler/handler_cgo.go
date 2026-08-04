//go:build cgo && (linux || darwin)

package crashhandler

/*
#include <stdbool.h>

extern bool duckgres_crashhandler_installed(void);
*/
import "C"

func installed() bool { return bool(C.duckgres_crashhandler_installed()) }
