package opa

import "net/http"

// allowAllForTest is the package-local permissive Auth used by tests that
// aren't exercising the auth path. Lives in a _test.go file so it isn't
// part of the production API surface -- production callers must build a
// real Auth function (typically BearerTokenAuth(secret)).
func allowAllForTest(*http.Request) bool { return true }
