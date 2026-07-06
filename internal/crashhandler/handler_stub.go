//go:build !cgo || (!linux && !darwin)

package crashhandler

func installed() bool { return false }
