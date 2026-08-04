package server

import (
	"errors"
	"testing"
)

func TestIsWorkerOutOfMemoryError(t *testing.T) {
	oom := []string{
		// prepare-phase shape (GetFlightInfo LIMIT 0 probe failed):
		"flight execute: rpc error: code = InvalidArgument desc = failed to prepare query: Out of Memory Error: failed to allocate data of size 16.0 MiB (24.9 GiB/25.0 GiB used)",
		// mid-stream shape (rows.Err() from a DoGet chunk):
		"Out of Memory Error: could not allocate block of size 256.0 KiB",
		"failed to allocate data of size 32.0 MiB",
	}
	notOOM := []string{
		"Catalog Error: Table with name t does not exist",
		"context canceled",
		"flight worker is dead",
		"Binder Error: Referenced column x not found",
		"",
	}
	for _, m := range oom {
		if !isWorkerOutOfMemoryError(errors.New(m)) {
			t.Errorf("want OOM: %q", m)
		}
	}
	for _, m := range notOOM {
		if m == "" {
			if isWorkerOutOfMemoryError(nil) {
				t.Error("nil must not be OOM")
			}
			continue
		}
		if isWorkerOutOfMemoryError(errors.New(m)) {
			t.Errorf("must not be OOM: %q", m)
		}
	}
}
