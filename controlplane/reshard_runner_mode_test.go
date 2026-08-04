//go:build kubernetes

package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestFetchReshardPassword pins the runner pod's password pull: success with
// the internal-secret header, a hard stop on 404 (stash gone — the op must
// fail with the clear re-run message, not retry forever), and retry on
// transient 5xx.
func TestFetchReshardPassword(t *testing.T) {
	t.Run("success sends internal secret", func(t *testing.T) {
		var sawSecret atomic.Value
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sawSecret.Store(r.Header.Get("X-Duckgres-Internal-Secret"))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"password":"hunter2"}`))
		}))
		defer srv.Close()
		pw, err := fetchReshardPassword(context.Background(), srv.URL, "sekrit")
		if err != nil || pw != "hunter2" {
			t.Fatalf("fetch = %q/%v", pw, err)
		}
		if sawSecret.Load() != "sekrit" {
			t.Fatalf("internal secret header = %v", sawSecret.Load())
		}
	})

	t.Run("404 is terminal, no retry", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusNotFound)
		}))
		defer srv.Close()
		_, err := fetchReshardPassword(context.Background(), srv.URL, "s")
		if err == nil || !strings.Contains(err.Error(), "404") {
			t.Fatalf("want 404 error, got %v", err)
		}
		if calls.Load() != 1 {
			t.Fatalf("404 retried %d times, want exactly 1 attempt", calls.Load())
		}
	})

	t.Run("transient 5xx retries then succeeds", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) < 2 {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			_, _ = w.Write([]byte(`{"password":"pw"}`))
		}))
		defer srv.Close()
		pw, err := fetchReshardPassword(context.Background(), srv.URL, "s")
		if err != nil || pw != "pw" {
			t.Fatalf("fetch = %q/%v after retry", pw, err)
		}
	})

	t.Run("empty password is an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"password":""}`))
		}))
		defer srv.Close()
		if _, err := fetchReshardPassword(context.Background(), srv.URL, "s"); err == nil {
			t.Fatal("want error for empty password")
		}
	})
}
