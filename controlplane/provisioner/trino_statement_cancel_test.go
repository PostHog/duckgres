//go:build kubernetes

package provisioner

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/posthog/duckgres/controlplane/provisioner/opa"
)

// statementFake is a coordinator whose SHOW CATALOGS stays queued: every GET
// on the nextUri blocks until the request context ends. It records the
// DELETEs that cancel the statement.
type statementFake struct {
	server *httptest.Server

	mu      sync.Mutex
	deletes []string
	auth    []bool
}

func newStatementFake(t *testing.T, terminal string) *statementFake {
	t.Helper()
	f := &statementFake{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			_, _ = fmt.Fprintf(w, `{"nextUri":%q}`, f.server.URL+"/v1/statement/queued/q1/token/1")
		case http.MethodGet:
			if terminal != "" {
				_, _ = fmt.Fprint(w, terminal)
				return
			}
			<-r.Context().Done()
		case http.MethodDelete:
			user, password, ok := r.BasicAuth()
			f.mu.Lock()
			f.deletes = append(f.deletes, r.URL.Path)
			f.auth = append(f.auth, ok && user == opa.AdminPrincipal && password == "test-password" && r.Header.Get("X-Trino-Source") == TrinoProvisionerSource)
			f.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

func (f *statementFake) cancels() ([]string, []bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deletes...), append([]bool(nil), f.auth...)
}

// A reconcile whose deadline expires mid-drain must cancel the statement.
// Otherwise the query keeps a slot in the provisioner's resource group until
// Trino abandons it, and every replica's next tick queues behind it.
func TestTrinoStatementDrainCancelsAbandonedStatement(t *testing.T) {
	fake := newStatementFake(t, "")
	client := NewTrinoCatalogHTTPClient(fake.server.URL, opa.AdminPrincipal, "test-password", "").(*trinoCatalogHTTPClient)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := client.ListCatalogs(ctx); err == nil {
		t.Fatal("ListCatalogs succeeded against a statement that never completes")
	}

	deletes, auth := fake.cancels()
	if len(deletes) != 1 || deletes[0] != "/v1/statement/queued/q1/token/1" {
		t.Fatalf("cancel requests = %v, want one DELETE on the pending nextUri", deletes)
	}
	if !auth[0] {
		t.Error("cancel request did not carry the provisioner credentials and source")
	}
}

// A statement Trino already finished or failed is terminal: nothing to cancel.
func TestTrinoStatementDrainDoesNotCancelTerminalStatements(t *testing.T) {
	for name, terminal := range map[string]string{
		"finished": `{"data":[["system"]]}`,
		"failed":   `{"error":{"errorName":"CATALOG_NOT_FOUND","errorType":"USER_ERROR","message":"nope"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			fake := newStatementFake(t, terminal)
			client := NewTrinoCatalogHTTPClient(fake.server.URL, opa.AdminPrincipal, "test-password", "").(*trinoCatalogHTTPClient)
			_, _ = client.ListCatalogs(context.Background())
			if deletes, _ := fake.cancels(); len(deletes) != 0 {
				t.Fatalf("cancel requests = %v, want none for a terminal statement", deletes)
			}
		})
	}
}
