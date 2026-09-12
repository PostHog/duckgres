//go:build kubernetes

package provisioner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/posthog/duckgres/controlplane/provisioner/opa"
)

func TestTrinoListNodesReadsAllPages(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != opa.AdminPrincipal || password != "test-password" || r.Header.Get("X-Trino-Source") != TrinoProvisionerSource {
			t.Error("node inventory request did not retain provisioner authentication and source")
		}
		if r.Method == http.MethodPost {
			body, _ := io.ReadAll(r.Body)
			if string(body) != "SELECT node_id, http_uri, coordinator, state FROM system.runtime.nodes" {
				t.Errorf("unexpected inventory SQL: %s", body)
			}
			_, _ = fmt.Fprintf(w, `{"data":[["coordinator","https://192.0.2.1:8443",true,"active"]],"nextUri":%q}`, server.URL+"/next")
			return
		}
		_, _ = fmt.Fprint(w, `{"data":[["worker","http://192.0.2.2:8080",false,"active"]]}`)
	}))
	defer server.Close()
	client := NewTrinoCatalogHTTPClient(server.URL, opa.AdminPrincipal, "test-password", "").(*trinoCatalogHTTPClient)
	nodes, err := client.ListNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []TrinoNode{
		{ID: "coordinator", URI: "https://192.0.2.1:8443", Coordinator: true, State: "active"},
		{ID: "worker", URI: "http://192.0.2.2:8080", State: "active"},
	}
	if !reflect.DeepEqual(nodes, want) {
		t.Fatalf("nodes = %+v, want %+v", nodes, want)
	}
}

func TestTrinoListNodesRejectsIncompleteInventory(t *testing.T) {
	valid := []any{"worker", "http://192.0.2.2:8080", false, "active"}
	tests := []struct {
		name string
		row  []any
	}{
		{"short row", []any{"other", "http://192.0.2.3:8080", false}},
		{"extra column", []any{"other", "http://192.0.2.3:8080", false, "active", "extra"}},
		{"null ID", []any{nil, "http://192.0.2.3:8080", false, "active"}},
		{"blank ID", []any{" ", "http://192.0.2.3:8080", false, "active"}},
		{"nonstring URI", []any{"other", 123, false, "active"}},
		{"relative URI", []any{"other", "/worker", false, "active"}},
		{"unsupported URI", []any{"other", "file:///worker", false, "active"}},
		{"missing host", []any{"other", "http://:8080", false, "active"}},
		{"invalid port", []any{"other", "http://192.0.2.3:wrong", false, "active"}},
		{"URI credentials", []any{"other", "http://user:password@192.0.2.3:8080", false, "active"}},
		{"URI query", []any{"other", "http://192.0.2.3:8080?x=y", false, "active"}},
		{"URI fragment", []any{"other", "http://192.0.2.3:8080#fragment", false, "active"}},
		{"null coordinator", []any{"other", "http://192.0.2.3:8080", nil, "active"}},
		{"string coordinator", []any{"other", "http://192.0.2.3:8080", "false", "active"}},
		{"null state", []any{"other", "http://192.0.2.3:8080", false, nil}},
		{"unknown state", []any{"other", "http://192.0.2.3:8080", false, "UNKNOWN"}},
		{"duplicate ID", []any{"worker", "http://192.0.2.3:8080", false, "active"}},
		{"duplicate URI", []any{"other", "http://192.0.2.2:8080", false, "active"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"data": [][]any{valid, test.row}})
			}))
			defer server.Close()
			client := NewTrinoCatalogHTTPClient(server.URL, "", "", "").(*trinoCatalogHTTPClient)
			nodes, err := client.ListNodes(context.Background())
			if err == nil || nodes != nil {
				t.Fatalf("invalid inventory must fail without partial results: nodes=%+v err=%v", nodes, err)
			}
		})
	}
}

func TestTrinoListNodesPreservesEmptyAndInactiveInventory(t *testing.T) {
	for _, state := range []string{"", "inactive", "draining", "drained", "shutting_down"} {
		t.Run(state, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if state == "" {
					_, _ = fmt.Fprint(w, `{"data":[]}`)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"data": [][]any{{"worker", "http://[2001:db8::1]:8080", false, state}}})
			}))
			defer server.Close()
			client := NewTrinoCatalogHTTPClient(server.URL, "", "", "").(*trinoCatalogHTTPClient)
			nodes, err := client.ListNodes(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if state == "" {
				if len(nodes) != 0 {
					t.Fatalf("empty inventory returned %+v", nodes)
				}
			} else if len(nodes) != 1 || nodes[0].State != state {
				t.Fatalf("inactive inventory was discarded: %+v", nodes)
			}
		})
	}
}

func TestTrinoListNodesPropagatesQueryFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"error":{"errorName":"PERMISSION_DENIED","errorType":"USER_ERROR","message":"node inventory denied"}}`)
	}))
	defer server.Close()
	client := NewTrinoCatalogHTTPClient(server.URL, "", "", "").(*trinoCatalogHTTPClient)
	nodes, err := client.ListNodes(context.Background())
	if nodes != nil || err == nil || !strings.Contains(err.Error(), "PERMISSION_DENIED") {
		t.Fatalf("query failure was not preserved: nodes=%+v err=%v", nodes, err)
	}
}
