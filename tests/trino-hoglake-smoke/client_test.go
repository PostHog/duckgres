package trino_hoglake_smoke

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQueryRejectsCredentialRedirects(t *testing.T) {
	for _, mode := range []string{"redirect", "continuation"} {
		t.Run(mode, func(t *testing.T) {
			leaked := false
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				leaked = true
				_, _ = w.Write([]byte(`{}`))
			}))
			defer target.Close()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "redirect" {
					http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
					return
				}
				_ = json.NewEncoder(w).Encode(map[string]string{"nextUri": target.URL + "/next"})
			}))
			defer server.Close()
			client, err := newSmokeClient(server.URL, "test-user", "test-password")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.query(context.Background(), "SELECT 1"); err == nil {
				t.Fatal("query accepted a redirect outside the configured origin")
			}
			if leaked {
				t.Fatal("query sent a request to a different origin")
			}
		})
	}
}

func TestQueryPreservesTenantIdentityAcrossPages(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != "test-user" || password != "test-password" || r.Header.Get("X-Trino-User") != user || r.Header.Get("X-Trino-Routing-Group") != "test-cell" {
			t.Error("query lost credentials or routing identity")
		}
		if r.URL.Path == "/v1/statement" {
			_, _ = fmt.Fprintf(w, `{"nextUri":%q}`, server.URL+"/next")
			return
		}
		_, _ = w.Write([]byte(`{"data":[[1,"123456789012345678901234567890.12"]]}`))
	}))
	defer server.Close()
	client, err := newSmokeClient(server.URL, "test-user", "test-password")
	if err != nil {
		t.Fatal(err)
	}
	client.routingGroup = "test-cell"
	rows, err := client.query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0] != json.Number("1") || rows[0][1] != "123456789012345678901234567890.12" {
		t.Fatalf("unexpected rows: %v", rows)
	}
}

func TestSmokeClientRejectsUnsafeURLs(t *testing.T) {
	for _, server := range []string{"http://example.test", "https://user:password@example.test", "https://example.test?token=secret", "https://example.test#fragment"} {
		if _, err := newSmokeClient(server, "test-user", "test-password"); err == nil {
			t.Fatalf("accepted unsafe URL %s", strings.Split(server, ":")[0])
		}
	}
}
