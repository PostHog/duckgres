//go:build kubernetes

package provisioner

import (
	"context"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const sharedTestQueryID = "20260101_000000_00001_abcde"

func TestSharedCatalogCreateRendersUnquotedConnector(t *testing.T) {
	for _, connector := range []string{"ducklake", "hoglake", "example_2"} {
		t.Run(connector, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				want := `CREATE CATALOG "org_quoted""name" USING ` + connector + ` WITH ("example.property" = 'it''s a value')`
				if r.Method != http.MethodPost || r.URL.Path != "/v1/statement" || string(body) != want {
					t.Errorf("unexpected catalog request: %s %s %q; want %q", r.Method, r.URL.Path, body, want)
				}
				_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"FINISHED"}}`, sharedTestQueryID)
			}))
			defer server.Close()
			props := map[string]string{"connector.name": connector, "example.property": "it's a value"}
			if err := sharedTestClient(t, server).CreateCatalog(context.Background(), `org_quoted"name`, props); err != nil {
				t.Fatal(err)
			}
			if requests.Load() != 1 || len(props) != 2 || props["connector.name"] != connector {
				t.Fatal("catalog creation changed the properties or request count")
			}
		})
	}
}

func TestSharedCatalogCreateRejectsInvalidConnectorBeforeHTTP(t *testing.T) {
	for _, connector := range []string{"", "DuckLake", " ducklake", "ducklake ", "ducklake\n", "1ducklake", "_ducklake", "duck-lake", "duck.lake", `"ducklake"`, "ducklake; DROP CATALOG example", "ducklake/*comment*/", "ducklàke", "ducklake\x00"} {
		t.Run(fmt.Sprintf("connector_%q", connector), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"FINISHED"}}`, sharedTestQueryID)
			}))
			defer server.Close()
			err := sharedTestClient(t, server).CreateCatalog(context.Background(), "org_example", map[string]string{"connector.name": connector})
			if err == nil || !TrinoCatalogOutcomeTerminal(err) || requests.Load() != 0 {
				t.Fatalf("invalid connector must fail locally with a terminal outcome: err=%v requests=%d", err, requests.Load())
			}
		})
	}
}

func TestSharedCatalogStatementRequiresTerminalProof(t *testing.T) {
	for _, tc := range []struct {
		name, body        string
		terminal, success bool
	}{
		{"finished", `{"id":"` + sharedTestQueryID + `","stats":{"state":"FINISHED"}}`, true, true},
		{"failed", `{"id":"` + sharedTestQueryID + `","stats":{"state":"FAILED"},"error":{"errorName":"INVALID_CATALOG_PROPERTY","message":"private-value"}}`, false, false},
		{"empty", `{}`, false, false},
		{"truncated", `{"id":`, false, false},
		{"running_without_next", `{"id":"` + sharedTestQueryID + `","stats":{"state":"RUNNING"}}`, false, false},
		{"failed_without_error", `{"id":"` + sharedTestQueryID + `","stats":{"state":"FAILED"}}`, false, false},
		{"error_without_terminal_state", `{"id":"` + sharedTestQueryID + `","error":{"errorName":"UNKNOWN"}}`, false, false},
		{"no_query_identity", `{"stats":{"state":"FINISHED"}}`, false, false},
		{"oversized", strings.Repeat(" ", sharedTrinoMaxPageBytes+1), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, tc.body) }))
			defer server.Close()
			client := sharedTestClient(t, server)
			_, err := client.runStatement(context.Background(), "CREATE CATALOG example USING example")
			if (err == nil) != tc.success || TrinoCatalogOutcomeTerminal(err) != tc.terminal {
				t.Fatalf("success=%v terminal=%v, expected %v/%v", err == nil, TrinoCatalogOutcomeTerminal(err), tc.success, tc.terminal)
			}
			if err != nil && strings.Contains(err.Error(), "private-value") {
				t.Fatal("response contents leaked into error")
			}
		})
	}
}

func TestSharedCatalogContinuationOriginAndIdentity(t *testing.T) {
	var foreignCalls atomic.Int32
	foreign := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { foreignCalls.Add(1) }))
	defer foreign.Close()
	for _, mode := range []string{"foreign", "redirect", "changed_id", "credentials_rotated", "wrong_path", "unknown_state"} {
		t.Run(mode, func(t *testing.T) {
			var server *httptest.Server
			var client *trinoSharedCatalogHTTPClient
			var requests atomic.Int32
			server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				user, password, _ := r.BasicAuth()
				if user != "provisioner" || password != "original" {
					t.Error("statement credentials changed between pages")
				}
				if r.Method == http.MethodPost {
					if mode == "redirect" {
						http.Redirect(w, r, foreign.URL, http.StatusTemporaryRedirect)
						return
					}
					next := server.URL + "/v1/statement/executing/" + sharedTestQueryID + "/token/1"
					if mode == "foreign" {
						next = foreign.URL + "/v1/statement/executing/" + sharedTestQueryID + "/token/1"
					}
					if mode == "wrong_path" {
						next = server.URL + "/admin"
					}
					if mode == "credentials_rotated" {
						client.SetCredentials("replacement", "replacement")
					}
					state := "RUNNING"
					if mode == "unknown_state" {
						state = "UNKNOWN"
					}
					_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":%q},"nextUri":%q}`, sharedTestQueryID, state, next)
					return
				}
				id := sharedTestQueryID
				if mode == "changed_id" {
					id = "20260101_000000_00002_abcde"
				}
				_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"FINISHED"}}`, id)
			}))
			defer server.Close()
			client = sharedTestClient(t, server)
			_, err := client.runStatement(context.Background(), "SELECT 1")
			if (err == nil) != (mode == "credentials_rotated") {
				t.Fatalf("unexpected success=%v", err == nil)
			}
			if mode != "credentials_rotated" && TrinoCatalogOutcomeTerminal(err) {
				t.Fatal("ambiguous continuation classified terminal")
			}
			if (mode == "foreign" || mode == "redirect" || mode == "wrong_path") && requests.Load() != 1 {
				t.Fatal("unsafe continuation was requested")
			}
		})
	}
	if foreignCalls.Load() != 0 {
		t.Fatal("credentials reached foreign server")
	}
}

func TestSharedCatalogFinishedPageStillRequiresAcknowledgement(t *testing.T) {
	for _, acknowledge := range []bool{true, false} {
		t.Run(fmt.Sprintf("acknowledge_%t", acknowledge), func(t *testing.T) {
			var server *httptest.Server
			var requests atomic.Int32
			server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method == http.MethodPost {
					_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"FINISHED"},"data":[["example","OPERATIONAL"]],"nextUri":%q}`, sharedTestQueryID, server.URL+"/v1/statement/executing/"+sharedTestQueryID+"/token/1")
					return
				}
				if !acknowledge {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"FINISHED"}}`, sharedTestQueryID)
			}))
			defer server.Close()
			states, err := sharedTestClient(t, server).CatalogStates(context.Background())
			if requests.Load() != 2 {
				t.Fatalf("expected final acknowledgement request, got %d requests", requests.Load())
			}
			if acknowledge {
				if err != nil || states["example"] != "OPERATIONAL" {
					t.Fatalf("finished result page was not retained: states=%v err=%v", states, err)
				}
			} else if err == nil || TrinoCatalogOutcomeTerminal(err) {
				t.Fatal("failed acknowledgement was classified as successful or terminal")
			}
		})
	}
}

func TestSharedCatalogProductionTransportTLSAndPaging(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != strings.TrimPrefix(server.URL, "https://") {
			t.Error("TLS name replaced HTTP origin")
		}
		if r.Method == http.MethodPost {
			_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"RUNNING"},"nextUri":%q}`, sharedTestQueryID, "https://"+r.Host+"/v1/statement/executing/"+sharedTestQueryID+"/token/1")
			return
		}
		_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"FINISHED"}}`, sharedTestQueryID)
	}))
	defer server.Close()
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	for _, tc := range []struct {
		name         string
		trust, valid bool
	}{{"trusted", true, true}, {"untrusted", false, true}, {"wrong_name", true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			serverName := server.Certificate().DNSNames[0]
			if !tc.valid {
				serverName = "wrong.example.invalid"
			}
			client, err := NewTrinoSharedCatalogHTTPClient(server.URL, "provisioner", "password", serverName)
			if err != nil {
				t.Fatal(err)
			}
			transport := client.hc.Transport.(*http.Transport)
			if transport.Proxy != nil || transport.TLSClientConfig.InsecureSkipVerify {
				t.Fatal("unsafe production transport")
			}
			if tc.trust {
				transport.TLSClientConfig.RootCAs = x509.NewCertPool()
				transport.TLSClientConfig.RootCAs.AddCert(server.Certificate())
			}
			_, err = client.runStatement(context.Background(), "SELECT 1")
			if (err == nil) != (tc.trust && tc.valid) {
				t.Fatalf("unexpected TLS result: %v", err)
			}
		})
	}
}

func TestSharedCatalogContinuationCanonicalOrigin(t *testing.T) {
	client, err := NewTrinoSharedCatalogHTTPClient("https://coordinator.example:443", "user", "password", "certificate.example")
	if err != nil {
		t.Fatal(err)
	}
	path := "/v1/statement/executing/" + sharedTestQueryID + "/slug/1"
	for _, tc := range []struct {
		uri   string
		valid bool
	}{
		{"https://coordinator.example" + path, true},
		{"https://coordinator.example:443" + path, true},
		{"https://coordinator.example:8443" + path, false},
		{"https://certificate.example" + path, false},
		{"http://coordinator.example" + path, false},
		{"https://coordinator.example/v1/statement/executing/" + sharedTestQueryID + "/../1", false},
		{"https://coordinator.example/v1/statement/executing/" + sharedTestQueryID + "/slug/..", false},
	} {
		if client.validContinuation(tc.uri, sharedTestQueryID) != tc.valid {
			t.Fatalf("incorrect origin/path validation for %q", tc.uri)
		}
	}
}

func TestSharedCatalogBulkStates(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"FINISHED"},"data":[`, sharedTestQueryID)
		for i := range 10000 {
			if i > 0 {
				_, _ = fmt.Fprint(w, ",")
			}
			_, _ = fmt.Fprintf(w, `["org_%d","OPERATIONAL"]`, i)
		}
		_, _ = fmt.Fprint(w, "]}")
	}))
	defer server.Close()
	states, err := sharedTestClient(t, server).CatalogStates(context.Background())
	if err != nil || len(states) != 10000 || requests.Load() != 1 {
		t.Fatalf("bulk catalog inventory failed: %v", err)
	}
}

func TestSharedCatalogReadFailuresRetainIntent(t *testing.T) {
	for _, mode := range []string{"short_body", "timeout", "non_200", "loop"} {
		t.Run(mode, func(t *testing.T) {
			var server *httptest.Server
			release := make(chan struct{})
			server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "timeout" {
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				if mode == "short_body" {
					w.Header().Set("Content-Length", "10000")
				}
				if mode == "non_200" {
					w.WriteHeader(http.StatusServiceUnavailable)
				}
				if mode == "loop" {
					_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"RUNNING"},"nextUri":%q}`, sharedTestQueryID, server.URL+"/v1/statement/executing/"+sharedTestQueryID+"/token/1")
					return
				}
				_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"FINISHED"}}`, sharedTestQueryID)
			}))
			defer server.Close()
			defer close(release)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := sharedTestClient(t, server).runStatement(ctx, "DROP CATALOG example")
			if err == nil || TrinoCatalogOutcomeTerminal(err) {
				t.Fatal("incomplete transport released intent")
			}
		})
	}
}

func TestSharedCatalogFailedResponseCanPrecedeMutationCompletion(t *testing.T) {
	finishMutation := make(chan struct{})
	mutationDone := make(chan struct{})
	defer func() {
		select {
		case <-finishMutation:
		default:
			close(finishMutation)
		}
	}()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		go func() { <-finishMutation; close(mutationDone) }()
		_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"FAILED"},"error":{"errorName":"USER_CANCELED"}}`, sharedTestQueryID)
	}))
	defer server.Close()
	_, err := sharedTestClient(t, server).runStatement(context.Background(), "DROP CATALOG example")
	select {
	case <-mutationDone:
		t.Fatal("fixture mutation already completed")
	default:
	}
	if err == nil || TrinoCatalogOutcomeTerminal(err) {
		t.Fatal("FAILED response authorized a later writer while mutation still runs")
	}
	close(finishMutation)
	select {
	case <-mutationDone:
	case <-time.After(time.Second):
		t.Fatal("fixture mutation did not finish")
	}
}

func TestSharedCatalogInvalidInventoryFailsAtomically(t *testing.T) {
	for _, data := range []string{`[["org_a","OPERATIONAL"],["org_a","OPERATIONAL"]]`, `[["org_a","UNKNOWN"]]`, `[["org_a"]]`, `[[1,"OPERATIONAL"]]`} {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"FINISHED"},"data":%s}`, sharedTestQueryID, data)
		}))
		states, err := sharedTestClient(t, server).CatalogStates(context.Background())
		server.Close()
		if err == nil || states != nil {
			t.Fatal("invalid inventory yielded a partial map")
		}
	}
}

func TestSharedCatalogFailedStartupIsNotUsable(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"id":%q,"stats":{"state":"FINISHED"},"data":[["org_a","FAILING"]]}`, sharedTestQueryID)
	}))
	defer server.Close()
	client := sharedTestClient(t, server)
	states, err := client.CatalogStates(context.Background())
	if err != nil || states["org_a"] != "FAILING" {
		t.Fatal("failed startup placeholder missing")
	}
	if _, err := client.ListCatalogs(context.Background()); err == nil {
		t.Fatal("failed startup reported usable catalog")
	}
}

func sharedTestClient(t *testing.T, server *httptest.Server) *trinoSharedCatalogHTTPClient {
	t.Helper()
	client, err := NewTrinoSharedCatalogHTTPClient(server.URL, "provisioner", "original", "")
	if err != nil {
		t.Fatal(err)
	}
	client.hc.Transport = server.Client().Transport
	return client
}
