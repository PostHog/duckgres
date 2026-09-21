package trinogateway

import (
	"context"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type authTransport struct {
	change func(*http.Request)
}

func (a authTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	a.change(request)
	return http.DefaultTransport.RoundTrip(request)
}

func TestClientAuthenticatesBothGatewayLayers(t *testing.T) {
	const token = "0123456789abcdef0123456789abcdef"
	const username = "pool-controller"
	for _, tc := range []struct {
		name   string
		change func(*http.Request)
		deny   bool
	}{
		{name: "both credentials"},
		{name: "missing Basic", deny: true, change: func(r *http.Request) { r.Header.Del("Authorization") }},
		{name: "wrong Basic", deny: true, change: func(r *http.Request) { r.SetBasicAuth("wrong-user", token) }},
		{name: "missing capability", deny: true, change: func(r *http.Request) { r.Header.Del("X-Gateway-Transaction-Admin-Token") }},
		{name: "wrong capability", deny: true, change: func(r *http.Request) { r.Header.Set("X-Gateway-Transaction-Admin-Token", "wrong-token") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, password, ok := r.BasicAuth()
				if !ok || user != username || password != token || r.Header.Get("X-Gateway-Transaction-Admin-Token") != token {
					http.Error(w, "rejected "+token, http.StatusForbidden)
					return
				}
				if r.URL.Path == "/gateway/backend/all" {
					writeJSON(t, w, http.StatusOK, []Backend{})
					return
				}
				writeJSON(t, w, http.StatusOK, PoolState{PoolID: "pool-test"})
			}))
			defer server.Close()
			config := Config{BaseURL: server.URL, AdminToken: token, APIUsername: username, AllowPlaintext: true}
			if tc.change != nil {
				config.HTTPClient = &http.Client{Transport: authTransport{change: tc.change}}
			}
			client, err := NewClient(config)
			if err != nil {
				t.Fatal(err)
			}
			_, configureErr := client.ConfigurePool(context.Background(), "pool-test", ConfigurePoolRequest{})
			backendErr := client.EnsureInactiveBackend(context.Background(), Backend{Name: "member-test", ProxyTo: "http://member.example.test:8080", RoutingGroup: "pool-test"})
			deleteErr := client.DeleteBackend(context.Background(), "member-test")
			for _, result := range []error{configureErr, backendErr, deleteErr} {
				if (result != nil) != tc.deny {
					t.Fatalf("request error=%v, want rejection=%v", result, tc.deny)
				}
				if result != nil && strings.Contains(result.Error(), token) {
					t.Fatal("authentication failure exposed its credential")
				}
			}
		})
	}
}

func TestClientRejectsInvalidAPIUsername(t *testing.T) {
	for _, username := range []string{"unsafe-user:other", "unsafe-user\n", "unsafe-user\x00", " unsafe-user", "unsafe-user ", strings.Repeat("u", 256)} {
		_, err := NewClient(Config{BaseURL: "https://gateway.example.test", AdminToken: "0123456789abcdef0123456789abcdef", APIUsername: username})
		if err == nil {
			t.Fatalf("accepted invalid API username %q", username)
		}
		if strings.Contains(err.Error(), username) {
			t.Fatal("validation exposed the rejected username")
		}
	}
}

func TestBasicGatewayClientDoesNotFollowRedirects(t *testing.T) {
	var forwarded, original atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded.Add(1) }))
	defer target.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		original.Add(1)
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	const token = "0123456789abcdef0123456789abcdef"
	client, err := NewClient(Config{BaseURL: server.URL, AdminToken: token, APIUsername: "pool-controller"})
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	client.http.Transport.(*http.Transport).TLSClientConfig.RootCAs = roots
	if _, err := client.ConfigurePool(context.Background(), "pool-test", ConfigurePoolRequest{}); err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("redirect error=%v", err)
	}
	if original.Load() != 1 || forwarded.Load() != 0 {
		t.Fatal("client forwarded admin credentials after a redirect")
	}
}
