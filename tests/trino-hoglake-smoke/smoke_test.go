package trino_hoglake_smoke

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

type statementResponse struct {
	Data    [][]any `json:"data"`
	NextURI string  `json:"nextUri"`
	Error   *struct {
		Name string `json:"errorName"`
	} `json:"error"`
}

type smokeClient struct {
	http           *http.Client
	server         *url.URL
	user, password string
	routingGroup   string
}

func newSmokeClient(server, user, password string) (*smokeClient, error) {
	u, err := url.Parse(server)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid Trino server URL")
	}
	if u.Scheme != "https" && (u.Scheme != "http" || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1")) {
		return nil, fmt.Errorf("Trino credentials require HTTPS outside loopback")
	}
	return &smokeClient{
		server: u, user: user, password: password,
		http: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}, nil
}

func (c *smokeClient) query(ctx context.Context, sql string) ([][]any, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	next := strings.TrimRight(c.server.String(), "/") + "/v1/statement"
	method := http.MethodPost
	var body io.Reader = strings.NewReader(sql)
	var rows [][]any
	for page := 0; page < 1000; page++ {
		u, err := url.Parse(next)
		if err != nil || u.User != nil || u.Scheme != c.server.Scheme || u.Host != c.server.Host {
			return nil, fmt.Errorf("Trino continuation changed origin")
		}
		req, err := http.NewRequestWithContext(ctx, method, next, body)
		if err != nil {
			return nil, err
		}
		req.SetBasicAuth(c.user, c.password)
		req.Header.Set("X-Trino-User", c.user)
		req.Header.Set("X-Trino-Transaction-Id", "NONE")
		if c.routingGroup != "" {
			req.Header.Set("X-Trino-Routing-Group", c.routingGroup)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		var result statementResponse
		decoder := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
		decoder.UseNumber()
		err = decoder.Decode(&result)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("Trino returned HTTP %d", resp.StatusCode)
		}
		if err != nil {
			return nil, err
		}
		if result.Error != nil {
			return nil, fmt.Errorf("Trino query failed: %s", result.Error.Name)
		}
		rows = append(rows, result.Data...)
		if result.NextURI == "" {
			return rows, nil
		}
		next, method, body = result.NextURI, http.MethodGet, nil
	}
	return nil, fmt.Errorf("Trino query exceeded its page bound")
}

func quote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// This opt-in test writes only generated tables, but compaction applies to the
// whole catalog. Run it against a dedicated, provisioned Hoglake test tenant.
func TestManagedHoglakeWritesAndCompaction(t *testing.T) {
	if os.Getenv("HOGLAKE_SMOKE_TEST") != "1" {
		t.Skip("set HOGLAKE_SMOKE_TEST=1 for the live managed-tenant smoke test")
	}
	env := func(key string) string {
		t.Helper()
		value := os.Getenv(key)
		if value == "" {
			t.Fatalf("%s is required", key)
		}
		return value
	}
	client, err := newSmokeClient(env("TRINO_SERVER"), env("TRINO_USER"), env("TRINO_PASSWORD"))
	if err != nil {
		t.Fatal(err)
	}
	client.routingGroup = os.Getenv("TRINO_ROUTING_GROUP")
	trinoCatalog, namespace := env("TRINO_CATALOG"), env("HOGLAKE_NAMESPACE")
	base, err := url.Parse(env("HOGLAKE_URI"))
	if err != nil || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || (base.Scheme != "http" && base.Scheme != "https") {
		t.Fatal("invalid Hoglake base URI")
	}
	catalogPath := strings.TrimRight(base.String(), "/") + "/v1/catalogs/" + url.PathEscape(env("HOGLAKE_CATALOG"))
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	query := func(sql string) [][]any {
		t.Helper()
		rows, err := client.query(ctx, sql)
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}
	id := make([]byte, 8)
	if _, err := rand.Read(id); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("hoglake_smoke_%x", id)
	table := quote(trinoCatalog) + "." + quote(namespace) + "." + quote(name)
	copyTable := quote(trinoCatalog) + "." + quote(namespace) + "." + quote(name+"_copy")
	for _, target := range []string{name, name + "_copy"} {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cleanupCancel()
			endpoint := catalogPath + "/namespaces/" + url.PathEscape(namespace) + "/tables/" + url.PathEscape(target)
			req, err := http.NewRequestWithContext(cleanupCtx, http.MethodDelete, endpoint, nil)
			if err != nil {
				t.Errorf("cleanup request failed: %v", err)
				return
			}
			resp, err := client.http.Do(req)
			if err != nil {
				t.Errorf("cleanup %s failed: %v", target, err)
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
				t.Errorf("cleanup %s returned HTTP %d", target, resp.StatusCode)
			}
		})
	}
	query("CREATE TABLE " + table + " (id bigint, amount decimal(38,2))")
	var expected [][]any
	for i := 1; i <= 8; i++ {
		query(fmt.Sprintf("INSERT INTO %s VALUES (%d, DECIMAL '123456789012345678901234567890.12')", table, i))
		expected = append(expected, []any{json.Number(fmt.Sprint(i)), "123456789012345678901234567890.12"})
	}
	read := func(target string) [][]any {
		return query("SELECT id, CAST(amount AS varchar) FROM " + target + " ORDER BY id")
	}
	if rows := read(table); !reflect.DeepEqual(rows, expected) {
		t.Fatal("INSERT did not preserve the expected rows and large decimals")
	}
	query("CREATE TABLE " + copyTable + " AS SELECT * FROM " + table)
	if rows := read(copyTable); !reflect.DeepEqual(rows, expected) {
		t.Fatal("CTAS did not preserve the expected rows and large decimals")
	}
	rest := func(method, endpoint string, result any) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(nil))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.http.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("Hoglake returned HTTP %d", resp.StatusCode)
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(result); err != nil {
			t.Fatal(err)
		}
	}
	scanPath := catalogPath + "/namespaces/" + url.PathEscape(namespace) + "/tables/" + url.PathEscape(name) + "/scan"
	var before, after []json.RawMessage
	rest(http.MethodGet, scanPath, &before)
	if len(before) < 2 {
		t.Fatal("compaction baseline already has fewer than two files; rerun with automatic maintenance paused for the test catalog")
	}
	var compact json.RawMessage
	rest(http.MethodPost, catalogPath+"/maintenance/compact?batch=100", &compact)
	rest(http.MethodGet, scanPath, &after)
	if len(after) >= len(before) {
		t.Fatalf("compaction did not reduce data files: before=%d after=%d", len(before), len(after))
	}
	if rows := read(table); !reflect.DeepEqual(rows, expected) {
		t.Fatal("compaction changed rows or decimal values")
	}
	t.Logf("CREATE, INSERT, CTAS and compaction passed; files %d -> %d", len(before), len(after))
}
