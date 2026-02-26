// pgx client compatibility test for duckgres.
//
// Loads shared queries from queries.yaml, executes each one,
// and reports results to the results-gatherer. Also exercises
// pgx-specific features: connection config, parameterized queries,
// DDL/DML, pgx.CollectRows, and batch queries.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

type query struct {
	Suite string `yaml:"suite"`
	Name  string `yaml:"name"`
	SQL   string `yaml:"sql"`
}

type reporter struct {
	client string
	url    string
	passed int
	failed int
}

func (r *reporter) report(suite, name, status, detail string) {
	if status == "pass" {
		r.passed++
		suffix := ""
		if detail != "" {
			suffix = " (" + detail + ")"
		}
		fmt.Printf("  PASS  %s%s\n", name, suffix)
	} else {
		r.failed++
		fmt.Printf("  FAIL  %s: %s\n", name, detail)
	}

	body, _ := json.Marshal(map[string]string{
		"client":    r.client,
		"suite":     suite,
		"test_name": name,
		"status":    status,
		"detail":    detail,
	})
	req, err := http.NewRequest("POST", r.url+"/result", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

func (r *reporter) done() {
	body, _ := json.Marshal(map[string]string{"client": r.client})
	req, err := http.NewRequest("POST", r.url+"/done", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func connect(ctx context.Context) (*pgx.Conn, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s sslmode=require",
		env("PGHOST", "duckgres"),
		env("PGPORT", "5432"),
		env("PGUSER", "postgres"),
		env("PGPASSWORD", "postgres"),
	)
	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}
	cfg.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	return pgx.ConnectConfig(ctx, cfg)
}

func waitForDuckgres(ctx context.Context) error {
	fmt.Println("Waiting for duckgres...")
	for i := 0; i < 30; i++ {
		conn, err := connect(ctx)
		if err == nil {
			conn.Close(ctx)
			fmt.Printf("Connected after %d attempt(s).\n", i+1)
			return nil
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("could not connect after 30 seconds")
}

func loadQueries(path string) ([]query, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var queries []query
	if err := yaml.Unmarshal(data, &queries); err != nil {
		return nil, err
	}
	return queries, nil
}

func testSharedQueries(ctx context.Context, r *reporter) {
	fmt.Println("\n=== Shared catalog queries ===")

	queries, err := loadQueries("/queries.yaml")
	if err != nil {
		r.report("shared", "load_queries", "fail", err.Error())
		return
	}

	conn, err := connect(ctx)
	if err != nil {
		r.report("shared", "connect", "fail", err.Error())
		return
	}
	defer conn.Close(ctx)

	for _, q := range queries {
		rows, err := conn.Query(ctx, q.SQL)
		if err != nil {
			r.report(q.Suite, q.Name, "fail", err.Error())
			continue
		}
		count := 0
		for rows.Next() {
			count++
		}
		if err := rows.Err(); err != nil {
			r.report(q.Suite, q.Name, "fail", err.Error())
			continue
		}
		rows.Close()
		r.report(q.Suite, q.Name, "pass", fmt.Sprintf("%d rows", count))
	}
}

func testConnectionProperties(ctx context.Context, r *reporter) {
	fmt.Println("\n=== Connection properties ===")
	suite := "connection"

	conn, err := connect(ctx)
	if err != nil {
		r.report(suite, "connect", "fail", err.Error())
		return
	}
	defer conn.Close(ctx)

	r.report(suite, "connect", "pass", "TLS + password auth")

	// server_version parameter
	ver := conn.PgConn().ParameterStatus("server_version")
	if ver != "" {
		r.report(suite, "server_version", "pass", ver)
	} else {
		r.report(suite, "server_version", "fail", "empty")
	}

	// encoding
	enc := conn.PgConn().ParameterStatus("client_encoding")
	if enc != "" {
		r.report(suite, "client_encoding", "pass", enc)
	} else {
		r.report(suite, "client_encoding", "fail", "empty")
	}
}

func testDDLDML(ctx context.Context, r *reporter) {
	fmt.Println("\n=== DDL and DML ===")
	suite := "ddl_dml"

	conn, err := connect(ctx)
	if err != nil {
		r.report(suite, "connect", "fail", err.Error())
		return
	}
	defer conn.Close(ctx)

	// CREATE TABLE
	_, err = conn.Exec(ctx, "DROP TABLE IF EXISTS pgx_test")
	if err != nil {
		r.report(suite, "DROP TABLE IF EXISTS", "fail", err.Error())
		return
	}
	_, err = conn.Exec(ctx, `CREATE TABLE pgx_test (
		id INTEGER,
		name VARCHAR,
		value DOUBLE,
		ts TIMESTAMP,
		flag BOOLEAN
	)`)
	if err != nil {
		r.report(suite, "CREATE TABLE", "fail", err.Error())
		return
	}
	r.report(suite, "CREATE TABLE", "pass", "")

	// INSERT parameterized
	_, err = conn.Exec(ctx, "INSERT INTO pgx_test VALUES ($1, $2, $3, $4, $5)",
		1, "alice", 3.14, "2024-01-01 10:00:00", true)
	if err != nil {
		r.report(suite, "INSERT parameterized", "fail", err.Error())
	} else {
		r.report(suite, "INSERT parameterized", "pass", "")
	}

	// INSERT literal
	_, err = conn.Exec(ctx, "INSERT INTO pgx_test VALUES (2, 'bob', 2.72, '2024-01-02 11:00:00', false)")
	if err != nil {
		r.report(suite, "INSERT literal", "fail", err.Error())
	} else {
		r.report(suite, "INSERT literal", "pass", "")
	}

	// SELECT *
	rows, err := conn.Query(ctx, "SELECT * FROM pgx_test ORDER BY id")
	if err != nil {
		r.report(suite, "SELECT *", "fail", err.Error())
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count == 2 {
			r.report(suite, "SELECT *", "pass", fmt.Sprintf("%d rows", count))
		} else {
			r.report(suite, "SELECT *", "fail", fmt.Sprintf("expected 2 rows, got %d", count))
		}
	}

	// SELECT WHERE parameterized
	var name string
	err = conn.QueryRow(ctx, "SELECT name FROM pgx_test WHERE value > $1", 3.0).Scan(&name)
	if err != nil {
		r.report(suite, "SELECT WHERE parameterized", "fail", err.Error())
	} else if name == "alice" {
		r.report(suite, "SELECT WHERE parameterized", "pass", "")
	} else {
		r.report(suite, "SELECT WHERE parameterized", "fail", fmt.Sprintf("expected alice, got %s", name))
	}

	// UPDATE
	_, err = conn.Exec(ctx, "UPDATE pgx_test SET value = $1 WHERE id = $2", 9.99, 1)
	if err != nil {
		r.report(suite, "UPDATE parameterized", "fail", err.Error())
	} else {
		r.report(suite, "UPDATE parameterized", "pass", "")
	}

	// DELETE
	_, err = conn.Exec(ctx, "DELETE FROM pgx_test WHERE id = $1", 2)
	if err != nil {
		r.report(suite, "DELETE parameterized", "fail", err.Error())
	} else {
		r.report(suite, "DELETE parameterized", "pass", "")
	}

	// Verify count
	var count int
	err = conn.QueryRow(ctx, "SELECT count(*) FROM pgx_test").Scan(&count)
	if err != nil {
		r.report(suite, "post-DML count", "fail", err.Error())
	} else if count == 1 {
		r.report(suite, "post-DML count", "pass", "1 row remaining")
	} else {
		r.report(suite, "post-DML count", "fail", fmt.Sprintf("expected 1, got %d", count))
	}

	// DROP
	_, err = conn.Exec(ctx, "DROP TABLE pgx_test")
	if err != nil {
		r.report(suite, "DROP TABLE", "fail", err.Error())
	} else {
		r.report(suite, "DROP TABLE", "pass", "")
	}
}

func testBatch(ctx context.Context, r *reporter) {
	fmt.Println("\n=== Batch queries ===")
	suite := "batch"

	conn, err := connect(ctx)
	if err != nil {
		r.report(suite, "connect", "fail", err.Error())
		return
	}
	defer conn.Close(ctx)

	batch := &pgx.Batch{}
	batch.Queue("SELECT 1 AS a")
	batch.Queue("SELECT 'hello' AS b")
	batch.Queue("SELECT 42 AS c")

	br := conn.SendBatch(ctx, batch)

	// Result 1
	rows1, err := br.Query()
	if err != nil {
		r.report(suite, "batch_query_1", "fail", err.Error())
	} else {
		if rows1.Next() {
			var a int
			if err := rows1.Scan(&a); err != nil {
				r.report(suite, "batch_query_1", "fail", err.Error())
			} else if a == 1 {
				r.report(suite, "batch_query_1", "pass", "")
			} else {
				r.report(suite, "batch_query_1", "fail", fmt.Sprintf("expected 1, got %d", a))
			}
		}
		rows1.Close()
	}

	// Result 2
	rows2, err := br.Query()
	if err != nil {
		r.report(suite, "batch_query_2", "fail", err.Error())
	} else {
		if rows2.Next() {
			var b string
			if err := rows2.Scan(&b); err != nil {
				r.report(suite, "batch_query_2", "fail", err.Error())
			} else if b == "hello" {
				r.report(suite, "batch_query_2", "pass", "")
			} else {
				r.report(suite, "batch_query_2", "fail", fmt.Sprintf("expected hello, got %s", b))
			}
		}
		rows2.Close()
	}

	// Result 3
	rows3, err := br.Query()
	if err != nil {
		r.report(suite, "batch_query_3", "fail", err.Error())
	} else {
		if rows3.Next() {
			var c int
			if err := rows3.Scan(&c); err != nil {
				r.report(suite, "batch_query_3", "fail", err.Error())
			} else if c == 42 {
				r.report(suite, "batch_query_3", "pass", "")
			} else {
				r.report(suite, "batch_query_3", "fail", fmt.Sprintf("expected 42, got %d", c))
			}
		}
		rows3.Close()
	}

	if err := br.Close(); err != nil {
		r.report(suite, "batch_close", "fail", err.Error())
	} else {
		r.report(suite, "batch_close", "pass", "")
	}
}

func main() {
	ctx := context.Background()

	if err := waitForDuckgres(ctx); err != nil {
		fmt.Println("FAIL:", err)
		os.Exit(1)
	}

	r := &reporter{
		client: "pgx",
		url:    env("RESULTS_URL", "http://results-gatherer:8080"),
	}

	testConnectionProperties(ctx, r)
	testSharedQueries(ctx, r)
	testDDLDML(ctx, r)
	testBatch(ctx, r)

	fmt.Printf("\n%s\n", "==================================================")
	fmt.Printf("Results: %d passed, %d failed\n", r.passed, r.failed)
	fmt.Printf("%s\n", "==================================================")

	r.done()

	if r.failed > 0 {
		os.Exit(1)
	}
}
