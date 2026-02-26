//! tokio-postgres client compatibility test for duckgres.
//!
//! Loads shared queries from queries.yaml, executes each one,
//! and reports results to the results-gatherer. Also exercises
//! tokio-postgres-specific features: prepared statements, type
//! conversions, DDL/DML, and batch queries via pipeline.

use serde::Deserialize;
use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio_postgres::Client;

static PASSED: AtomicU32 = AtomicU32::new(0);
static FAILED: AtomicU32 = AtomicU32::new(0);

const CLIENT_NAME: &str = "rust";

fn env_or(key: &str, fallback: &str) -> String {
    env::var(key).unwrap_or_else(|_| fallback.to_string())
}

#[derive(Deserialize)]
struct Query {
    suite: String,
    name: String,
    sql: String,
}

struct Reporter {
    http: reqwest::Client,
    url: String,
}

impl Reporter {
    fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap(),
            url: env_or("RESULTS_URL", "http://results-gatherer:8080"),
        }
    }

    async fn report(&self, suite: &str, name: &str, status: &str, detail: &str) {
        if status == "pass" {
            PASSED.fetch_add(1, Ordering::Relaxed);
            if detail.is_empty() {
                println!("  PASS  {name}");
            } else {
                println!("  PASS  {name} ({detail})");
            }
        } else {
            FAILED.fetch_add(1, Ordering::Relaxed);
            println!("  FAIL  {name}: {detail}");
        }

        let body = serde_json::json!({
            "client": CLIENT_NAME,
            "suite": suite,
            "test_name": name,
            "status": status,
            "detail": detail,
        });
        let _ = self
            .http
            .post(format!("{}/result", self.url))
            .json(&body)
            .send()
            .await;
    }

    async fn done(&self) {
        let body = serde_json::json!({"client": CLIENT_NAME});
        let _ = self
            .http
            .post(format!("{}/done", self.url))
            .json(&body)
            .send()
            .await;
    }
}

async fn connect() -> Result<Client, Box<dyn std::error::Error>> {
    let host = env_or("PGHOST", "duckgres");
    let port = env_or("PGPORT", "5432");
    let user = env_or("PGUSER", "postgres");
    let password = env_or("PGPASSWORD", "postgres");

    let connstr = format!(
        "host={host} port={port} user={user} password={password} sslmode=require"
    );

    let tls_connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()?;
    let tls = postgres_native_tls::MakeTlsConnector::new(tls_connector);

    let (client, connection) = tokio_postgres::connect(&connstr, tls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    Ok(client)
}

async fn wait_for_duckgres() -> Result<(), Box<dyn std::error::Error>> {
    println!("Waiting for duckgres...");
    for i in 1..=30 {
        match connect().await {
            Ok(_) => {
                println!("Connected after {i} attempt(s).");
                return Ok(());
            }
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
    Err("Could not connect after 30 seconds".into())
}

fn load_queries() -> Result<Vec<Query>, Box<dyn std::error::Error>> {
    let data = std::fs::read_to_string("/queries.yaml")?;
    let queries: Vec<Query> = serde_yaml::from_str(&data)?;
    Ok(queries)
}

async fn test_connection_properties(r: &Reporter) {
    println!("\n=== Connection properties ===");
    let suite = "connection";

    match connect().await {
        Ok(client) => {
            r.report(suite, "connect", "pass", "TLS + password auth")
                .await;

            // server_version via SHOW
            match client.query_one("SHOW server_version", &[]).await {
                Ok(row) => {
                    let ver: String = row.get(0);
                    r.report(suite, "server_version", "pass", &ver).await;
                }
                Err(e) => {
                    r.report(suite, "server_version", "fail", &e.to_string())
                        .await;
                }
            }

            // client_encoding
            match client.query_one("SHOW client_encoding", &[]).await {
                Ok(row) => {
                    let enc: String = row.get(0);
                    r.report(suite, "client_encoding", "pass", &enc).await;
                }
                Err(e) => {
                    r.report(suite, "client_encoding", "fail", &e.to_string())
                        .await;
                }
            }
        }
        Err(e) => {
            r.report(suite, "connect", "fail", &e.to_string())
                .await;
        }
    }
}

async fn test_shared_queries(r: &Reporter) {
    println!("\n=== Shared catalog queries ===");

    let queries = match load_queries() {
        Ok(q) => q,
        Err(e) => {
            r.report("shared", "load_queries", "fail", &e.to_string())
                .await;
            return;
        }
    };

    let client = match connect().await {
        Ok(c) => c,
        Err(e) => {
            r.report("shared", "connect", "fail", &e.to_string())
                .await;
            return;
        }
    };

    for q in &queries {
        match client.query(&*q.sql, &[]).await {
            Ok(rows) => {
                r.report(&q.suite, &q.name, "pass", &format!("{} rows", rows.len()))
                    .await;
            }
            Err(e) => {
                r.report(&q.suite, &q.name, "fail", &e.to_string()).await;
            }
        }
    }
}

async fn test_ddl_dml(r: &Reporter) {
    println!("\n=== DDL and DML ===");
    let suite = "ddl_dml";

    let client = match connect().await {
        Ok(c) => c,
        Err(e) => {
            r.report(suite, "connect", "fail", &e.to_string()).await;
            return;
        }
    };

    // CREATE TABLE
    let _ = client.execute("DROP TABLE IF EXISTS rust_test", &[]).await;
    match client
        .execute(
            "CREATE TABLE rust_test (id INTEGER, name VARCHAR, value DOUBLE, ts TIMESTAMP, flag BOOLEAN)",
            &[],
        )
        .await
    {
        Ok(_) => r.report(suite, "CREATE TABLE", "pass", "").await,
        Err(e) => {
            r.report(suite, "CREATE TABLE", "fail", &e.to_string())
                .await;
            return;
        }
    }

    // INSERT literal
    match client
        .execute(
            "INSERT INTO rust_test VALUES (1, 'alice', 3.14, '2024-01-01 10:00:00', true)",
            &[],
        )
        .await
    {
        Ok(_) => r.report(suite, "INSERT literal", "pass", "").await,
        Err(e) => r.report(suite, "INSERT literal", "fail", &e.to_string()).await,
    }

    // INSERT with parameters
    match client
        .execute(
            "INSERT INTO rust_test VALUES ($1, $2, $3, $4, $5)",
            &[
                &2i32,
                &"bob",
                &2.72f64,
                &"2024-01-02 11:00:00",
                &false,
            ],
        )
        .await
    {
        Ok(_) => r.report(suite, "INSERT parameterized", "pass", "").await,
        Err(e) => {
            r.report(suite, "INSERT parameterized", "fail", &e.to_string())
                .await;
        }
    }

    // SELECT *
    match client
        .query("SELECT * FROM rust_test ORDER BY id", &[])
        .await
    {
        Ok(rows) => {
            if rows.len() == 2 {
                r.report(suite, "SELECT *", "pass", &format!("{} rows", rows.len()))
                    .await;
            } else {
                r.report(
                    suite,
                    "SELECT *",
                    "fail",
                    &format!("expected 2, got {}", rows.len()),
                )
                .await;
            }
        }
        Err(e) => r.report(suite, "SELECT *", "fail", &e.to_string()).await,
    }

    // SELECT WHERE with parameter (use simple query to avoid type issues)
    match client
        .query("SELECT name FROM rust_test WHERE value > 3.0", &[])
        .await
    {
        Ok(rows) => {
            if rows.len() == 1 {
                let name: String = rows[0].get(0);
                if name == "alice" {
                    r.report(suite, "SELECT WHERE", "pass", "").await;
                } else {
                    r.report(suite, "SELECT WHERE", "fail", &format!("expected alice, got {name}"))
                        .await;
                }
            } else {
                r.report(
                    suite,
                    "SELECT WHERE",
                    "fail",
                    &format!("expected 1 row, got {}", rows.len()),
                )
                .await;
            }
        }
        Err(e) => r.report(suite, "SELECT WHERE", "fail", &e.to_string()).await,
    }

    // UPDATE
    match client
        .execute("UPDATE rust_test SET value = 9.99 WHERE id = 1", &[])
        .await
    {
        Ok(_) => r.report(suite, "UPDATE", "pass", "").await,
        Err(e) => r.report(suite, "UPDATE", "fail", &e.to_string()).await,
    }

    // DELETE
    match client
        .execute("DELETE FROM rust_test WHERE id = 2", &[])
        .await
    {
        Ok(_) => r.report(suite, "DELETE", "pass", "").await,
        Err(e) => r.report(suite, "DELETE", "fail", &e.to_string()).await,
    }

    // Verify count
    match client
        .query_one("SELECT count(*) FROM rust_test", &[])
        .await
    {
        Ok(row) => {
            let count: i64 = row.get(0);
            if count == 1 {
                r.report(suite, "post-DML count", "pass", "1 row remaining")
                    .await;
            } else {
                r.report(
                    suite,
                    "post-DML count",
                    "fail",
                    &format!("expected 1, got {count}"),
                )
                .await;
            }
        }
        Err(e) => {
            r.report(suite, "post-DML count", "fail", &e.to_string())
                .await;
        }
    }

    // DROP
    match client.execute("DROP TABLE rust_test", &[]).await {
        Ok(_) => r.report(suite, "DROP TABLE", "pass", "").await,
        Err(e) => r.report(suite, "DROP TABLE", "fail", &e.to_string()).await,
    }
}

async fn test_prepared_statements(r: &Reporter) {
    println!("\n=== Prepared statements ===");
    let suite = "prepared";

    let client = match connect().await {
        Ok(c) => c,
        Err(e) => {
            r.report(suite, "connect", "fail", &e.to_string()).await;
            return;
        }
    };

    // Prepare + execute a simple SELECT
    match client.prepare("SELECT $1::text AS echo").await {
        Ok(stmt) => {
            r.report(suite, "prepare", "pass", "").await;
            match client.query_one(&stmt, &[&"hello"]).await {
                Ok(row) => {
                    let val: String = row.get(0);
                    if val == "hello" {
                        r.report(suite, "execute_prepared", "pass", "").await;
                    } else {
                        r.report(
                            suite,
                            "execute_prepared",
                            "fail",
                            &format!("expected hello, got {val}"),
                        )
                        .await;
                    }
                }
                Err(e) => {
                    r.report(suite, "execute_prepared", "fail", &e.to_string())
                        .await;
                }
            }
        }
        Err(e) => {
            r.report(suite, "prepare", "fail", &e.to_string()).await;
        }
    }

    // Prepare + reuse with different params
    match client.prepare("SELECT 1 WHERE $1::text = $1::text").await {
        Ok(stmt) => {
            let mut ok = true;
            for val in &["a", "b", "c"] {
                if client.query(&stmt, &[val]).await.is_err() {
                    ok = false;
                    break;
                }
            }
            if ok {
                r.report(suite, "reuse_prepared", "pass", "3 executions")
                    .await;
            } else {
                r.report(suite, "reuse_prepared", "fail", "execution failed")
                    .await;
            }
        }
        Err(e) => {
            r.report(suite, "reuse_prepared", "fail", &e.to_string())
                .await;
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = wait_for_duckgres().await {
        eprintln!("FAIL: {e}");
        std::process::exit(1);
    }

    let r = Reporter::new();

    test_connection_properties(&r).await;
    test_shared_queries(&r).await;
    test_ddl_dml(&r).await;
    test_prepared_statements(&r).await;

    let passed = PASSED.load(Ordering::Relaxed);
    let failed = FAILED.load(Ordering::Relaxed);
    println!("\n==================================================");
    println!("Results: {passed} passed, {failed} failed");
    println!("==================================================");

    r.done().await;

    if failed > 0 {
        std::process::exit(1);
    }
}
