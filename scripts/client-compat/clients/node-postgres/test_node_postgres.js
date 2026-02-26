#!/usr/bin/env node
/**
 * Tests duckgres compatibility with node-postgres (pg) — the most widely
 * used PostgreSQL client for Node.js. Exercises connection setup, catalog
 * introspection, DDL, DML, parameterized queries, and prepared statements.
 *
 * Expects PGHOST, PGPORT, PGUSER, PGPASSWORD env vars.
 */

const { Client } = require("pg");
const fs = require("fs");
const yaml = require("js-yaml");
const http = require("http");

const CLIENT_NAME = "node-postgres";
const RESULTS_URL = process.env.RESULTS_URL || "http://results-gatherer:8080";

let passed = 0;
let failed = 0;
const errors = [];

// --- reporting ---

function postJSON(path, data) {
  return new Promise((resolve) => {
    try {
      const body = JSON.stringify(data);
      const url = new URL(path, RESULTS_URL);
      const opts = {
        hostname: url.hostname,
        port: url.port,
        path: url.pathname,
        method: "POST",
        headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) },
        timeout: 2000,
      };
      const req = http.request(opts, () => resolve());
      req.on("error", () => resolve());
      req.on("timeout", () => { req.destroy(); resolve(); });
      req.write(body);
      req.end();
    } catch {
      resolve();
    }
  });
}

function report(suite, testName, status, detail) {
  return postJSON("/result", { client: CLIENT_NAME, suite, test_name: testName, status, detail });
}

// --- helpers ---

let currentSuite = "";

async function ok(name, detail = "") {
  passed++;
  const suffix = detail ? ` (${detail})` : "";
  console.log(`  PASS  ${name}${suffix}`);
  await report(currentSuite, name, "pass", detail);
}

async function fail(name, reason) {
  failed++;
  errors.push([name, reason]);
  console.log(`  FAIL  ${name}: ${reason}`);
  await report(currentSuite, name, "fail", reason);
}

function setSuite(suite) {
  currentSuite = suite;
}

async function getClient() {
  const client = new Client({
    host: process.env.PGHOST || "duckgres",
    port: parseInt(process.env.PGPORT || "5432"),
    user: process.env.PGUSER || "postgres",
    password: process.env.PGPASSWORD || "postgres",
    ssl: { rejectUnauthorized: false },
  });
  await client.connect();
  return client;
}

function loadQueries(path = "/queries.yaml") {
  return yaml.load(fs.readFileSync(path, "utf8"));
}

// --- wait for duckgres ---

async function waitForDuckgres() {
  console.log("Waiting for duckgres...");
  for (let attempt = 0; attempt < 30; attempt++) {
    try {
      const client = await getClient();
      await client.end();
      console.log(`Connected after ${attempt + 1} attempt(s).`);
      return;
    } catch {
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
  console.log("FAIL: Could not connect after 30 seconds");
  process.exit(1);
}

// --- tests ---

async function testConnectionProperties() {
  console.log("\n=== Connection properties ===");
  setSuite("connection");
  const client = await getClient();
  try {
    await ok("connect", "TLS + password auth");

    // server_version from startup parameters
    const ver = client.serverVersion;
    if (ver) {
      await ok("server_version", ver);
    } else {
      await fail("server_version", "empty");
    }

    // basic query to verify connection
    const res = await client.query("SELECT current_database() AS db");
    if (res.rows.length === 1) {
      await ok("current_database", res.rows[0].db);
    } else {
      await fail("current_database", `expected 1 row, got ${res.rows.length}`);
    }
  } finally {
    await client.end();
  }
}

async function testSharedQueries() {
  console.log("\n=== Shared catalog queries ===");
  const client = await getClient();
  const queries = loadQueries();

  for (const q of queries) {
    setSuite(q.suite);
    try {
      const res = await client.query(q.sql);
      await ok(q.name, `${res.rowCount ?? res.rows.length} rows`);
    } catch (e) {
      await fail(q.name, e.message);
    }
  }

  await client.end();
}

async function testDdlDml() {
  console.log("\n=== DDL and DML ===");
  setSuite("ddl_dml");
  const client = await getClient();

  try {
    await client.query("DROP TABLE IF EXISTS node_pg_test");
    await client.query(`
      CREATE TABLE node_pg_test (
        id INTEGER,
        name VARCHAR,
        value DOUBLE,
        ts TIMESTAMP,
        flag BOOLEAN
      )
    `);
    await ok("CREATE TABLE");
  } catch (e) {
    await fail("CREATE TABLE", e.message);
    await client.end();
    return;
  }

  try {
    await client.query("INSERT INTO node_pg_test VALUES ($1, $2, $3, $4, $5)", [
      1, "alice", 3.14, "2024-01-01 10:00:00", true,
    ]);
    await ok("INSERT parameterized");
  } catch (e) {
    await fail("INSERT parameterized", e.message);
  }

  try {
    await client.query(
      "INSERT INTO node_pg_test VALUES (2, 'bob', 2.72, '2024-01-02 11:00:00', false)"
    );
    await ok("INSERT literal");
  } catch (e) {
    await fail("INSERT literal", e.message);
  }

  try {
    const res = await client.query("SELECT * FROM node_pg_test ORDER BY id");
    if (res.rows.length === 2) {
      await ok("SELECT *", `${res.rows.length} rows`);
    } else {
      await fail("SELECT *", `expected 2 rows, got ${res.rows.length}`);
    }
  } catch (e) {
    await fail("SELECT *", e.message);
  }

  try {
    const res = await client.query(
      "SELECT id, name FROM node_pg_test WHERE value > $1",
      [3.0]
    );
    if (res.rows.length === 1 && res.rows[0].name === "alice") {
      await ok("SELECT WHERE parameterized");
    } else {
      await fail("SELECT WHERE parameterized", `unexpected result: ${JSON.stringify(res.rows)}`);
    }
  } catch (e) {
    await fail("SELECT WHERE parameterized", e.message);
  }

  try {
    await client.query("UPDATE node_pg_test SET value = $1 WHERE id = $2", [9.99, 1]);
    await ok("UPDATE parameterized");
  } catch (e) {
    await fail("UPDATE parameterized", e.message);
  }

  try {
    await client.query("DELETE FROM node_pg_test WHERE id = $1", [2]);
    await ok("DELETE parameterized");
  } catch (e) {
    await fail("DELETE parameterized", e.message);
  }

  try {
    const res = await client.query("SELECT count(*) AS cnt FROM node_pg_test");
    const count = parseInt(res.rows[0].cnt);
    if (count === 1) {
      await ok("post-DML count", "1 row remaining");
    } else {
      await fail("post-DML count", `expected 1, got ${count}`);
    }
  } catch (e) {
    await fail("post-DML count", e.message);
  }

  try {
    await client.query("DROP TABLE node_pg_test");
    await ok("DROP TABLE");
  } catch (e) {
    await fail("DROP TABLE", e.message);
  }

  await client.end();
}

async function testPreparedStatements() {
  console.log("\n=== Prepared statements ===");
  setSuite("prepared");
  const client = await getClient();

  try {
    // Named prepared statement — node-postgres caches and reuses the parse
    const name = "get_answer";
    const res = await client.query({
      name,
      text: "SELECT $1::integer AS answer",
      values: [42],
    });
    if (res.rows[0].answer === 42) {
      await ok("named prepared", "answer=42");
    } else {
      await fail("named prepared", `expected 42, got ${res.rows[0].answer}`);
    }

    // Reuse the same prepared statement
    const res2 = await client.query({
      name,
      text: "SELECT $1::integer AS answer",
      values: [99],
    });
    if (res2.rows[0].answer === 99) {
      await ok("prepared reuse", "answer=99");
    } else {
      await fail("prepared reuse", `expected 99, got ${res2.rows[0].answer}`);
    }
  } catch (e) {
    await fail("named prepared", e.message);
  }

  try {
    // Multiple parameters
    const res = await client.query({
      text: "SELECT $1::integer + $2::integer AS sum",
      values: [10, 20],
    });
    if (res.rows[0].sum === 30) {
      await ok("multi-param prepared", "sum=30");
    } else {
      await fail("multi-param prepared", `expected 30, got ${res.rows[0].sum}`);
    }
  } catch (e) {
    await fail("multi-param prepared", e.message);
  }

  await client.end();
}

async function testResultMetadata() {
  console.log("\n=== Result metadata ===");
  setSuite("result_metadata");
  const client = await getClient();

  try {
    const res = await client.query(
      "SELECT 1::integer AS int_col, 'hello'::varchar AS str_col, 3.14::double AS dbl_col"
    );
    const fields = res.fields;
    if (!fields || fields.length !== 3) {
      await fail("field_count", `expected 3 fields, got ${fields ? fields.length : "null"}`);
    } else {
      await ok("field_count", "3");
      const names = fields.map((f) => f.name);
      if (names[0] === "int_col" && names[1] === "str_col" && names[2] === "dbl_col") {
        await ok("field_names", names.join(", "));
      } else {
        await fail("field_names", `unexpected: ${names.join(", ")}`);
      }
    }
  } catch (e) {
    await fail("field_count", e.message);
  }

  await client.end();
}

// --- main ---

async function main() {
  await waitForDuckgres();

  await testConnectionProperties();
  await testSharedQueries();
  await testDdlDml();
  await testPreparedStatements();
  await testResultMetadata();

  console.log(`\n${"=".repeat(50)}`);
  console.log(`Results: ${passed} passed, ${failed} failed`);
  if (errors.length > 0) {
    console.log("\nFailures:");
    for (const [name, reason] of errors) {
      console.log(`  - ${name}: ${reason}`);
    }
  }
  console.log("=".repeat(50));

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
