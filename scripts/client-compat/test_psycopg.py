#!/usr/bin/env python3
"""
Tests duckgres compatibility with psycopg2 — the most widely used
Python PostgreSQL driver. Exercises connection setup, catalog
introspection, DDL, DML, parameterized queries, COPY, and cursor
metadata.

Expects PGHOST, PGPORT, PGUSER, PGPASSWORD env vars.
"""

import os
import sys
import time

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import yaml

from report_client import ReportClient


def load_queries(path="/queries.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def get_conn(**kwargs):
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "duckgres"),
        port=int(os.environ.get("PGPORT", "5432")),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
        sslmode="require",
        **kwargs,
    )


def wait_for_duckgres():
    print("Waiting for duckgres...")
    for attempt in range(30):
        try:
            conn = get_conn()
            conn.close()
            print(f"Connected after {attempt + 1} attempt(s).")
            return
        except Exception:
            time.sleep(1)
    print("FAIL: Could not connect after 30 seconds")
    sys.exit(1)


class Results:
    def __init__(self, rc: ReportClient):
        self.passed = 0
        self.failed = 0
        self.errors = []
        self.rc = rc
        self.current_suite = ""

    def set_suite(self, suite: str):
        self.current_suite = suite

    def ok(self, name, detail=""):
        self.passed += 1
        suffix = f" ({detail})" if detail else ""
        print(f"  PASS  {name}{suffix}")
        self.rc.report(self.current_suite, name, "pass", detail)

    def fail(self, name, reason):
        self.failed += 1
        self.errors.append((name, reason))
        print(f"  FAIL  {name}: {reason}")
        self.rc.report(self.current_suite, name, "fail", reason)


def test_connection_properties(r):
    """Verify startup parameters and connection attributes."""
    print("\n=== Connection properties ===")
    r.set_suite("connection")
    conn = get_conn()
    try:
        r.ok("connect", "TLS + password auth")

        # server_version is set during startup handshake
        ver = conn.server_version
        if ver and ver > 0:
            r.ok("server_version", str(ver))
        else:
            r.fail("server_version", f"unexpected value: {ver}")

        # encoding
        enc = conn.encoding
        if enc:
            r.ok("encoding", enc)
        else:
            r.fail("encoding", "empty")

        # autocommit toggle
        conn.autocommit = True
        if conn.autocommit:
            r.ok("autocommit", "set to True")
        else:
            r.fail("autocommit", "failed to set")
    finally:
        conn.close()


def test_shared_queries(r):
    """Run all queries from the shared queries.yaml file."""
    print("\n=== Shared catalog queries ===")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    for q in load_queries():
        suite = q["suite"]
        name = q["name"]
        sql = q["sql"]
        r.set_suite(suite)
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            r.ok(name, f"{len(rows)} rows")
        except Exception as e:
            r.fail(name, str(e))

    cur.close()
    conn.close()


def test_ddl_dml(r):
    """CREATE TABLE, INSERT, SELECT, UPDATE, DELETE."""
    print("\n=== DDL and DML ===")
    r.set_suite("ddl_dml")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("DROP TABLE IF EXISTS psycopg_test")
        cur.execute("""
            CREATE TABLE psycopg_test (
                id INTEGER,
                name VARCHAR,
                value DOUBLE,
                ts TIMESTAMP,
                flag BOOLEAN
            )
        """)
        r.ok("CREATE TABLE")
    except Exception as e:
        r.fail("CREATE TABLE", str(e))
        cur.close()
        conn.close()
        return

    try:
        cur.execute(
            "INSERT INTO psycopg_test VALUES (%s, %s, %s, %s, %s)",
            (1, "alice", 3.14, "2024-01-01 10:00:00", True),
        )
        r.ok("INSERT parameterized")
    except Exception as e:
        r.fail("INSERT parameterized", str(e))

    try:
        cur.execute("INSERT INTO psycopg_test VALUES (2, 'bob', 2.72, '2024-01-02 11:00:00', false)")
        r.ok("INSERT literal")
    except Exception as e:
        r.fail("INSERT literal", str(e))

    try:
        cur.execute("SELECT * FROM psycopg_test ORDER BY id")
        rows = cur.fetchall()
        if len(rows) == 2:
            r.ok("SELECT *", f"{len(rows)} rows")
        else:
            r.fail("SELECT *", f"expected 2 rows, got {len(rows)}")
    except Exception as e:
        r.fail("SELECT *", str(e))

    try:
        cur.execute("SELECT id, name FROM psycopg_test WHERE value > %s", (3.0,))
        rows = cur.fetchall()
        if len(rows) == 1 and rows[0][1] == "alice":
            r.ok("SELECT WHERE parameterized")
        else:
            r.fail("SELECT WHERE parameterized", f"unexpected result: {rows}")
    except Exception as e:
        r.fail("SELECT WHERE parameterized", str(e))

    try:
        cur.execute("UPDATE psycopg_test SET value = %s WHERE id = %s", (9.99, 1))
        r.ok("UPDATE parameterized")
    except Exception as e:
        r.fail("UPDATE parameterized", str(e))

    try:
        cur.execute("DELETE FROM psycopg_test WHERE id = %s", (2,))
        r.ok("DELETE parameterized")
    except Exception as e:
        r.fail("DELETE parameterized", str(e))

    try:
        cur.execute("SELECT count(*) FROM psycopg_test")
        count = cur.fetchone()[0]
        if count == 1:
            r.ok("post-DML count", "1 row remaining")
        else:
            r.fail("post-DML count", f"expected 1, got {count}")
    except Exception as e:
        r.fail("post-DML count", str(e))

    try:
        cur.execute("DROP TABLE psycopg_test")
        r.ok("DROP TABLE")
    except Exception as e:
        r.fail("DROP TABLE", str(e))

    cur.close()
    conn.close()


def test_cursor_metadata(r):
    """cursor.description after SELECT — drivers and ORMs rely on this."""
    print("\n=== Cursor metadata ===")
    r.set_suite("cursor_metadata")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("SELECT 1::integer AS int_col, 'hello'::varchar AS str_col, 3.14::double AS dbl_col")
        desc = cur.description
        if desc is None:
            r.fail("cursor.description", "None after SELECT")
        elif len(desc) != 3:
            r.fail("cursor.description", f"expected 3 columns, got {len(desc)}")
        else:
            names = [d[0] for d in desc]
            if names == ["int_col", "str_col", "dbl_col"]:
                r.ok("cursor.description", f"columns: {names}")
            else:
                r.fail("cursor.description", f"unexpected names: {names}")
    except Exception as e:
        r.fail("cursor.description", str(e))

    cur.close()
    conn.close()


def test_executemany(r):
    """executemany — batch inserts used by ORMs and ETL tools."""
    print("\n=== executemany ===")
    r.set_suite("executemany")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("DROP TABLE IF EXISTS psycopg_batch")
        cur.execute("CREATE TABLE psycopg_batch (id INTEGER, label VARCHAR)")

        data = [(i, f"item_{i}") for i in range(100)]
        cur.executemany("INSERT INTO psycopg_batch VALUES (%s, %s)", data)
        r.ok("executemany", "100 rows")

        cur.execute("SELECT count(*) FROM psycopg_batch")
        count = cur.fetchone()[0]
        if count == 100:
            r.ok("executemany verify", f"{count} rows")
        else:
            r.fail("executemany verify", f"expected 100, got {count}")

        cur.execute("DROP TABLE psycopg_batch")
    except Exception as e:
        r.fail("executemany", str(e))

    cur.close()
    conn.close()


def test_dict_cursor(r):
    """RealDictCursor — used by many Python apps for dict-style row access."""
    print("\n=== RealDictCursor ===")
    r.set_suite("dict_cursor")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        cur.execute("SELECT 42 AS answer, 'hello' AS greeting")
        row = cur.fetchone()
        if row["answer"] == 42 and row["greeting"] == "hello":
            r.ok("RealDictCursor", f"row: {dict(row)}")
        else:
            r.fail("RealDictCursor", f"unexpected row: {row}")
    except Exception as e:
        r.fail("RealDictCursor", str(e))

    cur.close()
    conn.close()


def test_copy_to(r):
    """COPY TO STDOUT — bulk export."""
    print("\n=== COPY TO ===")
    r.set_suite("copy")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("DROP TABLE IF EXISTS psycopg_copy")
        cur.execute("CREATE TABLE psycopg_copy (id INTEGER, name VARCHAR)")
        cur.execute("INSERT INTO psycopg_copy VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")

        import io
        buf = io.StringIO()
        cur.copy_expert("COPY psycopg_copy TO STDOUT WITH CSV HEADER", buf)
        output = buf.getvalue()
        lines = output.strip().split("\n")
        if len(lines) == 4:  # header + 3 data rows
            r.ok("COPY TO STDOUT", f"{len(lines) - 1} data rows")
        else:
            r.fail("COPY TO STDOUT", f"expected 4 lines, got {len(lines)}: {output[:200]}")

        cur.execute("DROP TABLE psycopg_copy")
    except Exception as e:
        r.fail("COPY TO STDOUT", str(e))

    cur.close()
    conn.close()


def main():
    wait_for_duckgres()

    rc = ReportClient("psycopg")
    r = Results(rc)
    test_connection_properties(r)
    test_shared_queries(r)
    test_ddl_dml(r)
    test_cursor_metadata(r)
    test_executemany(r)
    test_dict_cursor(r)
    test_copy_to(r)

    print(f"\n{'='*50}")
    print(f"Results: {r.passed} passed, {r.failed} failed")
    if r.errors:
        print("\nFailures:")
        for name, reason in r.errors:
            print(f"  - {name}: {reason}")
    print(f"{'='*50}")

    rc.done()
    sys.exit(1 if r.failed else 0)


if __name__ == "__main__":
    main()
