#!/usr/bin/env python3
"""
Tests duckgres compatibility with pgAdmin 4's introspection queries.

pgAdmin is a web application that can't run headlessly, so we replay its
known SQL queries via psycopg2 (pgAdmin's actual driver). Queries are
extracted from pgAdmin 4 v9.x Jinja2 SQL templates, rendered for PG 15.0.

Source templates in the pgAdmin repo (github.com/pgadmin-org/pgadmin4):
  web/pgadmin/browser/server_groups/servers/
    databases/templates/databases/sql/default/  (nodes.sql, properties.sql)
    roles/templates/roles/sql/default/          (nodes.sql)
    databases/schemas/templates/schemas/pg/default/sql/  (nodes.sql)
    databases/schemas/tables/templates/tables/sql/default/  (nodes.sql)
    tablespaces/templates/tablespaces/sql/default/  (nodes.sql)

These templates rarely change between pgAdmin releases. If a pgAdmin user
reports breakage, update the queries here and in queries.yaml, noting the
new pgAdmin version.

Expects PGHOST, PGPORT, PGUSER, PGPASSWORD env vars.
"""

import os
import sys
import time

import psycopg2
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


def test_pgadmin_connect_sequence(r):
    """
    Replay pgAdmin's connection sequence — the queries it fires when
    you register a server and expand the browser tree.
    """
    print("\n=== pgAdmin connect sequence ===")
    r.set_suite("pgadmin_connect")
    conn = get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # 1. Version detection — pgAdmin parses this to set capabilities
    try:
        cur.execute("SELECT version()")
        ver = cur.fetchone()[0]
        if "PostgreSQL" in ver:
            r.ok("version_detection", ver[:60])
        else:
            r.fail("version_detection", f"unexpected: {ver[:60]}")
    except Exception as e:
        r.fail("version_detection", str(e))

    # 2. Server version string
    try:
        cur.execute("SHOW server_version")
        sv = cur.fetchone()[0]
        r.ok("server_version", sv)
    except Exception as e:
        r.fail("server_version", str(e))

    # 3. Current user identity
    try:
        cur.execute("SELECT current_user")
        user = cur.fetchone()[0]
        r.ok("current_user", user)
    except Exception as e:
        r.fail("current_user", str(e))

    # 4. Database listing with pg_shdescription JOIN
    #    (databases/sql/default/nodes.sql)
    try:
        cur.execute("""
            SELECT
                db.oid::bigint AS did, db.datname AS name,
                db.dattablespace AS spcoid,
                shd.description AS comment,
                db.datallowconn,
                has_database_privilege(db.oid, 'CREATE') AS cancreate,
                datistemplate
            FROM pg_catalog.pg_database db
            LEFT JOIN pg_catalog.pg_shdescription shd
                ON shd.objoid = db.oid
            ORDER BY datname
        """)
        rows = cur.fetchall()
        r.ok("database_list", f"{len(rows)} databases")
    except Exception as e:
        r.fail("database_list", str(e))

    # 5. Database properties with owner and encoding
    #    (databases/sql/default/properties.sql)
    try:
        cur.execute("""
            SELECT
                db.oid::bigint AS did, db.datname AS name,
                pg_catalog.pg_encoding_to_char(db.encoding) AS encoding,
                pg_catalog.pg_get_userbyid(db.datdba) AS datowner,
                has_database_privilege(db.oid, 'CREATE') AS cancreate,
                datistemplate, datallowconn, datconnlimit,
                datcollate, datctype
            FROM pg_catalog.pg_database db
            LEFT JOIN pg_catalog.pg_shdescription shd
                ON shd.objoid = db.oid
            ORDER BY datname
        """)
        rows = cur.fetchall()
        r.ok("database_properties", f"{len(rows)} databases")
    except Exception as e:
        r.fail("database_properties", str(e))

    # 6. Role listing with descriptions
    #    (roles/sql/default/nodes.sql)
    try:
        cur.execute("""
            SELECT
                r.oid::bigint AS id, r.rolname AS name,
                r.rolcanlogin, r.rolsuper,
                shd.description
            FROM pg_catalog.pg_roles r
            LEFT JOIN pg_catalog.pg_shdescription shd
                ON shd.objoid = r.oid
            ORDER BY r.rolcanlogin, r.rolname
        """)
        rows = cur.fetchall()
        r.ok("role_list", f"{len(rows)} roles")
    except Exception as e:
        r.fail("role_list", str(e))

    # 7. Schema listing with privilege checks
    #    (schemas/pg/default/sql/nodes.sql)
    try:
        cur.execute("""
            SELECT
                nsp.oid::bigint AS id, nsp.nspname AS name,
                has_schema_privilege(nsp.oid, 'CREATE') AS can_create,
                has_schema_privilege(nsp.oid, 'USAGE') AS has_usage
            FROM pg_catalog.pg_namespace nsp
            WHERE nspname NOT LIKE 'pg\\_%' AND nspname != 'information_schema'
            ORDER BY nspname
        """)
        rows = cur.fetchall()
        r.ok("schema_list", f"{len(rows)} schemas")
    except Exception as e:
        r.fail("schema_list", str(e))

    # 8. Table listing — simplified from tables/sql/default/nodes.sql
    try:
        cur.execute("""
            SELECT
                rel.oid::bigint AS id, rel.relname AS name,
                nsp.nspname AS schema
            FROM pg_catalog.pg_class rel
            JOIN pg_catalog.pg_namespace nsp ON nsp.oid = rel.relnamespace
            WHERE rel.relkind IN ('r', 'v')
                AND nsp.nspname NOT LIKE 'pg\\_%'
                AND nsp.nspname != 'information_schema'
            ORDER BY nsp.nspname, rel.relname
        """)
        rows = cur.fetchall()
        r.ok("table_list", f"{len(rows)} relations")
    except Exception as e:
        r.fail("table_list", str(e))

    # 9. Tablespace listing
    #    (tablespaces/sql/default/nodes.sql)
    try:
        cur.execute("""
            SELECT
                ts.oid::bigint AS id, spcname AS name,
                shobj_description(ts.oid, 'pg_tablespace') AS description
            FROM pg_catalog.pg_tablespace ts
            ORDER BY name
        """)
        rows = cur.fetchall()
        r.ok("tablespace_list", f"{len(rows)} tablespaces")
    except Exception as e:
        r.fail("tablespace_list", str(e))

    # 10. Per-database privilege check
    try:
        cur.execute("SELECT has_database_privilege('postgres', 'CONNECT')")
        val = cur.fetchone()[0]
        if val:
            r.ok("has_database_privilege", str(val))
        else:
            r.fail("has_database_privilege", f"expected true, got {val}")
    except Exception as e:
        r.fail("has_database_privilege", str(e))

    cur.close()
    conn.close()


def main():
    wait_for_duckgres()
    rc = ReportClient("pgadmin")
    r = Results(rc)

    test_shared_queries(r)
    test_pgadmin_connect_sequence(r)

    # Summary
    print(f"\n{'='*40}")
    print(f"pgAdmin compat: {r.passed} passed, {r.failed} failed")
    if r.errors:
        print("\nFailures:")
        for name, reason in r.errors:
            print(f"  {name}: {reason}")

    rc.done()
    sys.exit(1 if r.failed else 0)


if __name__ == "__main__":
    main()
