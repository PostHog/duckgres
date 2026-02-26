#!/usr/bin/env python3
"""
Tests duckgres compatibility with SQLAlchemy 2.x — the most widely used
Python ORM / SQL toolkit. Exercises Core and ORM patterns against the
PostgreSQL dialect (psycopg2 backend).

Expects PGHOST, PGPORT, PGUSER, PGPASSWORD env vars.
"""

import os
import sys
import time

import yaml
from sqlalchemy import (
    Column,
    Double,
    Integer,
    MetaData,
    String,
    Table,
    Boolean,
    DateTime,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from report_client import ReportClient


def load_queries(path="/queries.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def get_url():
    host = os.environ.get("PGHOST", "duckgres")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "postgres")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres?sslmode=require"


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


def wait_for_duckgres():
    print("Waiting for duckgres...")
    url = get_url()
    for attempt in range(30):
        try:
            engine = create_engine(url)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
            print(f"Connected after {attempt + 1} attempt(s).")
            return
        except Exception:
            time.sleep(1)
    print("FAIL: Could not connect after 30 seconds")
    sys.exit(1)


def test_connection(r):
    """Verify engine creation and basic connectivity."""
    print("\n=== Connection ===")
    r.set_suite("connection")
    try:
        engine = create_engine(get_url())
        with engine.connect() as conn:
            r.ok("connect", "SQLAlchemy + psycopg2")

            row = conn.execute(text("SELECT version()")).fetchone()
            if row and row[0]:
                r.ok("server_version", row[0][:60])
            else:
                r.fail("server_version", "empty")

        engine.dispose()
    except Exception as e:
        r.fail("connect", str(e))


def test_shared_queries(r):
    """Run all queries from shared queries.yaml via text()."""
    print("\n=== Shared catalog queries ===")
    engine = create_engine(get_url())

    with engine.connect() as conn:
        for q in load_queries():
            suite = q["suite"]
            name = q["name"]
            sql = q["sql"]
            r.set_suite(suite)
            try:
                rows = conn.execute(text(sql)).fetchall()
                r.ok(name, f"{len(rows)} rows")
            except Exception as e:
                r.fail(name, str(e))

    engine.dispose()


def test_core_ddl_dml(r):
    """SQLAlchemy Core: Table, insert, select, update, delete."""
    print("\n=== Core DDL/DML ===")
    r.set_suite("core_ddl_dml")
    engine = create_engine(get_url())
    metadata = MetaData()

    sa_test = Table(
        "sa_core_test",
        metadata,
        Column("id", Integer),
        Column("name", String),
        Column("value", Double),
    )

    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_core_test"))
            conn.commit()

        metadata.create_all(engine)
        r.ok("CREATE TABLE")
    except Exception as e:
        r.fail("CREATE TABLE", str(e))
        engine.dispose()
        return

    try:
        with engine.connect() as conn:
            # Insert
            conn.execute(sa_test.insert(), [
                {"id": 1, "name": "alice", "value": 3.14},
                {"id": 2, "name": "bob", "value": 2.72},
            ])
            conn.commit()
            r.ok("INSERT", "2 rows")

            # Select
            result = conn.execute(sa_test.select().order_by(sa_test.c.id)).fetchall()
            if len(result) == 2:
                r.ok("SELECT", f"{len(result)} rows")
            else:
                r.fail("SELECT", f"expected 2, got {len(result)}")

            # Select with where
            result = conn.execute(
                sa_test.select().where(sa_test.c.value > 3.0)
            ).fetchall()
            if len(result) == 1 and result[0][1] == "alice":
                r.ok("SELECT WHERE")
            else:
                r.fail("SELECT WHERE", f"unexpected: {result}")

            # Update
            conn.execute(
                sa_test.update().where(sa_test.c.id == 1).values(value=9.99)
            )
            conn.commit()
            r.ok("UPDATE")

            # Delete
            conn.execute(sa_test.delete().where(sa_test.c.id == 2))
            conn.commit()
            r.ok("DELETE")

            # Verify count
            count = conn.execute(text("SELECT count(*) FROM sa_core_test")).scalar()
            if count == 1:
                r.ok("post-DML count", "1 row remaining")
            else:
                r.fail("post-DML count", f"expected 1, got {count}")

            # Drop
            conn.execute(text("DROP TABLE sa_core_test"))
            conn.commit()
            r.ok("DROP TABLE")
    except Exception as e:
        r.fail("core_operation", str(e))

    engine.dispose()


class Base(DeclarativeBase):
    pass


class OrmTest(Base):
    __tablename__ = "sa_orm_test"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    value: Mapped[float] = mapped_column(Double)


def test_orm(r):
    """SQLAlchemy ORM: mapped classes, Session, add, query, delete."""
    print("\n=== ORM ===")
    r.set_suite("orm")
    engine = create_engine(get_url())

    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_orm_test"))
            conn.commit()

        Base.metadata.create_all(engine)
        r.ok("ORM create_all")
    except Exception as e:
        r.fail("ORM create_all", str(e))
        engine.dispose()
        return

    try:
        with Session(engine) as session:
            session.add_all([
                OrmTest(id=1, name="alice", value=3.14),
                OrmTest(id=2, name="bob", value=2.72),
            ])
            session.commit()
            r.ok("ORM add_all", "2 rows")

            results = session.query(OrmTest).order_by(OrmTest.id).all()
            if len(results) == 2 and results[0].name == "alice":
                r.ok("ORM query", f"{len(results)} rows")
            else:
                r.fail("ORM query", f"unexpected: {results}")

            obj = session.get(OrmTest, 1)
            if obj and obj.name == "alice":
                r.ok("ORM get")
            else:
                r.fail("ORM get", f"unexpected: {obj}")

            obj.value = 9.99
            session.commit()
            refreshed = session.get(OrmTest, 1)
            if refreshed and refreshed.value == 9.99:
                r.ok("ORM update")
            else:
                r.fail("ORM update", f"value: {refreshed.value if refreshed else 'None'}")

            bob = session.get(OrmTest, 2)
            if bob:
                session.delete(bob)
                session.commit()
                r.ok("ORM delete")
            else:
                r.fail("ORM delete", "could not find id=2")

            remaining = session.query(OrmTest).count()
            if remaining == 1:
                r.ok("ORM post-delete count", "1 row")
            else:
                r.fail("ORM post-delete count", f"expected 1, got {remaining}")

        with engine.connect() as conn:
            conn.execute(text("DROP TABLE sa_orm_test"))
            conn.commit()
            r.ok("ORM drop")
    except Exception as e:
        r.fail("orm_operation", str(e))

    engine.dispose()


def test_inspection(r):
    """SQLAlchemy inspect() — reflection of existing schema."""
    print("\n=== Inspection ===")
    r.set_suite("inspection")
    engine = create_engine(get_url())

    try:
        insp = inspect(engine)

        schemas = insp.get_schema_names()
        if schemas:
            r.ok("get_schema_names", f"{len(schemas)} schemas")
        else:
            r.fail("get_schema_names", "empty")

        tables = insp.get_table_names()
        r.ok("get_table_names", f"{len(tables)} tables")

        views = insp.get_view_names()
        r.ok("get_view_names", f"{len(views)} views")

    except Exception as e:
        r.fail("inspection", str(e))

    engine.dispose()


def test_raw_params(r):
    """Parameterized queries via text() with :param style."""
    print("\n=== Raw parameterized ===")
    r.set_suite("raw_params")
    engine = create_engine(get_url())

    try:
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS sa_params"))
            conn.execute(text("CREATE TABLE sa_params (id INTEGER, label VARCHAR)"))
            conn.commit()

            conn.execute(
                text("INSERT INTO sa_params VALUES (:id, :label)"),
                [{"id": 1, "label": "one"}, {"id": 2, "label": "two"}],
            )
            conn.commit()
            r.ok("INSERT :params", "2 rows")

            rows = conn.execute(
                text("SELECT label FROM sa_params WHERE id = :id"),
                {"id": 1},
            ).fetchall()
            if len(rows) == 1 and rows[0][0] == "one":
                r.ok("SELECT :params")
            else:
                r.fail("SELECT :params", f"unexpected: {rows}")

            conn.execute(text("DROP TABLE sa_params"))
            conn.commit()
    except Exception as e:
        r.fail("raw_params", str(e))

    engine.dispose()


def main():
    wait_for_duckgres()

    rc = ReportClient("sqlalchemy")
    r = Results(rc)
    test_connection(r)
    test_shared_queries(r)
    test_core_ddl_dml(r)
    test_orm(r)
    test_inspection(r)
    test_raw_params(r)

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
