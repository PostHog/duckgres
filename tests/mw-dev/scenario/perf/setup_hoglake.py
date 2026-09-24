"""Register existing immutable S3 Parquet fixtures in the scenario Hoglake catalog."""

import argparse
import io
import json
import re
import struct
import time
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.parquet as pq


def location(uri):
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError("expected an s3://bucket/prefix/ URI")
    prefix = parsed.path.lstrip("/").rstrip("/")
    return parsed.netloc, prefix + "/" if prefix else ""


def column_type(field):
    t = field.type
    if pa.types.is_boolean(t):
        return {"type": "boolean"}
    if pa.types.is_int32(t):
        return {"type": "int"}
    if pa.types.is_int64(t) or pa.types.is_uint64(t):
        return {"type": "long"}
    if pa.types.is_float32(t):
        return {"type": "float"}
    if pa.types.is_float64(t):
        return {"type": "double"}
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return {"type": "string"}
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return {"type": "binary"}
    if pa.types.is_date32(t):
        return {"type": "date"}
    if pa.types.is_timestamp(t) and t.unit in ("ms", "us"):
        return {"type": "timestamptz" if t.tz else "timestamp"}
    if pa.types.is_time64(t) and t.unit == "us":
        return {"type": "time"}
    if pa.types.is_decimal128(t):
        return {
            "type": "decimal",
            "type_params": {"precision": t.precision, "scale": t.scale},
        }
    raise ValueError(
        f"unsupported fixture type for {field.name}: {t}; no columns are omitted"
    )


def inspect_table(objects, read_footer, selected=None):
    columns = {}
    files = []
    field_ids = []
    for obj in objects:
        metadata = read_footer(obj)
        schema = metadata.schema.to_arrow_schema()
        if selected is not None and any(schema.names.count(name) != 1 for name in selected):
            raise ValueError("missing selected fixture column")
        for index, field in enumerate(schema):
            if selected is not None and field.name not in selected:
                continue
            if not re.fullmatch(r"[a-z_][a-z0-9_]{0,127}", field.name):
                raise ValueError(
                    f"unsupported identifier {field.name!r}; lowercase column names required"
                )
            definition = dict(name=field.name, nullable=True, **column_type(field))
            previous = columns.get(field.name)
            if previous and previous != definition:
                pair = {previous["type"], definition["type"]}
                if pair == {"int", "long"}:
                    definition["type"] = "long"
                elif pair == {"float", "double"}:
                    definition["type"] = "double"
                else:
                    raise ValueError(f"incompatible schema evolution for {field.name}")
            columns[field.name] = definition
            raw_id = (field.metadata or {}).get(b"PARQUET:field_id")
            if raw_id is not None:
                field_ids.append((field.name, int(raw_id)))
            if pa.types.is_uint64(field.type):
                for group in range(metadata.num_row_groups):
                    chunk = metadata.row_group(group).column(index)
                    stats = chunk.statistics
                    if chunk.num_values == 0:
                        continue
                    if (
                        stats is not None
                        and stats.has_null_count
                        and stats.null_count == chunk.num_values
                    ):
                        continue
                    if (
                        stats is None
                        or not stats.has_min_max
                        or stats.min < 0
                        or stats.max > 2**63 - 1
                    ):
                        raise ValueError(
                            f"uint64 column {field.name} cannot be proven safe as signed long from every row-group footer; lossless rewrite required"
                        )
        files.append(
            dict(obj, rows=metadata.num_rows, footer_size=metadata.serialized_size)
        )
    expected_ids = {name: i + 1 for i, name in enumerate(columns)}
    if any(expected_ids[name] != field_id for name, field_id in field_ids):
        raise ValueError(
            "Parquet field IDs differ from the new Hoglake schema; lossless rewrite required"
        )
    return list(columns.values()), files


def run(store, api, source, catalog):
    source_bucket, source_prefix = location(source)
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,62}", catalog):
        raise ValueError("invalid catalog identifier")
    objects = sorted(store.objects(source), key=lambda obj: obj["key"])
    plans = {}
    for table in ("events", "persons"):
        prefix = source_prefix + table + "/"
        table_objects = [
            obj
            for obj in objects
            if obj["key"].startswith(prefix)
            and obj["key"].endswith(".parquet")
            and "/" not in obj["key"][len(prefix) :]
        ]
        if not table_objects:
            raise ValueError(f"no {table}/*.parquet files in source")
        plans[table] = inspect_table(
            table_objects, lambda obj: store.footer(source_bucket, obj)
        )
    # Validate all files before registering the catalog.
    root = "/v1/catalogs/" + catalog
    api.post(
        "/v1/catalogs", {"name": catalog, "data_path": f"s3://{source_bucket}/"}
    )
    api.post(root + "/namespaces", {"name": "posthog"})
    registrations = []
    for table, (columns, files) in plans.items():
        info = api.post(
            root + "/namespaces/posthog/tables", {"name": table, "columns": columns}
        )
        if [(c["name"], c["field_id"]) for c in info["columns"]] != [
            (c["name"], i + 1) for i, c in enumerate(columns)
        ]:
            raise ValueError(
                "server assigned unexpected field IDs; no source files registered"
            )
        registered = []
        for obj in files:
            registered.append(
                {
                    "path": f"s3://{source_bucket}/{obj['key']}",
                    "record_count": obj["rows"],
                    "file_size_bytes": obj["size"],
                    "footer_size": obj["footer_size"],
                }
            )
        registrations.append(
            {
                "namespace": "posthog",
                "table": table,
                "expected_table_uuid": info["table_uuid"],
                "files": registered,
            }
        )
    # Both relations become visible in one snapshot; no writes during benchmarks.
    result = api.post(
        root + "/commit",
        {
            "appends": registrations,
            "author": "perf-fixture",
            "message": "Register immutable benchmark fixtures",
        },
    )
    return registered_tables(result, registrations)



def run_properties(store, api, source, catalog, representation):
    """Register properties in the existing fixture catalog, under its own namespace.

    column_type intentionally remains authoritative: unsupported physical VARIANT
    representations fail before writes, without coercion or omitted columns.
    """
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,62}", catalog):
        raise ValueError("invalid catalog identifier")
    bucket, _ = location(source)
    objects = sorted(
        (obj for obj in store.objects(source) if obj["key"].endswith(".parquet")),
        key=lambda obj: obj["key"],
    )
    if not objects:
        raise ValueError("no properties Parquet files in source")
    metadata = {}

    def read_footer(obj):
        if obj["key"] not in metadata:
            metadata[obj["key"]] = store.footer(bucket, obj)
        return metadata[obj["key"]]

    plans = {}
    projections = [("events_supported", ["event", "timestamp", "properties"])]
    if representation == "variant":
        projections.append(("events_variant", ["event", "timestamp", "properties", "properties_variant"]))
    for table, selected in projections:
        columns, files = inspect_table(objects, read_footer, selected)
        expected_types = {"event": "string", "timestamp": "timestamptz", "properties": "string", "properties_variant": "variant"}
        if any(c["type"] != expected_types[c["name"]] for c in columns if c["name"] in expected_types):
            raise ValueError("properties fixture logical column type mismatch")
        plans[table] = columns, files
    # Reuse the frozen fixture catalog. Its bucket-root data_path covers both
    # immutable prefixes; object discovery still uses only the selected prefix.
    root = "/v1/catalogs/" + catalog
    info = api.get(root)
    data_bucket, data_prefix = location(info["data_path"])
    if bucket != data_bucket or any(not obj["key"].startswith(data_prefix) for obj in objects):
        raise ValueError("properties files must be inside the existing fixture catalog data_path")
    api.post(root + "/namespaces", {"name": "properties_perf"})
    registrations = []
    for table, (columns, files) in plans.items():
        info = api.post(root + "/namespaces/properties_perf/tables", {"name": table, "columns": columns})
        if [(c["name"], c["field_id"]) for c in info["columns"]] != [
            (c["name"], i + 1) for i, c in enumerate(columns)
        ]:
            raise ValueError("server assigned unexpected field IDs; no source files registered")
        registrations.append({
            "namespace": "properties_perf", "table": table,
            "expected_table_uuid": info["table_uuid"],
            "files": [{"path": f"s3://{bucket}/{obj['key']}", "record_count": obj["rows"],
                       "file_size_bytes": obj["size"], "footer_size": obj["footer_size"]} for obj in files],
        })
    result = api.post(root + "/commit", {"appends": registrations, "author": "perf-fixture",
                                        "message": "Register immutable properties fixtures"})
    return registered_tables(result, registrations)


def registered_tables(result, registrations):
    """The commit receipt plus the (namespace, table) pairs it registered files into."""
    return dict(result, tables=[{"namespace": r["namespace"], "table": r["table"]} for r in registrations])


def wait_for_stats(api, catalog, tables, timeout, poll_interval=5.0, clock=time.monotonic, sleep=time.sleep):
    """Block until every registered file of ``tables`` has hydrated stats.

    The importer registers footers only (deferred stats), so each file
    starts ``pending`` and carries no column bounds until the Hoglake
    hydrator reads its footer. A benchmark measured before that runs every
    query against stat-less files, which no engine can prune: it would
    report the no-pruning cost as if it were the steady state, where
    production writers ship stats at commit. So the import is not done
    until hydration is, and a file the hydrator refuses (``failed``) or a
    sweep that never arrives fails the scenario instead of silently
    producing that benchmark.

    ``tables`` is a list of (namespace, table) pairs. Returns the number of
    files checked.
    """
    root = "/v1/catalogs/" + quote(catalog, safe="")
    deadline = clock() + timeout
    while True:
        counts = {"provided": 0, "pending": 0, "failed": 0}
        failed = []
        for namespace, table in tables:
            files = api.get(
                f"{root}/namespaces/{quote(namespace, safe='')}/tables/{quote(table, safe='')}/files"
            )
            if not files:
                raise ValueError(f"{namespace}.{table} has no registered files to hydrate")
            for f in files:
                state = f.get("stats_state")
                if state not in counts:
                    raise ValueError(f"{namespace}.{table}: unexpected stats_state {state!r}")
                counts[state] += 1
                if state == "failed" and len(failed) < 5:
                    failed.append(f"{namespace}.{table}:{f.get('path')}")
        if counts["failed"]:
            raise ValueError(
                f"Hoglake stats hydration failed for {counts['failed']} fixture file(s) "
                f"(e.g. {', '.join(failed)}); benchmarking stat-less files would measure no pruning"
            )
        if counts["pending"] == 0:
            return counts["provided"]
        if clock() >= deadline:
            raise TimeoutError(
                f"Hoglake stats hydration incomplete after {timeout:g}s: "
                f"{counts['pending']} of {sum(counts.values())} fixture files still pending "
                "(is the hydrator loop enabled with a short HOGLAKE_HYDRATOR_INTERVAL_MS?)"
            )
        print(f"Waiting for Hoglake stats hydration: {counts['pending']} of {sum(counts.values())} files pending")
        sleep(poll_interval)


class S3Store:
    def __init__(self, client):
        self.client = client

    def objects(self, uri):
        bucket, prefix = location(uri)
        return [
            {"key": obj["Key"], "size": obj["Size"], "etag": obj["ETag"]}
            for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix
            )
            for obj in page.get("Contents", [])
        ]

    def footer(self, bucket, obj):
        def read(start, end):
            response = self.client.get_object(
                Bucket=bucket,
                Key=obj["key"],
                Range=f"bytes={start}-{end}",
                IfMatch=obj["etag"],
            )
            with response["Body"] as body:
                return body.read()

        size = obj["size"]
        if size < 12:
            raise ValueError("invalid Parquet file size")
        trailer = read(size - 8, size - 1)
        if len(trailer) != 8 or trailer[4:] != b"PAR1":
            raise ValueError("invalid or encrypted Parquet footer")
        length = struct.unpack("<I", trailer[:4])[0]
        if length > size - 12:
            raise ValueError("invalid Parquet footer length")
        raw = read(size - 8 - length, size - 9)
        if len(raw) != length:
            raise ValueError("truncated Parquet footer")
        return pq.read_metadata(io.BytesIO(b"PAR1" + raw + trailer))

class RestAPI:
    def __init__(self, uri):
        parsed = urlparse(uri)
        if (
            parsed.scheme not in ("https", "http")
            or not parsed.hostname
            or parsed.query
            or parsed.fragment
            or parsed.username
        ):
            raise ValueError("invalid Hoglake API URI")
        self.uri = uri.rstrip("/")

    def get(self, path):
        with urlopen(self.uri + path, timeout=120) as response:
            return json.load(response)

    def post(self, path, body):
        headers = {"Content-Type": "application/json"}
        request = Request(
            self.uri + path,
            data=json.dumps(body).encode(),
            headers=headers,
            method="POST",
        )
        with urlopen(request, timeout=120) as response:
            return json.load(response)


def main():
    import boto3

    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--source")
    inputs.add_argument("--properties-source")
    parser.add_argument("--properties-representation", choices=("json", "variant"), default="variant")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--uri", required=True)
    parser.add_argument(
        "--hydration-timeout",
        type=float,
        default=600.0,
        help="seconds to wait for every registered file's stats to hydrate",
    )
    args = parser.parse_args()
    store, api = S3Store(boto3.client("s3")), RestAPI(args.uri)
    if args.properties_source:
        result = run_properties(store, api, args.properties_source, args.catalog, args.properties_representation)
    else:
        result = run(store, api, args.source, args.catalog)
    print(f"Registered frozen fixtures at snapshot {result['snapshot_id']}")
    tables = [(t["namespace"], t["table"]) for t in result["tables"]]
    checked = wait_for_stats(api, args.catalog, tables, args.hydration_timeout)
    print(f"Hoglake stats hydrated for all {checked} fixture files")


if __name__ == "__main__":
    main()
