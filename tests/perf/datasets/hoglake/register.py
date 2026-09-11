"""Copy immutable Parquet fixtures byte-for-byte and register a fresh Hoglake catalog."""

import argparse
import io
import json
import re
import struct
from urllib.parse import urlparse
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
    if pa.types.is_timestamp(t) and t.unit == "us":
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


def inspect_table(objects, read_footer):
    columns = {}
    files = []
    field_ids = []
    for obj in objects:
        metadata = read_footer(obj)
        schema = metadata.schema.to_arrow_schema()
        for index, field in enumerate(schema):
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


def run(store, api, source, destination, catalog):
    source_bucket, source_prefix = location(source)
    destination_bucket, destination_prefix = location(destination)
    if source_bucket == destination_bucket and (
        source_prefix.startswith(destination_prefix)
        or destination_prefix.startswith(source_prefix)
    ):
        raise ValueError("source and destination prefixes must not overlap")
    if not destination_prefix:
        raise ValueError("destination must be a dedicated nonempty prefix")
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,62}", catalog):
        raise ValueError("catalog must be a fresh lowercase identifier")
    if store.objects(destination):
        raise ValueError("destination prefix must be empty; use a fresh run prefix")
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
    # No API writes or S3 copies occur until every footer has passed validation.
    root = "/v1/catalogs/" + catalog
    api.post(
        "/v1/catalogs", {"name": catalog, "data_path": destination.rstrip("/") + "/"}
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
            key = destination_prefix + obj["key"][len(source_prefix) :]
            store.copy(source_bucket, obj, destination_bucket, key)
            registered.append(
                {
                    "path": f"s3://{destination_bucket}/{key}",
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
            "message": "Register immutable benchmark fixture copies",
        },
    )
    return {
        "catalog": catalog,
        "snapshot_id": result["snapshot_id"],
        "file_count": sum(len(files) for _, files in plans.values()),
        "record_count": sum(f["rows"] for _, files in plans.values() for f in files),
        "tables": {
            name: {
                "columns": columns,
                "file_count": len(files),
                "record_count": sum(f["rows"] for f in files),
            }
            for name, (columns, files) in plans.items()
        },
    }


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

    def copy(self, source_bucket, obj, destination_bucket, key):
        self.client.copy(
            {"Bucket": source_bucket, "Key": obj["key"]},
            destination_bucket,
            key,
            ExtraArgs={"CopySourceIfMatch": obj["etag"]},
        )
        if (
            self.client.head_object(Bucket=destination_bucket, Key=key)["ContentLength"]
            != obj["size"]
        ):
            raise ValueError("copied object size changed")


class RestAPI:
    def __init__(self, uri, token=None):
        parsed = urlparse(uri)
        if (
            parsed.scheme not in ("https", "http")
            or not parsed.hostname
            or parsed.query
            or parsed.fragment
            or parsed.username
        ):
            raise ValueError("invalid Hoglake API URI")
        if parsed.scheme == "http" and parsed.hostname not in (
            "localhost",
            "127.0.0.1",
            "::1",
        ):
            raise ValueError("HTTP allowed only for a local development server")
        self.uri = uri.rstrip("/")
        self.token = token

    def post(self, path, body):
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = Request(
            self.uri + path,
            data=json.dumps(body).encode(),
            headers=headers,
            method="POST",
        )
        with urlopen(request, timeout=120) as response:
            return json.load(response)


def main():
    import os

    import boto3

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()
    if os.path.exists(args.manifest):
        parser.error("manifest already exists; choose a fresh path")
    result = run(
        S3Store(boto3.client("s3")),
        RestAPI(args.uri, os.environ.get("HOGLAKE_TOKEN")),
        args.source,
        args.destination,
        args.catalog,
    )
    with open(args.manifest, "x") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    print(
        f"Registered {result['file_count']} files at snapshot {result['snapshot_id']}"
    )


if __name__ == "__main__":
    main()
