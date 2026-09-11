import io
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import register


def footer(table, statistics=True):
    out = io.BytesIO()
    pq.write_table(table, out, write_statistics=statistics)
    return pq.read_metadata(io.BytesIO(out.getvalue()))


class Store:
    def __init__(self, files):
        self.files = files
        self.copies = []

    def objects(self, uri):
        if uri.startswith("s3://destination/"):
            return []
        return [{"key": key, "size": 100, "etag": '"version"'} for key in self.files]

    def footer(self, bucket, obj):
        return self.files[obj["key"]]

    def copy(self, source_bucket, obj, destination_bucket, key):
        self.copies.append((obj["key"], key, obj["etag"]))


class API:
    def __init__(self):
        self.calls = []

    def post(self, path, body):
        self.calls.append((path, body))
        if path.endswith("/tables"):
            return {
                "table_uuid": "00000000-0000-0000-0000-000000000001",
                "columns": [
                    dict(c, field_id=i + 1) for i, c in enumerate(body["columns"])
                ],
            }
        return {"snapshot_id": 5}


class RegistrationTest(unittest.TestCase):
    def test_s3_footer_range_reads_and_conditional_byte_copy(self):
        out = io.BytesIO()
        pq.write_table(pa.table({"event": ["a", "b"]}), out)
        raw = out.getvalue()
        client = MagicMock()

        def read_object(**kwargs):
            start, end = map(int, kwargs["Range"][6:].split("-"))
            self.assertEqual(kwargs["IfMatch"], '"etag"')
            return {"Body": io.BytesIO(raw[start : end + 1])}

        client.get_object.side_effect = read_object
        client.head_object.return_value = {"ContentLength": len(raw)}
        store = register.S3Store(client)
        obj = {"key": "source/a.parquet", "size": len(raw), "etag": '"etag"'}
        self.assertEqual(store.footer("bucket", obj).num_rows, 2)
        self.assertEqual(client.get_object.call_count, 2)
        store.copy("bucket", obj, "other", "run/a.parquet")
        client.copy.assert_called_once_with(
            {"Bucket": "bucket", "Key": "source/a.parquet"},
            "other",
            "run/a.parquet",
            ExtraArgs={"CopySourceIfMatch": '"etag"'},
        )

    def test_rest_posts_wire_json(self):
        with patch.object(register, "urlopen") as opener:
            opener.return_value.__enter__.return_value = io.BytesIO(
                b'{"snapshot_id": 3}'
            )
            api = register.RestAPI("https://example.invalid", "token")
            self.assertEqual(
                api.post("/v1/catalogs", {"name": "fresh"}), {"snapshot_id": 3}
            )
            request = opener.call_args.args[0]
            self.assertEqual(request.full_url, "https://example.invalid/v1/catalogs")
            self.assertEqual(request.get_header("Authorization"), "Bearer token")
            self.assertEqual(request.data, b'{"name": "fresh"}')

    def test_existing_catalog_failure_prevents_copies(self):
        store = Store(
            {
                "frozen/events/a.parquet": footer(pa.table({"n": [1]})),
                "frozen/persons/a.parquet": footer(pa.table({"n": [1]})),
            }
        )
        api = API()
        api.post = MagicMock(side_effect=RuntimeError("409 exists"))
        with self.assertRaisesRegex(RuntimeError, "409"):
            register.run(
                store, api, "s3://source/frozen/", "s3://destination/run/", "fixture"
            )
        self.assertEqual(store.copies, [])

    def test_copies_all_files_and_registers_both_tables(self):
        store = Store(
            {
                "frozen/events/one.parquet": footer(
                    pa.table({"event": ["a"], "n": [1]})
                ),
                "frozen/events/two.parquet": footer(pa.table({"event": ["b"]})),
                "frozen/persons/one.parquet": footer(
                    pa.table(
                        {"person_version": pa.array([2**63 - 1], type=pa.uint64())}
                    )
                ),
            }
        )
        api = API()
        result = register.run(
            store, api, "s3://source/frozen/", "s3://destination/run/", "fixture"
        )
        self.assertEqual(len(store.copies), 3)
        self.assertEqual(result["record_count"], 3)
        tables = [body for path, body in api.calls if path.endswith("/tables")]
        self.assertEqual(tables[1]["columns"][0]["type"], "long")
        self.assertTrue(all(c["nullable"] for t in tables for c in t["columns"]))
        commits = [body for path, body in api.calls if path.endswith("/commit")]
        self.assertEqual(sum(len(a["files"]) for c in commits for a in c["appends"]), 3)
        self.assertTrue(
            all("expected_table_uuid" in a for c in commits for a in c["appends"])
        )

    def test_unsafe_uint64_fails_before_any_writes(self):
        for values, stats in [([2**63], True), ([1], False)]:
            with self.subTest(values=values, stats=stats):
                store = Store(
                    {
                        "frozen/events/a.parquet": footer(pa.table({"n": [1]})),
                        "frozen/persons/a.parquet": footer(
                            pa.table({"v": pa.array(values, type=pa.uint64())}), stats
                        ),
                    }
                )
                api = API()
                with self.assertRaisesRegex(ValueError, "uint64"):
                    register.run(
                        store,
                        api,
                        "s3://source/frozen/",
                        "s3://destination/run/",
                        "fixture",
                    )
                self.assertEqual(api.calls, [])
                self.assertEqual(store.copies, [])

    def test_overlapping_prefixes_rejected(self):
        for dest in ["s3://source/frozen/", "s3://source/frozen/sub/", "s3://source/"]:
            with self.assertRaisesRegex(ValueError, "overlap"):
                register.run(Store({}), API(), "s3://source/frozen/", dest, "fixture")

    def test_incompatible_schema_and_field_ids_rejected_before_writes(self):
        for second in [
            pa.table({"n": ["text"]}),
            pa.Table.from_arrays(
                [pa.array([1])],
                schema=pa.schema(
                    [pa.field("n", pa.int64(), metadata={b"PARQUET:field_id": b"9"})]
                ),
            ),
        ]:
            store = Store(
                {
                    "frozen/events/a.parquet": footer(pa.table({"n": [1]})),
                    "frozen/events/b.parquet": footer(second),
                    "frozen/persons/a.parquet": footer(pa.table({"id": [1]})),
                }
            )
            api = API()
            with self.assertRaises(ValueError):
                register.run(
                    store,
                    api,
                    "s3://source/frozen/",
                    "s3://destination/run/",
                    "fixture",
                )
            self.assertEqual(api.calls, [])
            self.assertEqual(store.copies, [])


if __name__ == "__main__":
    unittest.main()
