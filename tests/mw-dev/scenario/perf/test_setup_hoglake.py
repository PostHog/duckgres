"""Real Parquet footer checks for the manifest-selected properties bridge."""
import io
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

import setup_hoglake as setup


def footer(fields):
    schema = pa.schema(fields)
    buf = io.BytesIO()
    table = pa.Table.from_arrays([pa.array([None], type=f.type) for f in schema], schema=schema)
    pq.write_table(table, buf)
    return pq.read_metadata(io.BytesIO(buf.getvalue()))


def fields():
    return [pa.field("event", pa.string()), pa.field("timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("properties", pa.string()),
            pa.field("properties_typed", pa.struct([pa.field("$browser", pa.string())])),
            pa.field("properties_variant", pa.struct([pa.field("metadata", pa.binary()), pa.field("value", pa.binary())]))]


class Store:
    def __init__(self, objects, metadata):
        self.inventory = objects
        self.metadata = metadata
        self.reads = []

    def objects(self, uri):
        self.uri = uri
        return self.inventory

    def footer(self, bucket, obj):
        self.reads.append((bucket, obj))
        return self.metadata


class API:
    def __init__(self):
        self.calls = []

    def post(self, path, body):
        self.calls.append((path, body))
        raise AssertionError("must validate all fixture types before ANY API write")


class PropertiesRegistrationTests(unittest.TestCase):
    def setUp(self):
        self.objects = [{"key": "derived/day/data/part.parquet", "size": 500, "etag": '"' + "a" * 32 + '"'}]
        self.plan = {"destination_prefix": "s3://example-fixture/derived/day/", "outputs": self.objects, "rows": 1}
        self.api = API()

    def test_selected_supported_fields_ignore_unselected_nested_types(self):
        columns, files = setup.inspect_table(self.objects, lambda obj: footer(fields()), ["event", "timestamp", "properties"])
        self.assertEqual([c["name"] for c in columns], ["event", "timestamp", "properties"])
        self.assertEqual(files[0]["rows"], 1)

    def test_missing_selected_field_fails(self):
        with self.assertRaisesRegex(ValueError, "missing selected"):
            setup.inspect_table(self.objects, lambda obj: footer(fields()[:2]), ["event", "timestamp", "properties"])

    def test_projection_preserves_field_id_guard(self):
        schema = [pa.field("ignored", pa.string(), metadata={b"PARQUET:field_id": b"1"}),
                  pa.field("event", pa.string(), metadata={b"PARQUET:field_id": b"2"})]
        with self.assertRaisesRegex(ValueError, "field IDs differ"):
            setup.inspect_table(self.objects, lambda obj: footer(schema), ["event"])

    def test_variant_rejected_before_any_api_writes(self):
        store = Store(self.objects, footer(fields()))
        with self.assertRaisesRegex(ValueError, "unsupported fixture type for properties_variant"):
            setup.run_properties(store, self.api, self.plan, "example-catalog")
        self.assertEqual(self.api.calls, [])
        self.assertEqual(store.uri, "s3://example-fixture/derived/day/data/")

    def test_scalar_variant_is_not_a_native_variant_fallback(self):
        schema = fields()[:3] + [pa.field("properties_variant", pa.string())]
        with self.assertRaisesRegex(ValueError, "logical column type mismatch"):
            setup.run_properties(Store(self.objects, footer(schema)), self.api, self.plan, "example-catalog")
        self.assertEqual(self.api.calls, [])

    def test_row_count_is_checked_before_writes(self):
        plan = dict(self.plan, rows=2)
        with self.assertRaisesRegex(ValueError, "row count mismatch"):
            setup.run_properties(Store(self.objects, footer(fields())), self.api, plan, "example-catalog")
        self.assertEqual(self.api.calls, [])

    def test_exact_inventory_required_before_reading_footers(self):
        for change in ("extra", "missing", "etag", "size", "duplicate"):
            with self.subTest(change=change):
                live = [dict(self.objects[0])]
                if change == "extra":
                    live.append(dict(live[0], key="derived/day/data/extra.parquet"))
                elif change == "missing":
                    live = []
                elif change == "duplicate":
                    live.append(dict(live[0]))
                else:
                    live[0][change] = "different" if change == "etag" else 999
                store = Store(live, footer(fields()))
                with self.assertRaisesRegex(ValueError, "inventory"):
                    setup.run_properties(store, self.api, self.plan, "example-catalog")
                self.assertEqual(store.reads, [])
                self.assertEqual(self.api.calls, [])

    def test_manifest_objects_must_stay_under_exact_data_prefix(self):
        plan = dict(self.plan, outputs=[dict(self.objects[0], key="outside/part.parquet")])
        with self.assertRaisesRegex(ValueError, "inventory"):
            setup.run_properties(Store(plan["outputs"], footer(fields())), self.api, plan, "example-catalog")


if __name__ == "__main__":
    unittest.main()
