import io
import unittest

import pyarrow as pa
import pyarrow.parquet as pq
import setup_hoglake


def footer(table, statistics=True):
    out = io.BytesIO()
    pq.write_table(table, out, write_statistics=statistics)
    return pq.read_metadata(io.BytesIO(out.getvalue()))


class SetupTest(unittest.TestCase):
    def test_registers_original_files_and_every_column(self):
        metadata = footer(pa.table({'team_id': pa.array([1], type=pa.uint64()), 'event': ['test']}))
        class Store:
            def objects(self, uri):
                return [{'key': 'frozen/' + name + '/data.parquet', 'size': 100} for name in ('events', 'persons')]
            def footer(self, bucket, obj):
                return metadata
        calls = []
        class API:
            def post(self, path, body):
                calls.append((path, body))
                if path.endswith('/tables'):
                    return {'table_uuid': '00000000-0000-0000-0000-000000000001', 'columns': [dict(c, field_id=i+1) for i,c in enumerate(body['columns'])]}
                return {'snapshot_id': 5}
        setup_hoglake.run(Store(), API(), 's3://example/frozen/', 'ci-pr-123-cnpg')
        self.assertEqual(calls[0][1]['name'], 'ci-pr-123-cnpg')
        self.assertEqual([c['name'] for c in calls[2][1]['columns']], ['team_id', 'event'])
        appends = calls[-1][1]['appends']
        self.assertEqual([a['files'][0]['path'] for a in appends], ['s3://example/frozen/events/data.parquet', 's3://example/frozen/persons/data.parquet'])

    def test_uint64_overflow_or_missing_statistics_fails(self):
        for values, stats in [([2**63], True), ([1], False)]:
            metadata = footer(pa.table({'team_id': pa.array(values, type=pa.uint64())}), stats)
            with self.assertRaisesRegex(ValueError, 'cannot be proven safe'):
                setup_hoglake.inspect_table([{'key': 'x', 'size': 100}], lambda obj: metadata)

if __name__ == '__main__':
    unittest.main()
