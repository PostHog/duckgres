#!/usr/bin/env python3
"""Bounded, synthetic-only DuckDB memory experiment; each query uses a fresh process."""
import argparse
import hashlib
import json
from pathlib import Path
import resource
import subprocess
import sys
import time

BROWSERS = ('Chrome', 'Firefox', 'Safari', 'Edge')


def emit(path, record):
    line = json.dumps(record, sort_keys=True)
    with path.open('a') as stream:
        stream.write(line + '\n')
    print(line, flush=True)


def generate(args, fixture):
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([('properties', pa.string()), ('properties_typed', pa.struct([('$browser', pa.string())]))])
    with pq.ParquetWriter(fixture, schema, compression='zstd', use_dictionary=False) as writer:
        for start in range(0, args.rows, args.row_group_size):
            docs, typed = [], []
            for row in range(start, min(start + args.row_group_size, args.rows)):
                browser = BROWSERS[row % len(BROWSERS)]
                prefix = '{"$browser":' + json.dumps(browser) + ',"pad":"'
                pad_size = args.width - len(prefix) - 2
                # Unique deterministic hex payloads: moderately compressible, no repeated-row shortcut.
                pad = hashlib.shake_256(str(row).encode()).hexdigest((pad_size + 1) // 2)[:pad_size]
                docs.append(prefix + pad + '"}')
                typed.append({'$browser': browser})
            writer.write_table(pa.Table.from_arrays([pa.array(docs), pa.array(typed, type=schema.field('properties_typed').type)], schema=schema), row_group_size=args.row_group_size)
    return fixture.stat().st_size


def child(args):
    import duckdb
    baseline_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    con = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
    con.execute('SET threads = ' + str(args.child_threads))
    con.execute('SET memory_limit = ?', [args.memory_limit])
    spill = args.work_dir / ('spill-' + str(args.child_threads) + '-' + args.child_query)
    spill.mkdir(exist_ok=True)
    con.execute('SET temp_directory = ?', [str(spill)])
    # The pinned image bundles the same custom extensions as the perf worker.
    extension_dir = Path('/opt/duckdb-extensions')
    loaded = []
    if extension_dir.exists():
        for name in ('httpfs', 'ducklake'):
            con.execute("LOAD '" + str(extension_dir / (name + '.duckdb_extension')) + "'")
            loaded.append(name)
    con.execute("CREATE VIEW events AS SELECT * FROM read_parquet('" + str(args.work_dir / 'fixture.parquet').replace("'", "''") + "')")
    expression = 'json_extract_string(properties, \'$."$browser"\')' if args.child_query == 'json' else 'properties_typed."$browser"'
    query = 'SELECT SUM(length(properties)) FROM events' if args.child_query == 'length' else f'SELECT {expression} AS browser, COUNT(*) AS event_count FROM events WHERE {expression} IS NOT NULL GROUP BY 1 ORDER BY event_count DESC, browser ASC LIMIT 20'
    profile = args.work_dir / ('profile-' + str(args.child_threads) + '-' + args.child_query + '.json')
    con.execute("SET enable_profiling = 'json'")
    con.execute('SET profiling_output = ?', [str(profile)])
    record = {'duckdb_version': duckdb.__version__, 'loaded_extensions': loaded, 'effective_memory_limit': con.execute("SELECT current_setting('memory_limit')").fetchone()[0]}
    # The settings query can produce a profile: remove it so an error cannot report stale metrics.
    profile.unlink(missing_ok=True)
    start = time.perf_counter()
    try:
        result = con.execute(query).fetchall()
        expected = [(args.rows * args.width,)] if args.child_query == 'length' else sorted([(b, args.rows // 4 + (i < args.rows % 4)) for i, b in enumerate(BROWSERS)], key=lambda v: (-v[1], v[0]))
        record.update(status='ok' if result == expected else 'incorrect_result', result=result)
    except Exception as error:
        record.update(status='error', error_type=type(error).__name__, error=str(error)[:1500])
    record['elapsed_seconds'] = time.perf_counter() - start
    rss_scale = 1 if sys.platform == 'darwin' else 1024
    record['peak_rss_bytes'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * rss_scale
    record['baseline_peak_rss_bytes'] = baseline_rss * rss_scale
    if profile.exists():
        stats = json.loads(profile.read_text())
        record['duckdb_peak_buffer_bytes'] = stats.get('system_peak_buffer_memory')
        record['duckdb_peak_temp_bytes'] = stats.get('system_peak_temp_dir_size')
    else:
        record['duckdb_peak_buffer_bytes'] = None
        record['duckdb_peak_temp_bytes'] = None
    print(json.dumps(record), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--width', type=int, required=True)
    parser.add_argument('--rows', type=int, default=131072)
    parser.add_argument('--row-group-size', type=int, default=8192)
    parser.add_argument('--threads', default='1,4,8')
    parser.add_argument('--memory-limit', default='48GiB')
    parser.add_argument('--query-timeout', type=int, default=180)
    parser.add_argument('--work-dir', type=Path, default=Path('/work'))
    parser.add_argument('--output', type=Path, default=Path('/work/results.jsonl'))
    parser.add_argument('--child-query', choices=['json', 'length', 'struct'])
    parser.add_argument('--child-threads', type=int)
    args = parser.parse_args()
    if args.width < 64 or min(args.rows, args.row_group_size, args.query_timeout) < 1:
        parser.error('width must be >=64 and rows, row-group-size, timeout must be positive')
    threads = [int(value) for value in args.threads.split(',')]
    if any(value < 1 for value in threads):
        parser.error('threads must be positive')
    args.work_dir.mkdir(parents=True, exist_ok=True)
    if args.child_query:
        child(args)
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists():
        parser.error('output exists; use a fresh work directory/output for each run')
    start = time.perf_counter()
    size = generate(args, args.work_dir / 'fixture.parquet')
    base = {'width_bytes': args.width, 'rows': args.rows, 'row_group_rows': args.row_group_size, 'memory_limit': args.memory_limit}
    emit(args.output, dict(base, kind='fixture', parquet_bytes=size, generation_seconds=time.perf_counter() - start))
    failed = False
    for thread_count in threads:
        for query in ('json', 'length', 'struct'):
            command = [sys.executable, __file__, '--width', str(args.width), '--rows', str(args.rows), '--work-dir', str(args.work_dir), '--memory-limit', args.memory_limit, '--child-query', query, '--child-threads', str(thread_count)]
            start = time.perf_counter()
            try:
                run = subprocess.run(command, capture_output=True, text=True, timeout=args.query_timeout)
                if run.returncode == 0:
                    result = json.loads(run.stdout)
                else:
                    result = {'status': 'process_failed', 'exit_code': run.returncode, 'error': run.stderr[-1500:], 'elapsed_seconds': time.perf_counter() - start}
            except subprocess.TimeoutExpired:
                result = {'status': 'timeout', 'elapsed_seconds': time.perf_counter() - start}
            except json.JSONDecodeError:
                result = {'status': 'invalid_child_output', 'elapsed_seconds': time.perf_counter() - start}
            failed |= result['status'] != 'ok'
            emit(args.output, dict(base, kind='query', threads=thread_count, query=query, **result))
    emit(args.output, dict(base, kind='complete', status='failed' if failed else 'ok'))
    sys.exit(1 if failed else 0)


if __name__ == '__main__':
    main()
