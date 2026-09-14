"""Measure Bronze replay and new-message writes after pre-seeding the audit log.

The benchmark uses a temporary disk-backed DuckDB database. It never touches
the configured production lakehouse. Example:
    python -m scripts.benchmark_bronze_replay --rows 100000 \
        --output docs/bronze-replay-baseline.json
"""
import argparse
import json
import statistics
import tempfile
from pathlib import Path
from time import perf_counter

from lakehouse.db import LakehouseManager
from lakehouse.records import KafkaEnvelope


def percentile(values, fraction):
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * fraction
    lower, upper = int(rank), min(int(rank) + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def event_bytes(event_id):
    return json.dumps({
        'id': event_id,
        'source': 'RSS',
        'title': 'Benchmark headline for a new financial event',
        'published_at': '2026-09-01T14:00:00+00:00',
        'ingested_time': '2026-09-01T14:05:00+00:00',
        'content_snippet': '',
        'content_full': None,
        'metadata': {},
    }, separators=(',', ':')).encode('utf-8')


def measure(manager, messages, warmups=2, samples=30):
    for message in messages[:warmups]:
        manager.write_message_batch([message], {'rss': 'RSS'})
    timings = []
    for message in messages[warmups:warmups + samples]:
        started = perf_counter()
        manager.write_message_batch([message], {'rss': 'RSS'})
        timings.append((perf_counter() - started) * 1000)
    return {
        'samples': len(timings),
        'mean_ms': statistics.mean(timings),
        'p50_ms': statistics.median(timings),
        'p95_ms': percentile(timings, .95),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=100_000)
    parser.add_argument('--output', type=Path, default=Path('docs/bronze-replay-baseline.json'))
    args = parser.parse_args()
    if args.rows < 100_000:
        raise SystemExit('--rows must be at least 100000')

    with tempfile.TemporaryDirectory(prefix='bronze-replay-') as temp:
        database = Path(temp) / 'benchmark.duckdb'
        manager = LakehouseManager(str(database))
        manager.conn.execute('''INSERT INTO bronze_events_raw
            (kafka_topic,kafka_partition,kafka_offset,raw_payload,raw_bytes,provenance_status)
            SELECT 'benchmark', i % 4, i // 4, '{}', CAST('{}' AS BLOB), 'benchmark_seed'
            FROM range(?) t(i)''', [args.rows])
        manager.conn.execute('CHECKPOINT')
        bytes_before = database.stat().st_size
        replay_offset = (args.rows // 4) - 1
        replay = [KafkaEnvelope('benchmark', 0, replay_offset, b'not parsed on replay')]
        # Warm up the same statement path, then take 30 replay measurements.
        for _ in range(2):
            manager.write_message_batch(replay)
        replay_times = []
        for _ in range(30):
            started = perf_counter()
            result = manager.write_message_batch(replay)
            if result['replayed'] != 1:
                raise RuntimeError('Replay fixture was not recognized')
            replay_times.append((perf_counter() - started) * 1000)

        new_messages = [KafkaEnvelope('rss', 0, args.rows + i, event_bytes(f'benchmark-new-{i}'))
                        for i in range(32)]
        new_result = measure(manager, new_messages)
        plan = manager.conn.execute('''EXPLAIN ANALYZE
            SELECT 1 FROM bronze_events_raw
            WHERE kafka_topic=? AND kafka_partition=? AND kafka_offset=?''',
            ['benchmark', 0, replay_offset]).fetchone()[1]
        manager.close()
        bytes_after = database.stat().st_size

    output = {
        'measured_at': __import__('datetime').datetime.now(__import__('datetime').timezone.utc).isoformat(),
        'duckdb': __import__('duckdb').__version__,
        'seed_rows': args.rows,
        'seed_layout': 'disk-backed Bronze rows across four partitions; small JSON payloads',
        'replay': {
            'samples': len(replay_times),
            'mean_ms': statistics.mean(replay_times),
            'p50_ms': statistics.median(replay_times),
            'p95_ms': percentile(replay_times, .95),
        },
        'new_message_batch_size_one': new_result,
        'database_bytes_before_new_messages': bytes_before,
        'database_bytes_after_new_messages': bytes_after,
        'explain_analyze': {
            'scan_type': 'Sequential Scan' if 'Sequential Scan' in plan else 'not detected',
            'plan_excerpt': [line.strip() for line in plan.splitlines() if 'Scan' in line or 'rows' in line.lower()],
        },
        'limitations': [
            'Replay timing measures one-message write_message_batch calls, not sustained feed throughput.',
            'The seed uses small payloads and does not represent resident ART/index memory.',
            'DuckDB reports a sequential scan for the composite-key lookup; the primary key still enforces uniqueness.',
            'Run with --rows 1000000 for the million-row comparison before setting production targets.',
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
