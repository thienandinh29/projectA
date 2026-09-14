"""python -m scripts.benchmark_tier2 --output docs/tier2-baseline.json"""
import argparse
import json
import logging
import os
import platform
import statistics
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from config import REDIS_HOST, REDIS_PORT
from scripts.tier2_fixtures import QUERY, ScopedRedis, seed_candidates
from utils.redis_cache import RedisDeduplicator


def percentile(samples, q):
    ordered = sorted(samples)
    index = (len(ordered) - 1) * q
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def benchmark(repeats=30):
    rows = []
    for count in (0, 15, 256, 272, 1000, 5000):
        dedup = RedisDeduplicator(db=15)
        raw = dedup.client
        raw.ping()
        scoped = ScopedRedis(raw)
        dedup.client = scoped
        try:
            payload_bytes = seed_candidates(scoped, count)
            samples, phases = [], []
            for _ in range(repeats + 2):
                metrics = {}
                started = perf_counter()
                result = dedup.check_near_duplicate('probe', QUERY, metrics=metrics)
                samples.append((perf_counter() - started) * 1000)
                assert metrics['candidate_count'] == count, metrics
                assert result == ((True, 'original') if count else (False, None)), result
                phases.append(metrics)
            samples, phases = samples[2:], phases[2:]
            rows.append({'candidates': count, 'record_payload_bytes': payload_bytes,
                         'p50_ms': percentile(samples, .5), 'p95_ms': percentile(samples, .95),
                         'phase_mean_ms': {k: statistics.mean(p[k] for p in phases)
                                           for k in phases[0] if k.endswith('_ms')}})
            print(f'{count}: p50={rows[-1]["p50_ms"]:.2f}ms p95={rows[-1]["p95_ms"]:.2f}ms', flush=True)
        finally:
            scoped.cleanup()
    return {'measured_at': datetime.now(timezone.utc).isoformat(),
            'hardware': {'os': platform.platform(), 'python': platform.python_version(),
                         'cpu': os.getenv('PROCESSOR_IDENTIFIER', platform.processor()), 'logical_cpus': os.cpu_count()},
            'redis': {'host': REDIS_HOST, 'port': REDIS_PORT, 'db': 15},
            'samples_per_size': repeats, 'warmups': 2,
            'fixture': 'Synthetic repeated headlines; actual LSH signatures; final exact match wins.',
            'limitations': 'Instrumented single-thread Tier 2 only; timers add overhead. No live feed-rate target or Tier 3. Payload bytes exclude Redis allocator/bucket overhead.',
            'results': rows}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repeats', type=int, default=30)
    args = parser.parse_args()
    if args.repeats < 2:
        parser.error('--repeats must be >=2')
    logging.getLogger('utils.redis_cache').setLevel(logging.WARNING)
    report = benchmark(args.repeats)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding='utf-8')
