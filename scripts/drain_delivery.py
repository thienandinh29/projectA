"""Retry staged delivery without fetching feeds; wait for durable writer commits.

Stop ALL collectors first. Keep the writer running when deliveries remain;
an unchanged idle boundary can be inspected with the writer stopped. Exit 0
verifies transport progress; prospective export separately reconciles storage.
"""
import argparse
import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from config import (OUTBOX_PATH, KAFKA_BOOTSTRAP_SERVERS, LAKEHOUSE_GROUP_ID, LAKEHOUSE_DB_PATH,
                    TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC, TOPIC_OBSERVATIONS)
from utils.kafka_producer import RedpandaProducer
from research.collection_boundary import load_baseline


def writer_backlog(consumer, topics, deadline, baseline_partitions=None):
    from confluent_kafka import TopicPartition
    def budget():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Backlog inspection timed out')
        return min(5.0, remaining)
    metadata = consumer.list_topics(timeout=budget())
    partitions = []
    actual = set()
    for topic in topics:
        info = metadata.topics.get(topic)
        if info is None or info.error or not info.partitions:
            raise RuntimeError(f'Cannot verify topic {topic}')
        for partition in info.partitions:
            actual.add((topic, partition))
            partitions.append(TopicPartition(topic, partition))
    if baseline_partitions is not None and actual != set(baseline_partitions):
        missing = sorted(set(baseline_partitions) - actual)
        added = sorted(actual - set(baseline_partitions))
        raise RuntimeError(f'Kafka partition set changed; missing={missing}, added={added}')
    committed = consumer.committed(partitions, timeout=budget())
    progress = {}
    returned = [(p.topic, p.partition) for p in committed]
    if len(returned) != len(actual) or set(returned) != actual:
        raise RuntimeError('Incomplete or unexpected partition offset response')
    for p in committed:
        if p.error:
            raise RuntimeError(str(p.error))
        low, high = consumer.get_watermark_offsets(p, timeout=budget(), cached=False)
        key, label = (p.topic, p.partition), f'{p.topic}:{p.partition}'
        committed_offset = p.offset if p.offset >= 0 else None
        if baseline_partitions is None:
            # Legacy whole-retained-log check, kept for callers without a baseline.
            if (committed_offset is None and low > 0) or (
                    committed_offset is not None and committed_offset < low) or (
                    committed_offset is not None and committed_offset > high):
                raise RuntimeError(f'Unverifiable retained history: {label}')
            progress[label] = max(0, high - max(0, committed_offset or 0))
            continue
        baseline = baseline_partitions[key]
        start, captured_commit = baseline['start'], baseline['committed_at_capture']
        if low > high:
            raise RuntimeError(f'Invalid broker offset range: {label}')
        if high < start:
            raise RuntimeError(f'Broker high watermark regressed before baseline: {label}')
        if captured_commit is not None and (
                (committed_offset is None and high > start) or
                (committed_offset is not None and committed_offset < captured_commit)):
            raise RuntimeError(f'Writer commit regressed from baseline: {label}')
        if committed_offset is not None and committed_offset > high:
            raise RuntimeError(f'Writer commit exceeds broker high watermark: {label}')
        if high > start and committed_offset is None:
            raise RuntimeError(f'Post-baseline messages have no writer commit: {label}')
        if committed_offset is not None and committed_offset < start and high > start:
            raise RuntimeError(f'Writer commit is behind baseline: {label}')
        if low > start and (committed_offset is None or committed_offset < low):
            raise RuntimeError(f'Retention overtook unprocessed post-baseline data: {label}')
        effective_commit = start if committed_offset is None else max(start, committed_offset)
        lag = max(0, high - effective_commit)
        progress[label] = {
            'baseline_next_offset': start,
            'retained_start_offset': low,
            'broker_high_watermark_next_offset': high,
            'writer_committed_next_offset': committed_offset,
            'post_baseline_messages': high - start,
            'commit_expired_at_idle_boundary': captured_commit is not None and committed_offset is None,
            'lag': lag,
            'status': 'no_new_records' if high == start else 'complete' if lag == 0 else 'writer_lag',
        }
    if len(progress) != len(partitions):
        raise RuntimeError('Incomplete partition offset response')
    return progress


def _lag_values(progress):
    return {key: value['lag'] if isinstance(value, dict) else value
            for key, value in progress.items()}


def drain(producer, backlog, timeout=120, batch_size=256, allowed_topics=None):
    if not math.isfinite(timeout) or timeout <= 0 or batch_size < 1:
        raise ValueError('Positive timeout and batch size required')
    deadline = time.monotonic() + timeout
    last_lag, error = None, None
    while time.monotonic() < deadline:
        for topic, event_id, payload in producer.outbox.pending(limit=batch_size):
            if time.monotonic() >= deadline:
                break
            if allowed_topics is not None and topic not in allowed_topics:
                return {'drained': False, 'pending': producer.outbox.count(),
                        'writer_lag': last_lag, 'error': f'Unmonitored outbox topic: {topic}'}
            producer._queue_pending(topic, event_id, payload)
        producer.flush(timeout=max(0, min(1.0, deadline - time.monotonic())))
        if producer.outbox.count() == 0:
            try:
                last_lag = backlog(deadline)
                error = None
                lags = _lag_values(last_lag)
                if lags and all(value == 0 for value in lags.values()):
                    return {'drained': True, 'pending': 0, 'writer_lag': lags,
                            'writer_progress': last_lag}
            except Exception as exc:
                error = str(exc)
        time.sleep(max(0, min(.2, deadline - time.monotonic())))
    return {'drained': False, 'pending': producer.outbox.count(),
            'writer_lag': _lag_values(last_lag) if last_lag else last_lag,
            'writer_progress': last_lag,
            'error': error or 'Deadline reached; do not snapshot yet'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout', type=float, default=120)
    parser.add_argument('--outbox-path', default=OUTBOX_PATH)
    parser.add_argument('--baseline-path', default=os.getenv('COLLECTION_BASELINE_PATH'),
                        help='Active collection-baseline JSON (required)')
    parser.add_argument('--output', type=Path, help='Save a new drain report; refuses overwrite')
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error('--timeout must be finite and positive')
    if not Path(args.outbox_path).is_file():
        parser.error('Outbox is missing: verify the data mount; do not assume it is empty')
    if not args.baseline_path:
        parser.error('--baseline-path or COLLECTION_BASELINE_PATH is required')
    if args.output and args.output.exists():
        parser.error('Drain report already exists; choose a new path')
    topics = (TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC, TOPIC_OBSERVATIONS)
    try:
        baseline = load_baseline(args.baseline_path, topics, LAKEHOUSE_GROUP_ID)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(f'Invalid baseline: {exc}')
    from confluent_kafka import Consumer
    # Inspect offsets only: never subscribe/join, consume, or commit in this command.
    consumer = Consumer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                         'group.id': LAKEHOUSE_GROUP_ID, 'enable.auto.commit': False,
                         'enable.auto.offset.store': False})
    try:
        producer = RedpandaProducer(profile='critical', outbox_path=args.outbox_path)
        result = drain(producer, lambda deadline: writer_backlog(consumer,
                       topics, deadline, baseline['partitions']),
                       args.timeout, allowed_topics=topics)
        result['baseline'] = {key: baseline[key] for key in ('path', 'sha256', 'captured_at_utc')}
        result['database_id'] = os.getenv('RESEARCH_DATABASE_ID', f'file:{Path(LAKEHOUSE_DB_PATH).resolve()}')
        result['checked_at_utc'] = datetime.now(timezone.utc).isoformat()
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open('x', encoding='utf-8') as handle:
                handle.write(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result, indent=2))
    finally:
        consumer.close()
    raise SystemExit(0 if result['drained'] else 1)


if __name__ == '__main__':
    main()
