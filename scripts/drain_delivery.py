"""Retry staged delivery without fetching feeds; wait for durable writer commits.

Stop ALL collectors first and leave the writer running. Exit 0 is a delivery
boundary, not a payload-quality or research-readiness certificate.
"""
import argparse
import json
import math
import time
from pathlib import Path

from config import (OUTBOX_PATH, KAFKA_BOOTSTRAP_SERVERS, LAKEHOUSE_GROUP_ID,
                    TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC, TOPIC_OBSERVATIONS)
from utils.kafka_producer import RedpandaProducer


def writer_backlog(consumer, topics, deadline):
    from confluent_kafka import TopicPartition
    def budget():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Backlog inspection timed out')
        return min(5.0, remaining)
    metadata = consumer.list_topics(timeout=budget())
    partitions = []
    for topic in topics:
        info = metadata.topics.get(topic)
        if info is None or info.error or not info.partitions:
            raise RuntimeError(f'Cannot verify topic {topic}')
        partitions.extend(TopicPartition(topic, p) for p in info.partitions)
    committed = consumer.committed(partitions, timeout=budget())
    lag = {}
    for p in committed:
        if p.error:
            raise RuntimeError(str(p.error))
        low, high = consumer.get_watermark_offsets(p, timeout=budget(), cached=False)
        # Missing commits on a retained nonempty partition are not proof of processing.
        if (p.offset < 0 and low > 0) or (0 <= p.offset < low) or p.offset > high:
            raise RuntimeError(f'Unverifiable retained history: {p.topic}:{p.partition}')
        lag[f'{p.topic}:{p.partition}'] = max(0, high - max(0, p.offset))
    if len(lag) != len(partitions):
        raise RuntimeError('Incomplete partition offset response')
    return lag


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
                if last_lag and all(value == 0 for value in last_lag.values()):
                    return {'drained': True, 'pending': 0, 'writer_lag': last_lag}
            except Exception as exc:
                error = str(exc)
        time.sleep(max(0, min(.2, deadline - time.monotonic())))
    return {'drained': False, 'pending': producer.outbox.count(),
            'writer_lag': last_lag, 'error': error or 'Deadline reached; do not snapshot yet'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout', type=float, default=120)
    parser.add_argument('--outbox-path', default=OUTBOX_PATH)
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error('--timeout must be finite and positive')
    if not Path(args.outbox_path).is_file():
        parser.error('Outbox is missing: verify the data mount; do not assume it is empty')
    from confluent_kafka import Consumer
    # Inspect offsets only: never subscribe/join, consume, or commit in this command.
    consumer = Consumer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                         'group.id': LAKEHOUSE_GROUP_ID, 'enable.auto.commit': False,
                         'enable.auto.offset.store': False})
    try:
        producer = RedpandaProducer(profile='critical', outbox_path=args.outbox_path)
        topics = (TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC, TOPIC_OBSERVATIONS)
        result = drain(producer, lambda deadline: writer_backlog(consumer,
                       topics, deadline), args.timeout, allowed_topics=topics)
        print(json.dumps(result, indent=2))
    finally:
        consumer.close()
    raise SystemExit(0 if result['drained'] else 1)


if __name__ == '__main__':
    main()
