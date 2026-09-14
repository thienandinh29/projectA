"""One-process transactional Redpanda -> DuckDB writer; no Redis/inference."""
import argparse
import json
import logging
import math
import signal
import threading
import warnings
from collections import Counter
from datetime import datetime, timezone
from time import monotonic, perf_counter
from confluent_kafka import Consumer, KafkaError, TopicPartition
from config import (KAFKA_BOOTSTRAP_SERVERS, TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC,
                    LAKEHOUSE_DB_PATH, LAKEHOUSE_GROUP_ID, LAKEHOUSE_BATCH_SIZE,
                    LAKEHOUSE_BATCH_SECONDS)
from lakehouse.db import LakehouseManager
from lakehouse.records import KafkaEnvelope, validate_transport_identity, validate_payload
logger = logging.getLogger(__name__)


def parse_event_payload(raw):
    return validate_payload(raw)


class LakehouseWriter:
    def __init__(self, db_path=None, group_id=LAKEHOUSE_GROUP_ID,
                 batch_size=LAKEHOUSE_BATCH_SIZE, batch_seconds=LAKEHOUSE_BATCH_SECONDS,
                 topic_sources=None, consumer_factory=None, stop_event=None,
                 broker=KAFKA_BOOTSTRAP_SERVERS, manager=None, report_lag=True):
        if batch_size < 1 or batch_seconds <= 0 or not math.isfinite(batch_seconds):
            raise ValueError('Batch size and interval must be positive')
        self.topic_sources = topic_sources if topic_sources is not None else {TOPIC_RSS:'RSS', TOPIC_GDELT:'GDELT', TOPIC_SEC:'SEC'}
        if not self.topic_sources:
            raise ValueError('At least one source topic is required')
        self.batch_size, self.batch_seconds = batch_size, batch_seconds
        self.stop_event = stop_event or threading.Event()
        self.buffer, self.counts = [], Counter()
        self.report_lag = report_lag
        # Acquire DB ownership before constructing or subscribing a consumer.
        self.lakehouse = manager if manager is not None else LakehouseManager(db_path or LAKEHOUSE_DB_PATH)
        self.consumer = None
        try:
            self.consumer = (consumer_factory or Consumer)({
                'bootstrap.servers':broker, 'group.id':group_id,
                'auto.offset.reset':'earliest', 'enable.auto.commit':False,
                'enable.auto.offset.store':False})
        except BaseException:
            self.lakehouse.close()
            raise

    def on_revoke(self, consumer, partitions):
        revoked = {(p.topic, p.partition) for p in partitions}
        discarded = sum((m.topic, m.partition) in revoked for m in self.buffer)
        self.buffer = [m for m in self.buffer if (m.topic, m.partition) not in revoked]
        logger.warning('Discarded %s unpersisted records on partition revocation', discarded)

    def flush_batch(self):
        if not self.buffer:
            return {}
        messages = list(self.buffer)
        started = perf_counter()
        counts = self.lakehouse.write_message_batch(messages, self.topic_sources)
        positions = {}
        for m in messages:
            try:
                validate_transport_identity(m)
            except ValueError:
                # Quarantined envelopes have no safe Kafka position to commit.
                continue
            key = (m.topic, m.partition)
            positions[key] = max(positions.get(key, 0), m.offset + 1)
        offsets = [TopicPartition(t, p, o) for (t, p), o in sorted(positions.items())]
        committed = self.consumer.commit(offsets=offsets, asynchronous=False) if offsets else []
        for p in committed or []:
            if getattr(p, 'error', None):
                raise RuntimeError(f'Kafka offset commit failed: {p.error}')
        self.buffer.clear()
        self.counts.update(counts)
        lag = {}
        if self.report_lag:
            for p in offsets:
                try:
                    _, high = self.consumer.get_watermark_offsets(p, timeout=.5, cached=False)
                    lag[f'{p.topic}:{p.partition}'] = max(0, high - p.offset)
                except Exception as exc:
                    lag[f'{p.topic}:{p.partition}'] = {'unavailable':str(exc)}
        delays = []
        for m in messages:
            try:
                event = validate_payload(m.payload, self.topic_sources[m.topic])
                delays.append((datetime.now(timezone.utc)-event.ingested_time).total_seconds())
            except (ValueError, TypeError, UnicodeError, OverflowError):
                pass
        logger.info(json.dumps({'event':'batch_committed', **counts,
            'batch_ms':(perf_counter()-started)*1000,
            'committed_next_offsets':[{'topic':p.topic,'partition':p.partition,'offset':p.offset} for p in offsets],
            'consumer_lag':lag, 'storage_delay_seconds_max':max(delays) if delays else None}))
        return counts

    def run(self, once=False, idle_timeout=10.0):
        first_buffered = None
        last_message = monotonic()
        try:
            self.consumer.subscribe(list(self.topic_sources), on_revoke=self.on_revoke, on_lost=self.on_revoke)
            while not self.stop_event.is_set():
                message = self.consumer.poll(.2)
                now = monotonic()
                if message is not None:
                    if message.error():
                        if message.error().code() != KafkaError._PARTITION_EOF:
                            raise RuntimeError(f'Kafka polling failed: {message.error()}')
                    else:
                        kind, timestamp = message.timestamp()
                        if not self.buffer:
                            first_buffered = now
                        self.buffer.append(KafkaEnvelope(message.topic(),message.partition(),message.offset(),message.value(),
                            timestamp if kind and timestamp >= 0 else None, message.key(), message.headers() or []))
                        last_message = now
                if self.buffer and (len(self.buffer)>=self.batch_size or now-first_buffered>=self.batch_seconds):
                    self.flush_batch()
                    first_buffered = None
                    if once:
                        break
                if once and not self.buffer and now-last_message>=idle_timeout:
                    break
            # Only graceful termination flushes; failures go directly to cleanup.
            self.flush_batch()
            return dict(self.counts)
        finally:
            try:
                self.consumer.close()
            finally:
                self.lakehouse.close()


def sync_redpanda_to_lakehouse(limit_per_topic=500):
    warnings.warn('Wrapper now processes one total batch with a stable group; limit is no longer per-topic.',
                  DeprecationWarning, stacklevel=2)
    return LakehouseWriter(batch_size=limit_per_topic).run(once=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--once',action='store_true',help='Process one batch or exit after 10 idle seconds')
    parser.add_argument('--db-path',default=LAKEHOUSE_DB_PATH)
    parser.add_argument('--group-id',default=LAKEHOUSE_GROUP_ID)
    parser.add_argument('--batch-size','--limit',type=int,default=LAKEHOUSE_BATCH_SIZE)
    parser.add_argument('--batch-seconds',type=float,default=LAKEHOUSE_BATCH_SECONDS)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(name)s %(message)s')
    stop = threading.Event()
    signal.signal(signal.SIGINT,lambda *_:stop.set())
    signal.signal(signal.SIGTERM,lambda *_:stop.set())
    LakehouseWriter(db_path=args.db_path,group_id=args.group_id,batch_size=args.batch_size,
                    batch_seconds=args.batch_seconds,stop_event=stop).run(once=args.once)


if __name__=='__main__':
    main()
