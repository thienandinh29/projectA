"""Bounded collector runs with explicit normal and interrupted-session evidence."""
import argparse
import logging
import math
import signal
import time

from utils.kafka_producer import RedpandaProducer
from utils.redis_cache import RedisDeduplicator


def run_collector(description, topic, interval, source_config, cycle):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--duration-seconds', type=float, help='Stop normally after this observation window')
    args = parser.parse_args()
    if args.duration_seconds is not None and (args.duration_seconds <= 0 or not math.isfinite(args.duration_seconds)):
        parser.error('--duration-seconds must be positive')
    producer = RedpandaProducer(profile='fast')
    session = producer.start_session(topic, {
        **source_config, 'lifecycle': {'once': args.once, 'duration_seconds': args.duration_seconds}})
    stop_reason = None
    failed = True
    deadline = time.monotonic() + args.duration_seconds if args.duration_seconds else None
    log = logging.getLogger(description)

    def stop(signum, frame):
        nonlocal stop_reason
        if stop_reason is None:
            stop_reason = signal.Signals(signum).name
            session.event('stop_requested', reason=stop_reason)

    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        dedup = RedisDeduplicator()
        if not dedup.ping():
            session.event('dependency_warning', dependency='Redis', reason='unreachable')
            log.warning('Redis is unreachable')
        while stop_reason is None:
            if deadline is not None and time.monotonic() >= deadline:
                stop_reason = 'duration_elapsed'
                break
            stats = cycle(producer, dedup)
            log.info('Cycle finished: %s', stats)
            if args.once:
                stop_reason = 'single_cycle_complete'
                break
            sleep_until = time.monotonic() + interval
            if deadline is not None:
                sleep_until = min(sleep_until, deadline)
            while stop_reason is None and time.monotonic() < sleep_until:
                time.sleep(max(0, min(1, sleep_until - time.monotonic())))
        producer.flush(timeout=5)
        failed = False
    finally:
        try:
            producer.close_session(stop_reason or 'unhandled_exception', failed=failed)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
