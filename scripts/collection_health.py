"""Daily collection health from the session ledger and broker; read-only, never opens live DuckDB.

Gaps are measured between attempt starts and from the last attempt to now. They are
operational signals, not a continuous-coverage certificate. Healthy collectors do not
imply stored data, so the broker section flags a stopped writer, offsets already lost to
retention, and unread messages nearing retention. Never commits or joins the writer group.
"""
import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import (KAFKA_BOOTSTRAP_SERVERS, LAKEHOUSE_GROUP_ID, OUTBOX_PATH, TOPIC_GDELT, TOPIC_OBSERVATIONS,
                    TOPIC_RSS, TOPIC_SEC)
from research.protocol import utc
from research.sessions import read_session_evidence


def assess_broker(state, now, retention_warn_fraction=0.5):
    """Name broker problems from read_broker_state output; pure, so each rule is testable."""
    if state.get('error'):
        return [f"broker unavailable: {state['error']}"]
    problems = []
    if not state['writer_members']:
        problems.append('writer not running: consumer group has no members')
    for name, p in sorted(state['partitions'].items()):
        committed, low, high = p['committed'], p['log_start'], p['high']
        if committed is not None and committed < low:
            problems.append(f'{name}: {low - committed} unread messages already deleted by retention')
        unread = high - max(committed if committed is not None else low, low)
        p['unread'] = unread
        if unread <= 0 or p['oldest_unread_ms'] is None:
            continue
        age = utc(now).timestamp() - p['oldest_unread_ms'] / 1000
        p['oldest_unread_hours'] = round(age / 3600, 1)
        if age * 1000 > p['retention_ms'] * retention_warn_fraction:
            problems.append(f"{name}: oldest unread message is {p['oldest_unread_hours']} h old; "
                            f"retention deletes it at {round(p['retention_ms'] / 3600000)} h")
    return problems


def read_broker_state(bootstrap=KAFKA_BOOTSTRAP_SERVERS, group=LAKEHOUSE_GROUP_ID, timeout=10.0):
    """Read offsets, retention and group membership. Probes under its own group, assigned, no commits."""
    from confluent_kafka import ConsumerGroupTopicPartitions, Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, ConfigResource
    topics = [TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC, TOPIC_OBSERVATIONS]
    admin = AdminClient({'bootstrap.servers': bootstrap})
    described = admin.describe_consumer_groups([group], request_timeout=timeout)[group].result(timeout)
    configs = admin.describe_configs([ConfigResource(ConfigResource.Type.TOPIC, t) for t in topics],
                                     request_timeout=timeout)
    retention = {r.name: int(f.result(timeout)['retention.ms'].value) for r, f in configs.items()}
    listed = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)],
                                               request_timeout=timeout)[group].result(timeout)
    committed = {(t.topic, t.partition): t.offset for t in listed.topic_partitions if t.offset >= 0}
    probe = Consumer({'bootstrap.servers': bootstrap, 'group.id': f'{group}-health-probe',
                      'enable.auto.commit': False, 'enable.auto.offset.store': False,
                      'auto.offset.reset': 'earliest'})
    partitions = {}
    try:
        metadata = probe.list_topics(timeout=timeout)
        for topic in topics:
            for number in metadata.topics[topic].partitions:
                low, high = probe.get_watermark_offsets(TopicPartition(topic, number), timeout=timeout, cached=False)
                position = committed.get((topic, number))
                start = max(position if position is not None else low, low)
                oldest = None
                if high > start:
                    probe.assign([TopicPartition(topic, number, start)])
                    message = probe.poll(timeout)
                    if message is not None and not message.error():
                        oldest = message.timestamp()[1]
                partitions[f'{topic}:{number}'] = {
                    'committed': position, 'log_start': low, 'high': high,
                    'retention_ms': retention[topic], 'oldest_unread_ms': oldest}
    finally:
        probe.close()
    return {'group': group, 'writer_members': len(described.members), 'partitions': partitions}


def build_health(evidence, since, now, gap_minutes=30, broker=None):
    since, now = utc(since), utc(now)
    limit = gap_minutes * 60
    feeds = {}
    sessions = {s['run_id']: s for s in evidence['sessions']}
    groups = defaultdict(list)
    for request in evidence['requests']:
        if utc(request['started_at']) >= since:
            name = request['configuration'].get('feed_name') or request['endpoint']
            if name.endswith('.gkg.csv.zip'):
                name = 'GDELT GKG batch downloads'  # one timestamped URL per batch
            groups[(request['topic'], name)].append(request)
    for (topic, name), attempts in sorted(groups.items()):
        starts = sorted(utc(r['started_at']) for r in attempts)
        gaps = [(b - a).total_seconds() for a, b in zip(starts, starts[1:])]
        since_last = (now - starts[-1]).total_seconds()
        feeds[f'{topic} | {name}'] = {
            'attempts': len(attempts),
            'outcomes': dict(Counter(r['outcome'] for r in attempts)),
            'http_statuses': dict(Counter(str(r['http_status']) if r['http_status'] is not None
                                          else 'none' for r in attempts)),
            'items_reported': sum(r['item_count'] or 0 for r in attempts),
            'first_attempt': starts[0].isoformat(), 'last_attempt': starts[-1].isoformat(),
            'max_gap_minutes': round(max(gaps) / 60, 1) if gaps else None,
            'minutes_since_last_attempt': round(since_last / 60, 1),
            'gap_over_threshold': any(g > limit for g in gaps) or since_last > limit,
        }
    active = [s for s in sessions.values() if s['status'] == 'unfinished' and utc(s['started_at']) <= now]
    broker_problems = assess_broker(broker, now) if broker is not None else []
    return {
        'kind': 'collection_health_v1', 'generated_at': now.isoformat(), 'since': since.isoformat(),
        'gap_threshold_minutes': gap_minutes,
        'unfinished_sessions': [{'run_id': s['run_id'], 'topic': s['topic'], 'started_at': s['started_at']}
                                for s in sorted(active, key=lambda s: s['started_at'])],
        'sources': feeds,
        'broker': broker,
        'problems': broker_problems + sorted(name for name, f in feeds.items()
                                             if f['gap_over_threshold'] or f['outcomes'].get('success', 0) == 0),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ledger', default=OUTBOX_PATH)
    parser.add_argument('--hours', type=float, default=24, help='Look-back window ending now')
    parser.add_argument('--gap-minutes', type=float, default=30)
    parser.add_argument('--no-broker', action='store_true', help='Ledger only; broker problems go unreported')
    args = parser.parse_args()
    if not Path(args.ledger).is_file():
        parser.error('Ledger is missing')
    now = datetime.now(timezone.utc)
    broker = None
    if not args.no_broker:
        try:
            broker = read_broker_state()
        except Exception as e:  # An unreachable broker is itself a reportable health result.
            broker = {'error': f'{type(e).__name__}: {e}'}
    report = build_health(read_session_evidence(args.ledger), now - timedelta(hours=args.hours), now,
                          args.gap_minutes, broker=broker)
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
