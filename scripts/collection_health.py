"""Daily collection health from the session ledger; read-only, never opens live DuckDB.

Gaps are measured between attempt starts and from the last attempt to now. They are
operational signals, not a continuous-coverage certificate; writer health is checked
separately from its logs.
"""
import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import OUTBOX_PATH
from research.protocol import utc
from research.sessions import read_session_evidence


def build_health(evidence, since, now, gap_minutes=30):
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
    return {
        'kind': 'collection_health_v1', 'generated_at': now.isoformat(), 'since': since.isoformat(),
        'gap_threshold_minutes': gap_minutes,
        'unfinished_sessions': [{'run_id': s['run_id'], 'topic': s['topic'], 'started_at': s['started_at']}
                                for s in sorted(active, key=lambda s: s['started_at'])],
        'sources': feeds,
        'problems': sorted(name for name, f in feeds.items()
                           if f['gap_over_threshold'] or f['outcomes'].get('success', 0) == 0),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ledger', default=OUTBOX_PATH)
    parser.add_argument('--hours', type=float, default=24, help='Look-back window ending now')
    parser.add_argument('--gap-minutes', type=float, default=30)
    args = parser.parse_args()
    if not Path(args.ledger).is_file():
        parser.error('Ledger is missing')
    now = datetime.now(timezone.utc)
    report = build_health(read_session_evidence(args.ledger), now - timedelta(hours=args.hours), now,
                          args.gap_minutes)
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
