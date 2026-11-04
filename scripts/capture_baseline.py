"""Capture a new prospective collection boundary from a quiet, fully stored stack.

Writes nothing unless collectors and the writer are stopped, the outbox is empty, every
retained message is committed, the broker is unchanged across two reads, the database is
sealed without unresolved faults, and the code is a recorded clean revision. Output is
inactive unless --activate is given; load_baseline refuses an inactive file. This defines
where completeness can be verified from. It does not repair or certify earlier history.
"""
import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from config import LAKEHOUSE_DB_PATH, LAKEHOUSE_GROUP_ID, OUTBOX_PATH, TOPIC_GDELT, TOPIC_OBSERVATIONS, TOPIC_RSS, TOPIC_SEC
from lakehouse.migrations import VERSION
from research.collection_boundary import load_baseline
from scripts.collection_health import read_broker_state
from scripts.inspect_research_database import COUNTED_TABLES
from utils.outbox import DeliveryOutbox

TOPICS = (TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC, TOPIC_OBSERVATIONS)


def collectors_holding_locks(outbox_path, topics=TOPICS):
    """Topics whose exclusive collector lock is held; the OS releases it when a collector stops."""
    held = []
    for topic in topics:
        path = Path(outbox_path).parent / f'collector-{uuid.uuid5(uuid.NAMESPACE_URL, topic).hex}.lock'
        if not path.exists():
            continue
        with open(path, 'r+b') as handle:
            handle.seek(0)
            try:
                if os.name == 'nt':
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                held.append(topic)
    return held


def read_database(db_path):
    """Read-only; call only after the writer is confirmed stopped, since it holds the file lock."""
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return {
            'schema_version': conn.execute('SELECT COALESCE(MAX(version),0) FROM lakehouse_migrations').fetchone()[0],
            'counts': {t: conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in COUNTED_TABLES},
            'unresolved_faults': conn.execute(
                'SELECT COUNT(*) FROM lakehouse_transport_faults WHERE resolved_at IS NULL').fetchone()[0],
            'unsealed_observations': conn.execute(
                'SELECT COUNT(*) FROM research_observations WHERE available_at IS NULL').fetchone()[0],
        }
    finally:
        conn.close()


def stack_blockers(first, second, held_locks, pending):
    """Reasons the broker and collectors are not quiet. Checked before the database is opened."""
    blockers = []
    if first.get('error') or second.get('error'):
        return [f"broker unavailable: {first.get('error') or second.get('error')}"]
    if first['writer_members'] or second['writer_members']:
        blockers.append('writer is running: stop it before capture')
    if held_locks:
        blockers.append(f"collectors are running: {', '.join(sorted(held_locks))}")
    if pending:
        blockers.append(f'outbox has {pending} pending messages: let collectors deliver them first')
    if set(first['partitions']) != set(second['partitions']):
        blockers.append('partition set changed between reads')
        return blockers
    for name, p in sorted(first['partitions'].items()):
        if p != second['partitions'][name]:
            blockers.append(f'{name}: broker offsets changed between reads')
        committed, low, high = p['committed'], p['log_start'], p['high']
        if committed is None:
            if high != low:
                blockers.append(f'{name}: {high - low} retained messages have no writer commit')
        elif committed < low:
            blockers.append(f'{name}: retention passed the writer commit by {low - committed} messages')
        elif committed != high:
            blockers.append(f'{name}: writer is {high - committed} messages behind')
    return blockers


def database_blockers(database):
    blockers = []
    if database['schema_version'] != VERSION:
        blockers.append(f"database schema {database['schema_version']} is not supported version {VERSION}")
    if database['unresolved_faults']:
        blockers.append(f"{database['unresolved_faults']} unresolved transport faults")
    if database['unsealed_observations']:
        blockers.append(f"{database['unsealed_observations']} unsealed research observations")
    return blockers


def code_blockers(revision, worktree_state):
    if not revision or revision == 'unrecorded':
        return ['code revision is not recorded: set COLLECTION_CODE_REVISION']
    if worktree_state != 'clean':
        return [f'worktree state is {worktree_state!r}, not clean: commit first and set COLLECTION_WORKTREE_STATE']
    return []


def build_document(state, database, *, captured_at, revision, database_id, reason, activate):
    if not reason.strip():
        raise ValueError('A concrete reason for the new baseline is required')
    captured = captured_at.astimezone(timezone.utc).isoformat()
    partitions = []
    for name, p in sorted(state['partitions'].items()):
        topic, number = name.rsplit(':', 1)
        partitions.append({'topic': topic, 'partition': int(number), 'retained_start_offset': p['log_start'],
                           'high_watermark_next_offset': p['high'], 'writer_committed_next_offset': p['committed'],
                           'post_baseline_start_offset': p['high']})
    return {
        'kind': 'verified_collection_baseline_v1',
        'captured_at_utc': captured,
        'purpose': 'Define the earliest boundary from which future collection completeness may be '
                   'verified. This record does not prove or restore earlier Kafka history.',
        'reason': reason.strip(),
        'state_during_capture': {'collectors_running': False, 'writer_running': False,
                                 'shared_outbox_exists': True, 'shared_outbox_pending': 0},
        'repository': {'head': revision, 'worktree_clean': True},
        'kafka': {'writer_group': state['group'], 'total_reported_lag': 0, 'partitions': partitions},
        'duckdb': {'database_id': database_id, 'schema_version': database['schema_version'],
                   'counts': database['counts']},
        'research_scope': {
            'records_before_baseline': 'development_only',
            'final_paper_evaluation_eligible_from_utc': captured,
            'final_paper_evaluation_eligible_from_offsets':
                "Use each partition's post_baseline_start_offset, subject to later eligibility, annotation, "
                'grouping and freeze gates.'},
        'activation_gate': {
            'active': activate,
            'activated_at_utc': captured if activate else None,
            'limitation': 'Activation makes later transport ranges verifiable; it does not make '
                          'pre-baseline records eligible or establish source coverage.'},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true', help='Report blockers; write nothing')
    parser.add_argument('--output', type=Path, help='New baseline JSON; refuses overwrite')
    parser.add_argument('--reason', default='', help='Why a new baseline is needed')
    parser.add_argument('--activate', action='store_true', help='Mark active so drain and export accept it')
    parser.add_argument('--settle-seconds', type=float, default=5.0, help='Wait between the two broker reads')
    parser.add_argument('--outbox-path', default=OUTBOX_PATH)
    parser.add_argument('--db-path', default=LAKEHOUSE_DB_PATH)
    args = parser.parse_args()
    if not args.check and (not args.output or not args.reason.strip()):
        parser.error('Capture requires --output and --reason (or use --check)')
    if args.output and args.output.exists():
        parser.error('Baseline already exists; choose a new path')
    if not Path(args.outbox_path).is_file():
        parser.error('Outbox is missing: verify the data mount; do not assume it is empty')

    def read():
        try:
            return read_broker_state()
        except Exception as e:  # Reported as a blocker: never capture from a partial view.
            return {'error': f'{type(e).__name__}: {e}'}
    first = read()
    time.sleep(args.settle_seconds)
    second = read()
    revision, worktree_state = os.getenv('COLLECTION_CODE_REVISION'), os.getenv('COLLECTION_WORKTREE_STATE')
    blockers = stack_blockers(first, second, collectors_holding_locks(args.outbox_path),
                              DeliveryOutbox(args.outbox_path).count()) + code_blockers(revision, worktree_state)
    database = None
    if not any(b.startswith(('writer is running', 'broker unavailable')) for b in blockers):
        database = read_database(args.db_path)
        blockers += database_blockers(database)
    if blockers or args.check:
        print(json.dumps({'ready': not blockers, 'blockers': blockers, 'broker': second, 'database': database}, indent=2))
        raise SystemExit(1 if blockers else 0)
    database_id = os.getenv('RESEARCH_DATABASE_ID', f'file:{Path(args.db_path).resolve()}')
    document = build_document(second, database, captured_at=datetime.now(timezone.utc), revision=revision,
                              database_id=database_id, reason=args.reason, activate=args.activate)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8', newline='\n') as handle:
        handle.write(json.dumps(document, indent=2, ensure_ascii=False) + '\n')
    if args.activate:
        load_baseline(args.output, TOPICS, LAKEHOUSE_GROUP_ID)  # The readers must accept what was written.
    print(json.dumps({'written': str(args.output), 'active': args.activate,
                      'partitions': document['kafka']['partitions']}, indent=2))


if __name__ == '__main__':
    sys.exit(main())
