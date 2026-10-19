"""Append-only collector evidence in the shared delivery SQLite database.

This ledger records collector activity, not broker or DuckDB acknowledgement.
An unclosed session is unfinished evidence; a subsequent exclusive worker start
can identify it as interrupted, without inventing its actual termination time.
"""
import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path


def now():
    return datetime.now(timezone.utc).isoformat()


class CollectionSession:
    def __init__(self, outbox, run, topic, source_config):
        self.outbox, self.run, self.topic = outbox, run, topic
        # One local collector of each topic per shared volume. The OS releases
        # ownership on process death; only then can recovery append interruption.
        lock_name = uuid.uuid5(uuid.NAMESPACE_URL, topic).hex
        self.lock = open(outbox.path.parent / f'collector-{lock_name}.lock', 'a+b')
        try:
            if os.fstat(self.lock.fileno()).st_size == 0:
                self.lock.write(b'0')
                self.lock.flush()
            self.lock.seek(0)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise RuntimeError(f'Another collector owns topic {topic}')
        self.closed = False
        try:
            with outbox.connect() as conn:
                old = conn.execute('''SELECT s.run_id FROM collection_sessions s
                    WHERE s.topic=? AND NOT EXISTS (SELECT 1 FROM session_events e
                    WHERE e.run_id=s.run_id AND e.kind IN ('closed','failed','interrupted'))''', [topic]).fetchall()
                for (run_id,) in old:
                    conn.execute('INSERT INTO session_events VALUES (?,?,?,?,?)',
                                 [str(uuid.uuid4()), run_id, now(), 'interrupted',
                                  json.dumps({'reason': 'exclusive collector restarted; actual stop time unknown'})])
                conn.execute('INSERT INTO collection_sessions VALUES (?,?,?,?,?)',
                             [run['run_id'], topic, run['started_at'], json.dumps(run, sort_keys=True),
                              json.dumps(source_config, sort_keys=True)])
        except BaseException:
            self.release()
            raise

    def event(self, kind, **details):
        with self.outbox.connect() as conn:
            conn.execute('INSERT INTO session_events VALUES (?,?,?,?,?)',
                         [str(uuid.uuid4()), self.run['run_id'], now(), kind, json.dumps(details, sort_keys=True)])

    def release(self):
        if not self.lock.closed:
            if os.name == 'nt':
                import msvcrt
                self.lock.seek(0)
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_UNLCK, 1)
            self.lock.close()

    def close(self, reason='normal', failed=False):
        if self.closed:
            return
        try:
            self.event('failed' if failed else 'closed', reason=reason,
                       pending_delivery=self.outbox.count())
            self.closed = True
        finally:
            self.release()

    def begin_request(self, endpoint, configuration):
        request_id = str(uuid.uuid4())
        with self.outbox.connect() as conn:
            conn.execute('INSERT INTO source_requests VALUES (?,?,?,?,?,?)',
                         [request_id, self.run['run_id'], self.topic, now(), endpoint,
                          json.dumps(configuration, sort_keys=True)])
        return request_id

    def finish_request(self, request_id, outcome, *, http_status=None, item_count=None,
                       error=None, **details):
        if outcome not in ('success', 'empty', 'failed'):
            raise ValueError('Unknown source request outcome')
        with self.outbox.connect() as conn:
            conn.execute('INSERT INTO source_request_results VALUES (?,?,?,?,?,?,?)',
                         [request_id, now(), outcome, http_status, item_count, error,
                          json.dumps(details, sort_keys=True)])

    def context(self, request_id, publisher=None):
        return {'run_id': self.run['run_id'], 'request_id': request_id,
                'publisher': publisher}


def read_session_evidence(path):
    """Read without creating a missing ledger or modifying interrupted sessions."""
    path = Path(path).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('BEGIN')
        sessions = [dict(row) for row in conn.execute('SELECT * FROM collection_sessions ORDER BY started_at,rowid')]
        events = [dict(row) for row in conn.execute('SELECT * FROM session_events ORDER BY recorded_at,rowid')]
        requests = [dict(row) for row in conn.execute('''SELECT q.*,r.finished_at,r.outcome,r.http_status,
            r.item_count,r.error,r.details FROM source_requests q
            LEFT JOIN source_request_results r USING(request_id) ORDER BY q.started_at,q.rowid''')]
        for row in sessions:
            row['provenance'] = json.loads(row['provenance'])
            row['source_configuration'] = json.loads(row['source_configuration'])
            terminal = [e for e in events if e['run_id'] == row['run_id'] and
                        e['kind'] in ('closed', 'failed', 'interrupted')]
            row['status'] = terminal[-1]['kind'] if terminal else 'unfinished'
            row['invalidated'] = any(e['run_id'] == row['run_id'] and e['kind'] == 'invalidated' for e in events)
        for row in events:
            row['details'] = json.loads(row['details'])
        for row in requests:
            row['configuration'] = json.loads(row['configuration'])
            row['details'] = json.loads(row['details']) if row['details'] else None
            row['outcome'] = row['outcome'] or 'unfinished'
        return {'kind': 'collection_session_evidence_v1', 'sessions': sessions,
                'session_events': events, 'requests': requests}
    finally:
        conn.close()


def invalidate_session(outbox, run_id, reason):
    """Append a disclosed research exclusion; never delete source/delivery history."""
    if not reason.strip():
        raise ValueError('A concrete invalidation reason is required')
    with outbox.connect() as conn:
        if not conn.execute('SELECT 1 FROM collection_sessions WHERE run_id=?', [run_id]).fetchone():
            raise ValueError('Unknown collection session')
        if not conn.execute("SELECT 1 FROM session_events WHERE run_id=? AND kind IN ('closed','failed','interrupted')", [run_id]).fetchone():
            raise ValueError('Stop and record termination before invalidating a session')
        conn.execute('INSERT INTO session_events VALUES (?,?,?,?,?)',
                     [str(uuid.uuid4()), run_id, now(), 'invalidated', json.dumps({'reason': reason})])


def verify_request_links(rows, evidence):
    """Validate first-version links; legacy observations remain explicitly unknown."""
    requests = {row['request_id']: row for row in evidence['requests']}
    sessions = {row['run_id']: row for row in evidence['sessions']}
    linked, unknown = 0, 0
    for row in rows:
        snapshot = json.loads(row['snapshot']) if isinstance(row['snapshot'], str) else row['snapshot']
        context = snapshot.get('request_context')
        if not context:
            unknown += 1
            continue
        request = requests.get(context.get('request_id'))
        run = sessions.get(context.get('run_id'))
        if not request or not run or request['run_id'] != run['run_id']:
            raise ValueError('Observation request/session evidence is missing or inconsistent')
        if snapshot.get('collection_run') != run['provenance']:
            raise ValueError('Observation collection provenance differs from its session ledger')
        from config import TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC
        expected_topic = {'RSS': TOPIC_RSS, 'GDELT': TOPIC_GDELT, 'SEC': TOPIC_SEC}[snapshot['source']]
        # Fixture/custom topic ledgers must use the configured topic as well.
        if request['topic'] != expected_topic or run['topic'] != expected_topic:
            raise ValueError('Observation source differs from its request/session topic')
        from research.protocol import utc
        if utc(request['started_at']) > utc(snapshot['observed_at']):
            raise ValueError('Source request starts after observation')
        if request['outcome'] == 'empty':
            raise ValueError('An empty source response cannot contain an observed headline')
        linked += 1
    return {'linked_versions': linked, 'versions_with_unknown_request': unknown,
            'unfinished_sessions': sum(row['status'] == 'unfinished' for row in evidence['sessions']),
            'unfinished_requests': sum(row['outcome'] == 'unfinished' for row in evidence['requests'])}
