"""Human eligibility evidence, separate from operational delivery and sentiment labels."""
import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, ConfigDict
from typing import Literal

from research.observations import SourceObservation, canonical_json
from research.protocol import utc
from research.sessions import verify_request_links


class Review(BaseModel):
    model_config = ConfigDict(extra='forbid')
    version_id: str
    content_hash: str
    english: Literal['yes', 'no', 'uncertain']
    financially_relevant: Literal['yes', 'no', 'uncertain']
    genuine_headline: Literal['yes', 'no', 'uncertain']
    reason: str
    supersedes_decision_id: str | None = None


def load_snapshot(directory):
    folder = Path(directory)
    if (folder/'INVALID_FOR_RESEARCH.json').exists():
        raise ValueError('Invalidated snapshot cannot be reviewed for research')
    manifest_bytes = (folder/'manifest.json').read_bytes()
    manifest = json.loads(manifest_bytes)
    data = (folder/'dataset.jsonl').read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    rows = [json.loads(line) for line in data.splitlines()]
    if digest != manifest['dataset_sha256'] or len(rows) != manifest['rows']:
        raise ValueError('Snapshot hash or row count mismatch')
    if len({row['version_id'] for row in rows}) != len(rows):
        raise ValueError('Duplicate snapshot version ID')
    if manifest.get('session_evidence'):
        evidence_raw = (folder/'collection-sessions.json').read_bytes()
        if hashlib.sha256(evidence_raw).hexdigest() != manifest['session_evidence']['sha256']:
            raise ValueError('Snapshot session evidence hash mismatch')
        evidence = json.loads(evidence_raw)
        verify_request_links(rows, evidence)
        invalid = {run['run_id'] for run in evidence['sessions'] if run['invalidated']}
        if any((json.loads(row['snapshot']).get('collection_run') or {}).get('run_id') in invalid
               for row in rows):
            raise ValueError('Snapshot includes versions from an invalidated collector run')
    for row in rows:
        obs = SourceObservation.model_validate_json(row['snapshot'])
        if not obs.nlp_eligible:
            raise ValueError('Snapshot contains unsupported headline provenance')
        for name, expected in [('version_id', obs.id), ('content_hash', obs.content_hash),
                               ('article_id', obs.article_id), ('title', obs.title),
                               ('source', obs.source), ('url', obs.url)]:
            if row[name] != expected:
                raise ValueError('Snapshot row does not match its source content version')
        if utc(row['observed_at']) != utc(obs.observed_at) or not (
                utc(row['observed_at']) <= utc(row['available_at']) <= utc(manifest['cutoff'])):
            raise ValueError('Snapshot observation/availability timing mismatch')
    return rows, manifest, digest, hashlib.sha256(manifest_bytes).hexdigest()


def load_policy(path):
    raw = Path(path).read_bytes()
    text = raw.decode('utf-8')
    if not text.strip():
        raise ValueError('Eligibility policy must not be blank')
    return hashlib.sha256(raw).hexdigest(), text


def initialize(conn):
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS eligibility_policies (
            sha256 TEXT PRIMARY KEY, text TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS eligibility_decisions (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_id TEXT NOT NULL UNIQUE,
            version_id TEXT NOT NULL, content_hash TEXT NOT NULL,
            dataset_sha256 TEXT NOT NULL, policy_sha256 TEXT NOT NULL,
            reviewer TEXT NOT NULL, decision_available_at TEXT NOT NULL,
            review_json TEXT NOT NULL,
            FOREIGN KEY (policy_sha256) REFERENCES eligibility_policies(sha256));
        CREATE INDEX IF NOT EXISTS eligibility_version_policy
            ON eligibility_decisions(version_id, policy_sha256, sequence);
        CREATE TRIGGER IF NOT EXISTS eligibility_no_update
            BEFORE UPDATE ON eligibility_decisions
            BEGIN SELECT RAISE(ABORT, 'Eligibility decisions are append-only'); END;
        CREATE TRIGGER IF NOT EXISTS eligibility_no_delete
            BEFORE DELETE ON eligibility_decisions
            BEGIN SELECT RAISE(ABORT, 'Eligibility decisions are append-only'); END;
        CREATE TRIGGER IF NOT EXISTS eligibility_policy_no_update
            BEFORE UPDATE ON eligibility_policies
            BEGIN SELECT RAISE(ABORT, 'Eligibility policies are immutable'); END;
        CREATE TRIGGER IF NOT EXISTS eligibility_policy_no_delete
            BEFORE DELETE ON eligibility_policies
            BEGIN SELECT RAISE(ABORT, 'Eligibility policies are immutable'); END;
        CREATE TABLE IF NOT EXISTS eligibility_release_requirements (
            decision_id TEXT PRIMARY KEY REFERENCES eligibility_decisions(decision_id),
            data_available_at TEXT NOT NULL,
            origin TEXT NOT NULL CHECK(origin IN ('new', 'legacy')));
        CREATE TABLE IF NOT EXISTS eligibility_releases (
            decision_id TEXT PRIMARY KEY REFERENCES eligibility_release_requirements(decision_id),
            released_at TEXT NOT NULL,
            release_kind TEXT NOT NULL CHECK(release_kind IN
                ('initial', 'recovery', 'legacy_confirmation', 'legacy_recovery')));
    ''')
    for table in ('eligibility_release_requirements', 'eligibility_releases'):
        for action in ('UPDATE', 'DELETE'):
            conn.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()}
                BEFORE {action} ON {table}
                BEGIN SELECT RAISE(ABORT, 'Eligibility release evidence is append-only'); END''')


def _connect(path):
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA synchronous=FULL')
    return conn


def _seal(conn, decision_ids, recovery=False):
    """Sample the clock only after judgment/requirement contents have committed.

    Markers commit separately; selection excludes missing markers. Recovery
    confirms persisted contents now, never at a legacy recording timestamp.
    """
    conn.execute('BEGIN IMMEDIATE')
    try:
        stamp = datetime.now(timezone.utc).isoformat()
        result = []
        for decision_id in decision_ids:
            row = conn.execute('''SELECT d.*, q.data_available_at, q.origin
                FROM eligibility_decisions d JOIN eligibility_release_requirements q USING(decision_id)
                WHERE d.decision_id=?''', (decision_id,)).fetchone()
            if row is None:
                raise ValueError('Unknown eligibility release requirement')
            if conn.execute('SELECT 1 FROM eligibility_releases WHERE decision_id=?',
                            (decision_id,)).fetchone():
                continue
            if utc(stamp) < max(utc(row['decision_available_at']), utc(row['data_available_at'])):
                raise ValueError('Release clock precedes recorded judgment/data availability')
            prior = conn.execute('''SELECT r.released_at FROM eligibility_decisions d
                JOIN eligibility_releases r USING(decision_id)
                WHERE d.version_id=? AND d.policy_sha256=? AND d.sequence<?
                ORDER BY d.sequence DESC LIMIT 1''',
                (row['version_id'], row['policy_sha256'], row['sequence'])).fetchone()
            if prior and utc(stamp) <= utc(prior[0]):
                raise ValueError('Release clock did not advance')
            kind = ('legacy_recovery' if recovery else 'legacy_confirmation') if row['origin'] == 'legacy' \
                else ('recovery' if recovery else 'initial')
            conn.execute('INSERT INTO eligibility_releases VALUES (?,?,?)', (decision_id, stamp, kind))
            result.append({'decision_id': decision_id, 'version_id': row['version_id'],
                           'decision_available_at': stamp, 'release_kind': kind})
        conn.commit()
        return result
    except BaseException:
        conn.rollback()
        raise


def append_reviews(ledger, rows, dataset_sha256, policy, reviewer, reviews):
    """Persist judgment contents first, then append post-content-commit releases.

    No caller-supplied historical decision timestamp is accepted. A revision must
    explicitly identify the preceding decision; stale concurrent reviews fail.
    The legacy SQL column decision_available_at retains its recording-time meaning;
    it is never rewritten. read_decisions exposes recorded_at separately and derives
    decision_available_at from release evidence. Unsealed content requires recovery.
    """
    if not reviewer.strip():
        raise ValueError('Reviewer identity is required')
    validated = [Review.model_validate(value) for value in reviews]
    by_id = {row['version_id']: row for row in rows}
    if len(by_id) != len(rows) or not validated or len({r.version_id for r in validated}) != len(validated):
        raise ValueError('Provide a nonempty batch with distinct reviewed versions')
    for review in validated:
        row = by_id.get(review.version_id)
        if not row or review.content_hash != row['content_hash']:
            raise ValueError('Review does not match an exact snapshot version/hash')
        if not review.reason.strip():
            raise ValueError('Every decision needs an explicit reason')
    policy_hash, policy_text = policy
    if hashlib.sha256(policy_text.encode()).hexdigest() != policy_hash:
        raise ValueError('Policy text/hash mismatch')
    path = Path(ledger)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = _connect(path)
    committed = False
    try:
        initialize(conn)
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('INSERT OR IGNORE INTO eligibility_policies VALUES (?,?)', policy)
        stamp = datetime.now(timezone.utc).isoformat()
        ids = []
        for review in validated:
            if utc(by_id[review.version_id]['available_at']) > utc(stamp):
                raise ValueError('Cannot review research data before it is available')
            latest = conn.execute('''SELECT d.decision_id, d.decision_available_at,
                q.decision_id AS requires_release, r.released_at
                FROM eligibility_decisions d
                LEFT JOIN eligibility_release_requirements q USING(decision_id)
                LEFT JOIN eligibility_releases r USING(decision_id)
                WHERE d.version_id=? AND d.policy_sha256=? ORDER BY d.sequence DESC LIMIT 1''',
                (review.version_id, policy_hash)).fetchone()
            if latest and latest['requires_release'] and latest['released_at'] is None:
                raise ValueError('An unsealed judgment must be recovered before revising')
            if review.supersedes_decision_id != (latest['decision_id'] if latest else None):
                raise ValueError('Revision must reference the latest decision under this policy')
            if latest and utc(latest['released_at'] or latest['decision_available_at']) >= utc(stamp):
                raise ValueError('Decision clock did not advance; preserve history and retry')
            decision_id = str(uuid.uuid4())
            conn.execute('''INSERT INTO eligibility_decisions
                (decision_id,version_id,content_hash,dataset_sha256,policy_sha256,reviewer,
                 decision_available_at,review_json) VALUES (?,?,?,?,?,?,?,?)''',
                (decision_id, review.version_id, review.content_hash, dataset_sha256,
                 policy_hash, reviewer.strip(), stamp, canonical_json(review.model_dump())))
            conn.execute('INSERT INTO eligibility_release_requirements VALUES (?,?,?)',
                         (decision_id, by_id[review.version_id]['available_at'], 'new'))
            ids.append(decision_id)
        conn.commit()
        committed = True
        return _seal(conn, ids)
    except BaseException as exc:
        conn.rollback()
        if committed:
            raise RuntimeError('Judgments are committed but not released. Run recover; do not resubmit.') from exc
        raise
    finally:
        conn.close()


def recover_pending(ledger):
    """Recover registered new/legacy releases; never auto-confirm old decisions."""
    if not Path(ledger).exists():
        raise ValueError('Eligibility ledger does not exist')
    conn = _connect(ledger)
    try:
        initialize(conn)
        ids = [r[0] for r in conn.execute('''SELECT d.decision_id FROM eligibility_decisions d
            JOIN eligibility_release_requirements q USING(decision_id)
            LEFT JOIN eligibility_releases r USING(decision_id)
            WHERE r.decision_id IS NULL ORDER BY d.sequence''')]
        return _seal(conn, ids, recovery=True) if ids else []
    finally:
        conn.close()


def confirm_legacy(ledger, rows, policy_hash):
    """Explicitly confirm latest legacy contents now, without new human judgments.

    Superseded historical decisions and original columns remain untouched. Exact
    snapshot versions supply the data availability bound; confirmation is not
    retrospective proof of the old recording timestamp or annotation quality.
    """
    if not Path(ledger).exists():
        raise ValueError('Eligibility ledger does not exist')
    by_id = {r['version_id']: r for r in rows}
    if len(by_id) != len(rows):
        raise ValueError('Snapshot versions must be distinct')
    conn = _connect(ledger)
    committed = False
    try:
        initialize(conn)
        conn.execute('BEGIN IMMEDIATE')
        policy = conn.execute('SELECT text FROM eligibility_policies WHERE sha256=?', (policy_hash,)).fetchone()
        if policy is None or hashlib.sha256(policy[0].encode()).hexdigest() != policy_hash:
            raise ValueError('Unknown or invalid stored eligibility policy')
        latest = conn.execute('''SELECT d.*, q.decision_id AS requires_release, r.released_at
            FROM eligibility_decisions d
            LEFT JOIN eligibility_release_requirements q USING(decision_id)
            LEFT JOIN eligibility_releases r USING(decision_id)
            WHERE d.policy_sha256=? AND NOT EXISTS
                (SELECT 1 FROM eligibility_decisions newer WHERE newer.version_id=d.version_id
                 AND newer.policy_sha256=d.policy_sha256 AND newer.sequence>d.sequence)
            ORDER BY d.sequence''', (policy_hash,)).fetchall()
        ids = []
        for decision in latest:
            row = by_id.get(decision['version_id'])
            if row is None:
                continue
            item = Review.model_validate_json(decision['review_json'])
            if item.version_id != row['version_id'] or item.content_hash != row['content_hash'] \
                    or decision['content_hash'] != item.content_hash:
                raise ValueError('Legacy confirmation content identity mismatch')
            if decision['requires_release']:
                if decision['released_at'] is None:
                    raise ValueError('An unsealed judgment must be recovered before confirmation')
                continue
            if utc(decision['decision_available_at']) < utc(row['available_at']):
                raise ValueError('Legacy recording precedes research data availability')
            conn.execute('INSERT INTO eligibility_release_requirements VALUES (?,?,?)',
                         (decision['decision_id'], row['available_at'], 'legacy'))
            ids.append(decision['decision_id'])
        conn.commit()
        committed = True
        return _seal(conn, ids) if ids else []
    except BaseException as exc:
        conn.rollback()
        if committed:
            raise RuntimeError('Confirmation contents committed but not released. Run recover; do not resubmit.') from exc
        raise
    finally:
        conn.close()


def read_decisions(ledger, policy_hash):
    uri = Path(ledger).resolve().as_uri() + '?mode=ro'
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        policy = conn.execute('SELECT text FROM eligibility_policies WHERE sha256=?',
                              (policy_hash,)).fetchone()
        if policy is None:
            return []
        if hashlib.sha256(policy[0].encode()).hexdigest() != policy_hash:
            raise ValueError('Stored policy hash mismatch')
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        release_tables = {'eligibility_release_requirements', 'eligibility_releases'}
        if tables & release_tables and not release_tables <= tables:
            raise ValueError('Incomplete eligibility release schema')
        joined = release_tables <= tables
        sql = '''SELECT d.*, q.data_available_at, q.origin, r.released_at, r.release_kind
            FROM eligibility_decisions d
            LEFT JOIN eligibility_release_requirements q USING(decision_id)
            LEFT JOIN eligibility_releases r USING(decision_id)
            WHERE d.policy_sha256=? ORDER BY d.sequence''' if joined else '''SELECT *
            FROM eligibility_decisions WHERE policy_sha256=? ORDER BY sequence'''
        result = []
        for row in conn.execute(sql, (policy_hash,)):
            value = dict(row)
            value['recorded_at'] = value['decision_available_at']
            value['decision_available_at'] = value.pop('released_at', None)
            value['availability_contract'] = 'post_content_commit_v2' if value.get('origin') else 'legacy_precommit_v1'
            value.setdefault('data_available_at', None)
            value.setdefault('origin', None)
            value.setdefault('release_kind', None)
            result.append(value)
        return result
    finally:
        conn.close()


def select_as_of(rows, decisions, cutoff, *, timing_contract='post_content_commit_v2'):
    """Strict release selection by default; explicit legacy replay is development only."""
    if timing_contract not in ('post_content_commit_v2', 'legacy_precommit_v1'):
        raise ValueError('Unknown eligibility timing contract')
    cutoff = utc(cutoff)
    latest = {}
    for decision in sorted(decisions, key=lambda d: d['sequence']):
        if timing_contract == 'legacy_precommit_v1':
            if decision.get('origin') == 'new':
                raise ValueError('New release judgments cannot enter legacy replay')
            stamp = decision.get('recorded_at', decision['decision_available_at'])
        else:
            if decision.get('availability_contract') != 'post_content_commit_v2':
                continue
            stamp = decision['decision_available_at']
            if decision.get('origin') not in ('new', 'legacy') or (stamp is not None and
                    decision.get('release_kind') not in ({'initial', 'recovery'} if decision['origin'] == 'new'
                                                        else {'legacy_confirmation', 'legacy_recovery'})):
                raise ValueError('Stored eligibility release provenance mismatch')
            if stamp is not None and utc(stamp) < max(utc(decision['recorded_at']), utc(decision['data_available_at'])):
                raise ValueError('Stored release precedes judgment/data availability')
        if stamp is not None and utc(stamp) < cutoff:
            latest[decision['version_id']] = (decision, stamp)
    results = []
    for row in rows:
        pair = latest.get(row['version_id'])
        decision, stamp = pair if pair else (None, None)
        status = 'pending'
        if utc(row['available_at']) >= cutoff:
            status = 'data_not_available_before_cutoff'
            decision = None
        elif decision:
            review = Review.model_validate(json.loads(decision['review_json']))
            if review.version_id != row['version_id'] or review.content_hash != row['content_hash']:
                raise ValueError('Stored review content identity mismatch')
            if decision['content_hash'] != review.content_hash:
                raise ValueError('Stored decision content identity mismatch')
            if timing_contract == 'post_content_commit_v2' and utc(decision['data_available_at']) != utc(row['available_at']):
                raise ValueError('Stored release data availability identity mismatch')
            if utc(stamp) < utc(row['available_at']):
                raise ValueError('Stored decision precedes research data availability')
            values = (review.english, review.financially_relevant, review.genuine_headline)
            status = 'excluded' if 'no' in values else 'uncertain' if 'uncertain' in values else 'included'
        results.append({'version_id': row['version_id'], 'content_hash': row['content_hash'],
                        'status': status, 'decision_id': decision['decision_id'] if decision else None})
    return results
