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
    ''')


def append_reviews(ledger, rows, dataset_sha256, policy, reviewer, reviews):
    """Record a whole development review batch atomically, without backdating.

    No caller-supplied historical decision timestamp is accepted. A revision must
    explicitly identify the preceding decision; stale concurrent reviews fail.
    decision_available_at is currently sampled BEFORE the content commit. It is
    a recording timestamp, not post-commit availability proof. Use saved cutoffs
    after successful return; harden this contract before final research freeze.
    """
    if not reviewer.strip():
        raise ValueError('Reviewer identity is required')
    validated = [Review.model_validate(value) for value in reviews]
    by_id = {row['version_id']: row for row in rows}
    if not validated or len({r.version_id for r in validated}) != len(validated):
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
    conn = sqlite3.connect(path, timeout=30)
    try:
        conn.execute('PRAGMA foreign_keys=ON')
        initialize(conn)
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('INSERT OR IGNORE INTO eligibility_policies VALUES (?,?)', policy)
        stamp = datetime.now(timezone.utc).isoformat()
        recorded = []
        for review in validated:
            if utc(by_id[review.version_id]['available_at']) > utc(stamp):
                raise ValueError('Cannot review research data before it is available')
            latest = conn.execute('''SELECT decision_id, decision_available_at FROM eligibility_decisions
                WHERE version_id=? AND policy_sha256=? ORDER BY sequence DESC LIMIT 1''',
                (review.version_id, policy_hash)).fetchone()
            if review.supersedes_decision_id != (latest[0] if latest else None):
                raise ValueError('Revision must reference the latest decision under this policy')
            if latest and utc(latest[1]) >= utc(stamp):
                raise ValueError('Decision clock did not advance; preserve history and retry')
            decision_id = str(uuid.uuid4())
            conn.execute('''INSERT INTO eligibility_decisions
                (decision_id,version_id,content_hash,dataset_sha256,policy_sha256,reviewer,
                 decision_available_at,review_json) VALUES (?,?,?,?,?,?,?,?)''',
                (decision_id, review.version_id, review.content_hash, dataset_sha256,
                 policy_hash, reviewer.strip(), stamp, canonical_json(review.model_dump())))
            recorded.append({'decision_id': decision_id, 'version_id': review.version_id,
                             'decision_available_at': stamp})
        conn.commit()
        return recorded
    except BaseException:
        conn.rollback()
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
        return [dict(row) for row in conn.execute('''SELECT * FROM eligibility_decisions
            WHERE policy_sha256=? ORDER BY sequence''', (policy_hash,))]
    finally:
        conn.close()


def select_as_of(rows, decisions, cutoff):
    """Strict pre-cutoff decisions; pending/uncertain rows are never auto-included."""
    cutoff = utc(cutoff)
    latest = {}
    for decision in sorted(decisions, key=lambda d: d['sequence']):
        if utc(decision['decision_available_at']) < cutoff:
            latest[decision['version_id']] = decision
    results = []
    for row in rows:
        decision = latest.get(row['version_id'])
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
            if utc(decision['decision_available_at']) < utc(row['available_at']):
                raise ValueError('Stored decision precedes research data availability')
            values = (review.english, review.financially_relevant, review.genuine_headline)
            status = 'excluded' if 'no' in values else 'uncertain' if 'uncertain' in values else 'included'
        results.append({'version_id': row['version_id'], 'content_hash': row['content_hash'],
                        'status': status, 'decision_id': decision['decision_id'] if decision else None})
    return results
