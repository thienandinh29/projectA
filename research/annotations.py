"""Append-only human sentiment judgments with post-content-commit availability."""
import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, StrictBool, field_validator, model_validator

from research.eligibility import load_snapshot, select_as_of as select_eligibility
from research.observations import canonical_json
from research.protocol import utc


class Annotation(BaseModel):
    model_config = ConfigDict(extra='forbid')
    version_id: str
    content_hash: str
    subject: str
    status: Literal['labeled', 'unresolved']
    sentiment: Literal['positive', 'negative', 'neutral'] | None
    reason: str
    mixed: StrictBool
    entity_dependent: StrictBool
    forecast: StrictBool
    quotation: StrictBool
    question: StrictBool
    assisted: StrictBool
    assistance_note: str
    supersedes_decision_id: str | None = None

    @field_validator('subject', 'reason')
    @classmethod
    def nonblank(cls, value):
        if not value.strip():
            raise ValueError('Subject and reason must not be blank')
        return value.strip()

    @model_validator(mode='after')
    def consistent(self):
        if (self.status == 'labeled') != (self.sentiment is not None):
            raise ValueError('Labeled judgments need sentiment; unresolved judgments must leave it null')
        if self.mixed and self.sentiment == 'neutral':
            raise ValueError('Mixed directions are not neutral; resolve the dominant direction or leave unresolved')
        if self.assisted and not self.assistance_note.strip():
            raise ValueError('Assisted judgments must disclose the assistance')
        if not self.assisted and self.assistance_note.strip():
            raise ValueError('Assistance notes require assisted=true')
        return self


def load_guide(path):
    raw = Path(path).read_bytes()
    text = raw.decode('utf-8')
    if not text.strip():
        raise ValueError('Annotation guide must not be blank')
    return hashlib.sha256(raw).hexdigest(), text


def load_selection(snapshot_dir, selection_dir, eligibility_cutoff=None):
    """Verify saved eligibility evidence; never reuse a judgment for changed text."""
    rows, manifest, dataset_hash, manifest_hash = load_snapshot(snapshot_dir)
    folder = Path(selection_dir)
    report_raw = (folder/'selection.json').read_bytes()
    report = json.loads(report_raw)
    if report.get('kind') != 'development_eligibility_selection_v1':
        raise ValueError('A saved development eligibility selection is required')
    if (report['input_dataset_sha256'], report['input_manifest_sha256']) != (dataset_hash, manifest_hash):
        raise ValueError('Eligibility selection refers to another snapshot')
    for filename, key in [('eligible.jsonl', 'eligible_rows_sha256'),
                          ('decisions.jsonl', 'decision_history_sha256'), ('policy.md', 'policy_sha256')]:
        if hashlib.sha256((folder/filename).read_bytes()).hexdigest() != report[key]:
            raise ValueError('Eligibility selection evidence hash mismatch')
    decisions = [json.loads(line) for line in (folder/'decisions.jsonl').read_bytes().splitlines()]
    if any(d['policy_sha256'] != report['policy_sha256'] or
           utc(d['decision_available_at']) >= utc(report['cutoff']) for d in decisions):
        raise ValueError('Eligibility history does not match its policy/cutoff')
    selected = select_eligibility(rows, decisions, report['cutoff'])
    if selected != report['versions']:
        raise ValueError('Eligibility decisions do not reproduce the saved selection')
    included = {s['version_id'] for s in selected if s['status'] == 'included'}
    eligible = [r for r in rows if r['version_id'] in included]
    saved = [json.loads(line) for line in (folder/'eligible.jsonl').read_bytes().splitlines()]
    if saved != eligible:
        raise ValueError('Eligible rows differ from their exact snapshot versions')
    if eligibility_cutoff is not None:
        earlier = select_eligibility(rows, decisions, eligibility_cutoff)
        included = {s['version_id'] for s in earlier if s['status'] == 'included'}
        eligible = [r for r in rows if r['version_id'] in included]
    return eligible, manifest, dataset_hash, hashlib.sha256(report_raw).hexdigest()


def initialize(conn):
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS annotation_guides (
            sha256 TEXT PRIMARY KEY, text TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS annotation_decisions (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_id TEXT NOT NULL UNIQUE,
            version_id TEXT NOT NULL, content_hash TEXT NOT NULL,
            dataset_sha256 TEXT NOT NULL, eligibility_selection_sha256 TEXT NOT NULL,
            guide_sha256 TEXT NOT NULL REFERENCES annotation_guides(sha256),
            annotator TEXT NOT NULL, recorded_at TEXT NOT NULL,
            data_available_at TEXT NOT NULL, annotation_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS annotation_chain
            ON annotation_decisions(version_id, guide_sha256, annotator, sequence);
        CREATE TABLE IF NOT EXISTS annotation_releases (
            decision_id TEXT PRIMARY KEY REFERENCES annotation_decisions(decision_id),
            label_available_at TEXT NOT NULL,
            release_kind TEXT NOT NULL CHECK(release_kind IN ('initial', 'recovery')));
    ''')
    for table in ('annotation_guides', 'annotation_decisions', 'annotation_releases'):
        for action in ('UPDATE', 'DELETE'):
            conn.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()}
                BEFORE {action} ON {table}
                BEGIN SELECT RAISE(ABORT, 'Annotation evidence is append-only'); END''')


def _connect(path):
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA synchronous=FULL')
    return conn


def _seal(conn, decision_ids, kind):
    """Clock is sampled after the decision-content transaction committed.

    Release markers commit separately. Missing markers prohibit selection;
    recovery uses its present clock, never an earlier recorded_at timestamp.
    """
    conn.execute('BEGIN IMMEDIATE')
    try:
        stamp = datetime.now(timezone.utc).isoformat()
        result = []
        for decision_id in decision_ids:
            row = conn.execute('SELECT * FROM annotation_decisions WHERE decision_id=?',
                               (decision_id,)).fetchone()
            if row is None:
                raise ValueError('Unknown annotation decision')
            existing = conn.execute('SELECT * FROM annotation_releases WHERE decision_id=?',
                                    (decision_id,)).fetchone()
            if existing:
                continue
            if utc(stamp) < max(utc(row['recorded_at']), utc(row['data_available_at'])):
                raise ValueError('Release clock precedes recorded judgment/data availability')
            prior = conn.execute('''SELECT r.label_available_at FROM annotation_decisions d
                JOIN annotation_releases r USING(decision_id)
                WHERE d.version_id=? AND d.guide_sha256=? AND d.annotator=? AND d.sequence<?
                ORDER BY d.sequence DESC LIMIT 1''',
                (row['version_id'], row['guide_sha256'], row['annotator'], row['sequence'])).fetchone()
            if prior and utc(stamp) <= utc(prior[0]):
                raise ValueError('Release clock did not advance')
            conn.execute('INSERT INTO annotation_releases VALUES (?,?,?)', (decision_id, stamp, kind))
            result.append({'decision_id': decision_id, 'version_id': row['version_id'],
                           'label_available_at': stamp, 'release_kind': kind})
        conn.commit()
        return result
    except BaseException:
        conn.rollback()
        raise


def append_annotations(ledger, rows, dataset_hash, selection_hash, guide, annotator, annotations):
    """Human-only input; no caller-supplied availability time or silent revisions."""
    annotator = annotator.strip()
    if not annotator:
        raise ValueError('Human annotator identity is required')
    validated = [Annotation.model_validate(a) for a in annotations]
    by_id = {row['version_id']: row for row in rows}
    if len(by_id) != len(rows) or not validated or len({a.version_id for a in validated}) != len(validated):
        raise ValueError('Provide a nonempty batch with distinct exact versions')
    for item in validated:
        row = by_id.get(item.version_id)
        if row is None or row['content_hash'] != item.content_hash:
            raise ValueError('Annotation does not match an eligible exact version/hash')
    guide_hash, guide_text = guide
    if hashlib.sha256(guide_text.encode()).hexdigest() != guide_hash:
        raise ValueError('Annotation guide text/hash mismatch')
    if not dataset_hash or not selection_hash:
        raise ValueError('Snapshot and eligibility selection hashes are required')
    path = Path(ledger)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = _connect(path)
    committed = False
    try:
        initialize(conn)
        conn.execute('BEGIN IMMEDIATE')
        conn.execute('INSERT OR IGNORE INTO annotation_guides VALUES (?,?)', guide)
        stamp = datetime.now(timezone.utc).isoformat()
        ids = []
        for item in validated:
            row = by_id[item.version_id]
            if utc(stamp) < utc(row['available_at']):
                raise ValueError('Cannot annotate research data before it is available')
            latest = conn.execute('''SELECT d.decision_id, r.label_available_at
                FROM annotation_decisions d LEFT JOIN annotation_releases r USING(decision_id)
                WHERE d.version_id=? AND d.guide_sha256=? AND d.annotator=?
                ORDER BY d.sequence DESC LIMIT 1''', (item.version_id, guide_hash, annotator)).fetchone()
            if latest and latest['label_available_at'] is None:
                raise ValueError('An unsealed judgment must be recovered before revising')
            if item.supersedes_decision_id != (latest['decision_id'] if latest else None):
                raise ValueError('Revision must reference this annotator\'s latest decision under this guide')
            if latest and utc(stamp) <= utc(latest['label_available_at']):
                raise ValueError('Annotation clock did not advance')
            decision_id = str(uuid.uuid4())
            conn.execute('''INSERT INTO annotation_decisions
                (decision_id,version_id,content_hash,dataset_sha256,eligibility_selection_sha256,
                 guide_sha256,annotator,recorded_at,data_available_at,annotation_json)
                VALUES (?,?,?,?,?,?,?,?,?,?)''', (decision_id, item.version_id, item.content_hash,
                dataset_hash, selection_hash, guide_hash, annotator, stamp, row['available_at'],
                canonical_json(item.model_dump())))
            ids.append(decision_id)
        conn.commit()
        committed = True
        return _seal(conn, ids, 'initial')
    except BaseException as exc:
        conn.rollback()
        if committed:
            raise RuntimeError('Judgments are committed but not released. Run recover; do not resubmit.') from exc
        raise
    finally:
        conn.close()


def recover_pending(ledger):
    """Explicit crash recovery, without replacing decisions or earlier markers."""
    if not Path(ledger).exists():
        raise ValueError('Annotation ledger does not exist')
    conn = _connect(ledger)
    try:
        ids = [r[0] for r in conn.execute('''SELECT d.decision_id FROM annotation_decisions d
            LEFT JOIN annotation_releases r USING(decision_id) WHERE r.decision_id IS NULL
            ORDER BY d.sequence''')]
        return _seal(conn, ids, 'recovery') if ids else []
    finally:
        conn.close()


def read_annotations(ledger, guide_hash):
    conn = sqlite3.connect(Path(ledger).resolve().as_uri() + '?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    try:
        guide = conn.execute('SELECT text FROM annotation_guides WHERE sha256=?', (guide_hash,)).fetchone()
        if guide is None:
            return []
        if hashlib.sha256(guide[0].encode()).hexdigest() != guide_hash:
            raise ValueError('Stored annotation guide hash mismatch')
        return [dict(r) for r in conn.execute('''SELECT d.*, r.label_available_at, r.release_kind
            FROM annotation_decisions d LEFT JOIN annotation_releases r USING(decision_id)
            WHERE d.guide_sha256=? ORDER BY d.sequence''', (guide_hash,))]
    finally:
        conn.close()


def select_as_of(rows, history, cutoff, annotator):
    """One explicit annotator; never silently adjudicate multiple people's labels."""
    if not annotator.strip():
        raise ValueError('Select an explicit annotator')
    annotator = annotator.strip()
    cutoff = utc(cutoff)
    by_id = {r['version_id']: r for r in rows}
    latest = {}
    policies = {d['guide_sha256'] for d in history}
    if len(policies) > 1:
        raise ValueError('Select one annotation guide at a time')
    for decision in sorted(history, key=lambda d: d['sequence']):
        if decision['annotator'] != annotator or decision['version_id'] not in by_id:
            continue
        item = Annotation.model_validate_json(decision['annotation_json'])
        row = by_id[decision['version_id']]
        if item.version_id != row['version_id'] or item.content_hash != row['content_hash'] or \
                decision['content_hash'] != item.content_hash or \
                utc(decision['data_available_at']) != utc(row['available_at']):
            raise ValueError('Stored annotation content/availability identity mismatch')
        release = decision['label_available_at']
        if release is not None:
            if utc(release) < max(utc(decision['recorded_at']), utc(row['available_at'])):
                raise ValueError('Stored annotation release precedes content/data')
            if utc(release) < cutoff:
                latest[row['version_id']] = (decision, item)
    results = []
    for row in rows:
        pair = latest.get(row['version_id']) if utc(row['available_at']) < cutoff else None
        decision, item = pair if pair else (None, None)
        results.append({'version_id': row['version_id'], 'content_hash': row['content_hash'],
            'status': item.status if item else ('data_not_available_before_cutoff'
                if utc(row['available_at']) >= cutoff else 'pending'),
            'sentiment': item.sentiment if item else None,
            'decision_id': decision['decision_id'] if decision else None,
            'label_available_at': decision['label_available_at'] if decision else None})
    return results
