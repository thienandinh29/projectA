"""Human story grouping over an eligibility selection, kept apart from labels and predictions.

One review is a complete account of the cohort: every version is in a story, reviewed alone, or
uncertain, so nothing silently becomes a single-headline story. Revisions append a whole new review
naming the one they replace. Availability is sampled after contents commit. Word overlap only points
the reviewer at headlines to compare; it never assigns a group.
"""
import hashlib
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from research.eligibility import load_snapshot
from research.observations import canonical_json
from research.protocol import utc

OVERLAP_HINT = 0.3
STOPWORDS = {'a', 'an', 'and', 'as', 'at', 'by', 'for', 'from', 'in', 'is', 'it', 'its', 'of', 'on', 'or', 's',
             'the', 'to', 'with'}


def load_cohort(snapshot_dir, selection_dir):
    """Included versions, proven to be exact rows of the snapshot the selection was made from."""
    rows, _, dataset_sha256, _ = load_snapshot(snapshot_dir)
    folder = Path(selection_dir)
    report = json.loads((folder / 'selection.json').read_bytes())
    raw = (folder / 'eligible.jsonl').read_bytes()
    eligible_sha256 = hashlib.sha256(raw).hexdigest()
    if eligible_sha256 != report['eligible_rows_sha256']:
        raise ValueError('Eligible rows do not match their selection report')
    if report['input_dataset_sha256'] != dataset_sha256:
        raise ValueError('Selection was made from a different snapshot')
    by_id = {row['version_id']: row for row in rows}
    eligible = [json.loads(line) for line in raw.splitlines()]
    if any(by_id.get(row['version_id']) != row for row in eligible):
        raise ValueError('Eligible row is not an exact snapshot row')
    if len(eligible) != report['counts'].get('included', 0):
        raise ValueError('Eligible row count differs from the selection report')
    return sorted(eligible, key=_arrival), dataset_sha256, eligible_sha256


def _arrival(row):
    return utc(row['available_at']), utc(row['observed_at']), row['version_id']


def _words(title):
    return {word for word in re.findall(r"[a-z0-9$%]+", title.lower()) if word not in STOPWORDS}


def _compare_hints(rows):
    words = [_words(row['title']) for row in rows]
    hints = {index: [] for index in range(len(rows))}
    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            union = words[i] | words[j]
            overlap = len(words[i] & words[j]) / len(union) if union else 0
            if rows[i]['article_id'] == rows[j]['article_id'] or overlap >= OVERLAP_HINT:
                kind = 'same link' if rows[i]['article_id'] == rows[j]['article_id'] else 'shared words'
                hints[i].append(f'{j + 1} ({kind})')
                hints[j].append(f'{i + 1} ({kind})')
    return hints


def render_worksheet(rows):
    """Text for the reviewer and the number index. Shows no label, prediction or body text."""
    hints = _compare_hints(rows)
    lines = []
    for index, row in enumerate(rows):
        host = re.sub(r'^https?://(www\.)?', '', row['url']).split('/')[0]
        lines.append(f"{index + 1}. {row['title']}\n    {row['source']} {host}, available {row['available_at'][:16]}Z"
                     + (f"\n    compare: {', '.join(hints[index])}" if hints[index] else ''))
    index = {str(i + 1): {'version_id': row['version_id'], 'content_hash': row['content_hash']}
             for i, row in enumerate(rows)}
    return '\n'.join(lines) + '\n', index


def parse_answers(text, index):
    """`story: 1 7 | reason`, `uncertain: 3 9 | reason`, `alone: 2 4`. Each number exactly once."""
    stories, uncertain, seen = [], [], []
    for number, line in enumerate(text.splitlines(), 1):
        line = line.split('#', 1)[0].strip()
        if not line:
            continue
        match = re.fullmatch(r'(story|uncertain|alone):([\s\d]+)(?:\|(.*))?', line)
        if not match:
            raise ValueError(f'Line {number} is not story:, uncertain: or alone:')
        kind, numbers, reason = match[1], match[2].split(), (match[3] or '').strip()
        seen += numbers
        if kind != 'alone' and not reason:
            raise ValueError(f'Line {number}: {kind} needs a reason')
        if kind == 'story' and len(numbers) < 2:
            raise ValueError(f'Line {number}: a story needs at least two headlines; use alone:')
        unknown = [n for n in numbers if n not in index]
        if unknown:
            raise ValueError(f'Line {number}: unknown headline numbers {unknown}')
        if kind == 'alone':
            stories += [_story('alone', [n], index, None) for n in numbers]
        elif kind == 'story':
            stories.append(_story('multi', numbers, index, reason))
        else:
            uncertain.append({'members': _members(numbers, index), 'reason': reason})
    repeated = sorted({n for n in seen if seen.count(n) > 1}, key=int)
    missing = sorted(set(index) - set(seen), key=int)
    if repeated or missing:
        raise ValueError(f'Every headline must appear exactly once; repeated {repeated}, missing {missing}')
    return {'stories': sorted(stories, key=lambda s: s['story_id']), 'uncertain': uncertain}


def _members(numbers, index):
    return sorted((index[n] for n in numbers), key=lambda m: m['version_id'])


def _story(kind, numbers, index, reason):
    members = _members(numbers, index)
    digest = hashlib.sha256('\n'.join(m['version_id'] for m in members).encode()).hexdigest()
    return {'story_id': f'story-{digest[:16]}', 'kind': kind, 'members': members, 'reason': reason}


def initialize(conn):
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS story_reviews (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            review_id TEXT NOT NULL UNIQUE, supersedes_review_id TEXT REFERENCES story_reviews(review_id),
            dataset_sha256 TEXT NOT NULL, eligible_sha256 TEXT NOT NULL, guide_sha256 TEXT NOT NULL,
            reviewer TEXT NOT NULL, recorded_at TEXT NOT NULL, tool_revision TEXT NOT NULL,
            grouping_json TEXT NOT NULL, grouping_sha256 TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS story_releases (
            review_id TEXT PRIMARY KEY REFERENCES story_reviews(review_id), released_at TEXT NOT NULL);
    ''')
    for table in ('story_reviews', 'story_releases'):
        for action in ('UPDATE', 'DELETE'):
            conn.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()}
                BEFORE {action} ON {table}
                BEGIN SELECT RAISE(ABORT, 'Story grouping evidence is append-only'); END''')


def _connect(path):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA synchronous=FULL')
    initialize(conn)
    return conn


def _seal(conn):
    conn.execute('BEGIN IMMEDIATE')
    try:
        stamp = datetime.now(timezone.utc).isoformat()
        pending = [row[0] for row in conn.execute('''SELECT v.review_id FROM story_reviews v
            LEFT JOIN story_releases r USING (review_id) WHERE r.review_id IS NULL ORDER BY v.sequence''')]
        conn.executemany('INSERT INTO story_releases VALUES (?,?)', [(review_id, stamp) for review_id in pending])
        conn.execute('COMMIT')
        return pending
    except BaseException:
        conn.execute('ROLLBACK')
        raise


def append_review(ledger, grouping, *, dataset_sha256, eligible_sha256, guide_sha256, reviewer, tool_revision,
                  supersedes=None):
    """Commit one complete grouping, then release it. A revision must name the latest review."""
    if not reviewer.strip():
        raise ValueError('Reviewer identity is required')
    rendered = canonical_json(grouping)
    review_id = str(uuid.uuid4())
    conn = _connect(ledger)
    try:
        conn.execute('BEGIN IMMEDIATE')
        try:
            latest = conn.execute('''SELECT v.review_id, r.released_at FROM story_reviews v
                LEFT JOIN story_releases r USING (review_id) WHERE v.eligible_sha256=?
                ORDER BY v.sequence DESC LIMIT 1''', (eligible_sha256,)).fetchone()
            if latest and latest['released_at'] is None:
                raise ValueError('An unreleased review must be recovered before revising')
            if supersedes != (latest['review_id'] if latest else None):
                raise ValueError('A revision must name the latest review for this cohort')
            conn.execute('''INSERT INTO story_reviews (review_id, supersedes_review_id, dataset_sha256, eligible_sha256,
                guide_sha256, reviewer, recorded_at, tool_revision, grouping_json, grouping_sha256)
                VALUES (?,?,?,?,?,?,?,?,?,?)''', (
                review_id, supersedes, dataset_sha256, eligible_sha256, guide_sha256, reviewer.strip(),
                datetime.now(timezone.utc).isoformat(), tool_revision, rendered,
                hashlib.sha256(rendered.encode()).hexdigest()))
            conn.execute('COMMIT')
        except BaseException:
            conn.execute('ROLLBACK')
            raise
        _seal(conn)
        return review_id
    finally:
        conn.close()


def recover_pending(ledger):
    """Release committed reviews at the present time; never at their recording time."""
    conn = _connect(ledger)
    try:
        return _seal(conn)
    finally:
        conn.close()


def select_as_of(ledger, rows, eligible_sha256, cutoff):
    """Latest review released strictly before the cutoff, with each story's first available anchor."""
    cutoff = utc(cutoff)
    conn = _connect(ledger)
    try:
        reviews = conn.execute('''SELECT v.*, r.released_at FROM story_reviews v JOIN story_releases r USING (review_id)
            WHERE v.eligible_sha256=? ORDER BY v.sequence''', (eligible_sha256,)).fetchall()
    finally:
        conn.close()
    released = [review for review in reviews if utc(review['released_at']) < cutoff]
    if not released:
        raise ValueError('No story review was released before this cutoff')
    review = released[-1]
    if hashlib.sha256(review['grouping_json'].encode()).hexdigest() != review['grouping_sha256']:
        raise ValueError('Stored grouping does not match its hash')
    grouping = json.loads(review['grouping_json'])
    by_id = {row['version_id']: row for row in rows}
    stories = []
    for story in grouping['stories']:
        members = [by_id[member['version_id']] for member in story['members']]
        stories.append({**story, 'anchor_version_id': min(members, key=_arrival)['version_id']})
    return {'review_id': review['review_id'], 'released_at': review['released_at'], 'reviewer': review['reviewer'],
            'guide_sha256': review['guide_sha256'], 'grouping_sha256': review['grouping_sha256'],
            'tool_revision': review['tool_revision'], 'stories': stories, 'uncertain': grouping['uncertain']}
