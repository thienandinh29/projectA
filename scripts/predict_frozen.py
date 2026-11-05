"""Label-blind CPU predictions from a frozen model over a verified snapshot.

Reads only the snapshot, the frozen model and its committed manifest, and writes only its own
ledger; it never opens annotation or eligibility evidence. Rows are keyed by content hash and model
revision and never change: a re-run recomputes, compares and reports. Availability is sampled after
contents commit. Inputs are scored one at a time on one thread, so padding and batching cannot
change a logit. This records model output; it does not score, select or judge anything.
"""
import argparse
import hashlib
import json
import math
import os
import platform
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from research.eligibility import load_snapshot
from scripts.freeze_model import verify_frozen

LEDGER_TABLES = {'prediction_runs', 'predictions', 'prediction_releases', 'sqlite_sequence'}


def initialize(conn):
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS prediction_runs (
            run_id TEXT PRIMARY KEY,
            dataset_sha256 TEXT NOT NULL, snapshot_manifest_sha256 TEXT NOT NULL,
            model_repository TEXT NOT NULL, model_revision TEXT NOT NULL,
            model_manifest_sha256 TEXT NOT NULL, tool_revision TEXT NOT NULL,
            environment_json TEXT NOT NULL, recorded_at TEXT NOT NULL,
            inserted INTEGER NOT NULL, identical INTEGER NOT NULL, mismatched_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS predictions (
            content_hash TEXT NOT NULL, model_revision TEXT NOT NULL,
            run_id TEXT NOT NULL REFERENCES prediction_runs(run_id),
            input_text TEXT NOT NULL, logits_json TEXT NOT NULL, probabilities_json TEXT NOT NULL,
            predicted_index INTEGER NOT NULL, predicted_label TEXT NOT NULL, computed_at TEXT NOT NULL,
            PRIMARY KEY (content_hash, model_revision));
        CREATE TABLE IF NOT EXISTS prediction_releases (
            content_hash TEXT NOT NULL, model_revision TEXT NOT NULL, released_at TEXT NOT NULL,
            PRIMARY KEY (content_hash, model_revision),
            FOREIGN KEY (content_hash, model_revision) REFERENCES predictions(content_hash, model_revision));
    ''')
    for table in ('prediction_runs', 'predictions', 'prediction_releases'):
        for action in ('UPDATE', 'DELETE'):
            conn.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()}
                BEFORE {action} ON {table}
                BEGIN SELECT RAISE(ABORT, 'Prediction evidence is append-only'); END''')


def connect_ledger(path):
    """Refuses a file holding anything else, so a wrong path cannot touch other evidence."""
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA synchronous=FULL')
    foreign = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")} - LEDGER_TABLES
    if foreign:
        conn.close()
        raise ValueError(f'{path} is not a prediction ledger; it holds: {sorted(foreign)}')
    initialize(conn)
    return conn


def load_frozen(manifest_path, models_root):
    """The committed manifest decides which bytes are acceptable; any difference is refused."""
    raw = Path(manifest_path).read_bytes()
    manifest = json.loads(raw)
    if manifest.get('kind') != 'frozen_model_v1':
        raise ValueError('Not a frozen model manifest')
    directory = Path(models_root) / f"{manifest['repository'].replace('/', '__')}@{manifest['revision']}"
    if not directory.is_dir():
        raise ValueError(f'Frozen model directory is missing: {directory}')
    problems = verify_frozen(directory, manifest)
    if problems:
        raise ValueError(f'Frozen model rejected: {problems}')
    return manifest, directory, hashlib.sha256(raw).hexdigest()


class FrozenScorer:
    """Transformers inference, imported here so the collection image needs no torch."""

    def __init__(self, directory, manifest):
        import importlib.metadata as metadata

        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        torch.set_num_threads(1)
        self.torch = torch
        self.max_length = manifest['tokenizer']['model_max_length']
        self.tokenizer = AutoTokenizer.from_pretrained(directory, local_files_only=True)
        self.model, info = AutoModelForSequenceClassification.from_pretrained(
            directory, local_files_only=True, use_safetensors=True, output_loading_info=True)
        if info['missing_keys'] or info['unexpected_keys'] or info['mismatched_keys']:
            raise ValueError(f'Weights do not fit the architecture: {info}')  # Would otherwise run on random weights.
        self.model.eval()
        if {str(k): v for k, v in self.model.config.id2label.items()} != manifest['label_map']:
            raise ValueError('Loaded label map differs from the frozen manifest')
        self.environment = {'python': platform.python_version(), 'platform': platform.platform(), 'threads': 1,
                            **{name: metadata.version(name) for name in ('torch', 'transformers', 'tokenizers', 'safetensors')}}

    def score(self, text):
        encoded = self.tokenizer(text, truncation=True, max_length=self.max_length, return_tensors='pt')
        with self.torch.inference_mode():
            return self.model(**encoded).logits[0].tolist()


def outcome(logits, label_map):
    """Softmax and argmax from raw logits; on an exact tie the lowest index wins."""
    if len(logits) != len(label_map):
        raise ValueError(f'{len(logits)} logits for {len(label_map)} labels')
    top = max(logits)
    weights = [math.exp(value - top) for value in logits]
    index = logits.index(top)
    return {'logits': list(logits), 'probabilities': [weight / sum(weights) for weight in weights],
            'predicted_index': index, 'predicted_label': label_map[str(index)]}


def _same(stored, text, result):
    return (stored['input_text'] == text and json.loads(stored['logits_json']) == result['logits']
            and json.loads(stored['probabilities_json']) == result['probabilities']
            and stored['predicted_index'] == result['predicted_index'])


def release_pending(conn, revision, now=None):
    """Seal committed predictions. The clock is read only here, after contents are durable."""
    conn.execute('BEGIN IMMEDIATE')
    try:
        stamp = (now or datetime.now)(timezone.utc).isoformat()
        cursor = conn.execute('''INSERT INTO prediction_releases
            SELECT p.content_hash, p.model_revision, ? FROM predictions p
            LEFT JOIN prediction_releases r USING (content_hash, model_revision)
            WHERE p.model_revision=? AND r.released_at IS NULL''', (stamp, revision))
        conn.execute('COMMIT')
        return cursor.rowcount
    except BaseException:
        conn.execute('ROLLBACK')
        raise


def predict_snapshot(snapshot_dir, manifest_path, models_root, ledger, *, tool_revision, load_scorer=FrozenScorer):
    """Run one snapshot. Returns a summary with counts and hashes; never any headline text."""
    rows, _, dataset_sha256, snapshot_manifest_sha256 = load_snapshot(snapshot_dir)
    manifest, directory, manifest_sha256 = load_frozen(manifest_path, models_root)
    scorer = load_scorer(directory, manifest)
    revision, label_map = manifest['revision'], manifest['label_map']
    results = {row['content_hash']: (row['title'], outcome(scorer.score(row['title']), label_map)) for row in rows}
    computed_at = datetime.now(timezone.utc).isoformat()
    conn = connect_ledger(ledger)
    try:
        run_id, inserted, identical, mismatched = str(uuid.uuid4()), [], 0, []
        conn.execute('BEGIN IMMEDIATE')
        try:
            for content_hash, (text, result) in results.items():
                stored = conn.execute('SELECT * FROM predictions WHERE content_hash=? AND model_revision=?',
                                      (content_hash, revision)).fetchone()
                if stored is None:
                    inserted.append((content_hash, revision, run_id, text, json.dumps(result['logits']),
                                     json.dumps(result['probabilities']), result['predicted_index'],
                                     result['predicted_label'], computed_at))
                elif _same(stored, text, result):
                    identical += 1
                else:
                    mismatched.append(content_hash)
            conn.execute('INSERT INTO prediction_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', (
                run_id, dataset_sha256, snapshot_manifest_sha256, manifest['repository'], revision, manifest_sha256,
                tool_revision, json.dumps(scorer.environment, sort_keys=True), computed_at, len(inserted), identical,
                json.dumps(sorted(mismatched))))
            conn.executemany('INSERT INTO predictions VALUES (?,?,?,?,?,?,?,?,?)', inserted)
            conn.execute('COMMIT')
        except BaseException:
            conn.execute('ROLLBACK')
            raise
        released = release_pending(conn, revision)
    finally:
        conn.close()
    classes = {}
    for _, result in results.values():
        classes[result['predicted_label']] = classes.get(result['predicted_label'], 0) + 1
    return {'run_id': run_id, 'dataset_sha256': dataset_sha256, 'model_revision': revision,
            'model_manifest_sha256': manifest_sha256, 'tool_revision': tool_revision,
            'environment': scorer.environment, 'versions': len(rows),
            'distinct_inputs': len({text for text, _ in results.values()}), 'inserted': len(inserted),
            'identical_on_rerun': identical, 'mismatched_never_rewritten': sorted(mismatched),
            'released': released, 'class_counts': dict(sorted(classes.items()))}


def code_blockers(revision, worktree_state):
    if not revision or revision == 'unrecorded':
        return ['code revision is not recorded: set COLLECTION_CODE_REVISION']
    if worktree_state != 'clean':
        return [f'worktree state is {worktree_state!r}, not clean: commit first and set COLLECTION_WORKTREE_STATE']
    return []


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot-dir', type=Path, required=True)
    parser.add_argument('--model-manifest', type=Path, default=Path('/app/docs/model-freeze-finbert-20261104.json'))
    parser.add_argument('--models-root', type=Path, default=Path('/app/data/models'))
    parser.add_argument('--ledger', type=Path, default=Path('/app/data/research/predictions.sqlite3'))
    parser.add_argument('--check', action='store_true', help='Verify snapshot and model; run nothing, write nothing')
    args = parser.parse_args()
    revision, state = os.getenv('COLLECTION_CODE_REVISION'), os.getenv('COLLECTION_WORKTREE_STATE')
    blockers = code_blockers(revision, state)
    if args.check:
        rows, _, dataset_sha256, _ = load_snapshot(args.snapshot_dir)
        manifest, _, manifest_sha256 = load_frozen(args.model_manifest, args.models_root)
        print(json.dumps({'ready': not blockers, 'blockers': blockers, 'versions': len(rows),
                          'dataset_sha256': dataset_sha256, 'model_revision': manifest['revision'],
                          'model_manifest_sha256': manifest_sha256}, indent=2))
        raise SystemExit(1 if blockers else 0)
    if blockers:
        parser.error('; '.join(blockers))
    summary = predict_snapshot(args.snapshot_dir, args.model_manifest, args.models_root, args.ledger, tool_revision=revision)
    print(json.dumps(summary, indent=2))
    raise SystemExit(1 if summary['mismatched_never_rewritten'] else 0)


if __name__ == '__main__':
    sys.exit(main())
