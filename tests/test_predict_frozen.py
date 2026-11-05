"""Predictions are label-blind, append-only, repeatable and come only from verified bytes."""
import ast
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import unittest
import unittest.mock
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from research.observations import make_observation
from scripts import predict_frozen as predict

T = datetime(2026, 10, 1, tzinfo=timezone.utc)
REVISION = 'a' * 40
LABELS = {'0': 'positive', '1': 'negative', '2': 'neutral'}
AUDIT = {'paths': None}


def _audit(event, args):
    if AUDIT['paths'] is not None and event in ('open', 'sqlite3.connect') and args and isinstance(args[0], (str, os.PathLike)):
        AUDIT['paths'].append(str(args[0]))


sys.addaudithook(_audit)


def row(title, article_id):
    obs = make_observation(source='RSS', article_id=article_id, title=title, url='https://example.test/news',
                           source_item={'title': title}, title_provenance='source_feed', observed_at=T,
                           source_time_kind='unknown')
    return {'version_id': obs.id, 'content_hash': obs.content_hash, 'article_id': article_id, 'title': title,
            'source': 'RSS', 'url': obs.url, 'observed_at': T.isoformat(),
            'available_at': (T + timedelta(seconds=1)).isoformat(), 'snapshot': obs.model_dump_json()}


class Scorer:
    loads = 0

    def __init__(self, directory, manifest, shift=0.0):
        type(self).loads += 1
        self.shift = shift
        self.environment = {'torch': 'fake'}

    def score(self, text):
        digest = hashlib.sha256(text.encode()).digest()
        return [digest[0] / 25.0 + self.shift, digest[1] / 25.0, digest[2] / 25.0]


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.snapshot = self.root / 'research' / 'snapshot'
        self.snapshot.mkdir(parents=True)
        self.rows = [row('Invented firm raises guidance', 'a'), row('Invented firm cuts guidance', 'b'),
                     row('Invented firm raises guidance', 'c')]
        data = b''.join(json.dumps(r).encode() + b'\n' for r in self.rows)
        (self.snapshot / 'dataset.jsonl').write_bytes(data)
        (self.snapshot / 'manifest.json').write_text(json.dumps({
            'rows': len(self.rows), 'dataset_sha256': hashlib.sha256(data).hexdigest(),
            'cutoff': (T + timedelta(days=1)).isoformat()}), encoding='utf-8')
        self.models = self.root / 'models'
        self.model = self.models / f'org__model@{REVISION}'
        self.model.mkdir(parents=True)
        (self.model / 'model.safetensors').write_bytes(b'weights')
        self.manifest = self.root / 'freeze.json'
        self.manifest.write_text(json.dumps({
            'kind': 'frozen_model_v1', 'repository': 'org/model', 'revision': REVISION, 'label_map': LABELS,
            'tokenizer': {'model_max_length': 512},
            'files': {'model.safetensors': {'sha256': hashlib.sha256(b'weights').hexdigest()}}}), encoding='utf-8')
        self.ledger = self.root / 'research' / 'predictions.sqlite3'
        Scorer.loads = 0

    def tearDown(self):
        self.tmp.cleanup()

    def run_predict(self, load_scorer=Scorer, manifest=None):
        return predict.predict_snapshot(self.snapshot, manifest or self.manifest, self.models, self.ledger,
                                        tool_revision='tool', load_scorer=load_scorer)

    def table(self, name='predictions'):
        with closing(sqlite3.connect(self.ledger)) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute(f'SELECT * FROM {name} ORDER BY 1, 2')]


class TestPredictions(Case):
    def test_one_row_per_content_hash_and_revision_with_all_fields(self):
        summary = self.run_predict()
        rows = self.table()
        self.assertEqual((summary['versions'], summary['distinct_inputs'], summary['inserted']), (3, 2, 3))
        self.assertEqual({r['content_hash'] for r in rows}, {r['content_hash'] for r in self.rows})
        for stored in rows:
            logits, probabilities = json.loads(stored['logits_json']), json.loads(stored['probabilities_json'])
            self.assertEqual(stored['model_revision'], REVISION)
            self.assertAlmostEqual(sum(probabilities), 1.0)
            self.assertEqual(stored['predicted_index'], logits.index(max(logits)))
            self.assertEqual(stored['predicted_label'], LABELS[str(stored['predicted_index'])])
            self.assertTrue(stored['computed_at'])

    def test_outcome_uses_the_frozen_label_order(self):
        result = predict.outcome([0.0, 5.0, 1.0], {'0': 'neutral', '1': 'positive', '2': 'negative'})
        self.assertEqual((result['predicted_index'], result['predicted_label']), (1, 'positive'))
        with self.assertRaisesRegex(ValueError, 'logits'):
            predict.outcome([0.0, 1.0], LABELS)

    def test_huge_logits_do_not_overflow(self):
        self.assertAlmostEqual(sum(predict.outcome([1000.0, -1000.0, 0.0], LABELS)['probabilities']), 1.0)

    def test_summary_never_contains_headline_text(self):
        self.assertNotIn('Invented', json.dumps(self.run_predict()))

    def test_release_is_sampled_after_contents_commit(self):
        self.run_predict()
        computed = {r['content_hash']: r['computed_at'] for r in self.table()}
        releases = self.table('prediction_releases')
        self.assertEqual(len(releases), 3)
        for release in releases:
            self.assertGreaterEqual(release['released_at'], computed[release['content_hash']])

    def test_crash_before_release_is_sealed_later_at_the_later_time(self):
        with unittest.mock.patch.object(predict, 'release_pending', side_effect=RuntimeError):
            with self.assertRaises(RuntimeError):
                self.run_predict()
        self.assertEqual(self.table('prediction_releases'), [])
        computed = self.table()[0]['computed_at']
        self.assertEqual(self.run_predict()['released'], 3)
        self.assertGreater(self.table('prediction_releases')[0]['released_at'], computed)


class TestImmutability(Case):
    def test_update_and_delete_abort_on_every_table(self):
        self.run_predict()
        with closing(sqlite3.connect(self.ledger)) as conn:
            for table, column in (('predictions', 'predicted_label'), ('prediction_runs', 'tool_revision'),
                                  ('prediction_releases', 'released_at')):
                for sql in (f"UPDATE {table} SET {column}='x'", f'DELETE FROM {table}'):
                    with self.assertRaisesRegex(sqlite3.IntegrityError, 'append-only'):
                        conn.execute(sql)

    def test_identical_rerun_adds_no_rows_and_changes_none(self):
        self.run_predict()
        predictions, releases = self.table(), self.table('prediction_releases')
        summary = self.run_predict()
        self.assertEqual((summary['inserted'], summary['identical_on_rerun'], summary['mismatched_never_rewritten'],
                          summary['released']), (0, 3, [], 0))
        self.assertEqual((self.table(), self.table('prediction_releases')), (predictions, releases))
        self.assertEqual(len(self.table('prediction_runs')), 2)

    def test_changed_output_is_reported_and_never_rewrites(self):
        self.run_predict()
        before = self.table()
        summary = self.run_predict(lambda d, m: Scorer(d, m, shift=0.001))
        self.assertEqual(len(summary['mismatched_never_rewritten']), 3)
        self.assertEqual(self.table(), before)
        self.assertEqual(len(json.loads(self.table('prediction_runs')[0]['mismatched_json'])
                             + json.loads(self.table('prediction_runs')[1]['mismatched_json'])), 3)

    def test_another_revision_gets_its_own_rows(self):
        self.run_predict()
        other = 'b' * 40
        self.model.rename(self.models / f'org__model@{other}')
        document = json.loads(self.manifest.read_text(encoding='utf-8'))
        self.manifest.write_text(json.dumps({**document, 'revision': other}), encoding='utf-8')
        self.assertEqual(self.run_predict()['inserted'], 3)
        self.assertEqual(len(self.table()), 6)


class TestFrozenModelGate(Case):
    def assert_refused(self, pattern):
        with self.assertRaisesRegex(ValueError, pattern):
            self.run_predict()
        self.assertEqual(Scorer.loads, 0)
        self.assertFalse(self.ledger.exists())

    def test_wrong_weight_hash_is_rejected_before_loading(self):
        (self.model / 'model.safetensors').write_bytes(b'other weights')
        self.assert_refused('changed file: model.safetensors')

    def test_extra_pickle_file_is_rejected(self):
        (self.model / 'pytorch_model.bin').write_bytes(b'pickle')
        self.assert_refused('extra file')

    def test_missing_model_directory_is_rejected(self):
        (self.model / 'model.safetensors').unlink()
        self.model.rmdir()
        self.assert_refused('directory is missing')

    def test_other_json_is_not_a_manifest(self):
        self.manifest.write_text('{"kind": "other"}', encoding='utf-8')
        self.assert_refused('Not a frozen model manifest')

    def test_tampered_snapshot_is_rejected(self):
        path = self.snapshot / 'dataset.jsonl'
        path.write_bytes(path.read_bytes().replace(b'raises', b'lowers'))
        self.assert_refused('hash or row count')

    def test_invalidated_snapshot_is_rejected(self):
        (self.snapshot / 'INVALID_FOR_RESEARCH.json').write_text('{}', encoding='utf-8')
        self.assert_refused('Invalidated')


class TestLabelBlind(Case):
    def test_only_snapshot_model_manifest_and_own_ledger_are_opened(self):
        research = self.root / 'research'
        for name in ('annotations.sqlite3', 'eligibility.sqlite3'):
            with closing(sqlite3.connect(research / name)) as conn:
                conn.execute('CREATE TABLE labels (x)')
                conn.commit()
        selection = research / 'sentiment-selection'
        selection.mkdir()
        (selection / 'selection.json').write_text('{}', encoding='utf-8')
        AUDIT['paths'] = []
        try:
            self.run_predict()
        finally:
            seen, AUDIT['paths'] = AUDIT['paths'], None
        opened = {Path(path).resolve() for path in seen if Path(path).resolve().is_relative_to(self.root)}
        self.assertEqual(opened, {self.snapshot / 'dataset.jsonl', self.snapshot / 'manifest.json', self.manifest,
                                  self.model / 'model.safetensors', self.ledger})

    def test_source_imports_no_annotation_or_selection_code(self):
        tree = ast.parse(Path(predict.__file__).read_text(encoding='utf-8'))
        project = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module.split('.')[0] in ('research', 'scripts', 'lakehouse', 'utils'):
                project[node.module] = [alias.name for alias in node.names]
            elif isinstance(node, ast.Import):
                self.assertFalse([a.name for a in node.names if a.name.split('.')[0] in ('research', 'scripts', 'lakehouse', 'utils')])
        self.assertEqual(project, {'research.eligibility': ['load_snapshot'], 'scripts.freeze_model': ['verify_frozen']})

    def test_a_ledger_path_holding_other_evidence_is_refused_and_unchanged(self):
        other = self.root / 'annotations.sqlite3'
        with closing(sqlite3.connect(other)) as conn:
            conn.execute('CREATE TABLE sentiment_annotations (x)')
            conn.commit()
        before = other.read_bytes()
        self.ledger = other
        with self.assertRaisesRegex(ValueError, 'not a prediction ledger'):
            self.run_predict()
        self.assertEqual(other.read_bytes(), before)


class TestCodeGate(unittest.TestCase):
    def test_runs_need_a_recorded_clean_revision(self):
        self.assertEqual(predict.code_blockers('abc', 'clean'), [])
        self.assertTrue(predict.code_blockers('unrecorded', 'clean'))
        self.assertTrue(predict.code_blockers('abc', 'dirty'))


@unittest.skipUnless(os.getenv('FROZEN_MODEL_ROOT'), 'Set FROZEN_MODEL_ROOT in the inference image to use the real model')
class TestRealFrozenModel(unittest.TestCase):
    """Invented headlines only, kept in memory: this creates no research record."""

    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parent.parent / 'docs' / 'model-freeze-finbert-20261104.json'
        cls.manifest, directory, _ = predict.load_frozen(path, os.environ['FROZEN_MODEL_ROOT'])
        cls.scorer = predict.FrozenScorer(directory, cls.manifest)

    def label(self, text):
        return predict.outcome(self.scorer.score(text), self.manifest['label_map'])['predicted_label']

    def test_label_order_matches_clear_cases(self):
        self.assertEqual(self.label('Acme profit surges to a record high as sales beat expectations'), 'positive')
        self.assertEqual(self.label('Acme shares plunge after heavy losses and a profit warning'), 'negative')

    def test_scores_repeat_exactly(self):
        text = 'Acme will publish its results on Tuesday'
        self.assertEqual(self.scorer.score(text), self.scorer.score(text))

    def test_tokenizer_lower_cases(self):
        self.assertEqual(self.scorer.score('ACME OPENS A NEW OFFICE'), self.scorer.score('acme opens a new office'))

    def test_scoring_does_not_depend_on_other_inputs(self):
        text = 'Acme appoints a new chief financial officer'
        first = self.scorer.score(text)
        self.scorer.score('A much longer invented sentence about quarterly results and regional demand for widgets')
        self.assertEqual(self.scorer.score(text), first)


if __name__ == '__main__':
    unittest.main()
