"""The score report is built only from linked, verified inputs and is reproducible byte for byte."""
import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from research import stories
from research.annotations import append_annotations, load_selection
from research.eligibility import append_reviews, read_decisions, select_as_of as eligibility_as_of
from research.observations import canonical_json, make_observation
from scripts import predict_frozen as predict
from scripts import review_sentiment, review_stories, score
from tests.test_annotations import GUIDE, annotation
from tests.test_eligibility import POLICY, review
from tests.test_stories import later_than_now

T = datetime(2026, 10, 1, tzinfo=timezone.utc)
REVISION = 'c' * 40
LABELS = {'0': 'positive', '1': 'negative', '2': 'neutral'}
TITLES = {'v1': 'Acme profit jumps 20 percent', 'v2': 'Acme beats forecasts with a strong quarter',
          'v3': 'Acme quarterly results top estimates', 'v4': 'Central bank holds rates steady',
          'v5': 'Widget maker names new chief', 'v6': 'Acme opens a factory in Texas'}
# v1 is the story anchor and is predicted wrongly; v6 is unresolved.
PREDICTED = {'v1': 'negative', 'v2': 'positive', 'v3': 'positive', 'v4': 'negative', 'v5': 'neutral', 'v6': 'neutral'}


def row(name, seconds):
    observed = T + timedelta(seconds=seconds)
    obs = make_observation(source='RSS', article_id=name, title=TITLES[name], url=f'https://example.test/{name}',
                           source_item={'title': TITLES[name]}, title_provenance='source_feed', observed_at=observed,
                           source_time_kind='unknown')
    return {'version_id': obs.id, 'content_hash': obs.content_hash, 'article_id': name, 'title': TITLES[name],
            'source': 'RSS', 'url': obs.url, 'observed_at': observed.isoformat(),
            'available_at': (observed + timedelta(seconds=1)).isoformat(), 'snapshot': obs.model_dump_json()}


class FixedScorer:
    def __init__(self, directory, manifest):
        self.environment = {'fake': True}

    def score(self, text):
        name = next(key for key, title in TITLES.items() if title == text)
        return [{'positive': 0, 'negative': 1, 'neutral': 2}[PREDICTED[name]] == i and 5.0 or 0.0 for i in range(3)]


class Pipeline(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.rows = [row(name, 10 * int(name[1:])) for name in TITLES]
        self.by_name = {name: r for name, r in zip(TITLES, self.rows)}
        self.build_snapshot_and_eligibility()
        self.build_labels()
        self.build_stories()
        self.build_predictions()

    def tearDown(self):
        self.tmp.cleanup()

    def build_snapshot_and_eligibility(self):
        self.snapshot, self.selection = self.root / 'snapshot', self.root / 'eligibility-selection'
        self.snapshot.mkdir()
        self.selection.mkdir()
        data = b''.join((canonical_json(r) + '\n').encode() for r in self.rows)
        self.dataset_sha256 = hashlib.sha256(data).hexdigest()
        (self.snapshot / 'dataset.jsonl').write_bytes(data)
        manifest = json.dumps({'rows': len(self.rows), 'dataset_sha256': self.dataset_sha256,
                               'cutoff': (T + timedelta(days=1)).isoformat()}).encode()
        (self.snapshot / 'manifest.json').write_bytes(manifest)
        ledger = self.root / 'eligibility.sqlite3'
        with patch('research.eligibility.datetime') as clock:
            clock.now.side_effect = [T + timedelta(hours=1), T + timedelta(hours=1, seconds=1)]
            append_reviews(ledger, self.rows, self.dataset_sha256, POLICY, 'human', [review(r) for r in self.rows])
        decisions = read_decisions(ledger, POLICY[0])
        history = ''.join(canonical_json(d) + '\n' for d in decisions).encode()
        cutoff = T + timedelta(hours=2)
        versions = eligibility_as_of(self.rows, decisions, cutoff)
        (self.selection / 'eligible.jsonl').write_bytes(data)
        (self.selection / 'decisions.jsonl').write_bytes(history)
        (self.selection / 'policy.md').write_bytes(POLICY[1].encode())
        (self.selection / 'selection.json').write_text(json.dumps({
            'kind': 'development_eligibility_selection_v2', 'timing_contract': 'post_content_commit_v2',
            'input_dataset_sha256': self.dataset_sha256, 'input_manifest_sha256': hashlib.sha256(manifest).hexdigest(),
            'eligible_rows_sha256': self.dataset_sha256, 'decision_history_sha256': hashlib.sha256(history).hexdigest(),
            'policy_sha256': POLICY[0], 'cutoff': cutoff.isoformat(), 'counts': {'included': len(self.rows)},
            'versions': versions}), encoding='utf-8')

    def build_labels(self):
        guide = self.root / 'guide.md'
        guide.write_bytes(GUIDE[1].encode())
        ledger = self.root / 'annotations.sqlite3'
        sentiments = {'v1': 'positive', 'v2': 'positive', 'v3': 'positive', 'v4': 'negative', 'v5': 'neutral'}
        items = [annotation(self.by_name[name], sentiment=sentiments[name], assisted=name == 'v2',
                            assistance_note='Model suggestion shown' if name == 'v2' else '') for name in sentiments]
        items.append(annotation(self.by_name['v6'], status='unresolved', sentiment=None, mixed=True, reason='Competing directions'))
        selection_sha256 = load_selection(self.snapshot, self.selection)[3]
        with patch('research.annotations.datetime') as clock:
            clock.now.side_effect = [T + timedelta(hours=3), T + timedelta(hours=3, seconds=1)]
            append_annotations(ledger, self.rows, self.dataset_sha256, selection_sha256, GUIDE, 'human-a', items)
        self.sentiment = self.root / 'sentiment-selection'
        argv = ['x', 'select', '--snapshot-dir', str(self.snapshot), '--eligibility-selection', str(self.selection),
                '--guide-file', str(guide), '--ledger', str(ledger), '--annotator', 'human-a',
                '--cutoff', later_than_now().isoformat(), '--output', str(self.sentiment)]
        with patch('sys.argv', argv), redirect_stdout(io.StringIO()):
            review_sentiment.main()

    def build_stories(self):
        cohort = stories.load_cohort(self.snapshot, self.selection)[0]
        number = {r['title']: str(i + 1) for i, r in enumerate(cohort)}
        _, index = stories.render_worksheet(cohort)
        n = lambda *names: ' '.join(number[TITLES[name]] for name in names)
        answers = f"story: {n('v1', 'v2', 'v3')} | Same Acme earnings release\nalone: {n('v4', 'v5', 'v6')}"
        ledger = self.root / 'story-groups.sqlite3'
        stories.append_review(ledger, stories.parse_answers(answers, index), dataset_sha256=self.dataset_sha256,
                              eligible_sha256=hashlib.sha256((self.selection / 'eligible.jsonl').read_bytes()).hexdigest(),
                              guide_sha256='g', reviewer='human', tool_revision='tool')
        self.story_dir = self.root / 'story-selection'
        argv = ['x', 'select', '--snapshot-dir', str(self.snapshot), '--selection-dir', str(self.selection),
                '--ledger', str(ledger), '--cutoff', later_than_now().isoformat(), '--output', str(self.story_dir)]
        with patch('sys.argv', argv), redirect_stdout(io.StringIO()):
            review_stories.main()

    def build_predictions(self):
        models = self.root / 'models'
        model = models / f'org__model@{REVISION}'
        model.mkdir(parents=True)
        (model / 'model.safetensors').write_bytes(b'weights')
        self.manifest = self.root / 'freeze.json'
        self.manifest.write_text(json.dumps({
            'kind': 'frozen_model_v1', 'repository': 'org/model', 'revision': REVISION, 'label_map': LABELS,
            'tokenizer': {'model_max_length': 512},
            'files': {'model.safetensors': {'sha256': hashlib.sha256(b'weights').hexdigest()}}}), encoding='utf-8')
        self.predictions = self.root / 'predictions.sqlite3'
        predict.predict_snapshot(self.snapshot, self.manifest, models, self.predictions, tool_revision='tool',
                                 load_scorer=FixedScorer)

    def report(self, **overrides):
        paths = dict(snapshot_dir=self.snapshot, eligibility_dir=self.selection, sentiment_dir=self.sentiment,
                     story_dir=self.story_dir, ledger=self.predictions, model_manifest=self.manifest)
        paths.update(overrides)
        return score.build_report(paths['snapshot_dir'], paths['eligibility_dir'], paths['sentiment_dir'],
                                  paths['story_dir'], paths['ledger'], paths['model_manifest'], seed=3, resamples=100)

    def edit(self, path, old, new):
        raw = Path(path).read_bytes()
        self.assertIn(old, raw)
        Path(path).write_bytes(raw.replace(old, new))


class TestReport(Pipeline):
    def test_counts_and_hand_computed_conditions(self):
        report = self.report()
        counts = report['counts']
        self.assertEqual((counts['eligible_versions'], counts['labeled'], counts['unresolved'], counts['labeled_assisted']), (6, 5, 1, 1))
        self.assertEqual((counts['with_prediction'], counts['scored_all_labels'], counts['scored_unassisted_only']), (6, 5, 4))
        paired = report['results']['all_scorable_labels']['paired_A_B_C']['conditions']
        self.assertAlmostEqual(paired['A']['accuracy'], 4 / 5)
        self.assertAlmostEqual(paired['B']['accuracy'], (2 / 3 + 1 + 1) / 3)
        self.assertAlmostEqual(paired['C']['accuracy'], 2 / 3)
        self.assertEqual(report['results']['all_scorable_labels']['stories_with_several_scorable_members'], 1)

    def test_unresolved_is_neither_scored_nor_treated_as_neutral(self):
        matrix = self.report()['results']['all_scorable_labels']['unrestricted_A_B']['conditions']['A']['confusion_true_by_predicted']
        self.assertEqual(sum(matrix['neutral'].values()), 1.0)

    def test_unassisted_only_drops_assisted_labels_but_keeps_the_fixed_anchor(self):
        result = self.report()['results']['unassisted_labels_only']
        self.assertEqual(result['scorable_versions'], 4)
        self.assertEqual(result['stories_with_scorable_anchor'], 3)

    def test_timing_disclosure_records_that_labels_came_before_predictions(self):
        timing = self.report()['timing']
        self.assertEqual(timing['scored_versions_whose_label_was_released_before_their_prediction'], 5)
        self.assertLess(timing['label_released_range'][1], timing['prediction_released_range'][0])

    def test_two_runs_write_identical_bytes(self):
        outputs = []
        for name in ('first', 'second'):
            out = self.root / name
            argv = ['x', '--snapshot-dir', str(self.snapshot), '--eligibility-selection', str(self.selection),
                    '--sentiment-selection', str(self.sentiment), '--story-selection', str(self.story_dir),
                    '--predictions', str(self.predictions), '--model-manifest', str(self.manifest),
                    '--output', str(out), '--resamples', '100']
            with patch('sys.argv', argv), redirect_stdout(io.StringIO()):
                score.main()
            outputs.append([(out / f).read_bytes() for f in ('score-report.json', 'score-table.md')])
        self.assertEqual(outputs[0], outputs[1])
        self.assertIn(b'not a finding about the model', outputs[0][1])
        with patch('sys.argv', argv), redirect_stdout(io.StringIO()), self.assertRaises(FileExistsError):
            score.main()

    def test_scoring_never_writes_to_the_prediction_ledger(self):
        before = self.predictions.read_bytes()
        self.report()
        self.assertEqual(self.predictions.read_bytes(), before)


class TestRefusals(Pipeline):
    def test_changed_label_file_is_refused(self):
        self.edit(self.sentiment / 'labels.jsonl', b'"positive"', b'"negative"')
        with self.assertRaisesRegex(ValueError, 'differs from its report'):
            self.report()

    def test_labels_from_another_eligibility_selection_are_refused(self):
        raw = json.loads((self.sentiment / 'selection.json').read_text(encoding='utf-8'))
        raw['eligibility_selection_sha256'] = 'other'
        (self.sentiment / 'selection.json').write_text(json.dumps(raw), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'another snapshot or eligibility'):
            self.report()

    def test_story_selection_for_another_eligible_set_is_refused(self):
        report = json.loads((self.story_dir / 'story-groups.json').read_text(encoding='utf-8'))
        report['input_eligible_sha256'] = 'other'
        (self.story_dir / 'story-groups.json').write_text(json.dumps(report), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'another snapshot or eligible set'):
            self.report()

    def test_wrong_story_anchor_is_refused(self):
        path = self.story_dir / 'story-groups.json'
        report = json.loads(path.read_text(encoding='utf-8'))
        story = next(s for s in report['stories'] if s['kind'] == 'multi')
        story['anchor_version_id'] = story['members'][-1]['version_id']
        path.write_text(json.dumps(report), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'anchor'):
            self.report()

    def test_regrouped_story_file_is_refused(self):
        path = self.story_dir / 'story-groups.json'
        report = json.loads(path.read_text(encoding='utf-8'))
        report['stories'][0]['reason'] = 'Edited after the fact'
        path.write_text(json.dumps(report), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'recorded hash'):
            self.report()

    def test_prediction_with_changed_input_text_is_refused(self):
        with closing(sqlite3.connect(self.predictions)) as conn:
            conn.execute('DROP TRIGGER predictions_no_update')
            conn.execute("UPDATE predictions SET input_text='Something else' WHERE rowid=1")
            conn.commit()
        with self.assertRaisesRegex(ValueError, 'differs from the snapshot title'):
            self.report()

    def test_predictions_made_with_another_model_manifest_are_refused(self):
        raw = json.loads(self.manifest.read_text(encoding='utf-8'))
        raw['note'] = 'edited'
        self.manifest.write_text(json.dumps(raw), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'different model manifest'):
            self.report()

    def test_unreleased_predictions_are_not_scored(self):
        with closing(sqlite3.connect(self.predictions)) as conn:
            conn.execute('DROP TRIGGER prediction_releases_no_delete')
            conn.execute('DELETE FROM prediction_releases')
            conn.commit()
        counts = self.report()['counts']
        self.assertEqual((counts['with_prediction'], counts['scored_all_labels']), (0, 0))


if __name__ == '__main__':
    unittest.main()
