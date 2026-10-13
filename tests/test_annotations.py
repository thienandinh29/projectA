"""Regression checks for delayed human labels, revisions and interrupted release."""
import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from research.annotations import (append_annotations, read_annotations, recover_pending,
                                  select_as_of, load_selection)
from research.observations import canonical_json
from research.eligibility import append_reviews, read_decisions, select_as_of as eligibility_as_of
from research.protocol import utc
from tests.test_eligibility import T, POLICY, row, review


GUIDE = (hashlib.sha256(b'pilot guide').hexdigest(), 'pilot guide')


def annotation(r, **edits):
    return {'version_id': r['version_id'], 'content_hash': r['content_hash'],
        'subject': 'Company A', 'status': 'labeled', 'sentiment': 'positive',
        'reason': 'Reported higher profit for this company', 'mixed': False,
        'entity_dependent': False, 'forecast': False, 'quotation': False,
        'question': False, 'assisted': False, 'assistance_note': '',
        'supersedes_decision_id': None, **edits}


class TestAnnotations(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.ledger = self.root/'annotations.sqlite3'
        self.rows = [row()]

    def tearDown(self):
        self.tmp.cleanup()

    def save(self, items, at=T+timedelta(hours=1), annotator='human-a', guide=GUIDE):
        with patch('research.annotations.datetime') as clock:
            clock.now.side_effect = [at, at+timedelta(seconds=1)]
            return append_annotations(self.ledger, self.rows, 'snapshot', 'selection', guide, annotator, items)

    def history(self, guide=GUIDE):
        return read_annotations(self.ledger, guide[0])

    def selected(self, at, annotator='human-a'):
        return select_as_of(self.rows, self.history(), at, annotator)

    def test_post_content_commit_time_and_strict_cutoff(self):
        self.save([annotation(self.rows[0])])
        stored = self.history()[0]
        self.assertGreater(stored['label_available_at'], stored['recorded_at'])
        at = T+timedelta(hours=1, seconds=1)
        self.assertEqual(self.selected(at)[0]['status'], 'pending')
        self.assertEqual(self.selected(at+timedelta(microseconds=1))[0]['sentiment'], 'positive')
        self.assertEqual(self.selected(T+timedelta(seconds=1))[0]['status'], 'data_not_available_before_cutoff')

    def test_release_reads_committed_content_from_another_connection(self):
        import research.annotations as module
        original = module._seal
        def verify(conn, ids, kind):
            with closing(sqlite3.connect(self.ledger)) as other:
                self.assertEqual(other.execute('SELECT COUNT(*) FROM annotation_decisions').fetchone()[0], 1)
                self.assertEqual(other.execute('SELECT COUNT(*) FROM annotation_releases').fetchone()[0], 0)
            return original(conn, ids, kind)
        with patch('research.annotations._seal', side_effect=verify):
            self.save([annotation(self.rows[0])])

    def test_revision_preserves_old_cutoff_and_unresolved_is_not_neutral(self):
        first = self.save([annotation(self.rows[0])])[0]
        self.save([annotation(self.rows[0], status='unresolved', sentiment=None, mixed=True,
                  reason='Two competing financial directions', supersedes_decision_id=first['decision_id'])],
                  T+timedelta(hours=2))
        self.assertEqual(len(self.history()), 2)
        self.assertEqual(self.selected(T+timedelta(minutes=90))[0]['sentiment'], 'positive')
        current = self.selected(T+timedelta(hours=3))[0]
        self.assertEqual(current['status'], 'unresolved')
        self.assertIsNone(current['sentiment'])

    def test_crash_after_content_commit_is_unsealed_then_recovered_at_present_time(self):
        with patch('research.annotations._seal', side_effect=OSError('crash')):
            with self.assertRaisesRegex(RuntimeError, 'committed but not released'):
                self.save([annotation(self.rows[0])])
        original = self.history()[0]
        self.assertIsNone(original['label_available_at'])
        self.assertEqual(self.selected(T+timedelta(hours=3))[0]['status'], 'pending')
        with self.assertRaisesRegex(ValueError, 'unsealed'):
            self.save([annotation(self.rows[0], supersedes_decision_id=original['decision_id'])], T+timedelta(hours=2))
        with patch('research.annotations.datetime') as clock:
            clock.now.return_value = T+timedelta(hours=4)
            recovered = recover_pending(self.ledger)
        self.assertEqual(recovered[0]['release_kind'], 'recovery')
        self.assertEqual(self.history()[0]['annotation_json'], original['annotation_json'])
        self.assertEqual(self.selected(T+timedelta(hours=3))[0]['status'], 'pending')
        self.assertEqual(self.selected(T+timedelta(hours=5))[0]['status'], 'labeled')
        self.assertEqual(recover_pending(self.ledger), [])

    def test_stale_revision_and_failed_batch_roll_back_all_new_content(self):
        self.rows.append(row('Another headline', 'other'))
        first = self.save([annotation(self.rows[1])])[0]
        with self.assertRaisesRegex(ValueError, 'latest decision'):
            self.save([annotation(self.rows[0]), annotation(self.rows[1])], T+timedelta(hours=2))
        self.assertEqual(len(self.history()), 1)
        with self.assertRaisesRegex(ValueError, 'clock did not advance'):
            self.save([annotation(self.rows[1], supersedes_decision_id=first['decision_id'])])

    def test_unsealed_revision_preserves_released_predecessor_until_recovery(self):
        first = self.save([annotation(self.rows[0])])[0]
        with patch('research.annotations._seal', side_effect=OSError('crash')):
            with self.assertRaises(RuntimeError):
                self.save([annotation(self.rows[0], status='unresolved', sentiment=None,
                    mixed=True, reason='Competing directions', supersedes_decision_id=first['decision_id'])],
                    T+timedelta(hours=2))
        self.assertEqual(self.selected(T+timedelta(hours=3))[0]['sentiment'], 'positive')
        with patch('research.annotations.datetime') as clock:
            clock.now.return_value = T+timedelta(hours=4)
            recover_pending(self.ledger)
        self.assertEqual(self.selected(T+timedelta(hours=3))[0]['sentiment'], 'positive')
        self.assertEqual(self.selected(T+timedelta(hours=5))[0]['status'], 'unresolved')

    def test_wrong_hash_backdating_blank_subject_and_inconsistent_status_rejected(self):
        for edits in [{'content_hash': 'wrong'}, {'label_available_at': T.isoformat()},
                      {'subject': ' '}, {'reason': ' '}, {'status': 'unresolved'},
                      {'sentiment': None}, {'assisted': True}, {'assistance_note': 'help'},
                      {'mixed': 'false'}, {'mixed': True, 'sentiment': 'neutral'}]:
            with self.subTest(edits=edits), self.assertRaises(ValueError):
                self.save([annotation(self.rows[0], **edits)])
        with self.assertRaisesRegex(ValueError, 'before it is available'):
            self.save([annotation(self.rows[0])], T)
        self.assertEqual(self.history(), [])

    def test_two_annotators_do_not_overwrite_or_silently_adjudicate(self):
        self.save([annotation(self.rows[0])])
        self.save([annotation(self.rows[0], sentiment='negative', reason='Different human interpretation')],
                  T+timedelta(hours=2), 'human-b')
        self.assertEqual(self.selected(T+timedelta(hours=3))[0]['sentiment'], 'positive')
        self.assertEqual(self.selected(T+timedelta(hours=3), 'human-b')[0]['sentiment'], 'negative')
        with self.assertRaisesRegex(ValueError, 'explicit annotator'):
            self.selected(T+timedelta(hours=3), '')

    def test_new_content_and_new_guide_do_not_inherit_labels(self):
        self.save([annotation(self.rows[0])])
        self.rows.append(row('Company reports lower profit'))
        self.assertEqual([r['status'] for r in self.selected(T+timedelta(hours=2))], ['labeled', 'pending'])
        self.assertEqual(read_annotations(self.ledger, hashlib.sha256(b'new guide').hexdigest()), [])

    def test_append_only_tables_and_clock_regression_on_recovery(self):
        self.save([annotation(self.rows[0])])
        with closing(sqlite3.connect(self.ledger)) as conn:
            for table in ('annotation_guides', 'annotation_decisions', 'annotation_releases'):
                for action in ('DELETE FROM', 'UPDATE'):
                    sql = f'{action} {table}' if action == 'DELETE FROM' else f'UPDATE {table} SET ' + {
                        'annotation_guides': "text='changed'", 'annotation_decisions': "annotator='changed'",
                        'annotation_releases': "release_kind='recovery'"}[table]
                    with self.assertRaises(sqlite3.IntegrityError):
                        conn.execute(sql)
        self.rows.append(row('Other company', 'other'))
        with patch('research.annotations._seal', side_effect=OSError('crash')):
            with self.assertRaises(RuntimeError):
                self.save([annotation(self.rows[1])], T+timedelta(hours=2))
        with patch('research.annotations.datetime') as clock:
            clock.now.return_value = T
            with self.assertRaisesRegex(ValueError, 'Release clock precedes'):
                recover_pending(self.ledger)
        self.assertIsNone(self.history()[1]['label_available_at'])

    def freeze_eligible(self):
        snapshot = self.root/'snapshot'
        selection = self.root/'selection'
        snapshot.mkdir()
        selection.mkdir()
        data = (canonical_json(self.rows[0])+'\n').encode()
        dataset_hash = hashlib.sha256(data).hexdigest()
        (snapshot/'dataset.jsonl').write_bytes(data)
        manifest = json.dumps({'rows': 1, 'dataset_sha256': dataset_hash,
                              'cutoff': (T+timedelta(hours=1)).isoformat()}).encode()
        (snapshot/'manifest.json').write_bytes(manifest)
        with patch('research.eligibility.datetime') as clock:
            clock.now.return_value = T+timedelta(hours=1)
            append_reviews(self.root/'eligibility.sqlite3', self.rows, dataset_hash, POLICY, 'human', [review(self.rows[0])])
        decisions = read_decisions(self.root/'eligibility.sqlite3', POLICY[0])
        history = (canonical_json(decisions[0])+'\n').encode()
        report = {'kind': 'development_eligibility_selection_v1', 'input_dataset_sha256': dataset_hash,
            'input_manifest_sha256': hashlib.sha256(manifest).hexdigest(),
            'eligible_rows_sha256': dataset_hash, 'decision_history_sha256': hashlib.sha256(history).hexdigest(),
            'policy_sha256': POLICY[0], 'cutoff': (T+timedelta(hours=2)).isoformat(),
            'versions': eligibility_as_of(self.rows, decisions, T+timedelta(hours=2))}
        (selection/'selection.json').write_text(json.dumps(report))
        (selection/'eligible.jsonl').write_bytes(data)
        (selection/'decisions.jsonl').write_bytes(history)
        (selection/'policy.md').write_text(POLICY[1])
        return snapshot, selection

    def test_saved_eligibility_selection_hashes_and_reproduction(self):
        snapshot, selection = self.freeze_eligible()
        self.assertEqual(load_selection(snapshot, selection)[0], self.rows)
        self.assertEqual(load_selection(snapshot, selection, T+timedelta(hours=1))[0], [])
        data = (selection/'eligible.jsonl').read_bytes()
        (selection/'eligible.jsonl').write_bytes(data+b' ')
        with self.assertRaisesRegex(ValueError, 'hash mismatch'):
            load_selection(snapshot, selection)

    def cli(self, action, *extra):
        from scripts.review_sentiment import main
        args = ['review_sentiment', action, '--snapshot-dir', str(self.snapshot),
                '--eligibility-selection', str(self.selection), '--guide-file', str(self.guide),
                '--ledger', str(self.ledger), *extra]
        with patch('sys.argv', args), redirect_stdout(io.StringIO()):
            main()

    def prepare_cli(self):
        self.snapshot, self.selection = self.freeze_eligible()
        self.guide = self.root/'guide.md'
        self.guide.write_text(GUIDE[1])
        self.template = self.root/'template.json'
        self.cli('prepare', '--limit', '1', '--output', str(self.template))

    def test_cli_prepares_blank_only_and_refuses_changed_input_or_guide(self):
        self.prepare_cli()
        template = json.loads(self.template.read_bytes())
        self.assertIsNone(template['annotations'][0]['status'])
        self.assertIsNone(template['annotations'][0]['sentiment'])
        with self.assertRaises(ValueError):
            self.cli('record', '--reviews', str(self.template), '--annotator', 'human-a')
        self.assertFalse(self.ledger.exists())
        template['annotations'] = [annotation(self.rows[0])]
        template['inputs'][0]['title'] = 'Changed display'
        self.template.write_text(json.dumps(template))
        with self.assertRaisesRegex(ValueError, 'Displayed annotation input'):
            self.cli('record', '--reviews', str(self.template), '--annotator', 'human-a')
        self.guide.write_text('Another guide')
        with self.assertRaisesRegex(ValueError, 'different snapshot, selection or guide'):
            self.cli('record', '--reviews', str(self.template), '--annotator', 'human-a')

    def test_cli_record_select_and_unresolved_revision_export(self):
        self.prepare_cli()
        template = json.loads(self.template.read_bytes())
        template['annotations'] = [annotation(self.rows[0])]
        self.template.write_text(json.dumps(template))
        with patch('research.annotations.datetime') as clock:
            clock.now.side_effect = [T+timedelta(hours=3), T+timedelta(hours=3, seconds=1)]
            self.cli('record', '--reviews', str(self.template), '--annotator', 'human-a')
        first = self.history()[0]
        out = self.root/'selected'
        self.cli('select', '--annotator', 'human-a', '--cutoff', (T+timedelta(hours=4)).isoformat(), '--output', str(out))
        label = json.loads((out/'labels.jsonl').read_text())
        self.assertEqual(label['sentiment'], 'positive')
        self.assertEqual(utc(label['label_available_at']), utc(first['label_available_at']))
        template['annotations'] = [annotation(self.rows[0], status='unresolved', sentiment=None,
            mixed=True, reason='Competing directions', supersedes_decision_id=first['decision_id'])]
        self.template.write_text(json.dumps(template))
        with patch('research.annotations.datetime') as clock:
            clock.now.side_effect = [T+timedelta(hours=5), T+timedelta(hours=5, seconds=1)]
            self.cli('record', '--reviews', str(self.template), '--annotator', 'human-a')
        current = self.root/'unresolved'
        self.cli('select', '--annotator', 'human-a', '--cutoff', (T+timedelta(hours=6)).isoformat(), '--output', str(current))
        self.assertEqual((current/'labels.jsonl').read_bytes(), b'')
        self.assertEqual(json.loads((current/'selection.json').read_bytes())['counts'], {'unresolved': 1})
        self.assertEqual(len((current/'decisions.jsonl').read_text().splitlines()), 2)


if __name__ == '__main__':
    unittest.main()
