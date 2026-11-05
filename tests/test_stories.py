"""Story groups are complete human judgments, append-only, and blind to labels and predictions."""
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

from research import stories
from research.observations import canonical_json, make_observation
from scripts import review_stories

T = datetime(2026, 10, 1, tzinfo=timezone.utc)
AUDIT = {'paths': None}


def _audit(event, args):
    if AUDIT['paths'] is not None and event in ('open', 'sqlite3.connect') and args and isinstance(args[0], (str, os.PathLike)):
        AUDIT['paths'].append(str(args[0]))


sys.addaudithook(_audit)


def later_than_now():
    """Waits out coarse clocks, so anything released so far is strictly before the result."""
    start = datetime.now(timezone.utc)
    while datetime.now(timezone.utc) <= start:
        pass
    return datetime.now(timezone.utc)


def row(title, article_id, seconds):
    observed = T + timedelta(seconds=seconds)
    obs = make_observation(source='RSS', article_id=article_id, title=title, url=f'https://www.example.test/{article_id}',
                           source_item={'title': title}, title_provenance='source_feed', observed_at=observed,
                           source_time_kind='unknown')
    return {'version_id': obs.id, 'content_hash': obs.content_hash, 'article_id': article_id, 'title': title,
            'source': 'RSS', 'url': obs.url, 'observed_at': observed.isoformat(),
            'available_at': (observed + timedelta(seconds=1)).isoformat(), 'snapshot': obs.model_dump_json()}


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name).resolve()
        self.rows = [row('Central bank holds rates steady', 'a', 30), row('Central bank holds rates steady today', 'b', 10),
                     row('Acme opens a new office', 'c', 20), row('Widget maker names new chief', 'd', 40)]
        self.snapshot = self.root / 'research' / 'snapshot'
        self.selection = self.root / 'research' / 'selection'
        self.snapshot.mkdir(parents=True)
        self.selection.mkdir()
        data = b''.join(json.dumps(r).encode() + b'\n' for r in self.rows)
        (self.snapshot / 'dataset.jsonl').write_bytes(data)
        self.dataset_sha256 = hashlib.sha256(data).hexdigest()
        (self.snapshot / 'manifest.json').write_text(json.dumps({
            'rows': len(self.rows), 'dataset_sha256': self.dataset_sha256,
            'cutoff': (T + timedelta(days=1)).isoformat()}), encoding='utf-8')
        eligible = ''.join(canonical_json(r) + '\n' for r in self.rows)
        (self.selection / 'eligible.jsonl').write_text(eligible, encoding='utf-8', newline='\n')
        self.eligible_sha256 = hashlib.sha256(eligible.encode()).hexdigest()
        self.report = {'eligible_rows_sha256': self.eligible_sha256, 'input_dataset_sha256': self.dataset_sha256,
                       'counts': {'included': len(self.rows)}}
        self.write_report()
        self.guide = self.root / 'guide.md'
        self.guide.write_text('guide', encoding='utf-8')
        self.ledger = self.root / 'research' / 'story-groups.sqlite3'
        self.worksheet = self.root / 'research' / 'worksheet'
        self.cohort = stories.load_cohort(self.snapshot, self.selection)[0]
        self.index = stories.render_worksheet(self.cohort)[1]

    def tearDown(self):
        self.tmp.cleanup()

    def write_report(self):
        (self.selection / 'selection.json').write_text(json.dumps(self.report), encoding='utf-8')

    def append(self, answers='story: 1 3 | Same rate decision\nalone: 2 4', **kwargs):
        return stories.append_review(self.ledger, stories.parse_answers(answers, self.index),
                                     dataset_sha256=self.dataset_sha256, eligible_sha256=self.eligible_sha256,
                                     guide_sha256='g', reviewer='human', tool_revision='tool', **kwargs)


class TestCohort(Case):
    def test_cohort_is_ordered_by_arrival(self):
        self.assertEqual([r['article_id'] for r in self.cohort], ['b', 'c', 'a', 'd'])

    def test_tampered_eligible_rows_are_refused(self):
        path = self.selection / 'eligible.jsonl'
        path.write_bytes(path.read_bytes().replace(b'Acme', b'Beta'))
        with self.assertRaisesRegex(ValueError, 'do not match'):
            stories.load_cohort(self.snapshot, self.selection)

    def test_selection_from_another_snapshot_is_refused(self):
        self.report['input_dataset_sha256'] = 'other'
        self.write_report()
        with self.assertRaisesRegex(ValueError, 'different snapshot'):
            stories.load_cohort(self.snapshot, self.selection)

    def test_row_that_differs_from_the_snapshot_is_refused(self):
        changed = dict(self.rows[0], title='Another title')
        text = canonical_json(changed) + '\n'
        (self.selection / 'eligible.jsonl').write_text(text, encoding='utf-8', newline='\n')
        self.report.update(eligible_rows_sha256=hashlib.sha256(text.encode()).hexdigest(), counts={'included': 1})
        self.write_report()
        with self.assertRaisesRegex(ValueError, 'exact snapshot row'):
            stories.load_cohort(self.snapshot, self.selection)


class TestWorksheet(Case):
    def test_shows_titles_and_hints_but_never_assigns_groups(self):
        text, index = stories.render_worksheet(self.cohort)
        self.assertIn('1. Central bank holds rates steady today', text)
        self.assertIn('compare: 3 (shared words)', text)
        self.assertEqual(set(index), {'1', '2', '3', '4'})
        self.assertNotIn('story:', text)

    def test_same_link_is_named_in_the_hint(self):
        twin = row('Totally different words here', 'a', 60)
        text, _ = stories.render_worksheet([self.rows[0], twin])
        self.assertIn('compare: 2 (same link)', text)

    def test_prepare_generates_no_ledger_and_refuses_overwrite(self):
        argv = ['x', 'prepare', '--snapshot-dir', str(self.snapshot), '--selection-dir', str(self.selection),
                '--ledger', str(self.ledger), '--worksheet-dir', str(self.worksheet)]
        with unittest.mock.patch('sys.argv', argv), unittest.mock.patch('builtins.print'):
            review_stories.main()
            with self.assertRaises(FileExistsError):
                review_stories.main()
        self.assertFalse(self.ledger.exists())


class TestAnswers(Case):
    def test_complete_answers_parse_into_stories_and_uncertain(self):
        grouping = stories.parse_answers('story: 1 3 | Same decision  # note\n\nuncertain: 2 | Unclear\nalone: 4', self.index)
        self.assertEqual(sorted(s['kind'] for s in grouping['stories']), ['alone', 'multi'])
        self.assertEqual(grouping['uncertain'][0]['members'], [self.index['2']])

    def test_every_headline_must_be_accounted_for_exactly_once(self):
        with self.assertRaisesRegex(ValueError, r"missing \['4'\]"):
            stories.parse_answers('story: 1 3 | Same\nalone: 2', self.index)
        with self.assertRaisesRegex(ValueError, r"repeated \['2'\]"):
            stories.parse_answers('story: 1 3 | Same\nalone: 2 2 4', self.index)

    def test_bad_lines_are_refused(self):
        for answers, pattern in (('story: 1 3\nalone: 2 4', 'needs a reason'), ('uncertain: 1 3\nalone: 2 4', 'needs a reason'),
                                 ('story: 1 | Alone\nalone: 2 3 4', 'at least two'), ('alone: 1 2 3 9', 'unknown'),
                                 ('positive: 1 2 3 4', 'not story')):
            with self.assertRaisesRegex(ValueError, pattern):
                stories.parse_answers(answers, self.index)


class TestLedger(Case):
    def test_update_and_delete_abort(self):
        self.append()
        with closing(sqlite3.connect(self.ledger)) as conn:
            for sql in ("UPDATE story_reviews SET reviewer='x'", 'DELETE FROM story_reviews',
                        "UPDATE story_releases SET released_at='x'", 'DELETE FROM story_releases'):
                with self.assertRaisesRegex(sqlite3.IntegrityError, 'append-only'):
                    conn.execute(sql)

    def test_revision_must_name_the_latest_and_keeps_the_old_review(self):
        first = self.append()
        with self.assertRaisesRegex(ValueError, 'latest review'):
            self.append()
        second = self.append('alone: 1 2 3 4', supersedes=first)
        with self.assertRaisesRegex(ValueError, 'latest review'):
            self.append('alone: 1 2 3 4', supersedes=first)
        with closing(sqlite3.connect(self.ledger)) as conn:
            self.assertEqual([r[0] for r in conn.execute('SELECT review_id FROM story_reviews ORDER BY sequence')], [first, second])

    def test_release_is_sampled_after_commit_and_recovers_later(self):
        with unittest.mock.patch.object(stories, '_seal', side_effect=RuntimeError):
            with self.assertRaises(RuntimeError):
                self.append()
        with self.assertRaisesRegex(ValueError, 'No story review'):
            stories.select_as_of(self.ledger, self.cohort, self.eligible_sha256, datetime.now(timezone.utc) + timedelta(days=1))
        with self.assertRaisesRegex(ValueError, 'recovered'):
            self.append()
        self.assertEqual(len(stories.recover_pending(self.ledger)), 1)
        with closing(sqlite3.connect(self.ledger)) as conn:
            recorded, released = conn.execute('SELECT recorded_at, released_at FROM story_reviews JOIN story_releases USING (review_id)').fetchone()
        self.assertGreater(released, recorded)

    def test_selection_uses_the_review_released_before_the_cutoff(self):
        first = self.append()
        cutoff = datetime.now(timezone.utc) + timedelta(microseconds=1)
        while datetime.now(timezone.utc) <= cutoff:
            pass
        self.append('alone: 1 2 3 4', supersedes=first)
        chosen = stories.select_as_of(self.ledger, self.cohort, self.eligible_sha256, cutoff)
        self.assertEqual(chosen['review_id'], first)

    def test_anchor_is_the_first_available_member_not_the_first_listed(self):
        self.append()
        chosen = stories.select_as_of(self.ledger, self.cohort, self.eligible_sha256, datetime.now(timezone.utc) + timedelta(days=1))
        story = next(s for s in chosen['stories'] if s['kind'] == 'multi')
        self.assertEqual(story['anchor_version_id'], self.cohort[0]['version_id'])

    def test_uncertain_versions_never_become_stories(self):
        self.append('uncertain: 1 3 | Unclear\nalone: 2 4')
        chosen = stories.select_as_of(self.ledger, self.cohort, self.eligible_sha256, datetime.now(timezone.utc) + timedelta(days=1))
        members = {m['version_id'] for s in chosen['stories'] for m in s['members']}
        self.assertNotIn(self.index['1']['version_id'], members)
        self.assertEqual(len(chosen['uncertain'][0]['members']), 2)


class TestCommandLine(Case):
    def run_cli(self, *extra, env=None):
        environment = {'COLLECTION_CODE_REVISION': 'abc', 'COLLECTION_WORKTREE_STATE': 'clean', **(env or {})}
        argv = ['x', extra[0], '--snapshot-dir', str(self.snapshot), '--selection-dir', str(self.selection),
                '--guide-file', str(self.guide), '--ledger', str(self.ledger), *extra[1:]]
        with unittest.mock.patch('sys.argv', argv), unittest.mock.patch.dict(os.environ, environment), \
             unittest.mock.patch('builtins.print'), unittest.mock.patch('sys.stderr'):
            review_stories.main()

    def record(self, **kwargs):
        answers = self.root / 'answers.txt'
        answers.write_text('story: 1 3 | Same rate decision\nalone: 2 4\n', encoding='utf-8')
        self.run_cli('record', '--worksheet-dir', str(self.worksheet), '--answers', str(answers), '--reviewer', 'human', **kwargs)

    def test_prepare_record_select_round_trip(self):
        self.run_cli('prepare', '--worksheet-dir', str(self.worksheet))
        self.record()
        output = self.root / 'selected'
        self.run_cli('select', '--cutoff', later_than_now().isoformat(), '--output', str(output))
        report = json.loads((output / 'story-groups.json').read_text(encoding='utf-8'))
        self.assertEqual(report['counts'], {'multi_headline_stories': 1, 'reviewed_alone': 2, 'versions_in_stories': 4, 'uncertain_versions': 0})
        self.assertFalse(report['paper_ready'])
        self.assertEqual(report['guide_sha256'], hashlib.sha256(b'guide').hexdigest())

    def test_recording_needs_a_clean_revision(self):
        self.run_cli('prepare', '--worksheet-dir', str(self.worksheet))
        answers = self.root / 'answers.txt'
        answers.write_text('alone: 1 2 3 4\n', encoding='utf-8')
        with self.assertRaises(SystemExit):
            self.run_cli('record', '--worksheet-dir', str(self.worksheet), '--answers', str(answers), '--reviewer', 'human',
                         env={'COLLECTION_WORKTREE_STATE': 'dirty'})
        self.assertFalse(self.ledger.exists())

    def test_future_cutoff_is_refused(self):
        with self.assertRaisesRegex(ValueError, 'future'):
            self.run_cli('select', '--cutoff', (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(), '--output', str(self.root / 'x'))


class TestBlindness(Case):
    def test_labels_and_predictions_are_never_opened(self):
        research = self.root / 'research'
        for name in ('annotations.sqlite3', 'predictions.sqlite3'):
            with closing(sqlite3.connect(research / name)) as conn:
                conn.execute('CREATE TABLE evidence (x)')
                conn.commit()
        answers = self.root / 'answers.txt'
        answers.write_text('alone: 1 2 3 4\n', encoding='utf-8')
        common = ['--snapshot-dir', str(self.snapshot), '--selection-dir', str(self.selection), '--guide-file', str(self.guide), '--ledger', str(self.ledger)]
        calls = [['prepare', '--worksheet-dir', str(self.worksheet), *common],
                 ['record', '--worksheet-dir', str(self.worksheet), '--answers', str(answers), '--reviewer', 'human', *common],
                 ['select', '--cutoff', None, '--output', str(self.root / 'selected'), *common]]
        AUDIT['paths'] = []
        try:
            for call in calls:
                argv = ['x', *[later_than_now().isoformat() if part is None else part for part in call]]
                with unittest.mock.patch('sys.argv', argv), unittest.mock.patch('builtins.print'), unittest.mock.patch.dict(
                        os.environ, {'COLLECTION_CODE_REVISION': 'abc', 'COLLECTION_WORKTREE_STATE': 'clean'}):
                    review_stories.main()
        finally:
            seen, AUDIT['paths'] = AUDIT['paths'], None
        opened = {Path(path).resolve().name for path in seen if Path(path).resolve().is_relative_to(self.root)}
        self.assertFalse(opened & {'annotations.sqlite3', 'predictions.sqlite3', 'eligibility.sqlite3', 'decisions.jsonl'})
        self.assertIn('story-groups.sqlite3', opened)

    def test_source_imports_no_label_or_prediction_code(self):
        for module in (stories, review_stories):
            tree = ast.parse(Path(module.__file__).read_text(encoding='utf-8'))
            imported = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
            self.assertFalse([name for name in imported if name and ('annotations' in name or 'predict' in name)])


if __name__ == '__main__':
    unittest.main()
