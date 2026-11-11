"""The one-command pilot reproduction must pass only when both builds and the saved copies agree."""
import io
import json
import hashlib
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import reproduce_pilot, score
from tests.test_score_pipeline import Pipeline


class TestReproducePilot(Pipeline):
    def setUp(self):
        super().setUp()
        self.docs = self.root / 'docs'
        self.docs.mkdir()
        report = score._rounded(self.report())
        self.saved = {n: t.encode('utf-8') for n, t in score.report_files(report).items()}
        for name, copy in (('score-report.json', 'copy.json'), ('score-table.md', 'copy.md')):
            (self.docs / copy).write_bytes(self.saved[name])
        self.inputs = self.docs / 'inputs.json'
        self.write_inputs()

    def write_inputs(self, **changes):
        spec = {'inputs': {'snapshot_dir': str(self.snapshot), 'eligibility_selection': str(self.selection),
                           'sentiment_selection': str(self.sentiment), 'story_selection': str(self.story_dir),
                           'predictions': str(self.predictions), 'model_manifest': str(self.manifest)},
                'seed': 3, 'resamples': 100,
                'expected': {name: {'saved_copy': copy, 'sha256': hashlib.sha256(self.saved[name]).hexdigest()}
                             for name, copy in (('score-report.json', 'copy.json'), ('score-table.md', 'copy.md'))}}
        spec.update(changes)
        self.inputs.write_text(json.dumps(spec), encoding='utf-8')

    def test_matching_saved_copies_reproduce(self):
        result = reproduce_pilot.reproduce(self.inputs)
        self.assertTrue(result['reproduced'])
        self.assertTrue(all(f['matches_saved_copy'] and f['matches_recorded_hash'] for f in result['files'].values()))

    def test_changed_saved_copy_fails(self):
        (self.docs / 'copy.md').write_bytes(self.saved['score-table.md'] + b'x')
        result = reproduce_pilot.reproduce(self.inputs)
        self.assertFalse(result['reproduced'])
        self.assertFalse(result['files']['score-table.md']['matches_saved_copy'])

    def test_wrong_recorded_hash_fails(self):
        spec = json.loads(self.inputs.read_text())
        spec['expected']['score-report.json']['sha256'] = '0' * 64
        self.inputs.write_text(json.dumps(spec))
        result = reproduce_pilot.reproduce(self.inputs)
        self.assertFalse(result['reproduced'])
        self.assertFalse(result['files']['score-report.json']['matches_recorded_hash'])

    def test_different_seed_does_not_reproduce_the_saved_table(self):
        spec = json.loads(self.inputs.read_text())
        spec['seed'] = 4
        self.inputs.write_text(json.dumps(spec))
        self.assertFalse(reproduce_pilot.reproduce(self.inputs)['reproduced'])

    def test_unstable_builds_fail(self):
        calls = []

        def build(*args, **kwargs):
            calls.append(1)
            report = score.build_report(*args, **kwargs)
            report['counts']['labeled'] += len(calls) - 1
            return report
        result = reproduce_pilot.reproduce(self.inputs, build=build)
        self.assertFalse(result['reproduced'])
        self.assertFalse(result['files']['score-report.json']['second_build_identical'] and
                         result['files']['score-table.md']['second_build_identical'])

    def test_changed_input_is_refused_not_reported_as_a_mismatch(self):
        path = self.story_dir / 'story-groups.json'
        report = json.loads(path.read_text(encoding='utf-8'))
        report['stories'][0]['reason'] = 'Edited after the fact'
        path.write_text(json.dumps(report), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'recorded hash'):
            reproduce_pilot.reproduce(self.inputs)

    def test_main_prints_result_and_returns_exit_status(self):
        with patch('sys.argv', ['x', '--inputs', str(self.inputs)]), redirect_stdout(io.StringIO()) as out:
            self.assertEqual(reproduce_pilot.main(), 0)
        self.assertTrue(json.loads(out.getvalue())['reproduced'])
        (self.docs / 'copy.json').write_bytes(b'{}')
        with patch('sys.argv', ['x', '--inputs', str(self.inputs)]), redirect_stdout(io.StringIO()):
            self.assertEqual(reproduce_pilot.main(), 1)

    def test_the_repository_inputs_file_points_at_the_saved_pilot_files(self):
        spec = json.loads(Path('docs/pilot-inputs-20261111.json').read_text(encoding='utf-8'))
        for expected in spec['expected'].values():
            saved = Path('docs', expected['saved_copy']).read_bytes()
            self.assertEqual(hashlib.sha256(saved).hexdigest(), expected['sha256'])


if __name__ == '__main__':
    unittest.main()
