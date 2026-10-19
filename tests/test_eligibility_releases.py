"""Post-commit eligibility timing and explicit legacy-history compatibility."""
import hashlib
import io
import json
import sqlite3
import unittest
from contextlib import closing, redirect_stdout
from datetime import timedelta
from unittest.mock import patch

from research.eligibility import (append_reviews, confirm_legacy, initialize, read_decisions,
                                  recover_pending, select_as_of)
from research.observations import canonical_json
from tests import test_eligibility as fixtures
from tests.test_eligibility import T, POLICY, row, review


class TestEligibilityReleases(unittest.TestCase):
    setUp = fixtures.TestEligibility.setUp
    tearDown = fixtures.TestEligibility.tearDown
    save = fixtures.TestEligibility.save
    history = fixtures.TestEligibility.history

    def selected(self, at):
        return select_as_of(self.rows, self.history(), at)

    def raw_decisions(self):
        with closing(sqlite3.connect(self.ledger)) as conn:
            return conn.execute('SELECT * FROM eligibility_decisions ORDER BY sequence').fetchall()

    def legacy(self, r=None, decision_id='old', at=T+timedelta(hours=1), **edits):
        r = r or self.rows[0]
        with closing(sqlite3.connect(self.ledger)) as conn:
            initialize(conn)
            conn.execute('INSERT OR IGNORE INTO eligibility_policies VALUES (?,?)', POLICY)
            conn.execute('''INSERT INTO eligibility_decisions
                (decision_id,version_id,content_hash,dataset_sha256,policy_sha256,reviewer,
                 decision_available_at,review_json) VALUES (?,?,?,?,?,?,?,?)''',
                (decision_id, r['version_id'], r['content_hash'], 'snapshot', POLICY[0], 'human-fixture',
                 at.isoformat(), canonical_json(review(r, **edits))))
            conn.commit()
        return decision_id

    def test_release_sample_follows_content_commit_and_cutoff_is_strict(self):
        import research.eligibility as module
        original = module._seal
        def inspect(conn, ids, recovery=False):
            self.assertEqual(conn.execute('PRAGMA synchronous').fetchone()[0], 2)
            with closing(sqlite3.connect(self.ledger)) as other:
                self.assertEqual(other.execute('SELECT COUNT(*) FROM eligibility_decisions').fetchone()[0], 1)
                self.assertEqual(other.execute('SELECT COUNT(*) FROM eligibility_release_requirements').fetchone()[0], 1)
                self.assertEqual(other.execute('SELECT COUNT(*) FROM eligibility_releases').fetchone()[0], 0)
            return original(conn, ids, recovery)
        with patch('research.eligibility._seal', side_effect=inspect):
            release = self.save([review(self.rows[0])])[0]
        h = self.history()[0]
        self.assertEqual(h['recorded_at'], (T+timedelta(hours=1)).isoformat())
        self.assertEqual(h['decision_available_at'], release['decision_available_at'])
        self.assertEqual(self.selected(T+timedelta(hours=1, milliseconds=500))[0]['status'], 'pending')
        self.assertEqual(self.selected(T+timedelta(hours=1, seconds=1))[0]['status'], 'pending')
        self.assertEqual(self.selected(T+timedelta(hours=1, seconds=1, microseconds=1))[0]['status'], 'included')

    def test_crash_then_present_time_recovery_does_not_resubmit_or_backdate(self):
        with patch('research.eligibility._seal', side_effect=OSError('crash')):
            with self.assertRaisesRegex(RuntimeError, 'Run recover; do not resubmit'):
                self.save([review(self.rows[0])])
        original = self.raw_decisions()
        h = self.history()[0]
        self.assertIsNone(h['decision_available_at'])
        self.assertEqual(self.selected(T+timedelta(hours=3))[0]['status'], 'pending')
        with self.assertRaisesRegex(ValueError, 'unsealed'):
            self.save([review(self.rows[0], supersedes_decision_id=h['decision_id'])], T+timedelta(hours=2))
        with patch('research.eligibility.datetime') as clock:
            clock.now.return_value = T+timedelta(hours=4)
            released = recover_pending(self.ledger)
        self.assertEqual(released[0]['release_kind'], 'recovery')
        self.assertEqual(self.raw_decisions(), original)
        self.assertEqual(self.selected(T+timedelta(hours=3))[0]['status'], 'pending')
        self.assertEqual(self.selected(T+timedelta(hours=5))[0]['status'], 'included')
        self.assertEqual(recover_pending(self.ledger), [])

    def test_unsealed_revision_keeps_released_predecessor(self):
        first = self.save([review(self.rows[0])])[0]
        with patch('research.eligibility._seal', side_effect=OSError('crash')):
            with self.assertRaises(RuntimeError):
                self.save([review(self.rows[0], financially_relevant='no',
                    supersedes_decision_id=first['decision_id'])], T+timedelta(hours=2))
        self.assertEqual(self.selected(T+timedelta(hours=3))[0]['status'], 'included')
        with patch('research.eligibility.datetime') as clock:
            clock.now.return_value = T+timedelta(hours=4)
            recover_pending(self.ledger)
        self.assertEqual(self.selected(T+timedelta(hours=3))[0]['status'], 'included')
        self.assertEqual(self.selected(T+timedelta(hours=5))[0]['status'], 'excluded')

    def test_seal_clock_regression_leaves_inspectable_unreleased_content(self):
        with patch('research.eligibility.datetime') as clock:
            clock.now.side_effect = [T+timedelta(hours=1), T+timedelta(minutes=30)]
            with self.assertRaisesRegex(RuntimeError, 'not released'):
                append_reviews(self.ledger, self.rows, 'snapshot', POLICY, 'human', [review(self.rows[0])])
        with patch('research.eligibility.datetime') as clock:
            clock.now.return_value = T+timedelta(minutes=40)
            with self.assertRaisesRegex(ValueError, 'precedes recorded'):
                recover_pending(self.ledger)
        self.assertIsNone(self.history()[0]['decision_available_at'])

    def test_release_tables_are_immutable(self):
        self.save([review(self.rows[0])])
        with closing(sqlite3.connect(self.ledger)) as conn:
            for sql in ('DELETE FROM eligibility_releases', 'DELETE FROM eligibility_release_requirements',
                        "UPDATE eligibility_releases SET released_at='changed'",
                        "UPDATE eligibility_release_requirements SET origin='legacy'"):
                with self.subTest(sql=sql), self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(sql)

    def test_legacy_read_is_readonly_and_replay_requires_explicit_contract(self):
        self.legacy()
        # Simulate a pre-v2 ledger, including absence of new release tables.
        with closing(sqlite3.connect(self.ledger)) as conn:
            conn.execute('DROP TABLE eligibility_releases')
            conn.execute('DROP TABLE eligibility_release_requirements')
            conn.commit()
        digest = hashlib.sha256(self.ledger.read_bytes()).hexdigest()
        h = self.history()
        self.assertEqual(hashlib.sha256(self.ledger.read_bytes()).hexdigest(), digest)
        self.assertEqual(h[0]['availability_contract'], 'legacy_precommit_v1')
        self.assertIsNone(h[0]['decision_available_at'])
        self.assertEqual(select_as_of(self.rows, h, T+timedelta(hours=3))[0]['status'], 'pending')
        self.assertEqual(select_as_of(self.rows, h, T+timedelta(hours=3),
                         timing_contract='legacy_precommit_v1')[0]['status'], 'included')
        self.assertEqual(recover_pending(self.ledger), [])  # Never silently confirm legacy content.

    def test_legacy_confirmation_only_latest_decision_and_present_time(self):
        self.legacy()
        self.legacy(decision_id='revision', at=T+timedelta(hours=2), english='uncertain',
                    supersedes_decision_id='old')
        original = self.raw_decisions()
        with patch('research.eligibility.datetime') as clock:
            clock.now.return_value = T+timedelta(hours=4)
            releases = confirm_legacy(self.ledger, self.rows, POLICY[0])
        self.assertEqual([r['decision_id'] for r in releases], ['revision'])
        self.assertEqual(releases[0]['release_kind'], 'legacy_confirmation')
        self.assertEqual(self.raw_decisions(), original)
        self.assertEqual(self.selected(T+timedelta(hours=3))[0]['status'], 'pending')
        self.assertEqual(self.selected(T+timedelta(hours=5))[0]['status'], 'uncertain')
        self.assertEqual(select_as_of(self.rows, self.history(), T+timedelta(minutes=90),
                         timing_contract='legacy_precommit_v1')[0]['status'], 'included')
        self.assertEqual(confirm_legacy(self.ledger, self.rows, POLICY[0]), [])

    def test_confirmation_crash_recovers_without_changing_legacy_columns(self):
        self.legacy()
        original = self.raw_decisions()
        with patch('research.eligibility._seal', side_effect=OSError('crash')):
            with self.assertRaisesRegex(RuntimeError, 'Run recover'):
                confirm_legacy(self.ledger, self.rows, POLICY[0])
        with self.assertRaisesRegex(ValueError, 'unsealed'):
            confirm_legacy(self.ledger, self.rows, POLICY[0])
        with patch('research.eligibility.datetime') as clock:
            clock.now.return_value = T+timedelta(hours=4)
            recovered = recover_pending(self.ledger)
        self.assertEqual(recovered[0]['release_kind'], 'legacy_recovery')
        self.assertEqual(self.raw_decisions(), original)

    def test_wrong_confirmation_hash_and_duplicate_input_do_not_register_releases(self):
        self.legacy()
        with self.assertRaisesRegex(ValueError, 'identity mismatch'):
            confirm_legacy(self.ledger, [{**self.rows[0], 'content_hash':'wrong'}], POLICY[0])
        with self.assertRaisesRegex(ValueError, 'distinct'):
            confirm_legacy(self.ledger, self.rows+self.rows, POLICY[0])
        with self.assertRaisesRegex(ValueError, 'policy'):
            confirm_legacy(self.ledger, self.rows, 'wrong')
        self.assertIsNone(self.history()[0]['decision_available_at'])

    def freeze_snapshot(self):
        self.snapshot = self.root/'snapshot'
        self.snapshot.mkdir()
        raw = (canonical_json(self.rows[0])+'\n').encode()
        digest = hashlib.sha256(raw).hexdigest()
        (self.snapshot/'dataset.jsonl').write_bytes(raw)
        (self.snapshot/'manifest.json').write_text(json.dumps({'rows':1, 'dataset_sha256':digest,
            'cutoff':(T+timedelta(hours=1)).isoformat()}))
        self.policy = self.root/'policy.md'
        self.policy.write_text(POLICY[1])

    def cli(self, action, *extra):
        from scripts.review_eligibility import main
        argv = ['review_eligibility',action,'--snapshot-dir',str(self.snapshot),
                '--policy-file',str(self.policy),'--ledger',str(self.ledger),*extra]
        with patch('sys.argv',argv), redirect_stdout(io.StringIO()):
            main()

    def test_cli_v2_selection_round_trip_and_legacy_annotation_opt_in(self):
        from research.annotations import load_selection
        self.freeze_snapshot()
        self.save([review(self.rows[0])])
        selected = self.root/'v2'
        self.cli('select','--cutoff',(T+timedelta(hours=2)).isoformat(),'--output',str(selected))
        self.assertEqual(load_selection(self.snapshot, selected)[0], self.rows)
        self.assertEqual(load_selection(self.snapshot, selected, T+timedelta(hours=1))[0], [])
        # An authentic v1 archive stores the original pre-commit columns.
        old = self.root/'v1'
        old.mkdir()
        with closing(sqlite3.connect(self.ledger)) as conn:
            conn.row_factory = sqlite3.Row
            d = dict(conn.execute('SELECT * FROM eligibility_decisions').fetchone())
        data = (self.snapshot/'dataset.jsonl').read_bytes()
        manifest = (self.snapshot/'manifest.json').read_bytes()
        history = (canonical_json(d)+'\n').encode()
        cutoff = (T+timedelta(hours=2)).isoformat()
        report = {'kind':'development_eligibility_selection_v1','cutoff':cutoff,
            'input_dataset_sha256':hashlib.sha256(data).hexdigest(),
            'input_manifest_sha256':hashlib.sha256(manifest).hexdigest(),
            'eligible_rows_sha256':hashlib.sha256(data).hexdigest(),
            'decision_history_sha256':hashlib.sha256(history).hexdigest(),'policy_sha256':POLICY[0],
            'versions':select_as_of(self.rows,[d],cutoff,timing_contract='legacy_precommit_v1')}
        (old/'selection.json').write_text(json.dumps(report))
        (old/'eligible.jsonl').write_bytes(data)
        (old/'decisions.jsonl').write_bytes(history)
        (old/'policy.md').write_text(POLICY[1])
        with self.assertRaisesRegex(ValueError, 'explicit allow_legacy'):
            load_selection(self.snapshot, old)
        self.assertEqual(load_selection(self.snapshot, old, allow_legacy=True)[0], self.rows)

    def test_new_release_records_cannot_be_replayed_as_legacy(self):
        self.save([review(self.rows[0])])
        with self.assertRaisesRegex(ValueError, 'cannot enter legacy'):
            select_as_of(self.rows, self.history(), T+timedelta(hours=2), timing_contract='legacy_precommit_v1')

    def test_wrong_stored_release_provenance_and_data_time_fail(self):
        self.save([review(self.rows[0])])
        for edits in ({'release_kind':'legacy_confirmation'}, {'data_available_at':T.isoformat()},
                      {'decision_available_at':T.isoformat()}):
            h = [{**self.history()[0], **edits}]
            with self.subTest(edits=edits), self.assertRaises(ValueError):
                select_as_of(self.rows, h, T+timedelta(hours=2))


if __name__ == '__main__':
    unittest.main()
