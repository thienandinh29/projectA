"""Eligibility revisions must preserve exact content and chronological information."""
import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from research.eligibility import append_reviews, load_snapshot, read_decisions, select_as_of
from research.observations import canonical_json, make_observation


T = datetime(2026, 10, 1, tzinfo=timezone.utc)
POLICY = (hashlib.sha256(b'test policy').hexdigest(), 'test policy')


def row(title='Company reports higher earnings', article_id='article'):
    obs = make_observation(source='RSS', article_id=article_id, title=title,
        url='https://example.test/news', source_item={'title': title},
        title_provenance='source_feed', observed_at=T, source_time_kind='unknown')
    return {'version_id': obs.id, 'content_hash': obs.content_hash, 'article_id': article_id,
            'title': title, 'source': 'RSS', 'url': obs.url, 'observed_at': T.isoformat(),
            'available_at': (T+timedelta(seconds=1)).isoformat(), 'snapshot': obs.model_dump_json()}


def review(r, **kwargs):
    return {'version_id': r['version_id'], 'content_hash': r['content_hash'], 'english': 'yes',
            'financially_relevant': 'yes', 'genuine_headline': 'yes',
            'reason': 'English financial headline from a feed', 'supersedes_decision_id': None, **kwargs}


class TestEligibility(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.ledger = self.root/'eligibility.sqlite3'
        self.rows = [row()]

    def tearDown(self):
        self.tmp.cleanup()

    def save(self, reviews, at=T+timedelta(hours=1)):
        with patch('research.eligibility.datetime') as clock:
            clock.now.side_effect = [at, at+timedelta(seconds=1)]
            return append_reviews(self.ledger, self.rows, 'snapshot-hash', POLICY, 'human-fixture', reviews)

    def history(self):
        return read_decisions(self.ledger, POLICY[0])

    def test_revision_keeps_history_and_does_not_change_old_cutoff(self):
        first = self.save([review(self.rows[0])])[0]
        self.save([review(self.rows[0], financially_relevant='no', reason='Correction: not financial',
                          supersedes_decision_id=first['decision_id'])], T+timedelta(hours=2))
        history = self.history()
        self.assertEqual(len(history), 2)
        self.assertEqual(select_as_of(self.rows, history, T+timedelta(minutes=90))[0]['status'], 'included')
        self.assertEqual(select_as_of(self.rows, history, T+timedelta(hours=3))[0]['status'], 'excluded')
        self.assertEqual(select_as_of(self.rows, history, T+timedelta(hours=1))[0]['status'], 'pending')

    def test_uncertain_pending_and_unavailable_data_are_not_included(self):
        self.save([review(self.rows[0], english='uncertain', reason='Mixed language')])
        self.assertEqual(select_as_of(self.rows, self.history(), T+timedelta(hours=2))[0]['status'], 'uncertain')
        self.assertEqual(select_as_of(self.rows, [], T+timedelta(hours=2))[0]['status'], 'pending')
        self.assertEqual(select_as_of(self.rows, [], T+timedelta(seconds=1))[0]['status'],
                         'data_not_available_before_cutoff')

    def test_unreviewed_revision_of_same_article_does_not_inherit_decision(self):
        self.save([review(self.rows[0])])
        revised = row('Company reports lower earnings')
        results = select_as_of(self.rows+[revised], self.history(), T+timedelta(hours=2))
        self.assertEqual([r['status'] for r in results], ['included', 'pending'])

    def test_blank_reason_wrong_hash_backdating_and_stale_revision_are_rejected(self):
        for edits in [{'reason':' '}, {'content_hash':'wrong'}, {'decision_available_at':T.isoformat()}]:
            with self.assertRaises(ValueError):
                self.save([review(self.rows[0], **edits)])
        first = self.save([review(self.rows[0])])[0]
        with self.assertRaisesRegex(ValueError, 'latest decision'):
            self.save([review(self.rows[0])], T+timedelta(hours=2))
        with self.assertRaisesRegex(ValueError, 'clock did not advance'):
            self.save([review(self.rows[0], supersedes_decision_id=first['decision_id'])])
        self.assertEqual(len(self.history()), 1)

    def test_failed_batch_rolls_back_all_new_decisions(self):
        self.rows.append(row('Interest rates rise', 'other'))
        self.save([review(self.rows[1])])
        with self.assertRaisesRegex(ValueError, 'latest decision'):
            self.save([review(self.rows[0]), review(self.rows[1])], T+timedelta(hours=2))
        self.assertEqual([d['version_id'] for d in self.history()], [self.rows[1]['version_id']])

    def test_sql_updates_deletes_and_policy_mutations_fail(self):
        self.save([review(self.rows[0])])
        with closing(sqlite3.connect(self.ledger)) as conn:
            for sql in ["UPDATE eligibility_decisions SET reviewer='changed'",
                        'DELETE FROM eligibility_decisions',
                        "UPDATE eligibility_policies SET text='changed'", 'DELETE FROM eligibility_policies']:
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(sql)
        self.assertEqual(len(self.history()), 1)
        self.assertEqual(read_decisions(self.ledger, hashlib.sha256(b'new policy').hexdigest()), [])

    def test_snapshot_rejects_changed_title_even_with_rehashed_manifest(self):
        def freeze(rows):
            data = ''.join(canonical_json(r)+'\n' for r in rows).encode()
            (self.root/'dataset.jsonl').write_bytes(data)
            (self.root/'manifest.json').write_text(json.dumps({'rows':len(rows),
                'dataset_sha256': hashlib.sha256(data).hexdigest(),
                'cutoff': (T+timedelta(hours=1)).isoformat()}), encoding='utf-8')
        freeze(self.rows)
        self.assertEqual(len(load_snapshot(self.root)[0]), 1)
        freeze([{**self.rows[0], 'title':'Different text'}])
        with self.assertRaisesRegex(ValueError, 'source content version'):
            load_snapshot(self.root)
        freeze(self.rows)
        (self.root/'INVALID_FOR_RESEARCH.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'Invalidated'):
            load_snapshot(self.root)

    def test_decision_cannot_precede_storage_availability(self):
        with self.assertRaisesRegex(ValueError, 'before it is available'):
            self.save([review(self.rows[0])], T)
        self.assertEqual(self.history(), [])


if __name__ == '__main__':
    unittest.main()
