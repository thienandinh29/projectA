"""Do not confuse idle broker state with stored evidence or prospective data."""
import hashlib
import json
import os
import tempfile
import time
import unittest
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from config import TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC, TOPIC_OBSERVATIONS, LAKEHOUSE_GROUP_ID
from lakehouse.db import LakehouseManager
from lakehouse.records import KafkaEnvelope
from scripts.drain_delivery import writer_backlog
from scripts.export_research_dataset import export_dataset
from tests.test_research_pipeline import T, observation

TOPICS = (TOPIC_RSS, TOPIC_GDELT, TOPIC_SEC, TOPIC_OBSERVATIONS)


class TestExpiredCommits(unittest.TestCase):
    def test_expired_commit_at_unchanged_boundary_passes_but_new_messages_fail(self):
        consumer = MagicMock()
        consumer.list_topics.return_value.topics = {'topic': SimpleNamespace(error=None, partitions={0: None})}
        consumer.committed.return_value = [SimpleNamespace(topic='topic', partition=0, offset=-1001, error=None)]
        baseline = {('topic', 0): {'start': 150, 'committed_at_capture': 150}}
        consumer.get_watermark_offsets.return_value = (150, 150)
        progress = writer_backlog(consumer, ['topic'], time.monotonic()+5, baseline)
        self.assertEqual(progress['topic:0']['lag'], 0)
        self.assertTrue(progress['topic:0']['commit_expired_at_idle_boundary'])
        consumer.get_watermark_offsets.return_value = (150, 151)
        with self.assertRaises(RuntimeError):
            writer_backlog(consumer, ['topic'], time.monotonic()+5, baseline)
        # Even when retained data disappears, no new records can be assumed complete.
        consumer.get_watermark_offsets.return_value = (151, 151)
        with self.assertRaises(RuntimeError):
            writer_backlog(consumer, ['topic'], time.monotonic()+5, baseline)
        consumer.subscribe.assert_not_called()
        consumer.commit.assert_not_called()

    def test_unexpected_or_duplicate_commit_partitions_fail(self):
        consumer = MagicMock()
        consumer.list_topics.return_value.topics = {'topic': SimpleNamespace(error=None, partitions={0: None})}
        for entries in ([], [SimpleNamespace(topic='wrong', partition=0)],
                        [SimpleNamespace(topic='topic', partition=0)]*2):
            consumer.committed.return_value = entries
            with self.assertRaisesRegex(RuntimeError, 'partition offset response'):
                writer_backlog(consumer, ['topic'], time.monotonic()+5)


class TestProspectiveExport(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db = self.root/'lake.duckdb'
        self.lake = LakehouseManager(str(self.db))
        self.baseline = self.root/'baseline.json'
        self.report = self.root/'drain.json'
        self.env = patch.dict(os.environ, {'RESEARCH_DATABASE_ID': f'file:{self.db.resolve()}'})
        self.env.start()

    def tearDown(self):
        self.lake.close()
        self.env.stop()
        self.tmp.cleanup()

    def write(self, obs, offset, minute):
        with patch('lakehouse.db.datetime') as clock:
            clock.now.return_value = T+timedelta(minutes=minute)
            self.lake.write_message_batch([KafkaEnvelope(TOPIC_OBSERVATIONS, 0, offset, obs.to_json_bytes())],
                                         {TOPIC_OBSERVATIONS: 'RESEARCH'})

    def evidence(self, start=1, end=5):
        identity = f'file:{self.db.resolve()}'
        document = {'kind': 'verified_collection_baseline_v1', 'activation_gate': {'active': True},
                    'captured_at_utc': (T+timedelta(minutes=15)).isoformat(),
                    'duckdb': {'database_id': identity},
                    'kafka': {'writer_group': LAKEHOUSE_GROUP_ID, 'partitions': [
                        {'topic': topic, 'partition': 0, 'post_baseline_start_offset': start if topic == TOPIC_OBSERVATIONS else 0,
                         'writer_committed_next_offset': None} for topic in TOPICS]}}
        self.baseline.write_text(json.dumps(document))
        self.report.write_text(json.dumps({'drained': True, 'pending': 0, 'database_id': identity,
            'baseline': {'sha256': hashlib.sha256(self.baseline.read_bytes()).hexdigest()},
            'writer_progress': {f'{topic}:0': {'baseline_next_offset': start if topic == TOPIC_OBSERVATIONS else 0,
                'broker_high_watermark_next_offset': end if topic == TOPIC_OBSERVATIONS else 0,
                'writer_committed_next_offset': end if topic == TOPIC_OBSERVATIONS else None,
                'lag': 0} for topic in TOPICS}}))

    def populate(self):
        self.write(observation(), 0, 10)
        # Redelivery keeps the first version's old transport coordinates and observation time.
        self.write(observation(observed_at=T+timedelta(minutes=16)), 1, 20)
        self.write(observation('corrected headline', observed_at=T+timedelta(minutes=16)), 2, 20)
        self.write(observation('old delayed headline'), 3, 20)
        self.write(observation('after cutoff', observed_at=T+timedelta(minutes=30)), 4, 31)
        self.evidence()

    def export(self):
        self.lake.close()
        return export_dataset(str(self.db), self.root/'export', T+timedelta(minutes=25),
                              baseline_path=self.baseline, drain_report_path=self.report)

    def test_offset_time_redelivery_and_cutoff_boundaries(self):
        self.populate()
        manifest = self.export()
        rows = [json.loads(line) for line in (self.root/'export/dataset.jsonl').read_text().splitlines()]
        self.assertEqual([row['title'] for row in rows], ['corrected headline'])
        self.assertEqual(manifest['boundary']['eligible_versions_excluded_by_boundary'], 2)
        self.assertEqual(manifest['dataset_scope'], 'prospective_since_baseline')
        self.assertEqual(manifest['storage_reconciliation']['ranges'][-1]['bronze_rows'], 4)

    def test_explicit_mode_and_missing_evidence_rejected_without_output(self):
        self.lake.close()
        with self.assertRaisesRegex(ValueError, 'require baseline'):
            export_dataset(str(self.db), self.root/'export', T)
        self.assertFalse((self.root/'export').exists())

    def test_offset_advance_does_not_hide_missing_bronze_or_outcome_or_version(self):
        for table, condition in (
            ('bronze_events_raw', 'kafka_offset=2'),
            ('lakehouse_message_outcomes', 'kafka_offset=2'),
            ('research_observations', "title='corrected headline'")):
            with self.subTest(table=table):
                self.populate()
                self.lake.conn.execute(f'DELETE FROM {table} WHERE {condition}')
                with self.assertRaisesRegex(ValueError, 'Stored delivery range is incomplete'):
                    self.export()
                self.assertFalse((self.root/'export').exists())
                self.lake = LakehouseManager(str(self.db))
                for name in ('research_observations', 'lakehouse_message_outcomes', 'bronze_events_raw'):
                    self.lake.conn.execute(f'DELETE FROM {name}')

    def test_failure_pending_hash_database_and_stale_report_fail(self):
        self.populate()
        self.lake.close()
        original = json.loads(self.report.read_text())
        cases = ({'drained': False}, {'pending': 1}, {'baseline': {'sha256': 'wrong'}},
                 {'database_id': 'wrong'})
        for changes in cases:
            with self.subTest(changes=changes):
                self.report.write_text(json.dumps({**original, **changes}))
                with self.assertRaises(ValueError):
                    self.export()
        self.report.write_text(json.dumps(original))
        self.lake = LakehouseManager(str(self.db))
        self.write(observation('new since report', observed_at=T+timedelta(minutes=35)), 5, 36)
        with self.assertRaisesRegex(ValueError, 'stale'):
            self.export()

    def test_no_post_baseline_records_gives_empty_export_and_preserves_data(self):
        self.write(observation(), 0, 10)
        self.evidence(start=1, end=1)
        manifest = self.export()
        self.assertEqual(manifest['rows'], 0)
        self.assertEqual(manifest['database']['total_research_versions'], 1)
        self.assertEqual((self.root/'export/dataset.jsonl').read_text(), '')

    def test_exact_baseline_and_cutoff_are_inclusive_and_old_source_time_is_allowed(self):
        self.write(observation(), 0, 10)
        self.write(observation('at boundary', observed_at=T+timedelta(minutes=15)), 1, 15)
        self.evidence(start=1, end=2)
        self.lake.close()
        manifest = export_dataset(str(self.db), self.root/'export', T+timedelta(minutes=15),
                                  baseline_path=self.baseline, drain_report_path=self.report)
        self.assertEqual(manifest['rows'], 1)


if __name__ == '__main__':
    unittest.main()
