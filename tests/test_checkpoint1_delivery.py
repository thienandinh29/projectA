"""Snapshot-boundary regressions: failures must never certify completeness."""
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from research.provenance import code_identity
from scripts.drain_delivery import drain, load_baseline, writer_backlog
from tests.test_research_pipeline import observation, message, T
from tests import test_research_pipeline as research_tests
from scripts.export_research_dataset import export_dataset
from utils.kafka_producer import RedpandaProducer
from datetime import timedelta


class TestDrain(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'outbox.sqlite3')

    def tearDown(self):
        self.tmp.cleanup()

    def producer(self):
        with patch.object(RedpandaProducer, '_init_producer'):
            producer = RedpandaProducer(outbox_path=self.path, profile='critical')
        producer._is_confluent = True
        producer._producer = MagicMock()
        return producer

    def test_offline_timeout_then_new_process_drains_all_batches_original_bytes(self):
        first = self.producer()
        for i in range(600):
            first.outbox.enqueue('observations', str(i), f'original-{i}'.encode())
        first._producer.produce.side_effect = lambda **kw: kw['on_delivery']('offline', None)
        with patch('utils.kafka_producer.logger'):
            result = drain(first, lambda _: {'topic:0': 0}, timeout=.02)
        self.assertFalse(result['drained'])
        self.assertEqual(first.outbox.count(), 600)
        second, received = self.producer(), []
        def acknowledge(**kw):
            received.append(kw['value'])
            kw['on_delivery'](None, None)
        second._producer.produce.side_effect = acknowledge
        result = drain(second, lambda _: {'topic:0': 0}, timeout=30)
        self.assertTrue(result['drained'])
        self.assertEqual(received, [f'original-{i}'.encode() for i in range(600)])

    def test_empty_outbox_does_not_hide_lag_or_unknown_offsets(self):
        producer = self.producer()
        self.assertFalse(drain(producer, lambda _: {'topic:0': 1}, timeout=.02)['drained'])
        self.assertFalse(drain(producer, lambda _: {}, timeout=.02)['drained'])
        def unavailable(_):
            raise RuntimeError('broker unavailable')
        result = drain(producer, unavailable, timeout=.02)
        self.assertFalse(result['drained'])
        self.assertIn('unavailable', result['error'])

    def test_unmonitored_topic_stays_pending(self):
        producer = self.producer()
        producer.outbox.enqueue('old-topic', 'one', b'original')
        result = drain(producer, lambda _: {'topic:0': 0}, allowed_topics=['topic'])
        self.assertFalse(result['drained'])
        self.assertEqual(producer.outbox.count(), 1)
        producer._producer.produce.assert_not_called()

    def test_partition_inspection_never_subscribes_or_commits_and_rejects_retention_gap(self):
        consumer = MagicMock()
        consumer.list_topics.return_value.topics = {'topic': SimpleNamespace(error=None, partitions={0: None})}
        p = SimpleNamespace(topic='topic', partition=0, offset=4, error=None)
        consumer.committed.return_value = [p]
        consumer.get_watermark_offsets.return_value = (0, 5)
        self.assertEqual(writer_backlog(consumer, ['topic'], time.monotonic()+5), {'topic:0': 1})
        consumer.get_watermark_offsets.return_value = (5, 7)
        with self.assertRaisesRegex(RuntimeError, 'retained history'):
            writer_backlog(consumer, ['topic'], time.monotonic()+5)
        consumer.subscribe.assert_not_called()
        consumer.commit.assert_not_called()

    def test_baseline_allows_newly_empty_topic_without_historical_commit(self):
        consumer = MagicMock()
        consumer.list_topics.return_value.topics = {
            'topic': SimpleNamespace(error=None, partitions={0: None})}
        consumer.committed.return_value = [
            SimpleNamespace(topic='topic', partition=0, offset=-1001, error=None)]
        consumer.get_watermark_offsets.return_value = (5, 5)
        baseline = {('topic', 0): {'start': 5, 'committed_at_capture': None}}
        progress = writer_backlog(consumer, ['topic'], time.monotonic()+5, baseline)
        self.assertEqual(progress['topic:0']['status'], 'no_new_records')
        self.assertEqual(progress['topic:0']['lag'], 0)

    def test_baseline_reports_complete_post_baseline_range(self):
        consumer = MagicMock()
        consumer.list_topics.return_value.topics = {
            'topic': SimpleNamespace(error=None, partitions={0: None})}
        consumer.committed.return_value = [
            SimpleNamespace(topic='topic', partition=0, offset=8, error=None)]
        consumer.get_watermark_offsets.return_value = (5, 8)
        baseline = {('topic', 0): {'start': 5, 'committed_at_capture': None}}
        progress = writer_backlog(consumer, ['topic'], time.monotonic()+5, baseline)
        self.assertEqual(progress['topic:0']['post_baseline_messages'], 3)
        self.assertEqual(progress['topic:0']['status'], 'complete')

    def test_baseline_rejects_lag_retention_and_partition_changes(self):
        consumer = MagicMock()
        consumer.list_topics.return_value.topics = {
            'topic': SimpleNamespace(error=None, partitions={0: None})}
        baseline = {('topic', 0): {'start': 5, 'committed_at_capture': None}}
        consumer.committed.return_value = [
            SimpleNamespace(topic='topic', partition=0, offset=6, error=None)]
        consumer.get_watermark_offsets.return_value = (5, 8)
        progress = writer_backlog(consumer, ['topic'], time.monotonic()+5, baseline)
        self.assertEqual(progress['topic:0']['status'], 'writer_lag')
        self.assertEqual(progress['topic:0']['lag'], 2)
        consumer.get_watermark_offsets.return_value = (7, 8)
        with self.assertRaisesRegex(RuntimeError, 'Retention overtook'):
            writer_backlog(consumer, ['topic'], time.monotonic()+5, baseline)
        consumer.list_topics.return_value.topics['topic'].partitions = {0: None, 1: None}
        with self.assertRaisesRegex(RuntimeError, 'partition set changed'):
            writer_backlog(consumer, ['topic'], time.monotonic()+5, baseline)

    def test_baseline_rejects_offset_or_commit_regression(self):
        consumer = MagicMock()
        consumer.list_topics.return_value.topics = {
            'topic': SimpleNamespace(error=None, partitions={0: None})}
        baseline = {('topic', 0): {'start': 5, 'committed_at_capture': 5}}
        consumer.committed.return_value = [
            SimpleNamespace(topic='topic', partition=0, offset=4, error=None)]
        consumer.get_watermark_offsets.return_value = (0, 5)
        with self.assertRaisesRegex(RuntimeError, 'commit regressed'):
            writer_backlog(consumer, ['topic'], time.monotonic()+5, baseline)
        consumer.committed.return_value[0].offset = 5
        consumer.get_watermark_offsets.return_value = (0, 4)
        with self.assertRaisesRegex(RuntimeError, 'watermark regressed'):
            writer_backlog(consumer, ['topic'], time.monotonic()+5, baseline)

    def test_load_baseline_requires_active_matching_group_and_topics(self):
        path = Path(self.tmp.name) / 'baseline.json'
        document = {
            'kind': 'verified_collection_baseline_v1',
            'captured_at_utc': '2026-09-28T08:46:55Z',
            'activation_gate': {'active': True},
            'kafka': {'writer_group': 'writer', 'partitions': [{
                'topic': 'topic', 'partition': 0, 'post_baseline_start_offset': 5,
                'writer_committed_next_offset': None}]}}
        path.write_text(json.dumps(document))
        loaded = load_baseline(path, ['topic'], 'writer')
        self.assertEqual(loaded['partitions'][('topic', 0)]['start'], 5)
        document['activation_gate']['active'] = False
        path.write_text(json.dumps(document))
        with self.assertRaisesRegex(ValueError, 'not active'):
            load_baseline(path, ['topic'], 'writer')

    def test_collection_metadata_survives_staging_and_is_created_once(self):
        producer = self.producer()
        fields = observation().model_dump()
        for name in ('kind', 'id', 'content_hash', 'schema_version', 'collection_run'):
            fields.pop(name)
        with patch('research.provenance.collection_run', return_value={'run_id': 'collector'}) as capture:
            a = producer.capture_observation(**fields)
            b = producer.capture_observation(**fields)
        capture.assert_called_once()
        self.assertEqual(a.id, b.id)
        payload = json.loads(producer.outbox.pending()[0][2])
        self.assertEqual(payload['collection_run']['run_id'], 'collector')


class TestProvenance(unittest.TestCase):
    def test_no_git_is_unknown_not_empty_diff_and_code_edits_change_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'config.py'
            path.write_text('setting = 1')
            with patch('research.provenance.subprocess.check_output', side_effect=FileNotFoundError):
                before = code_identity(tmp)
                path.write_text('setting = 2')
                after = code_identity(tmp)
            self.assertIsNone(before['git_commit'])
            self.assertIsNone(before['tracked_diff_sha256'])
            self.assertNotEqual(before['application_sha256'], after['application_sha256'])

    def test_provenance_does_not_change_content_identity_or_legacy_validation(self):
        a = observation()
        b = observation(collection_run={'run_id': 'new-session'})
        self.assertEqual(a.id, b.id)
        from research.observations import SourceObservation
        legacy = a.model_dump(mode='json')
        legacy.pop('collection_run')
        self.assertEqual(SourceObservation.model_validate(legacy).id, b.id)

    def test_collection_metadata_survives_storage_export_and_exporter_env_changes(self):
        # Reuse storage fixture without inheriting/re-running its entire test suite.
        fixture = research_tests.TestResearchStorage()
        fixture.setUp()
        try:
            run = {'run_id': 'real-collector-run', 'configuration': {'TOPIC_RSS': 'captured-topic'}}
            fixture.write_at([message(observation(collection_run=run))], T+timedelta(minutes=10))
            fixture.write_at([message(observation(collection_run={'run_id': 'later-run'}), 1)],
                             T+timedelta(minutes=20))
            fixture.lake.close()
            with patch.dict('os.environ', {'TOPIC_RSS': 'exporter-topic', 'COLLECTION_CODE_REVISION': 'wrong'}):
                manifest = export_dataset(fixture.path, Path(fixture.tmp.name)/'export', T+timedelta(hours=1))
            self.assertEqual(manifest['collection_provenance']['runs'], [run])
            self.assertEqual(manifest['collection_provenance']['versions_with_unknown_provenance'], 0)
            self.assertEqual(manifest['exporter_provenance']['configuration']['TOPIC_RSS'], 'exporter-topic')
        finally:
            fixture.tearDown()


if __name__ == '__main__':
    unittest.main()
