import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from lakehouse.sync import parse_event_payload
from utils.kafka_producer import RedpandaProducer
from utils.redis_cache import DedupResult
from workers import rss_worker, gdelt_worker


class TestWorkerHandoff(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        with patch.object(RedpandaProducer, '_init_producer'):
            self.producer = RedpandaProducer(outbox_path=str(Path(self.tmp.name) / 'outbox.sqlite3'))
        self.producer._is_confluent = True
        self.producer._producer = MagicMock()
        self.sent = []
        self.observations = []
        def acknowledge(**kwargs):
            from config import TOPIC_OBSERVATIONS
            from research.observations import SourceObservation
            if kwargs['topic'] == TOPIC_OBSERVATIONS:
                self.observations.append(SourceObservation.model_validate_json(kwargs['value']))
            else:
                self.sent.append(parse_event_payload(kwargs['value']))
            kwargs['on_delivery'](None, None)
        self.producer._producer.produce.side_effect = acknowledge
        self.dedup = MagicMock()
        self.dedup.check_dedup_result.return_value = DedupResult(
            is_near=True, canonical_id='original', embedding=[1.0] + [0.0] * 383,
            semantic_score=.91)

    def tearDown(self):
        self.tmp.cleanup()

    def rss_fixture(self):
        entry = SimpleNamespace(title='Tesla beats earnings estimates', link='https://example.com/news',
                                summary='Tesla earnings', published_parsed=datetime.now(timezone.utc).timetuple())
        return SimpleNamespace(bozo=False, entries=[entry])

    def assert_handoff(self, stats):
        self.assertEqual(stats['queued'], 1)
        self.assertEqual(stats['published'], 1)
        self.assertEqual(stats['pending_delivery'], 0)
        self.assertNotIn('_delivery_start', stats)
        self.assertEqual(self.sent[0].embedding, [1.0] + [0.0] * 383)
        self.assertEqual(self.sent[0].semantic_score, .91)
        self.assertEqual(self.sent[0].canonical_cluster_id, 'original')
        self.assertTrue(self.dedup.check_dedup_result.call_args.kwargs['reserve_for_delivery'])
        self.dedup.confirm_staged.assert_called_once()

    def test_rss_worker_serializes_public_fields_and_reports_acknowledgements(self):
        with patch.object(rss_worker, 'MACRO_FEEDS', [{'name': 'test', 'url': 'https://example.com/rss'}]), \
             patch.object(rss_worker.feedparser, 'parse', return_value=self.rss_fixture()):
            stats = rss_worker.run_rss_fetch_cycle(self.producer, self.dedup)
        self.assert_handoff(stats)
        self.assertEqual(self.sent[0].metadata['title_provenance'], 'source_feed')
        self.assertEqual(self.sent[0].metadata['published_time_provenance'], 'feed:published_parsed')

    def test_gdelt_worker_serializes_public_fields_and_reports_acknowledgements(self):
        response = MagicMock(status_code=200, text='{"articles": []}')
        response.json.return_value = {'articles': [{'title': 'Tesla beats earnings estimates',
                                                   'url': 'https://example.com/news',
                                                   'seendate': '20260901T140000Z'}]}
        with patch.object(gdelt_worker.requests, 'get', return_value=response):
            stats = gdelt_worker.run_gdelt_fetch_cycle(self.producer, self.dedup)
        self.assert_handoff(stats)
        self.assertEqual(self.sent[0].metadata['title_provenance'], 'source_api')
        self.assertEqual(self.sent[0].metadata['published_time_provenance'], 'source_api')

    def test_invalid_source_times_are_labeled_as_ingestion_fallbacks(self):
        entry = SimpleNamespace()
        _, rss_provenance = rss_worker.parse_published_time_with_provenance(entry)
        _, gdelt_provenance = gdelt_worker.parse_gdelt_timestamp_with_provenance('invalid')
        self.assertEqual((rss_provenance, gdelt_provenance),
                         ('ingestion_fallback', 'ingestion_fallback'))

    def test_source_version_is_captured_even_when_live_exact_dedup_suppresses_it(self):
        self.dedup.check_dedup_result.return_value = DedupResult(is_exact=True)
        with patch.object(rss_worker, 'MACRO_FEEDS', [{'name': 'test', 'url': 'https://example.com/rss'}]), \
             patch.object(rss_worker.feedparser, 'parse', return_value=self.rss_fixture()):
            stats = rss_worker.run_rss_fetch_cycle(self.producer, self.dedup)
        self.assertEqual(stats['duplicates'], 1)
        self.assertEqual(len(self.observations), 1)
        self.assertEqual(self.sent, [])

    def test_gkg_fallback_is_preserved_without_touching_headline_dedup(self):
        import io
        import zipfile
        payload = io.BytesIO()
        with zipfile.ZipFile(payload, 'w') as archive:
            archive.writestr('fixture.gkg.csv', 'id\t20260901140000\t1\texample.com\thttps://example.com/synthetic-slug\t\t\tECON_INFLATION\n')
        manifest = MagicMock(status_code=200, text='1 hash http://example.com/fixture.gkg.csv.zip')
        response = MagicMock(content=payload.getvalue())
        stats = {'fetched':0, 'duplicates':0, 'published':0, 'queued':0, 'errors':0, '_delivery_start':0}
        with patch.object(gdelt_worker.requests, 'get', side_effect=[manifest, response]):
            result = gdelt_worker.run_gdelt_raw_stream_cycle(self.producer, self.dedup, stats)
        self.assertEqual(result['research_captured'], 1)
        self.assertEqual(len(self.observations), 1)
        self.assertFalse(self.observations[0].nlp_eligible)
        self.assertEqual(self.sent, [])
        self.dedup.check_dedup_result.assert_not_called()

    def test_source_cycle_evidence_survives_outbox_delivery(self):
        with patch.object(rss_worker, 'MACRO_FEEDS', [{'name': 'test', 'url': 'https://example.com/rss'}]), \
             patch.object(rss_worker.feedparser, 'parse', return_value=self.rss_fixture()):
            rss_worker.run_rss_fetch_cycle(self.producer, self.dedup)
        with self.producer.outbox.connect() as conn:
            row = conn.execute('SELECT started_at,finished_at,statistics FROM source_cycles').fetchone()
        self.assertIsNotNone(row)
        self.assertLessEqual(datetime.fromisoformat(row[0]), datetime.fromisoformat(row[1]))

    def test_local_staging_failure_releases_dedup_reservation(self):
        with patch.object(rss_worker, 'MACRO_FEEDS', [{'name': 'test', 'url': 'https://example.com/rss'}]), \
             patch.object(rss_worker.feedparser, 'parse', return_value=self.rss_fixture()), \
             patch.object(self.producer.outbox, 'enqueue', side_effect=OSError('Disk full')):
            stats = rss_worker.run_rss_fetch_cycle(self.producer, self.dedup)
        self.assertEqual(stats['published'], 0)
        self.assertEqual(stats['queued'], 0)
        self.assertEqual(stats['errors'], 1)
        # Capture failed before Redis was touched, so there is no reservation.
        self.dedup.check_dedup_result.assert_not_called()
        self.dedup.release_unstaged.assert_not_called()
        self.dedup.confirm_staged.assert_not_called()


if __name__ == '__main__':
    unittest.main()
