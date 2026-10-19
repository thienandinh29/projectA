"""Request failures, restart evidence, content identity and archived traceability."""
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import requests

from lakehouse.db import LakehouseManager
from lakehouse.records import KafkaEnvelope
from research.observations import SourceObservation, make_observation
from research.sessions import CollectionSession, read_session_evidence
from research.sessions import verify_request_links
from research.sessions import invalidate_session
from scripts.export_research_dataset import export_dataset
from utils.outbox import DeliveryOutbox
from utils.kafka_producer import RedpandaProducer
from utils.redis_cache import DedupResult
from workers import gdelt_worker, rss_worker


class TestSessionLedger(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / 'outbox.sqlite3')
        self.outbox = DeliveryOutbox(self.path)
        self.session = None

    def tearDown(self):
        if self.session:
            self.session.close('test_complete')
        self.tmp.cleanup()

    def start(self):
        run = {'run_id': str(uuid.uuid4()), 'started_at': datetime.now(timezone.utc).isoformat()}
        from config import TOPIC_RSS
        self.session = CollectionSession(self.outbox, run, TOPIC_RSS, {'feed': 'fixture'})
        return self.session

    def test_second_collector_fails_without_changing_owner(self):
        session = self.start()
        with self.assertRaisesRegex(RuntimeError, 'Another collector'):
            self.start()
        evidence = read_session_evidence(self.path)
        self.assertEqual(len(evidence['sessions']), 1)
        self.assertEqual(evidence['sessions'][0]['status'], 'unfinished')
        self.assertEqual(evidence['session_events'], [])
        session.close()

    def test_process_death_keeps_unfinished_request_and_restart_appends_interruption(self):
        script = '''import os,sys,uuid
from datetime import datetime,timezone
from utils.outbox import DeliveryOutbox
from config import TOPIC_RSS
from research.sessions import CollectionSession
s=CollectionSession(DeliveryOutbox(sys.argv[1]),{'run_id':str(uuid.uuid4()),'started_at':datetime.now(timezone.utc).isoformat()},TOPIC_RSS,{})
s.begin_request('fixture',{})
os._exit(0)
'''
        subprocess.run([sys.executable, '-c', script, self.path], check=True, timeout=30)
        first = read_session_evidence(self.path)
        self.assertEqual(first['sessions'][0]['status'], 'unfinished')
        self.assertEqual(first['requests'][0]['outcome'], 'unfinished')
        self.start().close('single_cycle_complete')
        recovered = read_session_evidence(self.path)
        self.assertEqual([s['status'] for s in recovered['sessions']], ['interrupted', 'closed'])
        self.assertIn('actual stop time unknown', recovered['session_events'][0]['details']['reason'])
        self.assertEqual(recovered['requests'][0]['outcome'], 'unfinished')

    def test_closed_session_survives_and_request_result_cannot_be_overwritten(self):
        session = self.start()
        request_id = session.begin_request('fixture', {'query': 'inflation'})
        session.finish_request(request_id, 'empty', http_status=200, item_count=0)
        with self.assertRaises(Exception):
            session.finish_request(request_id, 'failed', error='overwrite attempt')
        session.close('SIGTERM')
        evidence = read_session_evidence(self.path)
        self.assertEqual(evidence['requests'][0]['outcome'], 'empty')
        self.assertEqual(evidence['sessions'][0]['status'], 'closed')
        self.assertEqual(evidence['session_events'][0]['details']['reason'], 'SIGTERM')

    def test_request_context_does_not_change_content_hash_and_old_payload_still_validates(self):
        fields = dict(source='RSS', article_id='a', title='Inflation falls', url='https://example.com/a',
                      source_item={'title': 'Inflation falls'}, title_provenance='source_feed')
        first = make_observation(**fields)
        repeated = make_observation(**fields, request_context={'run_id': 'new', 'request_id': 'new'})
        self.assertEqual(first.id, repeated.id)
        old = first.model_dump(mode='json')
        del old['request_context']
        self.assertEqual(SourceObservation.model_validate(old).id, first.id)

    def test_export_archives_ledger_and_rejects_missing_trace_without_writing_output(self):
        session = self.start()
        request_id = session.begin_request('https://example.com/rss', {'feed_name': 'fixture'})
        session.finish_request(request_id, 'success', http_status=200, item_count=1)
        obs = make_observation(source='RSS', article_id='a', title='Inflation falls',
                               url='https://example.com/a', source_item={'title': 'Inflation falls'},
                               title_provenance='source_feed', collection_run=session.run,
                               request_context=session.context(request_id, 'example.com'))
        db = str(Path(self.tmp.name) / 'lake.duckdb')
        lake = LakehouseManager(db)
        lake.write_message_batch([KafkaEnvelope('observations', 0, 0, obs.to_json_bytes())],
                                 {'observations': 'RESEARCH'})
        lake.close()
        session.close()
        cutoff = datetime.now(timezone.utc) + timedelta(seconds=1)
        out = Path(self.tmp.name) / 'export'
        manifest = export_dataset(db, out, cutoff, development=True, session_ledger_path=self.path)
        archived = out / 'collection-sessions.json'
        self.assertEqual(manifest['session_evidence']['sha256'], hashlib.sha256(archived.read_bytes()).hexdigest())
        self.assertEqual(manifest['session_evidence']['linked_versions'], 1)
        stored_rows = [json.loads((out/'dataset.jsonl').read_text().splitlines()[0])]
        wrong_evidence = read_session_evidence(self.path)
        wrong_evidence['requests'][0]['topic'] = 'wrong-topic'
        with self.assertRaisesRegex(ValueError, 'source differs'):
            verify_request_links(stored_rows, wrong_evidence)
        with self.assertRaisesRegex(ValueError, 'require --session-ledger'):
            export_dataset(db, Path(self.tmp.name)/'missing', cutoff, development=True)
        self.assertFalse((Path(self.tmp.name)/'missing').exists())
        wrong = str(Path(self.tmp.name) / 'wrong.sqlite3')
        DeliveryOutbox(wrong)
        with self.assertRaisesRegex(ValueError, 'missing or inconsistent'):
            export_dataset(db, Path(self.tmp.name)/'wrong', cutoff, development=True, session_ledger_path=wrong)
        invalidate_session(self.outbox, session.run['run_id'], 'Fixture collector produced parser artifacts')
        excluded = export_dataset(db, Path(self.tmp.name)/'excluded', cutoff,
                                  development=True, session_ledger_path=self.path)
        self.assertEqual(excluded['rows'], 0)
        self.assertEqual(excluded['session_evidence']['versions_excluded_from_invalidated_runs'], 1)
        lake = LakehouseManager(db, read_only=True)
        try:
            self.assertEqual(len(lake.query_research_as_of(cutoff)), 1)
        finally:
            lake.close()

    def test_equal_timestamp_events_keep_append_order(self):
        # Coarse clocks can stamp consecutive events identically; random IDs must not reorder them.
        session = self.start()
        with patch('research.sessions.now', return_value='2026-10-06T00:00:00+00:00'),              patch('research.sessions.uuid.uuid4', side_effect=[uuid.UUID(int=2), uuid.UUID(int=1)]):
            session.event('stop_requested', reason='SIGTERM')
            session.event('closed', reason='SIGTERM')
        evidence = read_session_evidence(self.path)
        self.assertEqual([e['kind'] for e in evidence['session_events']], ['stop_requested', 'closed'])
        self.assertEqual(evidence['sessions'][0]['status'], 'closed')

    def test_active_session_cannot_be_invalidated(self):
        session = self.start()
        with self.assertRaisesRegex(ValueError, 'Stop and record termination'):
            invalidate_session(self.outbox, session.run['run_id'], 'fixture')


class TestSourceOutcomes(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        with patch.object(RedpandaProducer, '_init_producer'):
            self.producer = RedpandaProducer(outbox_path=str(Path(self.tmp.name)/'outbox.sqlite3'))
        self.producer._is_confluent = True
        self.producer._producer = MagicMock()
        self.observations = []

        def ack(**kwargs):
            from config import TOPIC_OBSERVATIONS
            if kwargs['topic'] == TOPIC_OBSERVATIONS:
                self.observations.append(SourceObservation.model_validate_json(kwargs['value']))
            kwargs['on_delivery'](None, None)
        self.producer._producer.produce.side_effect = ack
        self.dedup = MagicMock()
        self.dedup.check_dedup_result.return_value = DedupResult(is_exact=True)

    def tearDown(self):
        self.producer.close_session('test_complete')
        self.tmp.cleanup()

    def test_rss_http_bytes_preserve_original_url_parser_identity(self):
        from feedparser.api import http
        url = 'https://example.com/news/feed.xml'
        body = b'''<rss version="2.0"><channel><title>Economy</title><item>
            <title>Inflation falls</title><link>story.html</link>
            <description>&lt;a href="detail.html"&gt;Details&lt;/a&gt;</description>
            </item></channel></rss>'''

        def get(*args):
            args[-1]['href'] = url
            args[-1]['headers'] = {'content-type': 'application/rss+xml'}
            return body

        with patch.object(http, 'get', side_effect=get):
            original = rss_worker.feedparser.parse(url)
        response = SimpleNamespace(url=url, content=body, headers={'Content-Type': 'application/rss+xml'})
        captured = rss_worker.parse_feed_response(response)
        self.assertEqual(original.entries, captured.entries)
        self.assertEqual(captured.entries[0].title_detail.base, url)
        self.assertEqual(captured.entries[0].link, 'https://example.com/news/story.html')

    def test_rss_distinguishes_valid_empty_http_failure_and_html_response(self):
        ok = MagicMock(status_code=200, content=b'<rss version="2.0"><channel><title>x</title></channel></rss>',
                       url='https://example.com/rss')
        bad = MagicMock(status_code=503)
        bad.raise_for_status.side_effect = requests.HTTPError('503', response=bad)
        html = MagicMock(status_code=200, content=b'<html>Unavailable</html>', url='https://example.com/html')
        feeds = [{'name': str(n), 'url': f'https://example.com/{n}'} for n in range(3)]
        with patch.object(rss_worker, 'MACRO_FEEDS', feeds), \
             patch.object(rss_worker.requests, 'get', side_effect=[ok, bad, html]):
            result = rss_worker.run_rss_fetch_cycle(self.producer, self.dedup)
        evidence = read_session_evidence(self.producer.outbox.path)
        self.assertEqual([r['outcome'] for r in evidence['requests']], ['empty','failed','failed'])
        self.assertEqual(result['errors'], 2)
        self.assertEqual(self.observations, [])

    def test_gdelt_empty_is_success_not_fallback(self):
        response = MagicMock(status_code=200)
        response.json.return_value = {'articles': []}
        with patch.object(gdelt_worker.requests, 'get', return_value=response) as get:
            stats = gdelt_worker.run_gdelt_fetch_cycle(self.producer, self.dedup)
        self.assertEqual(stats['errors'], 0)
        get.assert_called_once()
        evidence = read_session_evidence(self.producer.outbox.path)
        self.assertEqual(evidence['requests'][0]['outcome'], 'empty')

    def test_all_429_attempts_remain_failures_even_when_gkg_is_available(self):
        rate = MagicMock(status_code=429, text='Rate limit')
        rate.raise_for_status.side_effect = requests.HTTPError('429', response=rate)
        with patch.object(gdelt_worker.requests, 'get', return_value=rate), \
             patch.object(gdelt_worker.time, 'sleep'), \
             patch.object(gdelt_worker, 'run_gdelt_raw_stream_cycle', side_effect=lambda p,d,s: p.finish_cycle(gdelt_worker.TOPIC_GDELT,s)):
            stats = gdelt_worker.run_gdelt_fetch_cycle(self.producer, self.dedup)
        evidence = read_session_evidence(self.producer.outbox.path)
        self.assertEqual(len(evidence['requests']), 3)
        self.assertTrue(all(r['outcome'] == 'failed' and r['http_status'] == 429 for r in evidence['requests']))
        self.assertEqual(stats['errors'], 3)

    def test_malformed_doc_response_is_not_mislabeled_as_rate_limit(self):
        response = MagicMock(status_code=200, text='not json')
        response.json.side_effect = ValueError('Malformed JSON')
        with patch.object(gdelt_worker.requests, 'get', return_value=response), \
             patch.object(gdelt_worker, 'run_gdelt_raw_stream_cycle', side_effect=lambda p,d,s: p.finish_cycle(gdelt_worker.TOPIC_GDELT,s)):
            gdelt_worker.run_gdelt_fetch_cycle(self.producer, self.dedup)
        row = read_session_evidence(self.producer.outbox.path)['requests'][0]
        self.assertEqual((row['outcome'], row['http_status']), ('failed', 200))
        self.assertIn('Malformed JSON', row['error'])

    def test_lifecycle_records_planned_signal_and_startup_exception(self):
        import signal
        from utils import collector_lifecycle as lifecycle

        def cycle(producer, dedup):
            signal.raise_signal(signal.SIGTERM)
            return {'fetched': 0}

        with patch.object(sys, 'argv', ['collector']), \
             patch.object(lifecycle, 'RedpandaProducer', return_value=self.producer), \
             patch.object(lifecycle, 'RedisDeduplicator', return_value=self.dedup):
            lifecycle.run_collector('test', 'rss', 10, {}, cycle)
        evidence = read_session_evidence(self.producer.outbox.path)
        self.assertEqual([e['kind'] for e in evidence['session_events']], ['stop_requested', 'closed'])
        self.assertEqual(evidence['session_events'][-1]['details']['reason'], 'SIGTERM')
        self.producer.session = None
        with patch.object(sys, 'argv', ['collector','--once']), \
             patch.object(lifecycle, 'RedpandaProducer', return_value=self.producer), \
             patch.object(lifecycle, 'RedisDeduplicator', side_effect=RuntimeError('startup failure')):
            with self.assertRaisesRegex(RuntimeError, 'startup failure'):
                lifecycle.run_collector('test', 'rss', 10, {}, cycle)
        evidence = read_session_evidence(self.producer.outbox.path)
        self.assertEqual(evidence['sessions'][-1]['status'], 'failed')


if __name__ == '__main__':
    unittest.main()
